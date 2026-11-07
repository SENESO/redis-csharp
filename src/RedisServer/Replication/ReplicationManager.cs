using System.Security.Cryptography;

namespace RedisServer.Replication;

/// <summary>How a PSYNC request is answered: ship a snapshot, or continue from the backlog.</summary>
public enum SyncKind
{
    Full,
    Continue
}

/// <summary>
/// The replication state machine shared by the command processor (which
/// serves PSYNC, enforces read-only replicas, and reports INFO) and the
/// replication service (which runs the replica-side sync loop). All state
/// sits behind one lock; nothing here does I/O.
///
/// Identity and offsets:
/// <list type="bullet">
/// <item>Every server has a <em>served stream</em>: a replication id plus
/// a byte offset that advances by the encoded length of every effect
/// frame the server produces (as a primary) or applies (as a replica —
/// applied frames are byte-identical, so the offsets line up). The
/// recent tail of that stream is retained in a bounded backlog ring.</item>
/// <item>A replica additionally tracks the <em>follow position</em>: the
/// id and applied offset of the primary's stream it is following. A full
/// sync adopts the primary's identity as the served identity, so a
/// replica's own replicas (or its post-promotion clients) see one
/// continuous stream.</item>
/// <item>The replication id is per-process: it is generated at startup
/// and adopted from the primary at full sync. It is deliberately not
/// persisted (the RCDB format carries no replication metadata), so a
/// primary restart changes the id and its replicas full-resync — the
/// same recovery Redis performs when its id no longer matches, just
/// reached more often.</item>
/// </list>
/// </summary>
public sealed class ReplicationManager
{
    /// <summary>Default backlog (and per-replica outbox) bound: 16 MiB of effect frames.</summary>
    public const long DefaultBacklogSizeBytes = 16L * 1024 * 1024;

    private readonly object _gate = new();
    private readonly Queue<(long EndOffset, byte[] Frame)> _backlog = new();
    private readonly List<ReplicaLink> _links = new();
    private readonly long _backlogCapBytes;
    private long _backlogBytes;
    private long _backlogFirstByteOffset;
    private TaskCompletionSource _changed = NewSignal();

    // The served stream.
    private string _replId;
    private long _offset;

    // Role and follow state.
    private bool _isReplica;
    private string? _followHost;
    private int _followPort;
    private string? _followReplId;
    private long _followOffset;
    private bool _masterLinkUp;
    private bool _syncInProgress;

    public ReplicationManager(long backlogSizeBytes, string? replicaOfHost = null, int replicaOfPort = 0)
    {
        _backlogCapBytes = Math.Max(backlogSizeBytes, 1024);
        _replId = NewReplId();
        if (replicaOfHost is not null)
        {
            _isReplica = true;
            _followHost = replicaOfHost;
            _followPort = replicaOfPort;
        }
    }

    private static string NewReplId() =>
        Convert.ToHexString(RandomNumberGenerator.GetBytes(20)).ToLowerInvariant();

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // ------------------------------------------------------------------
    // Served-stream state
    // ------------------------------------------------------------------

    public string ReplId
    {
        get { lock (_gate) return _replId; }
    }

    /// <summary>Byte offset of the end of the served stream (== the offset a full sync advertises).</summary>
    public long Offset
    {
        get { lock (_gate) return _offset; }
    }

    /// <summary>The oldest byte offset still retained in the backlog; partial resync needs an offset at or after this.</summary>
    public long BacklogFirstByteOffset
    {
        get { lock (_gate) return _backlogFirstByteOffset; }
    }

    public long BacklogSizeBytes => _backlogCapBytes;

    public long BacklogHistoryLength
    {
        get { lock (_gate) return _offset - _backlogFirstByteOffset; }
    }

    public int ConnectedReplicas
    {
        get { lock (_gate) return _links.Count; }
    }

    /// <summary>
    /// Appends one encoded effect frame to the served stream: advances
    /// the offset, retains the frame in the bounded backlog (dropping the
    /// oldest frames past the cap, which is what eventually pushes a
    /// lagging replica out of partial-resync range), and fans the frame
    /// out to every live replica link. Called under the command gate from
    /// both producing paths — local writes on a primary and applied
    /// frames on a replica — so stream order is execution order.
    /// </summary>
    public void AppendServedFrame(byte[] frame)
    {
        ReplicaLink[] targets;
        lock (_gate)
        {
            _offset += frame.Length;
            _backlog.Enqueue((_offset, frame));
            _backlogBytes += frame.Length;
            // Keep at least the newest frame even if it alone exceeds the
            // cap; offsets must always be able to advance.
            while (_backlogBytes > _backlogCapBytes && _backlog.Count > 1)
            {
                var (end, old) = _backlog.Dequeue();
                _backlogBytes -= old.Length;
                _backlogFirstByteOffset = end;
            }
            targets = _links.ToArray();
        }
        foreach (var link in targets)
            link.Enqueue(frame);
    }

    /// <summary>
    /// The full-vs-partial decision for a PSYNC: partial (Continue) only
    /// when the request names this server's current replication id and an
    /// offset still inside the backlog; the slice is every retained frame
    /// ending after that offset, in stream order. Anything else — unknown
    /// id, an offset from the future, an offset already dropped — is a
    /// full sync.
    /// </summary>
    public SyncKind DecideSync(string requestedReplId, long requestedOffset, out List<byte[]>? backlogSlice)
    {
        lock (_gate)
        {
            backlogSlice = null;
            if (requestedOffset < 0 ||
                !string.Equals(requestedReplId, _replId, StringComparison.Ordinal) ||
                requestedOffset > _offset ||
                requestedOffset < _backlogFirstByteOffset)
                return SyncKind.Full;

            backlogSlice = new List<byte[]>();
            foreach (var (end, frame) in _backlog)
            {
                if (end > requestedOffset)
                    backlogSlice.Add(frame);
            }
            return SyncKind.Continue;
        }
    }

    /// <summary>
    /// Registers a replica link (created by the PSYNC handler) and returns
    /// it. The link's outbox bound is the backlog bound. The caller posts
    /// the sync header, enqueues any backlog slice, then starts the link
    /// live — in that order.
    /// </summary>
    public ReplicaLink RegisterLink(Action<byte[]> postFrame, Action? forceClose)
    {
        var link = new ReplicaLink(postFrame, forceClose, UnregisterLink, _backlogCapBytes);
        lock (_gate)
            _links.Add(link);
        return link;
    }

    public void UnregisterLink(ReplicaLink link)
    {
        lock (_gate)
            _links.Remove(link);
    }

    // ------------------------------------------------------------------
    // Role and follow state
    // ------------------------------------------------------------------

    public bool IsReplica
    {
        get { lock (_gate) return _isReplica; }
    }

    /// <summary>The endpoint this server replicates from, when it is a replica.</summary>
    public (string Host, int Port)? FollowTarget
    {
        get
        {
            lock (_gate)
                return _isReplica && _followHost is not null ? (_followHost, _followPort) : null;
        }
    }

    /// <summary>The identity the sync loop presents to the primary: ("?", -1) when nothing is known yet.</summary>
    public (string ReplId, long Offset) FollowIdentity
    {
        get
        {
            lock (_gate)
                return _followReplId is not null ? (_followReplId, _followOffset) : ("?", -1);
        }
    }

    /// <summary>Applied position in the primary's stream (INFO's master_repl_offset on a replica).</summary>
    public long FollowOffset
    {
        get { lock (_gate) return _followOffset; }
    }

    private int _followDb;

    /// <summary>
    /// The logical database the follow position has reached: the
    /// stream's SELECT frames move it, and it survives reconnects, so a
    /// partial resync resumes applying frames in the right database
    /// without the primary re-framing anything. A full sync resets it —
    /// the snapshot carries every entry's database itself, and the
    /// primary frames the next effect with an explicit SELECT.
    /// </summary>
    public int FollowDb
    {
        get { lock (_gate) return _followDb; }
        set { lock (_gate) _followDb = value; }
    }

    public bool MasterLinkUp
    {
        get { lock (_gate) return _masterLinkUp; }
    }

    public bool SyncInProgress
    {
        get { lock (_gate) return _syncInProgress; }
    }

    /// <summary>
    /// REPLICAOF host port: become (or stay) a replica of the target.
    /// Pointing at a different target than the current one resets the
    /// follow identity — there is no valid offset into a stream that was
    /// never followed — so the next sync starts from a full sync.
    /// </summary>
    public void SetReplicaOf(string host, int port)
    {
        lock (_gate)
        {
            if (!_isReplica || _followHost != host || _followPort != port)
            {
                _followReplId = null;
                _followOffset = 0;
            }
            _isReplica = true;
            _followHost = host;
            _followPort = port;
            _masterLinkUp = false;
            SignalChangeLocked();
        }
    }

    /// <summary>
    /// REPLICAOF NO ONE: promotion. The served identity and stream are
    /// kept exactly as adopted/applied, so the promoted server's offset
    /// continues the stream it was following and its own replicas can
    /// still partial-resync against it.
    /// </summary>
    public void Promote()
    {
        lock (_gate)
        {
            _isReplica = false;
            _followHost = null;
            _followPort = 0;
            _followReplId = null;
            _followOffset = 0;
            _masterLinkUp = false;
            _syncInProgress = false;
            SignalChangeLocked();
        }
    }

    /// <summary>
    /// A full sync completed: adopt the primary's stream identity and the
    /// advertised offset as the served stream (the backlog restarts empty
    /// at that offset), and record the follow position to match.
    /// </summary>
    public void AdoptSyncedStream(string replId, long offset)
    {
        lock (_gate)
        {
            _replId = replId;
            _offset = offset;
            _backlog.Clear();
            _backlogBytes = 0;
            _backlogFirstByteOffset = offset;
            _followReplId = replId;
            _followOffset = offset;
            _followDb = 0;
        }
    }

    /// <summary>
    /// A propagated frame was applied on this replica: it advances the
    /// follow position and joins the served stream verbatim (see
    /// <see cref="AppendServedFrame"/>), which is what lets a replica
    /// itself serve downstream replicas.
    /// </summary>
    public void NoteAppliedFrame(byte[] rawFrame)
    {
        lock (_gate)
            _followOffset += rawFrame.Length;
        AppendServedFrame(rawFrame);
    }

    public void SetMasterLinkUp(bool up)
    {
        lock (_gate)
            _masterLinkUp = up;
    }

    public void SetSyncInProgress(bool inProgress)
    {
        lock (_gate)
            _syncInProgress = inProgress;
    }

    /// <summary>Completes when the role or follow target next changes (SetReplicaOf / Promote).</summary>
    public async Task WaitForChangeAsync(CancellationToken cancellationToken)
    {
        Task wait;
        lock (_gate)
            wait = _changed.Task;
        await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private void SignalChangeLocked()
    {
        var previous = _changed;
        _changed = NewSignal();
        previous.TrySetResult();
    }
}
