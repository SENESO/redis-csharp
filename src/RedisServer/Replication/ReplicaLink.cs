namespace RedisServer.Replication;

/// <summary>
/// One connected replica's outbound path on the primary: a byte-bounded
/// queue drained by a single pump task into the connection's frame
/// channel. The bound is the contract that a slow or dead replica can
/// never stall the primary's command gate: producers only ever enqueue
/// (a lock, a counter, no I/O), and when the queue would pass the cap
/// the link closes itself — the session is force-closed, and the replica
/// reconnects and resyncs (partially, while its offset is still inside
/// the primary's backlog; fully otherwise).
///
/// Lifecycle: the PSYNC handler creates the link, posts the sync header
/// frame itself, enqueues any backlog slice, then calls
/// <see cref="StartLive"/> — so the header always precedes every
/// propagated frame on the wire. Frames enqueued before
/// <see cref="StartLive"/> only accumulate; the pump does not exist yet.
/// </summary>
public sealed class ReplicaLink
{
    private readonly object _gate = new();
    private readonly Queue<byte[]> _pending = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly Action<byte[]> _postFrame;
    private readonly Action? _forceClose;
    private readonly Action<ReplicaLink> _onClosed;
    private readonly long _outboxCapBytes;
    private long _queuedBytes;
    private bool _closed;
    private bool _closeNotified;

    internal ReplicaLink(Action<byte[]> postFrame, Action? forceClose,
        Action<ReplicaLink> onClosed, long outboxCapBytes)
    {
        _postFrame = postFrame;
        _forceClose = forceClose;
        _onClosed = onClosed;
        _outboxCapBytes = outboxCapBytes;
    }

    public bool IsClosed
    {
        get { lock (_gate) return _closed; }
    }

    /// <summary>Starts the pump task; see the class remarks for the ordering contract.</summary>
    public void StartLive() => _ = Task.Run(PumpAsync);

    /// <summary>
    /// Enqueues one propagation frame. Returns false when the link is
    /// closed — including when this very frame overflowed the outbox, in
    /// which case the link has already force-closed its session and
    /// notified the manager.
    /// </summary>
    public bool Enqueue(byte[] frame)
    {
        var overflow = false;
        lock (_gate)
        {
            if (_closed)
                return false;
            if (_queuedBytes + frame.Length > _outboxCapBytes)
            {
                _closed = true;
                _pending.Clear();
                _queuedBytes = 0;
                overflow = true;
            }
            else
            {
                _pending.Enqueue(frame);
                _queuedBytes += frame.Length;
            }
        }

        if (overflow)
        {
            NotifyClosed();
            _forceClose?.Invoke();
            return false;
        }
        _signal.Release();
        return true;
    }

    /// <summary>
    /// Closes the link without touching the connection (the session is
    /// already ending, or the manager is dropping the link). Idempotent.
    /// </summary>
    public void Close()
    {
        lock (_gate)
        {
            if (_closed)
                return;
            _closed = true;
            _pending.Clear();
            _queuedBytes = 0;
        }
        _signal.Release();
        NotifyClosed();
    }

    private void NotifyClosed()
    {
        bool notify;
        lock (_gate)
        {
            notify = !_closeNotified;
            _closeNotified = true;
        }
        if (notify)
            _onClosed(this);
    }

    private async Task PumpAsync()
    {
        while (true)
        {
            await _signal.WaitAsync().ConfigureAwait(false);
            byte[]? combined = null;
            var done = false;
            lock (_gate)
            {
                if (_pending.Count > 0)
                {
                    var total = 0;
                    foreach (var frame in _pending)
                        total += frame.Length;
                    combined = new byte[total];
                    var at = 0;
                    while (_pending.Count > 0)
                    {
                        var frame = _pending.Dequeue();
                        frame.CopyTo(combined, at);
                        at += frame.Length;
                    }
                    _queuedBytes = 0;
                }
                else if (_closed)
                {
                    done = true;
                }
            }
            if (combined is not null)
                _postFrame(combined);
            if (done)
                return;
        }
    }
}
