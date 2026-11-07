using System.Buffers;
using System.Globalization;
using System.Text;
using RedisServer.Core;
using RedisServer.Persistence;
using RedisServer.Replication;

namespace RedisServer.Protocol;

/// <summary>A command-level error whose message is the full RESP error text.</summary>
public sealed class RedisCommandException : Exception
{
    public RedisCommandException(string message) : base(message)
    {
    }
}

/// <summary>
/// Executes parsed commands against the store and renders RESP replies.
///
/// Like Redis's own main thread, command execution is serialised: a single
/// gate wraps every command (and a whole EXEC), which is what makes
/// transactions atomic and AOF appends land in exact execution order.
/// Connection I/O happens outside the gate. Replies are appended to a
/// caller-owned buffer so the session flushes pipelined batches at once.
///
/// Mutating commands also report their <em>effects</em> — the canonical
/// commands the persistence layer appends to the AOF. Conditional/relative
/// forms are normalised on the way out (SET with EX becomes a plain SET
/// plus an absolute PEXPIREAT), so replay reproduces the same state
/// regardless of when it runs.
/// </summary>
public sealed partial class CommandProcessor
{
    private readonly RedisStore _store;
    private readonly RedisStats _stats;
    private readonly TimeProvider _clock;
    private readonly PersistenceManager _persistence;
    private readonly PubSubHub _hub;
    private readonly ReplicationManager _replication;
    private readonly int _port;
    private readonly int _tlsPort;
    private readonly byte[]? _requirePass;
    private readonly DateTimeOffset _startedAt;
    private readonly object _gate = new();
    private int _backgroundSaveRunning;

    private readonly Dictionary<byte[], CommandSpec> _specs;

    private delegate void CommandHandler(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects);

    private sealed record CommandSpec(int MinArgs, int MaxArgs, bool AllowedInPubSub, CommandHandler Handler, bool DenyOom = false, bool Write = false);

    private readonly long _maxMemory;
    private readonly MaxMemoryPolicy _maxMemoryPolicy;

    public CommandProcessor(RedisStore store, RedisStats stats, TimeProvider clock,
        PersistenceManager persistence, PubSubHub hub, int port, string? requirePass = null, int tlsPort = 0,
        long maxMemory = 0, MaxMemoryPolicy maxMemoryPolicy = MaxMemoryPolicy.NoEviction,
        ReplicationManager? replication = null)
    {
        _store = store;
        _stats = stats;
        _clock = clock;
        _persistence = persistence;
        _hub = hub;
        _replication = replication ?? new ReplicationManager(ReplicationManager.DefaultBacklogSizeBytes);
        _port = port;
        _tlsPort = tlsPort;
        _requirePass = requirePass is null ? null : Encoding.UTF8.GetBytes(requirePass);
        _maxMemory = maxMemory;
        _maxMemoryPolicy = maxMemoryPolicy;
        _startedAt = clock.GetUtcNow();
        _specs = BuildSpecs();
    }

    /// <summary>The replication state machine this processor serves and enforces.</summary>
    public ReplicationManager Replication => _replication;

    private Dictionary<byte[], CommandSpec> BuildSpecs()
    {
        // Dispatch is by the raw name bytes under an ASCII case-insensitive
        // comparison, so the hot path never materialises the name as a
        // string; names are only stringified on error paths.
        var specs = new Dictionary<byte[], CommandSpec>(AsciiNameComparer.Instance);
        void Add(string name, int min, int max, CommandHandler handler, bool pubsub = false, bool denyOom = false, bool write = false) =>
            specs[Ascii(name)] = new CommandSpec(min, max, pubsub, handler, denyOom, write);

        // Connection / server
        Add("PING", 1, 2, Ping, pubsub: true);
        Add("ECHO", 2, 2, Echo);
        Add("AUTH", 2, 3, Auth);
        Add("HELLO", 1, -1, Hello);
        Add("CLIENT", 2, -1, Client);
        Add("QUIT", 1, 1, Quit, pubsub: true);
        Add("DBSIZE", 1, 1, DbSize);
        Add("INFO", 1, 2, Info);
        Add("FLUSHDB", 1, 2, FlushDb, write: true);
        Add("FLUSHALL", 1, 2, FlushDb, write: true);
        Add("SAVE", 1, 1, Save);
        Add("BGSAVE", 1, 1, BgSave);
        Add("BGREWRITEAOF", 1, 1, BgRewriteAof);

        // Replication. The Write flag on every spec is the read/write
        // classification read-only enforcement consults on a replica; it
        // is deliberately NOT the DenyOom set — that one answers "may
        // this allocate against a full memory ceiling", a different
        // question (LPOP allocates nothing yet is a write; SAVE is
        // refused by neither set's logic but is an admin action, not a
        // dataset write, and stays available on replicas).
        Add("REPLICAOF", 3, 3, ReplicaOf);
        Add("PSYNC", 3, 3, PSync);
        Add("REPLCONF", 2, -1, ReplConf);

        // Strings
        Add("SET", 3, -1, Set, denyOom: true, write: true);
        Add("GET", 2, 2, Get);
        Add("DEL", 2, -1, Del, write: true);
        Add("EXISTS", 2, -1, Exists);
        Add("INCR", 2, 2, Incr, denyOom: true, write: true);
        Add("DECR", 2, 2, Decr, denyOom: true, write: true);
        Add("INCRBY", 3, 3, IncrBy, denyOom: true, write: true);
        Add("DECRBY", 3, 3, DecrBy, denyOom: true, write: true);
        Add("APPEND", 3, 3, Append, denyOom: true, write: true);
        Add("STRLEN", 2, 2, StrLen);

        // Keys
        Add("EXPIRE", 3, 4, Expire, write: true);
        Add("PEXPIRE", 3, 4, PExpire, write: true);
        Add("EXPIREAT", 3, 4, ExpireAt, write: true);
        Add("PEXPIREAT", 3, 4, PExpireAt, write: true);
        Add("TTL", 2, 2, Ttl);
        Add("PTTL", 2, 2, PTtl);
        Add("PERSIST", 2, 2, Persist, write: true);
        Add("TYPE", 2, 2, TypeCommand);
        Add("KEYS", 2, 2, Keys);
        Add("SCAN", 2, -1, Scan);

        // Collections (CommandProcessorCollections.cs)
        Add("LPUSH", 3, -1, LeftPush, denyOom: true, write: true);
        Add("RPUSH", 3, -1, RightPush, denyOom: true, write: true);
        Add("LPOP", 2, 3, LeftPop, write: true);
        Add("RPOP", 2, 3, RightPop, write: true);
        Add("LLEN", 2, 2, ListLength);
        Add("LRANGE", 4, 4, ListRange);
        Add("LINDEX", 3, 3, ListIndex);
        Add("SADD", 3, -1, SetAdd, denyOom: true, write: true);
        Add("SREM", 3, -1, SetRemove, write: true);
        Add("SMEMBERS", 2, 2, SetMembers);
        Add("SISMEMBER", 3, 3, SetIsMember);
        Add("SCARD", 2, 2, SetCard);
        Add("HSET", 4, -1, HashSet, denyOom: true, write: true);
        Add("HGET", 3, 3, HashGet);
        Add("HDEL", 3, -1, HashDelete, write: true);
        Add("HGETALL", 2, 2, HashGetAll);
        Add("HLEN", 2, 2, HashLength);
        Add("ZADD", 4, -1, ZAdd, denyOom: true, write: true);
        Add("ZINCRBY", 4, 4, ZIncrBy, denyOom: true, write: true);
        Add("ZSCORE", 3, 3, ZScore);
        Add("ZRANGE", 4, 5, ZRange);
        Add("ZREM", 3, -1, ZRemove, write: true);
        Add("ZCARD", 2, 2, ZCard);

        // Transactions are intercepted before dispatch (see ExecuteLocked);
        // these entries exist so queue-time validation can see them.
        Add("MULTI", 1, 1, NoDispatch);
        Add("EXEC", 1, 1, NoDispatch);
        Add("DISCARD", 1, 1, NoDispatch);

        // Pub/sub
        Add("SUBSCRIBE", 2, -1, Subscribe, pubsub: true);
        Add("UNSUBSCRIBE", 1, -1, Unsubscribe, pubsub: true);
        Add("PSUBSCRIBE", 2, -1, SubscribePattern, pubsub: true);
        Add("PUNSUBSCRIBE", 1, -1, UnsubscribePattern, pubsub: true);
        Add("PUBLISH", 3, 3, Publish);

        return specs;
    }

    private static void NoDispatch(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        throw new InvalidOperationException("Transaction commands are handled by ExecuteLocked.");

    // ------------------------------------------------------------------
    // Entry points
    // ------------------------------------------------------------------

    /// <summary>Executes one client command, writing its reply to <paramref name="output"/>.</summary>
    public void Execute(byte[][] args, ClientContext ctx, IBufferWriter<byte> output) =>
        ExecuteBatch(new[] { args }, ctx, output);

    /// <summary>
    /// Executes a batch of commands parsed from one read under a single
    /// gate acquisition, so a pipelined batch pays the serialisation cost
    /// once instead of once per command. Per-command semantics are exactly
    /// those of <see cref="Execute"/>: each command's AOF effects are
    /// appended, in order, before the caller flushes the batch's replies,
    /// and execution stops after a command that requests close (QUIT).
    /// </summary>
    public void ExecuteBatch(IReadOnlyList<byte[][]> commands, ClientContext ctx, IBufferWriter<byte> output)
    {
        lock (_gate)
        {
            var effects = new List<byte[][]>();
            foreach (var args in commands)
            {
                ExecuteLocked(args, ctx, output, effects, inTransactionBody: false);
                if (effects.Count > 0)
                {
                    _persistence.AppendEffects(effects);
                    RecordLocalEffects(effects);
                    effects.Clear();
                }
                if (ctx.CloseRequested)
                    break;
            }
        }
    }

    /// <summary>
    /// Executes one command during AOF replay: no client, no reply stream,
    /// no re-appending. Returns false when the command errored (replay
    /// carries on regardless — the AOF only ever contains commands that
    /// succeeded when they first ran).
    /// </summary>
    public bool ExecuteReplay(byte[][] args)
    {
        var ctx = new ClientContext { IsAuthenticated = true, IsServerInternal = true };
        var sink = new ArrayBufferWriter<byte>();
        lock (_gate)
        {
            var before = sink.WrittenCount;
            ExecuteLocked(args, ctx, sink, new List<byte[][]>(), inTransactionBody: false);
            return !(sink.WrittenCount > before && sink.WrittenSpan[0] == (byte)'-');
        }
    }

    /// <summary>
    /// Feeds one executed command's normalised effects into the
    /// replication stream: each effect is encoded once and appended to
    /// the served stream (backlog + replica fan-out) in execution order.
    /// The encoding is the AOF's, so the replication stream and the AOF
    /// are byte-for-byte the same history.
    /// </summary>
    private void RecordLocalEffects(List<byte[][]> effects)
    {
        foreach (var effect in effects)
            _replication.AppendServedFrame(RespWriter.EncodeCommand(effect));
    }

    /// <summary>
    /// Applies one propagated frame on a replica: executes it with a
    /// server-internal context (no reply stream, no read-only or memory
    /// refusal — the primary already admitted the write), then records
    /// the frame itself — it is already a normalised effect — in this
    /// server's AOF and served stream. Returns false when the frame
    /// errored (a stream that disagrees with the primary's history;
    /// nothing is recorded for it, so offsets never claim it applied).
    /// </summary>
    public bool ApplyReplicationFrame(byte[][] args, byte[] rawFrame)
    {
        var ctx = new ClientContext { IsAuthenticated = true, IsServerInternal = true };
        var sink = new ArrayBufferWriter<byte>();
        lock (_gate)
        {
            var before = sink.WrittenCount;
            ExecuteLocked(args, ctx, sink, new List<byte[][]>(), inTransactionBody: false);
            var errored = sink.WrittenCount > before && sink.WrittenSpan[0] == (byte)'-';
            if (errored)
                return false;
            _persistence.AppendEffects(new[] { args });
            _replication.NoteAppliedFrame(rawFrame);
            return true;
        }
    }

    /// <summary>
    /// Replaces the whole keyspace with a snapshot received during a full
    /// sync. The snapshot load and the AOF reset happen under the command
    /// gate, so no client read can observe a half-loaded state: after a
    /// full sync the replica's AOF is rewritten from the synced state
    /// (the pre-sync history belongs to a dataset that no longer exists),
    /// and subsequent propagated frames append onto it.
    /// </summary>
    public void LoadReplicationSnapshot(byte[] snapshotBytes)
    {
        var status = SnapshotFile.TryParse(snapshotBytes, out var entries, out var error);
        if (status != SnapshotLoadStatus.Loaded)
            throw new InvalidDataException($"Replication snapshot is unusable: {error}");
        lock (_gate)
        {
            _store.FlushAll();
            foreach (var entry in entries)
                _store.ImportEntry(entry);
            if (_persistence.AofEnabled)
                _persistence.RewriteAof(_store);
        }
    }

    private void ExecuteLocked(byte[][] args, ClientContext ctx, IBufferWriter<byte> output,
        List<byte[][]> effects, bool inTransactionBody)
    {
        _stats.AddCommand();
        if (args.Length == 0)
        {
            RespWriter.WriteError(output, "ERR empty command");
            return;
        }

        // A replication link speaks only the replication stream. Anything
        // the replica side sends after PSYNC (REPLCONF bookkeeping, a
        // stray PING) is dropped without a reply: reply bytes would be
        // parsed as propagation frames by the replica and corrupt it.
        if (ctx.IsReplicationLink)
            return;

        var rawName = args[0];

        // Authentication gate: with a password configured, an
        // unauthenticated connection may only AUTH, HELLO or QUIT — every
        // other command (including MULTI/EXEC and unknown commands) is
        // answered NOAUTH, exactly as Redis orders it. HELLO is exempt
        // because it carries its own authentication clause and its own,
        // longer NOAUTH text (see the Hello handler). Transaction bodies
        // are not re-checked: the EXEC that carries them passed the gate.
        if (!inTransactionBody && _requirePass is not null && !ctx.IsAuthenticated &&
            !IsName(rawName, "AUTH") && !IsName(rawName, "QUIT") && !IsName(rawName, "HELLO"))
        {
            RespWriter.WriteError(output, "NOAUTH Authentication required.");
            return;
        }

        // Transaction control flow.
        if (!inTransactionBody)
        {
            if (IsName(rawName, "MULTI"))
            {
                if (ctx.InTransaction)
                {
                    RespWriter.WriteError(output, "ERR MULTI calls can not be nested");
                    return;
                }
                ctx.InTransaction = true;
                ctx.TransactionAborted = false;
                ctx.TransactionQueue.Clear();
                RespWriter.WriteSimpleString(output, "OK");
                return;
            }
            if (IsName(rawName, "EXEC"))
            {
                ExecuteTransaction(ctx, output, effects);
                return;
            }
            if (IsName(rawName, "DISCARD"))
            {
                if (!ctx.InTransaction)
                {
                    RespWriter.WriteError(output, "ERR DISCARD without MULTI");
                    return;
                }
                ctx.InTransaction = false;
                ctx.TransactionAborted = false;
                ctx.TransactionQueue.Clear();
                RespWriter.WriteSimpleString(output, "OK");
                return;
            }
            if (ctx.InTransaction)
            {
                QueueCommand(args, ctx, output);
                return;
            }
        }

        if (!_specs.TryGetValue(rawName, out var spec))
        {
            RespWriter.WriteError(output, $"ERR unknown command '{RespParser.Describe(args[0])}', with args beginning with: {DescribeArgs(args)}");
            return;
        }

        if (ctx.IsSubscribed && !spec.AllowedInPubSub)
        {
            RespWriter.WriteError(output,
                $"ERR Can't execute '{NameOf(rawName).ToLowerInvariant()}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context");
            return;
        }

        if (args.Length < spec.MinArgs || (spec.MaxArgs >= 0 && args.Length > spec.MaxArgs))
        {
            RespWriter.WriteError(output, ArityError(NameOf(rawName)));
            return;
        }

        // Read-only replica enforcement, at the point of actual dispatch
        // (so commands queued in a MULTI are judged when EXEC runs them,
        // and their READONLY errors land inside the EXEC array, as in
        // Redis). The classification is the spec's Write flag — the true
        // write set, not the maxmemory deny-OOM set. Server-internal
        // execution (AOF replay, replication apply) is not a client and
        // is never refused.
        if (_replication.IsReplica && !ctx.IsServerInternal && spec.Write)
        {
            RespWriter.WriteError(output, "READONLY You can't write against a read only replica.");
            return;
        }

        // Memory ceiling (--maxmemory), checked before the command runs,
        // exactly where Redis checks it: with an evicting policy, keys
        // are evicted first (their DEL effects join this command's
        // effect list, so they reach the AOF ahead of it and replay in
        // the same order); a write that may allocate is refused with the
        // OOM error only when the store is still over the ceiling after
        // eviction — or immediately under noeviction. Reads are never
        // refused. Replay and other server-internal execution skip the
        // gate: the AOF only holds commands that already passed it.
        if (!ctx.IsServerInternal && _maxMemory > 0 && _store.UsedMemory > _maxMemory)
        {
            if (_maxMemoryPolicy != MaxMemoryPolicy.NoEviction)
            {
                var guard = 0;
                while (_store.UsedMemory > _maxMemory && guard++ < 100_000 &&
                       _store.TryEvictOne(_maxMemoryPolicy) is { } victim)
                {
                    effects.Add(new[] { Ascii("DEL"), victim });
                }
            }
            if (spec.DenyOom && _store.UsedMemory > _maxMemory)
            {
                RespWriter.WriteError(output, "OOM command not allowed when used memory > 'maxmemory'.");
                return;
            }
        }

        try
        {
            spec.Handler(args, ctx, output, effects);
        }
        catch (WrongTypeException ex)
        {
            RespWriter.WriteError(output, ex.Message);
        }
        catch (ValueErrorException ex)
        {
            RespWriter.WriteError(output, ex.Message);
        }
        catch (RedisCommandException ex)
        {
            RespWriter.WriteError(output, ex.Message);
        }
    }

    private void QueueCommand(byte[][] args, ClientContext ctx, IBufferWriter<byte> output)
    {
        if (!_specs.TryGetValue(args[0], out var spec))
        {
            ctx.TransactionAborted = true;
            RespWriter.WriteError(output, $"ERR unknown command '{RespParser.Describe(args[0])}', with args beginning with: {DescribeArgs(args)}");
            return;
        }
        if (IsName(args[0], "SUBSCRIBE") || IsName(args[0], "PSUBSCRIBE") || IsName(args[0], "UNSUBSCRIBE") ||
            IsName(args[0], "PUNSUBSCRIBE") || IsName(args[0], "PUBLISH"))
        {
            ctx.TransactionAborted = true;
            RespWriter.WriteError(output, $"ERR {NameOf(args[0])} is not allowed inside a transaction");
            return;
        }
        if (args.Length < spec.MinArgs || (spec.MaxArgs >= 0 && args.Length > spec.MaxArgs))
        {
            ctx.TransactionAborted = true;
            RespWriter.WriteError(output, ArityError(NameOf(args[0])));
            return;
        }
        ctx.TransactionQueue.Add(args);
        RespWriter.WriteSimpleString(output, "QUEUED");
    }

    private void ExecuteTransaction(ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (!ctx.InTransaction)
        {
            RespWriter.WriteError(output, "ERR EXEC without MULTI");
            return;
        }

        var queued = ctx.TransactionQueue.ToArray();
        ctx.InTransaction = false;
        ctx.TransactionQueue.Clear();
        if (ctx.TransactionAborted)
        {
            ctx.TransactionAborted = false;
            RespWriter.WriteError(output, "EXECABORT Transaction discarded because of previous errors.");
            return;
        }

        RespWriter.WriteArrayHeader(output, queued.Length);
        foreach (var queuedArgs in queued)
        {
            var reply = new ArrayBufferWriter<byte>();
            ExecuteLocked(queuedArgs, ctx, reply, effects, inTransactionBody: true);
            output.Write(reply.WrittenSpan);
        }
    }

    // ------------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------------

    private static string NameOf(byte[] raw) => Encoding.ASCII.GetString(raw).ToUpperInvariant();

    /// <summary>Allocation-free ASCII case-insensitive comparison of a raw command name against an uppercase literal.</summary>
    private static bool IsName(byte[] raw, string upperLiteral)
    {
        if (raw.Length != upperLiteral.Length)
            return false;
        for (var i = 0; i < raw.Length; i++)
        {
            var b = raw[i];
            if (b is >= (byte)'a' and <= (byte)'z')
                b -= 32;
            if (b != (byte)upperLiteral[i])
                return false;
        }
        return true;
    }

    /// <summary>FNV-1a over ASCII-uppercased bytes: dictionary equality for command names in any client-sent case.</summary>
    private sealed class AsciiNameComparer : IEqualityComparer<byte[]>
    {
        public static readonly AsciiNameComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y)
        {
            if (x is null || y is null || x.Length != y.Length)
                return ReferenceEquals(x, y);
            for (var i = 0; i < x.Length; i++)
            {
                if (Upper(x[i]) != Upper(y[i]))
                    return false;
            }
            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            unchecked
            {
                var hash = 2166136261u;
                foreach (var b in obj)
                {
                    hash ^= Upper(b);
                    hash *= 16777619u;
                }
                return (int)hash;
            }
        }

        private static byte Upper(byte b) => b is >= (byte)'a' and <= (byte)'z' ? (byte)(b - 32) : b;
    }

    internal static string ArityError(string upperName) =>
        $"ERR wrong number of arguments for '{upperName.ToLowerInvariant()}' command";

    private static string DescribeArgs(byte[][] args)
    {
        var parts = new List<string>();
        for (var i = 1; i < args.Length && i <= 3; i++)
            parts.Add($"'{RespParser.Describe(args[i])}'");
        return string.Join(" ", parts);
    }

    private static RedisCommandException SyntaxError() => new("ERR syntax error");

    private static long ParseLong(byte[] arg)
    {
        if (!RedisStore.TryParseStrictLong(arg, out var value))
            throw ValueErrorException.NotAnInteger();
        return value;
    }

    private static byte[] Ascii(string text) => Encoding.ASCII.GetBytes(text);

    private static byte[] LongBytes(long value) => Ascii(value.ToString(CultureInfo.InvariantCulture));

    private static List<byte[]> Tail(byte[][] args, int from)
    {
        var list = new List<byte[]>(args.Length - from);
        for (var i = from; i < args.Length; i++)
            list.Add(args[i]);
        return list;
    }

    // ------------------------------------------------------------------
    // Connection / server commands
    // ------------------------------------------------------------------

    /// <summary>
    /// CLIENT SETNAME / GETNAME / SETINFO / ID. SETINFO arrived in Redis
    /// 7.2 (a 7.0 server answers "unknown subcommand"), but real client
    /// libraries — redis-py among them — send it on every connect and
    /// tolerate its failure; accepting it costs nothing and the library
    /// name/version are kept on the connection. Error texts probed
    /// against redis-server 7.0.15 (the unknown-subcommand text also
    /// covers SETINFO there; the SETINFO-attribute error is the 7.2
    /// form).
    /// </summary>
    private void Client(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        switch (NameOf(args[1]))
        {
            case "SETNAME":
                if (args.Length != 3)
                {
                    RespWriter.WriteError(output, ArityError("CLIENT"));
                    return;
                }
                SetClientName(ctx, args[2]); // throws the client-name error on bad characters
                RespWriter.WriteSimpleString(output, "OK");
                return;
            case "GETNAME":
                if (args.Length != 2)
                {
                    RespWriter.WriteError(output, ArityError("CLIENT"));
                    return;
                }
                RespWriter.WriteBulkStringOrNull(output, ctx.Name, ctx.ProtocolVersion);
                return;
            case "SETINFO":
                if (args.Length != 4)
                {
                    RespWriter.WriteError(output, ArityError("CLIENT"));
                    return;
                }
                var attribute = NameOf(args[2]);
                if (attribute == "LIB-NAME")
                    ctx.LibraryName = Encoding.UTF8.GetString(args[3]);
                else if (attribute == "LIB-VER")
                    ctx.LibraryVersion = Encoding.UTF8.GetString(args[3]);
                else
                {
                    RespWriter.WriteError(output,
                        $"ERR Unknown subcommand or wrong number of arguments for '{attribute.ToLowerInvariant()}'. Try CLIENT SETINFO HELP.");
                    return;
                }
                RespWriter.WriteSimpleString(output, "OK");
                return;
            case "ID":
                if (args.Length != 2)
                {
                    RespWriter.WriteError(output, ArityError("CLIENT"));
                    return;
                }
                RespWriter.WriteInteger(output, ctx.Id);
                return;
            default:
                RespWriter.WriteError(output,
                    $"ERR unknown subcommand '{RespParser.Describe(args[1])}'. Try CLIENT HELP.");
                return;
        }
    }

    private void Ping(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (ctx.IsSubscribed)
        {
            if (ctx.ProtocolVersion >= 3)
            {
                // RESP3 drops the pong array: no argument answers a plain
                // +PONG and an argument answers with the bare bulk string
                // (both probed byte-for-byte against redis-server 7.0.15).
                if (args.Length == 2)
                    RespWriter.WriteBulkString(output, args[1]);
                else
                    RespWriter.WriteSimpleString(output, "PONG");
                return;
            }
            RespWriter.WriteArrayHeader(output, 2);
            RespWriter.WriteBulkString(output, "pong");
            RespWriter.WriteBulkString(output, args.Length == 2 ? args[1] : Array.Empty<byte>());
            return;
        }
        if (args.Length == 2)
            RespWriter.WriteBulkString(output, args[1]);
        else
            RespWriter.WriteSimpleString(output, "PONG");
    }

    private void Echo(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteBulkString(output, args[1]);

    private void Auth(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        byte[] presented;
        if (args.Length == 3)
        {
            // AUTH <username> <password>: only the default user exists;
            // any other name is indistinguishable from a bad password.
            if (!args[1].AsSpan().SequenceEqual("default"u8))
                throw new RedisCommandException("WRONGPASS invalid username-password pair or user is disabled.");
            presented = args[2];
        }
        else
        {
            if (_requirePass is null)
                throw new RedisCommandException(
                    "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
            presented = args[1];
        }

        // With no password configured the default user is passwordless, so
        // the username form above authenticates unconditionally (as in
        // Redis, whose default user carries no password then). A failed
        // attempt never revokes an already authenticated connection.
        if (_requirePass is not null &&
            !System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(presented, _requirePass))
            throw new RedisCommandException("WRONGPASS invalid username-password pair or user is disabled.");

        ctx.IsAuthenticated = true;
        RespWriter.WriteSimpleString(output, "OK");
    }

    /// <summary>
    /// Validates and stores a client name (HELLO ... SETNAME and CLIENT
    /// SETNAME share the rule): bytes must be printable, non-space ASCII;
    /// an empty name clears the current one. Returns the stored copy.
    /// </summary>
    internal static void SetClientName(ClientContext ctx, byte[] name)
    {
        foreach (var b in name)
        {
            if (b < 33 || b > 126)
                throw new RedisCommandException(
                    "ERR Client names cannot contain spaces, newlines or special characters.");
        }
        ctx.Name = name.Length == 0 ? null : name.ToArray();
    }

    /// <summary>
    /// HELLO [protover] [AUTH user pass] [SETNAME name]: negotiates the
    /// protocol version and answers with the server handshake. The reply
    /// is a flat array of key/value pairs in RESP2 and a map in RESP3 —
    /// rendered in the version being switched <em>to</em>, as in Redis.
    ///
    /// Error order, probed against redis-server 7.0.15: a non-integer
    /// protocol version, then NOPROTO for a version other than 2 or 3,
    /// then per-clause syntax errors and WRONGPASS as the clauses are
    /// applied in order, and only at the end — when a password is
    /// configured and the connection is still unauthenticated — HELLO's
    /// own NOAUTH text.
    /// </summary>
    private void Hello(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var proto = ctx.ProtocolVersion;
        var i = 1;
        if (args.Length > 1)
        {
            if (!long.TryParse(RespParser.Describe(args[1]), NumberStyles.Integer,
                    CultureInfo.InvariantCulture, out var version))
                throw new RedisCommandException("ERR Protocol version is not an integer or out of range");
            if (version is not (2 or 3))
                throw new RedisCommandException("NOPROTO unsupported protocol version");
            proto = (int)version;
            i = 2;
        }

        for (; i < args.Length; i++)
        {
            switch (NameOf(args[i]))
            {
                case "AUTH":
                    if (i + 2 >= args.Length)
                        throw new RedisCommandException("ERR Syntax error in HELLO option 'AUTH'");
                    AuthenticateHello(args[i + 1], args[i + 2], ctx);
                    i += 2;
                    break;
                case "SETNAME":
                    if (i + 1 >= args.Length)
                        throw new RedisCommandException("ERR Syntax error in HELLO option 'SETNAME'");
                    SetClientName(ctx, args[i + 1]);
                    i += 1;
                    break;
                default:
                    throw new RedisCommandException(
                        $"ERR Syntax error in HELLO option '{RespParser.Describe(args[i])}'");
            }
        }

        if (_requirePass is not null && !ctx.IsAuthenticated)
            throw new RedisCommandException(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");

        ctx.ProtocolVersion = proto;
        if (proto >= 3)
            RespWriter.WriteMapHeader(output, 7);
        else
            RespWriter.WriteArrayHeader(output, 14);
        WriteHelloPair(output, "server", "redis-csharp");
        WriteHelloPair(output, "version", "1.0.0");
        RespWriter.WriteBulkString(output, "proto");
        RespWriter.WriteInteger(output, proto);
        RespWriter.WriteBulkString(output, "id");
        RespWriter.WriteInteger(output, ctx.Id);
        WriteHelloPair(output, "mode", "standalone");
        WriteHelloPair(output, "role", _replication.IsReplica ? "slave" : "master");
        RespWriter.WriteBulkString(output, "modules");
        RespWriter.WriteArrayHeader(output, 0);
    }

    private static void WriteHelloPair(IBufferWriter<byte> output, string key, string value)
    {
        RespWriter.WriteBulkString(output, key);
        RespWriter.WriteBulkString(output, value);
    }

    /// <summary>
    /// The AUTH clause of HELLO: unlike the AUTH command, presenting any
    /// password succeeds when none is configured (the default user is
    /// passwordless then); a wrong one fails with WRONGPASS and leaves
    /// both the protocol version and the authentication state untouched.
    /// </summary>
    private void AuthenticateHello(byte[] user, byte[] password, ClientContext ctx)
    {
        if (!user.AsSpan().SequenceEqual("default"u8))
            throw new RedisCommandException("WRONGPASS invalid username-password pair or user is disabled.");
        if (_requirePass is not null &&
            !System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(password, _requirePass))
            throw new RedisCommandException("WRONGPASS invalid username-password pair or user is disabled.");
        ctx.IsAuthenticated = true;
    }

    private void Quit(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        RespWriter.WriteSimpleString(output, "OK");
        ctx.CloseRequested = true;
    }

    private void DbSize(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.Count);

    internal static string PolicyName(MaxMemoryPolicy policy) => policy switch
    {
        MaxMemoryPolicy.AllKeysLru => "allkeys-lru",
        MaxMemoryPolicy.AllKeysRandom => "allkeys-random",
        MaxMemoryPolicy.VolatileLru => "volatile-lru",
        MaxMemoryPolicy.VolatileRandom => "volatile-random",
        MaxMemoryPolicy.VolatileTtl => "volatile-ttl",
        _ => "noeviction"
    };

    private void Info(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var section = args.Length == 2 ? NameOf(args[1]) : null;
        var builder = new StringBuilder();
        void Section(string name, Action body)
        {
            if (section is null || section == "ALL" || section == name.ToUpperInvariant())
            {
                builder.Append("# ").Append(name).Append("\r\n");
                body();
            }
        }

        var uptime = (long)(_clock.GetUtcNow() - _startedAt).TotalSeconds;
        Section("Server", () =>
        {
            builder.Append("server_name:redis-csharp\r\n");
            builder.Append("version:1.0.0\r\n");
            builder.Append("os:").Append(Environment.OSVersion.ToString()).Append("\r\n");
            builder.Append("tcp_port:").Append(_port).Append("\r\n");
            builder.Append("tls_port:").Append(_tlsPort).Append("\r\n");
            builder.Append("uptime_in_seconds:").Append(uptime).Append("\r\n");
        });
        Section("Clients", () =>
        {
            builder.Append("connected_clients:").Append(_stats.CurrConnections).Append("\r\n");
        });
        Section("Memory", () =>
        {
            // used_memory is this server's data-only estimate (see
            // RedisStore): Redis's figure also counts allocator and
            // server overhead, so the two are comparable in shape, not
            // in absolute value.
            builder.Append("used_memory:").Append(_store.UsedMemory).Append("\r\n");
            builder.Append("maxmemory:").Append(_maxMemory).Append("\r\n");
            builder.Append("maxmemory_policy:").Append(PolicyName(_maxMemoryPolicy)).Append("\r\n");
        });
        Section("Persistence", () =>
        {
            builder.Append("aof_enabled:").Append(_persistence.AofEnabled ? 1 : 0).Append("\r\n");
            builder.Append("rdb_last_save_time:").Append(_persistence.LastSaveTime?.ToUnixTimeSeconds() ?? 0).Append("\r\n");
        });
        Section("Stats", () =>
        {
            builder.Append("total_connections_received:").Append(_stats.TotalConnectionsReceived).Append("\r\n");
            builder.Append("total_commands_processed:").Append(_stats.TotalCommandsProcessed).Append("\r\n");
            builder.Append("keyspace_hits:").Append(_stats.KeyspaceHits).Append("\r\n");
            builder.Append("keyspace_misses:").Append(_stats.KeyspaceMisses).Append("\r\n");
            builder.Append("expired_keys:").Append(_stats.ExpiredKeys).Append("\r\n");
            builder.Append("evicted_keys:").Append(_stats.EvictedKeys).Append("\r\n");
            builder.Append("pubsub_channels:").Append(_hub.ChannelCount).Append("\r\n");
            builder.Append("pubsub_patterns:").Append(_hub.PatternCount).Append("\r\n");
        });
        Section("Replication", () =>
        {
            if (_replication.IsReplica)
            {
                builder.Append("role:slave\r\n");
                var target = _replication.FollowTarget;
                builder.Append("master_host:").Append(target?.Host ?? string.Empty).Append("\r\n");
                builder.Append("master_port:").Append(target?.Port ?? 0).Append("\r\n");
                builder.Append("master_link_status:").Append(_replication.MasterLinkUp ? "up" : "down").Append("\r\n");
                builder.Append("master_sync_in_progress:").Append(_replication.SyncInProgress ? 1 : 0).Append("\r\n");
                builder.Append("master_repl_offset:").Append(_replication.FollowOffset).Append("\r\n");
            }
            else
            {
                builder.Append("role:master\r\n");
                builder.Append("master_repl_offset:").Append(_replication.Offset).Append("\r\n");
            }
            builder.Append("connected_slaves:").Append(_replication.ConnectedReplicas).Append("\r\n");
            builder.Append("master_replid:").Append(_replication.ReplId).Append("\r\n");
            builder.Append("repl_backlog_active:1\r\n");
            builder.Append("repl_backlog_size:").Append(_replication.BacklogSizeBytes).Append("\r\n");
            builder.Append("repl_backlog_first_byte_offset:").Append(_replication.BacklogFirstByteOffset).Append("\r\n");
            builder.Append("repl_backlog_histlen:").Append(_replication.BacklogHistoryLength).Append("\r\n");
        });
        Section("Keyspace", () =>
        {
            builder.Append("db0:keys=").Append(_store.Count)
                .Append(",expires=").Append(_store.ExpiresCount)
                .Append(",avg_ttl=0\r\n");
        });

        var text = builder.ToString();
        if (ctx.ProtocolVersion >= 3)
        {
            // In RESP3, INFO is a verbatim string (format tag "txt") —
            // including for an unknown section, whose text is empty.
            RespWriter.WriteVerbatimString(output, text);
            return;
        }
        if (builder.Length == 0)
        {
            // An unknown section is not an error in Redis: the reply is
            // an empty bulk string.
            RespWriter.WriteBulkString(output, "");
            return;
        }
        RespWriter.WriteBulkString(output, text);
    }

    private void FlushDb(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (args.Length == 2)
        {
            var mode = NameOf(args[1]);
            if (mode is not ("ASYNC" or "SYNC"))
                throw SyntaxError();
        }
        _store.FlushAll();
        effects.Add(new[] { Ascii("FLUSHALL") });
        RespWriter.WriteSimpleString(output, "OK");
    }

    private void Save(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        try
        {
            _persistence.SaveSnapshot(_store);
            RespWriter.WriteSimpleString(output, "OK");
        }
        catch (Exception ex)
        {
            RespWriter.WriteError(output, $"ERR {ex.Message}");
        }
    }

    private void BgSave(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (Interlocked.CompareExchange(ref _backgroundSaveRunning, 1, 0) != 0)
        {
            RespWriter.WriteError(output, "ERR Background save already in progress");
            return;
        }
        _ = Task.Run(() =>
        {
            try
            {
                _persistence.SaveSnapshot(_store);
            }
            catch
            {
                // The failure is recorded on the persistence manager
                // (LastSaveError) and logged on the next INFO/SAVE.
            }
            finally
            {
                Interlocked.Exchange(ref _backgroundSaveRunning, 0);
            }
        });
        RespWriter.WriteSimpleString(output, "Background saving started");
    }

    private void BgRewriteAof(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (!_persistence.AofEnabled)
        {
            RespWriter.WriteError(output, "ERR AOF is not enabled");
            return;
        }
        // We hold the command gate here, so the rewrite's view of the
        // keyspace is consistent and no append can slip into the swapped
        // file. (Real Redis forks instead; see the README's design notes.)
        _persistence.RewriteAof(_store);
        RespWriter.WriteSimpleString(output, "Background append only file rewriting started");
    }

    // ------------------------------------------------------------------
    // Replication commands
    // ------------------------------------------------------------------

    /// <summary>
    /// REPLCONF ...: replica bookkeeping (listening-port, capabilities,
    /// acknowledgements). Accepted and otherwise ignored — progress here
    /// is tracked by stream position, not by replica acknowledgements.
    /// </summary>
    private void ReplConf(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteSimpleString(output, "OK");

    /// <summary>
    /// REPLICAOF host port | REPLICAOF NO ONE. The role change takes
    /// effect immediately for read-only enforcement and INFO; the
    /// replication service observes the state change and connects,
    /// reconnects, or stops by itself. The dataset is left untouched by
    /// the switch: an incoming full sync replaces it wholesale.
    /// </summary>
    private void ReplicaOf(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (IsName(args[1], "NO") && IsName(args[2], "ONE"))
        {
            _replication.Promote();
            RespWriter.WriteSimpleString(output, "OK");
            return;
        }

        var host = Encoding.UTF8.GetString(args[1]);
        if (!long.TryParse(RespParser.Describe(args[2]), NumberStyles.Integer,
                CultureInfo.InvariantCulture, out var port) || port is < 1 or > 65535)
            throw new RedisCommandException("ERR Invalid port");
        if (port == _port && host is "127.0.0.1" or "localhost" or "::1")
            throw new RedisCommandException("ERR Can't replicate from itself.");
        _replication.SetReplicaOf(host, (int)port);
        RespWriter.WriteSimpleString(output, "OK");
    }

    /// <summary>
    /// PSYNC replid offset: claims this connection as a replication link.
    /// It runs under the command gate, so the decision, the snapshot and
    /// the link registration are atomic against client writes: a full
    /// sync's snapshot is the state at exactly the advertised offset, and
    /// every effect after it reaches the link through its bounded outbox,
    /// in order. Nothing is written to the normal reply buffer — the sync
    /// header (and snapshot) go out as one pushed frame, ahead of the
    /// link pump's frames, so they are the first bytes the replica reads.
    ///
    /// The full-sync wire form mirrors Redis's RDB transfer: a
    /// +FULLRESYNC line, then a bulk-string header and the raw snapshot
    /// bytes with NO trailing CRLF.
    /// </summary>
    private void PSync(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (ctx.PostFrame is null)
        {
            RespWriter.WriteError(output, "ERR PSYNC is only available on a framed connection");
            return;
        }

        var requestedId = RespParser.Describe(args[1]);
        if (!long.TryParse(RespParser.Describe(args[2]), NumberStyles.Integer,
                CultureInfo.InvariantCulture, out var requestedOffset))
            requestedOffset = -1;

        var kind = _replication.DecideSync(requestedId, requestedOffset, out var slice);
        byte[] header;
        if (kind == SyncKind.Full)
        {
            var snapshot = SnapshotFile.Serialize(_store.ExportState());
            var head = Encoding.ASCII.GetBytes(
                $"+FULLRESYNC {_replication.ReplId} {_replication.Offset}\r\n${snapshot.Length}\r\n");
            header = new byte[head.Length + snapshot.Length];
            head.CopyTo(header, 0);
            snapshot.CopyTo(header, head.Length);
        }
        else
        {
            header = "+CONTINUE\r\n"u8.ToArray();
        }

        var postFrame = ctx.PostFrame;
        var link = _replication.RegisterLink(postFrame, ctx.ForceClose);
        ctx.IsReplicationLink = true;
        ctx.Disconnected = () =>
        {
            _replication.UnregisterLink(link);
            link.Close();
        };
        postFrame(header);
        if (slice is not null)
        {
            foreach (var frame in slice)
                link.Enqueue(frame);
        }
        link.StartLive();
    }

    // ------------------------------------------------------------------
    // String commands
    // ------------------------------------------------------------------

    private void Set(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var key = args[1];
        var value = args[2];
        var condition = SetCondition.Always;
        TimeSpan? ttl = null;
        var keepTtl = false;
        var returnOld = false;

        for (var i = 3; i < args.Length; i++)
        {
            var option = NameOf(args[i]);
            switch (option)
            {
                case "NX":
                    if (condition == SetCondition.IfPresent)
                        throw SyntaxError();
                    condition = SetCondition.IfAbsent;
                    break;
                case "XX":
                    if (condition == SetCondition.IfAbsent)
                        throw SyntaxError();
                    condition = SetCondition.IfPresent;
                    break;
                case "GET":
                    returnOld = true;
                    break;
                case "KEEPTTL":
                    keepTtl = true;
                    break;
                case "EX":
                case "PX":
                    if (ttl is not null || i + 1 >= args.Length)
                        throw SyntaxError();
                    var amount = ParseLong(args[++i]);
                    if (amount <= 0)
                        throw new RedisCommandException("ERR invalid expire time in 'set' command");
                    ttl = option == "EX" ? TimeSpan.FromSeconds(amount) : TimeSpan.FromMilliseconds(amount);
                    break;
                default:
                    throw SyntaxError();
            }
        }
        if (ttl is not null && keepTtl)
            throw SyntaxError();

        byte[]? oldValue = null;
        if (returnOld)
            oldValue = _store.Get(key); // throws WRONGTYPE for non-strings, like Redis

        var outcome = _store.Set(key, value, condition, ttl, keepTtl);
        if (outcome == SetOutcome.Stored)
        {
            var effect = new List<byte[]> { Ascii("SET"), key, value };
            effects.Add(effect.ToArray());
            if (ttl is { } span)
            {
                var deadlineMs = (_clock.GetUtcNow() + span).ToUnixTimeMilliseconds();
                effects.Add(new[] { Ascii("PEXPIREAT"), key, LongBytes(deadlineMs) });
            }
        }

        if (returnOld)
            // Redis returns the old value even when NX/XX declined the
            // write; only a missing key answers nil.
            RespWriter.WriteBulkStringOrNull(output, oldValue, ctx.ProtocolVersion);
        else if (outcome == SetOutcome.Stored)
            RespWriter.WriteSimpleString(output, "OK");
        else
            RespWriter.WriteNull(output, ctx.ProtocolVersion);
    }

    private void Get(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var value = _store.Get(args[1]);
        if (value is null)
            _stats.AddKeyspaceMiss();
        else
            _stats.AddKeyspaceHit();
        RespWriter.WriteBulkStringOrNull(output, value, ctx.ProtocolVersion);
    }

    private void Del(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var removed = _store.Delete(Tail(args, 1));
        if (removed > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, removed);
    }

    private void Exists(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.Exists(Tail(args, 1)));

    private void Incr(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        Delta(args, 1, output, effects);

    private void Decr(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        Delta(args, -1, output, effects);

    private void IncrBy(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        Delta(args, ParseLong(args[2]), output, effects);

    private void DecrBy(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var amount = ParseLong(args[2]);
        if (amount == long.MinValue)
            throw new ValueErrorException("ERR decrement would overflow");
        Delta(args, -amount, output, effects);
    }

    private void Delta(byte[][] args, long delta, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var next = _store.ApplyDelta(args[1], delta);
        effects.Add(args);
        RespWriter.WriteInteger(output, next);
    }

    private void Append(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var length = _store.Append(args[1], args[2]);
        effects.Add(args);
        RespWriter.WriteInteger(output, length);
    }

    private void StrLen(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.StrLen(args[1]));

    // ------------------------------------------------------------------
    // Key commands
    // ------------------------------------------------------------------

    private void Expire(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        ExpireCore(args, milliseconds: false, output, effects);

    private void PExpire(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        ExpireCore(args, milliseconds: true, output, effects);

    private void ExpireCore(byte[][] args, bool milliseconds, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var amount = ParseLong(args[2]);
        var candidate = milliseconds
            ? _clock.GetUtcNow() + TimeSpan.FromMilliseconds(amount)
            : _clock.GetUtcNow() + TimeSpan.FromSeconds(amount);
        ConditionalDeadline(args, candidate, output, effects);
    }

    private void ExpireAt(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        ExpireAtCore(args, milliseconds: false, output, effects);

    private void PExpireAt(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        ExpireAtCore(args, milliseconds: true, output, effects);

    private void ExpireAtCore(byte[][] args, bool milliseconds, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var unix = ParseLong(args[2]);
        var candidate = milliseconds
            ? DateTimeOffset.FromUnixTimeMilliseconds(unix)
            : DateTimeOffset.FromUnixTimeSeconds(unix);
        ConditionalDeadline(args, candidate, output, effects);
    }

    /// <summary>
    /// The shared tail of the EXPIRE family: validate the optional
    /// NX / XX / GT / LT condition against the key's current deadline,
    /// then apply the candidate deadline (which may delete the key when
    /// it already lies in the past).
    /// </summary>
    private void ConditionalDeadline(byte[][] args, DateTimeOffset candidate, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var key = args[1];
        var condition = args.Length == 4 ? NameOf(args[3]) : null;
        if (condition is not (null or "NX" or "XX" or "GT" or "LT"))
            throw new RedisCommandException($"ERR Unsupported option {RespParser.Describe(args[3])}");

        if (!_store.TryGetExpiry(key, out var current))
        {
            RespWriter.WriteInteger(output, 0);
            return;
        }

        var blocked = condition switch
        {
            "NX" => current is not null,
            "XX" => current is null,
            // A key without an expiry is treated as having an infinite TTL:
            // GT can never beat it, LT always can.
            "GT" => current is null || candidate <= current.Value,
            "LT" => current is not null && candidate >= current.Value,
            _ => false
        };
        if (blocked)
        {
            RespWriter.WriteInteger(output, 0);
            return;
        }

        ApplyDeadline(key, candidate, output, effects);
    }

    private void ApplyDeadline(byte[] key, DateTimeOffset candidate, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var applied = _store.ExpireAt(key, candidate, out var removedKey);
        if (applied)
        {
            effects.Add(removedKey
                ? new[] { Ascii("DEL"), key }
                : new[] { Ascii("PEXPIREAT"), key, LongBytes(candidate.ToUnixTimeMilliseconds()) });
        }
        RespWriter.WriteInteger(output, applied ? 1 : 0);
    }

    private void Ttl(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var ms = _store.TtlMilliseconds(args[1]);
        // Redis reports whole seconds, rounded to the nearest second.
        RespWriter.WriteInteger(output, ms < 0 ? ms : (ms + 500) / 1000);
    }

    private void PTtl(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.TtlMilliseconds(args[1]));

    private void Persist(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var persisted = _store.Persist(args[1]);
        if (persisted)
            effects.Add(args);
        RespWriter.WriteInteger(output, persisted ? 1 : 0);
    }

    private void TypeCommand(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var name = _store.TypeOf(args[1]) switch
        {
            RedisType.String => "string",
            RedisType.List => "list",
            RedisType.Hash => "hash",
            RedisType.Set => "set",
            RedisType.ZSet => "zset",
            _ => "none"
        };
        RespWriter.WriteSimpleString(output, name);
    }

    private void Keys(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var keys = _store.Keys(args[1]);
        RespWriter.WriteArrayHeader(output, keys.Count);
        foreach (var key in keys)
            RespWriter.WriteBulkString(output, key);
    }
}
