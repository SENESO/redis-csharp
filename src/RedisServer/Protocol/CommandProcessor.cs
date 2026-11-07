using System.Buffers;
using System.Globalization;
using System.Text;
using RedisServer.Core;
using RedisServer.Persistence;

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
    private readonly int _port;
    private readonly DateTimeOffset _startedAt;
    private readonly object _gate = new();
    private int _backgroundSaveRunning;

    private readonly Dictionary<string, CommandSpec> _specs;

    private delegate void CommandHandler(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects);

    private sealed record CommandSpec(int MinArgs, int MaxArgs, bool AllowedInPubSub, CommandHandler Handler);

    public CommandProcessor(RedisStore store, RedisStats stats, TimeProvider clock,
        PersistenceManager persistence, PubSubHub hub, int port)
    {
        _store = store;
        _stats = stats;
        _clock = clock;
        _persistence = persistence;
        _hub = hub;
        _port = port;
        _startedAt = clock.GetUtcNow();
        _specs = BuildSpecs();
    }

    private Dictionary<string, CommandSpec> BuildSpecs()
    {
        var specs = new Dictionary<string, CommandSpec>(StringComparer.Ordinal);
        void Add(string name, int min, int max, CommandHandler handler, bool pubsub = false) =>
            specs[name] = new CommandSpec(min, max, pubsub, handler);

        // Connection / server
        Add("PING", 1, 2, Ping, pubsub: true);
        Add("ECHO", 2, 2, Echo);
        Add("QUIT", 1, 1, Quit, pubsub: true);
        Add("DBSIZE", 1, 1, DbSize);
        Add("INFO", 1, 2, Info);
        Add("FLUSHDB", 1, 2, FlushDb);
        Add("FLUSHALL", 1, 2, FlushDb);
        Add("SAVE", 1, 1, Save);
        Add("BGSAVE", 1, 1, BgSave);
        Add("BGREWRITEAOF", 1, 1, BgRewriteAof);

        // Strings
        Add("SET", 3, -1, Set);
        Add("GET", 2, 2, Get);
        Add("DEL", 2, -1, Del);
        Add("EXISTS", 2, -1, Exists);
        Add("INCR", 2, 2, Incr);
        Add("DECR", 2, 2, Decr);
        Add("INCRBY", 3, 3, IncrBy);
        Add("DECRBY", 3, 3, DecrBy);
        Add("APPEND", 3, 3, Append);
        Add("STRLEN", 2, 2, StrLen);

        // Keys
        Add("EXPIRE", 3, 4, Expire);
        Add("PEXPIRE", 3, 4, PExpire);
        Add("EXPIREAT", 3, 4, ExpireAt);
        Add("PEXPIREAT", 3, 4, PExpireAt);
        Add("TTL", 2, 2, Ttl);
        Add("PTTL", 2, 2, PTtl);
        Add("PERSIST", 2, 2, Persist);
        Add("TYPE", 2, 2, TypeCommand);
        Add("KEYS", 2, 2, Keys);
        Add("SCAN", 2, -1, Scan);

        // Collections (CommandProcessorCollections.cs)
        Add("LPUSH", 3, -1, LeftPush);
        Add("RPUSH", 3, -1, RightPush);
        Add("LPOP", 2, 3, LeftPop);
        Add("RPOP", 2, 3, RightPop);
        Add("LLEN", 2, 2, ListLength);
        Add("LRANGE", 4, 4, ListRange);
        Add("LINDEX", 3, 3, ListIndex);
        Add("SADD", 3, -1, SetAdd);
        Add("SREM", 3, -1, SetRemove);
        Add("SMEMBERS", 2, 2, SetMembers);
        Add("SISMEMBER", 3, 3, SetIsMember);
        Add("SCARD", 2, 2, SetCard);
        Add("HSET", 4, -1, HashSet);
        Add("HGET", 3, 3, HashGet);
        Add("HDEL", 3, -1, HashDelete);
        Add("HGETALL", 2, 2, HashGetAll);
        Add("HLEN", 2, 2, HashLength);
        Add("ZADD", 4, -1, ZAdd);
        Add("ZSCORE", 3, 3, ZScore);
        Add("ZRANGE", 4, 5, ZRange);
        Add("ZREM", 3, -1, ZRemove);
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
    public void Execute(byte[][] args, ClientContext ctx, IBufferWriter<byte> output)
    {
        lock (_gate)
        {
            var effects = new List<byte[][]>();
            ExecuteLocked(args, ctx, output, effects, inTransactionBody: false);
            if (effects.Count > 0)
                _persistence.AppendEffects(effects);
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
        var ctx = new ClientContext();
        var sink = new ArrayBufferWriter<byte>();
        lock (_gate)
        {
            var before = sink.WrittenCount;
            ExecuteLocked(args, ctx, sink, new List<byte[][]>(), inTransactionBody: false);
            return !(sink.WrittenCount > before && sink.WrittenSpan[0] == (byte)'-');
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

        var name = NameOf(args[0]);

        // Transaction control flow.
        if (!inTransactionBody)
        {
            if (name == "MULTI")
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
            if (name == "EXEC")
            {
                ExecuteTransaction(ctx, output, effects);
                return;
            }
            if (name == "DISCARD")
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
                QueueCommand(args, name, ctx, output);
                return;
            }
        }

        if (!_specs.TryGetValue(name, out var spec))
        {
            RespWriter.WriteError(output, $"ERR unknown command '{RespParser.Describe(args[0])}', with args beginning with: {DescribeArgs(args)}");
            return;
        }

        if (ctx.IsSubscribed && !spec.AllowedInPubSub)
        {
            RespWriter.WriteError(output,
                $"ERR Can't execute '{name.ToLowerInvariant()}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context");
            return;
        }

        if (args.Length < spec.MinArgs || (spec.MaxArgs >= 0 && args.Length > spec.MaxArgs))
        {
            RespWriter.WriteError(output, ArityError(name));
            return;
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

    private void QueueCommand(byte[][] args, string name, ClientContext ctx, IBufferWriter<byte> output)
    {
        if (!_specs.TryGetValue(name, out var spec))
        {
            ctx.TransactionAborted = true;
            RespWriter.WriteError(output, $"ERR unknown command '{RespParser.Describe(args[0])}', with args beginning with: {DescribeArgs(args)}");
            return;
        }
        if (name is "SUBSCRIBE" or "PSUBSCRIBE" or "UNSUBSCRIBE" or "PUNSUBSCRIBE" or "PUBLISH")
        {
            ctx.TransactionAborted = true;
            RespWriter.WriteError(output, $"ERR {name} is not allowed inside a transaction");
            return;
        }
        if (args.Length < spec.MinArgs || (spec.MaxArgs >= 0 && args.Length > spec.MaxArgs))
        {
            ctx.TransactionAborted = true;
            RespWriter.WriteError(output, ArityError(name));
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

    private void Ping(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if (ctx.IsSubscribed)
        {
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

    private void Quit(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        RespWriter.WriteSimpleString(output, "OK");
        ctx.CloseRequested = true;
    }

    private void DbSize(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.Count);

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
            builder.Append("uptime_in_seconds:").Append(uptime).Append("\r\n");
        });
        Section("Clients", () =>
        {
            builder.Append("connected_clients:").Append(_stats.CurrConnections).Append("\r\n");
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
            builder.Append("pubsub_channels:").Append(_hub.ChannelCount).Append("\r\n");
            builder.Append("pubsub_patterns:").Append(_hub.PatternCount).Append("\r\n");
        });
        Section("Keyspace", () =>
        {
            builder.Append("db0:keys=").Append(_store.Count)
                .Append(",expires=").Append(_store.ExpiresCount)
                .Append(",avg_ttl=0\r\n");
        });

        if (builder.Length == 0)
        {
            // An unknown section is not an error in Redis: the reply is
            // an empty bulk string.
            RespWriter.WriteBulkString(output, "");
            return;
        }
        RespWriter.WriteBulkString(output, builder.ToString());
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
            RespWriter.WriteBulkStringOrNull(output, oldValue);
        else if (outcome == SetOutcome.Stored)
            RespWriter.WriteSimpleString(output, "OK");
        else
            RespWriter.WriteNullBulkString(output);
    }

    private void Get(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var value = _store.Get(args[1]);
        if (value is null)
            _stats.AddKeyspaceMiss();
        else
            _stats.AddKeyspaceHit();
        RespWriter.WriteBulkStringOrNull(output, value);
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
