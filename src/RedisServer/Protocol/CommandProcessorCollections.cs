using System.Buffers;
using System.Globalization;
using RedisServer.Core;

namespace RedisServer.Protocol;

/// <summary>
/// The collection commands, SCAN, and pub/sub handlers for
/// <see cref="CommandProcessor"/> (split out of the main file by family,
/// the way the store is split).
///
/// SCAN deviation, stated plainly: real Redis walks its hash table
/// incrementally, so a scan sees a live, mutating keyspace. This server
/// snapshots the matching keys (sorted) when a scan starts and pages
/// through the snapshot under an opaque cursor — a full scan of a quiet
/// keyspace returns every key exactly once; keys created mid-scan are not
/// returned, keys deleted mid-scan may still be. Cursors expire after a
/// minute of inactivity.
/// </summary>
public sealed partial class CommandProcessor
{
    private sealed class ScanState
    {
        public required List<byte[]> Keys;
        public required DateTimeOffset CreatedAt;
    }

    private readonly Dictionary<long, ScanState> _scanStates = new();
    private long _nextScanId;

    private void Scan(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        if (!long.TryParse(RespParser.Describe(args[1]), NumberStyles.Integer, CultureInfo.InvariantCulture, out var cursorInput)
            || cursorInput < 0)
        {
            RespWriter.WriteError(output, "ERR invalid cursor");
            return;
        }

        byte[]? match = null;
        long count = 10;
        string? typeFilter = null;
        for (var i = 2; i < args.Length; i++)
        {
            var option = NameOf(args[i]);
            switch (option)
            {
                case "MATCH":
                    if (i + 1 >= args.Length)
                        throw SyntaxError();
                    match = args[++i];
                    break;
                case "COUNT":
                    if (i + 1 >= args.Length)
                        throw SyntaxError();
                    count = ParseLong(args[++i]);
                    if (count < 1)
                        throw SyntaxError();
                    break;
                case "TYPE":
                    if (i + 1 >= args.Length)
                        throw SyntaxError();
                    typeFilter = NameOf(args[++i]);
                    break;
                default:
                    throw SyntaxError();
            }
        }

        List<byte[]> keys;
        long offset;
        long stateId = 0;
        if (cursorInput == 0)
        {
            keys = new List<byte[]>(match is null ? _store.AllKeysSorted(ctx.Db) : _store.Keys(match, ctx.Db));
            if (typeFilter is not null)
            {
                var wanted = typeFilter switch
                {
                    "STRING" => RedisType.String,
                    "LIST" => RedisType.List,
                    "HASH" => RedisType.Hash,
                    "SET" => RedisType.Set,
                    "ZSET" => RedisType.ZSet,
                    _ => RedisType.None
                };
                keys = keys.Where(k => _store.TypeOf(k, ctx.Db) == wanted).ToList();
            }
            offset = 0;
        }
        else
        {
            // Our cursor encodes "<stateId>:<offset>" packed into one number
            // space: ids are issued sequentially, so the incoming value is
            // split as id * 1e9 + offset. 1e9 page steps per scan is ample.
            stateId = cursorInput / 1_000_000_000L;
            offset = cursorInput % 1_000_000_000L;
            if (stateId == 0 || !_scanStates.TryGetValue(stateId, out var state))
            {
                RespWriter.WriteError(output, "ERR invalid cursor");
                return;
            }
            keys = state.Keys;
        }

        var page = new List<byte[]>();
        var i2 = offset;
        for (; i2 < keys.Count && page.Count < count; i2++)
            page.Add(keys[(int)i2]);

        string nextCursor;
        if (i2 >= keys.Count)
        {
            if (stateId != 0)
                _scanStates.Remove(stateId);
            nextCursor = "0";
        }
        else
        {
            if (stateId == 0)
            {
                stateId = ++_nextScanId;
                _scanStates[stateId] = new ScanState { Keys = keys, CreatedAt = _clock.GetUtcNow() };
                PurgeOldScanStates();
            }
            nextCursor = (stateId * 1_000_000_000L + i2).ToString(CultureInfo.InvariantCulture);
        }

        RespWriter.WriteArrayHeader(output, 2);
        RespWriter.WriteBulkString(output, nextCursor);
        RespWriter.WriteArrayHeader(output, page.Count);
        foreach (var key in page)
            RespWriter.WriteBulkString(output, key);
    }

    private void PurgeOldScanStates()
    {
        var cutoff = _clock.GetUtcNow() - TimeSpan.FromMinutes(1);
        List<long>? stale = null;
        foreach (var (id, state) in _scanStates)
        {
            if (state.CreatedAt < cutoff)
                (stale ??= new List<long>()).Add(id);
        }
        if (stale is not null)
        {
            foreach (var id in stale)
                _scanStates.Remove(id);
        }
    }

    // ------------------------------------------------------------------
    // Lists
    // ------------------------------------------------------------------

    private void LeftPush(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var length = _store.LeftPush(args[1], Tail(args, 2), ctx.Db);
        effects.Add(args);
        RespWriter.WriteInteger(output, length);
    }

    private void RightPush(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var length = _store.RightPush(args[1], Tail(args, 2), ctx.Db);
        effects.Add(args);
        RespWriter.WriteInteger(output, length);
    }

    private void LeftPop(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        PopCore(args, left: true, ctx, output, effects, effectDbs);

    private void RightPop(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        PopCore(args, left: false, ctx, output, effects, effectDbs);

    private void PopCore(byte[][] args, bool left, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var key = args[1];
        if (args.Length == 2)
        {
            var value = left ? _store.LeftPop(key, ctx.Db) : _store.RightPop(key, ctx.Db);
            if (value is not null)
                effects.Add(args);
            RespWriter.WriteBulkStringOrNull(output, value, ctx.ProtocolVersion);
            return;
        }

        if (!RedisStore.TryParseStrictLong(args[2], out var count) || count < 0)
            throw new ValueErrorException("ERR value is out of range, must be positive");
        if (_store.TypeOf(key, ctx.Db) == RedisType.None)
        {
            RespWriter.WriteNullArrayValue(output, ctx.ProtocolVersion);
            return;
        }
        var values = left ? _store.LeftPop(key, count, ctx.Db) : _store.RightPop(key, count, ctx.Db);
        if (values.Count > 0)
            effects.Add(args);
        RespWriter.WriteArrayHeader(output, values.Count);
        foreach (var value in values)
            RespWriter.WriteBulkString(output, value);
    }

    private void ListLength(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        RespWriter.WriteInteger(output, _store.ListLength(args[1], ctx.Db));

    private void ListRange(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var values = _store.ListRange(args[1], ParseLong(args[2]), ParseLong(args[3]), ctx.Db);
        RespWriter.WriteArrayHeader(output, values.Count);
        foreach (var value in values)
            RespWriter.WriteBulkString(output, value);
    }

    private void ListIndex(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        RespWriter.WriteBulkStringOrNull(output, _store.ListIndex(args[1], ParseLong(args[2]), ctx.Db), ctx.ProtocolVersion);

    // ------------------------------------------------------------------
    // Sets
    // ------------------------------------------------------------------

    private void SetAdd(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var added = _store.SetAdd(args[1], Tail(args, 2), ctx.Db);
        if (added > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, added);
    }

    private void SetRemove(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var removed = _store.SetRemove(args[1], Tail(args, 2), ctx.Db);
        if (removed > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, removed);
    }

    private void SetMembers(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var members = _store.SetMembers(args[1], ctx.Db);
        // A RESP3 set (~) instead of an array; RESP2 keeps the array.
        if (ctx.ProtocolVersion >= 3)
            RespWriter.WriteSetHeader(output, members.Count);
        else
            RespWriter.WriteArrayHeader(output, members.Count);
        foreach (var member in members)
            RespWriter.WriteBulkString(output, member);
    }

    private void SetIsMember(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        RespWriter.WriteInteger(output, _store.SetIsMember(args[1], args[2], ctx.Db) ? 1 : 0);

    private void SetCard(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        RespWriter.WriteInteger(output, _store.SetCard(args[1], ctx.Db));

    // ------------------------------------------------------------------
    // Hashes
    // ------------------------------------------------------------------

    private void HashSet(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        if ((args.Length - 2) % 2 != 0)
        {
            RespWriter.WriteError(output, ArityError("HSET"));
            return;
        }
        var pairs = new List<KeyValuePair<byte[], byte[]>>();
        for (var i = 2; i < args.Length; i += 2)
            pairs.Add(new KeyValuePair<byte[], byte[]>(args[i], args[i + 1]));
        var added = _store.HashSet(args[1], pairs, ctx.Db);
        effects.Add(args);
        RespWriter.WriteInteger(output, added);
    }

    private void HashGet(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        RespWriter.WriteBulkStringOrNull(output, _store.HashGet(args[1], args[2], ctx.Db), ctx.ProtocolVersion);

    private void HashDelete(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var removed = _store.HashDelete(args[1], Tail(args, 2), ctx.Db);
        if (removed > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, removed);
    }

    private void HashGetAll(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var pairs = _store.HashGetAll(args[1], ctx.Db);
        // A RESP3 map (%) of field→value; RESP2 keeps the flat array.
        if (ctx.ProtocolVersion >= 3)
            RespWriter.WriteMapHeader(output, pairs.Count);
        else
            RespWriter.WriteArrayHeader(output, pairs.Count * 2);
        foreach (var (field, value) in pairs)
        {
            RespWriter.WriteBulkString(output, field);
            RespWriter.WriteBulkString(output, value);
        }
    }

    private void HashLength(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        RespWriter.WriteInteger(output, _store.HashLength(args[1], ctx.Db));

    // ------------------------------------------------------------------
    // Sorted sets
    // ------------------------------------------------------------------

    private void ZAdd(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var nx = false;
        var xx = false;
        var gt = false;
        var lt = false;
        var countChanged = false;
        var increment = false;
        var i = 2;
        for (; i < args.Length; i++)
        {
            var option = NameOf(args[i]);
            if (option is not ("NX" or "XX" or "GT" or "LT" or "CH" or "INCR"))
                break;
            switch (option)
            {
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "GT": gt = true; break;
                case "LT": lt = true; break;
                case "CH": countChanged = true; break;
                case "INCR": increment = true; break;
            }
        }

        if (nx && xx)
            throw new RedisCommandException("ERR XX and NX options at the same time are not compatible");
        if ((gt && lt) || (nx && (gt || lt)))
            throw new RedisCommandException("ERR GT, LT, and/or NX options at the same time are not compatible");

        var mode = nx ? ZAddMode.OnlyNew
            : gt ? ZAddMode.GreaterThan
            : lt ? ZAddMode.LessThan
            : xx ? ZAddMode.OnlyExisting
            : ZAddMode.None;

        var pairArgs = args.Length - i;
        if (pairArgs <= 0 || pairArgs % 2 != 0 || (increment && pairArgs != 2))
        {
            RespWriter.WriteError(output, ArityError("ZADD"));
            return;
        }

        var items = new List<(double, byte[])>();
        for (var j = i; j < args.Length; j += 2)
        {
            if (!RedisStore.TryParseDouble(args[j], out var score))
                throw ValueErrorException.NotAFloat();
            items.Add((score, args[j + 1]));
        }

        var result = _store.ZAdd(args[1], mode, countChanged, increment, items, requireExisting: xx, db: ctx.Db);
        effects.Add(args);
        if (increment)
        {
            // A RESP3 double (or _) for the new score; RESP2 keeps the
            // bulk-string form.
            if (ctx.ProtocolVersion >= 3)
                RespWriter.WriteDoubleOrNull(output, result.IncrementScore, ctx.ProtocolVersion);
            else if (result.IncrementScore is { } newScore)
                RespWriter.WriteBulkString(output, RedisStore.FormatDouble(newScore));
            else
                RespWriter.WriteNullBulkString(output);
        }
        else
        {
            RespWriter.WriteInteger(output, result.Added);
        }
    }

    private void ZIncrBy(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        if (!RedisStore.TryParseDouble(args[2], out var delta))
            throw ValueErrorException.NotAFloat();
        var result = _store.ZAdd(args[1], ZAddMode.None, false, increment: true,
            new[] { (delta, args[3]) }, db: ctx.Db);
        effects.Add(args);
        if (ctx.ProtocolVersion >= 3)
            RespWriter.WriteDoubleOrNull(output, result.IncrementScore, ctx.ProtocolVersion);
        else if (result.IncrementScore is { } newScore)
            RespWriter.WriteBulkString(output, RedisStore.FormatDouble(newScore));
        else
            RespWriter.WriteNullBulkString(output);
    }

    private void ZScore(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var score = _store.ZScore(args[1], args[2], ctx.Db);
        if (ctx.ProtocolVersion >= 3)
        {
            RespWriter.WriteDoubleOrNull(output, score, ctx.ProtocolVersion);
            return;
        }
        if (score is { } value)
            RespWriter.WriteBulkString(output, RedisStore.FormatDouble(value));
        else
            RespWriter.WriteNullBulkString(output);
    }

    private void ZRange(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var withScores = false;
        if (args.Length == 5)
        {
            if (NameOf(args[4]) != "WITHSCORES")
                throw SyntaxError();
            withScores = true;
        }
        var range = _store.ZRange(args[1], ParseLong(args[2]), ParseLong(args[3]), ctx.Db);
        if (withScores && ctx.ProtocolVersion >= 3)
        {
            // RESP3 nests each (member, score) pair in its own array,
            // with the score as a double; RESP2 flattens them into bulk
            // strings.
            RespWriter.WriteArrayHeader(output, range.Count);
            foreach (var (member, score) in range)
            {
                RespWriter.WriteArrayHeader(output, 2);
                RespWriter.WriteBulkString(output, member);
                RespWriter.WriteDouble(output, score);
            }
            return;
        }
        RespWriter.WriteArrayHeader(output, withScores ? range.Count * 2 : range.Count);
        foreach (var (member, score) in range)
        {
            RespWriter.WriteBulkString(output, member);
            if (withScores)
                RespWriter.WriteBulkString(output, RedisStore.FormatDouble(score));
        }
    }

    private void ZRemove(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var removed = _store.ZRemove(args[1], Tail(args, 2), ctx.Db);
        if (removed > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, removed);
    }

    private void ZCard(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs) =>
        RespWriter.WriteInteger(output, _store.ZCard(args[1], ctx.Db));

    // ------------------------------------------------------------------
    // Pub/sub
    // ------------------------------------------------------------------

    private void Subscribe(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        foreach (var channel in Tail(args, 1))
        {
            CheckChannelPermission(ctx, channel);
            _hub.Subscribe(ctx, channel);
            WriteSubscriptionReply(output, "subscribe", channel, ctx.SubscriptionCount, ctx);
        }
    }

    private void SubscribePattern(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        foreach (var pattern in Tail(args, 1))
        {
            CheckChannelPermission(ctx, pattern);
            _hub.SubscribePattern(ctx, pattern);
            WriteSubscriptionReply(output, "psubscribe", pattern, ctx.SubscriptionCount, ctx);
        }
    }

    /// <summary>
    /// ACL channel patterns: a user with channel rules may only
    /// subscribe to matching channels. The message mirrors the key
    /// pattern refusal.
    /// </summary>
    private static void CheckChannelPermission(ClientContext ctx, byte[] channel)
    {
        if (ctx.User is { } user && !user.AllowsChannel(channel))
            throw new RedisCommandException(
                "NOPERM this user has no permissions to access one of the channels used as arguments");
    }

    private void Unsubscribe(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var targets = args.Length > 1 ? Tail(args, 1) : ctx.Channels.ToList();
        if (targets.Count == 0)
        {
            WriteSubscriptionReply(output, "unsubscribe", null, ctx.SubscriptionCount, ctx);
            return;
        }
        foreach (var channel in targets)
        {
            _hub.Unsubscribe(ctx, channel);
            WriteSubscriptionReply(output, "unsubscribe", channel, ctx.SubscriptionCount, ctx);
        }
    }

    private void UnsubscribePattern(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var targets = args.Length > 1 ? Tail(args, 1) : ctx.Patterns.ToList();
        if (targets.Count == 0)
        {
            WriteSubscriptionReply(output, "punsubscribe", null, ctx.SubscriptionCount, ctx);
            return;
        }
        foreach (var pattern in targets)
        {
            _hub.UnsubscribePattern(ctx, pattern);
            WriteSubscriptionReply(output, "punsubscribe", pattern, ctx.SubscriptionCount, ctx);
        }
    }

    private void WriteSubscriptionReply(IBufferWriter<byte> output, string kind, byte[]? target, long count, ClientContext ctx)
    {
        // A push (>) in RESP3, an array in RESP2.
        RespWriter.WritePushOrArrayHeader(output, 3, ctx.ProtocolVersion);
        RespWriter.WriteBulkString(output, kind);
        RespWriter.WriteBulkStringOrNull(output, target);
        RespWriter.WriteInteger(output, count);
    }

    private void Publish(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var receivers = _hub.Publish(args[1], args[2]);
        _stats.AddPublishedMessage();
        RespWriter.WriteInteger(output, receivers);
    }
}
