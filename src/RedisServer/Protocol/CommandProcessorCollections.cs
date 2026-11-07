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

    private void Scan(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
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
                    if (count <= 0)
                        throw new RedisCommandException("ERR value is not an integer or out of range");
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
            keys = new List<byte[]>(match is null ? _store.AllKeysSorted() : _store.Keys(match));
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
                keys = keys.Where(k => _store.TypeOf(k) == wanted).ToList();
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

    private void LeftPush(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var length = _store.LeftPush(args[1], Tail(args, 2));
        effects.Add(args);
        RespWriter.WriteInteger(output, length);
    }

    private void RightPush(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var length = _store.RightPush(args[1], Tail(args, 2));
        effects.Add(args);
        RespWriter.WriteInteger(output, length);
    }

    private void LeftPop(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        PopCore(args, left: true, output, effects);

    private void RightPop(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        PopCore(args, left: false, output, effects);

    private void PopCore(byte[][] args, bool left, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var key = args[1];
        if (args.Length == 2)
        {
            var value = left ? _store.LeftPop(key) : _store.RightPop(key);
            if (value is not null)
                effects.Add(args);
            RespWriter.WriteBulkStringOrNull(output, value);
            return;
        }

        var count = ParseLong(args[2]);
        if (count < 0)
            throw new ValueErrorException("ERR value is out of range, must be positive");
        if (_store.TypeOf(key) == RedisType.None)
        {
            RespWriter.WriteNullArray(output);
            return;
        }
        var values = left ? _store.LeftPop(key, count) : _store.RightPop(key, count);
        if (values.Count > 0)
            effects.Add(args);
        RespWriter.WriteArrayHeader(output, values.Count);
        foreach (var value in values)
            RespWriter.WriteBulkString(output, value);
    }

    private void ListLength(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.ListLength(args[1]));

    private void ListRange(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var values = _store.ListRange(args[1], ParseLong(args[2]), ParseLong(args[3]));
        RespWriter.WriteArrayHeader(output, values.Count);
        foreach (var value in values)
            RespWriter.WriteBulkString(output, value);
    }

    private void ListIndex(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteBulkStringOrNull(output, _store.ListIndex(args[1], ParseLong(args[2])));

    // ------------------------------------------------------------------
    // Sets
    // ------------------------------------------------------------------

    private void SetAdd(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var added = _store.SetAdd(args[1], Tail(args, 2));
        if (added > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, added);
    }

    private void SetRemove(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var removed = _store.SetRemove(args[1], Tail(args, 2));
        if (removed > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, removed);
    }

    private void SetMembers(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var members = _store.SetMembers(args[1]);
        RespWriter.WriteArrayHeader(output, members.Count);
        foreach (var member in members)
            RespWriter.WriteBulkString(output, member);
    }

    private void SetIsMember(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.SetIsMember(args[1], args[2]) ? 1 : 0);

    private void SetCard(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.SetCard(args[1]));

    // ------------------------------------------------------------------
    // Hashes
    // ------------------------------------------------------------------

    private void HashSet(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        if ((args.Length - 2) % 2 != 0)
        {
            RespWriter.WriteError(output, ArityError("HSET"));
            return;
        }
        var pairs = new List<KeyValuePair<byte[], byte[]>>();
        for (var i = 2; i < args.Length; i += 2)
            pairs.Add(new KeyValuePair<byte[], byte[]>(args[i], args[i + 1]));
        var added = _store.HashSet(args[1], pairs);
        effects.Add(args);
        RespWriter.WriteInteger(output, added);
    }

    private void HashGet(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteBulkStringOrNull(output, _store.HashGet(args[1], args[2]));

    private void HashDelete(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var removed = _store.HashDelete(args[1], Tail(args, 2));
        if (removed > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, removed);
    }

    private void HashGetAll(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var pairs = _store.HashGetAll(args[1]);
        RespWriter.WriteArrayHeader(output, pairs.Count * 2);
        foreach (var (field, value) in pairs)
        {
            RespWriter.WriteBulkString(output, field);
            RespWriter.WriteBulkString(output, value);
        }
    }

    private void HashLength(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.HashLength(args[1]));

    // ------------------------------------------------------------------
    // Sorted sets
    // ------------------------------------------------------------------

    private void ZAdd(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var mode = ZAddMode.None;
        var countChanged = false;
        var increment = false;
        var i = 2;
        for (; i < args.Length; i++)
        {
            var option = NameOf(args[i]);
            var isOption = option is "NX" or "XX" or "GT" or "LT" or "CH" or "INCR";
            if (!isOption)
                break;
            switch (option)
            {
                case "NX":
                    if (mode is ZAddMode.OnlyExisting or ZAddMode.GreaterThan or ZAddMode.LessThan)
                        throw new RedisCommandException("ERR GT, LT, and/or NX options are mutually exclusive");
                    mode = ZAddMode.OnlyNew;
                    break;
                case "XX":
                    if (mode == ZAddMode.OnlyNew)
                        throw new RedisCommandException("ERR GT, LT, and/or NX options are mutually exclusive");
                    mode = ZAddMode.OnlyExisting;
                    break;
                case "GT":
                    if (mode == ZAddMode.OnlyNew)
                        throw new RedisCommandException("ERR GT, LT, and/or NX options are mutually exclusive");
                    mode = ZAddMode.GreaterThan;
                    break;
                case "LT":
                    if (mode == ZAddMode.OnlyNew)
                        throw new RedisCommandException("ERR GT, LT, and/or NX options are mutually exclusive");
                    mode = ZAddMode.LessThan;
                    break;
                case "CH":
                    countChanged = true;
                    break;
                case "INCR":
                    increment = true;
                    break;
            }
        }

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

        var result = _store.ZAdd(args[1], mode, countChanged, increment, items);
        effects.Add(args);
        if (increment)
        {
            if (result.IncrementScore is { } newScore)
                RespWriter.WriteBulkString(output, RedisStore.FormatDouble(newScore));
            else
                RespWriter.WriteNullBulkString(output);
        }
        else
        {
            RespWriter.WriteInteger(output, result.Added);
        }
    }

    private void ZScore(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var score = _store.ZScore(args[1], args[2]);
        if (score is { } value)
            RespWriter.WriteBulkString(output, RedisStore.FormatDouble(value));
        else
            RespWriter.WriteNullBulkString(output);
    }

    private void ZRange(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var withScores = false;
        if (args.Length == 5)
        {
            if (NameOf(args[4]) != "WITHSCORES")
                throw SyntaxError();
            withScores = true;
        }
        var range = _store.ZRange(args[1], ParseLong(args[2]), ParseLong(args[3]));
        RespWriter.WriteArrayHeader(output, withScores ? range.Count * 2 : range.Count);
        foreach (var (member, score) in range)
        {
            RespWriter.WriteBulkString(output, member);
            if (withScores)
                RespWriter.WriteBulkString(output, RedisStore.FormatDouble(score));
        }
    }

    private void ZRemove(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var removed = _store.ZRemove(args[1], Tail(args, 2));
        if (removed > 0)
            effects.Add(args);
        RespWriter.WriteInteger(output, removed);
    }

    private void ZCard(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects) =>
        RespWriter.WriteInteger(output, _store.ZCard(args[1]));

    // ------------------------------------------------------------------
    // Pub/sub
    // ------------------------------------------------------------------

    private void Subscribe(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        foreach (var channel in Tail(args, 1))
        {
            _hub.Subscribe(ctx, channel);
            WriteSubscriptionReply(output, "subscribe", channel, ctx.SubscriptionCount);
        }
    }

    private void SubscribePattern(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        foreach (var pattern in Tail(args, 1))
        {
            _hub.SubscribePattern(ctx, pattern);
            WriteSubscriptionReply(output, "psubscribe", pattern, ctx.SubscriptionCount);
        }
    }

    private void Unsubscribe(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var targets = args.Length > 1 ? Tail(args, 1) : ctx.Channels.ToList();
        if (targets.Count == 0)
        {
            WriteSubscriptionReply(output, "unsubscribe", null, ctx.SubscriptionCount);
            return;
        }
        foreach (var channel in targets)
        {
            _hub.Unsubscribe(ctx, channel);
            WriteSubscriptionReply(output, "unsubscribe", channel, ctx.SubscriptionCount);
        }
    }

    private void UnsubscribePattern(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var targets = args.Length > 1 ? Tail(args, 1) : ctx.Patterns.ToList();
        if (targets.Count == 0)
        {
            WriteSubscriptionReply(output, "punsubscribe", null, ctx.SubscriptionCount);
            return;
        }
        foreach (var pattern in targets)
        {
            _hub.UnsubscribePattern(ctx, pattern);
            WriteSubscriptionReply(output, "punsubscribe", pattern, ctx.SubscriptionCount);
        }
    }

    private static void WriteSubscriptionReply(IBufferWriter<byte> output, string kind, byte[]? target, long count)
    {
        RespWriter.WriteArrayHeader(output, 3);
        RespWriter.WriteBulkString(output, kind);
        RespWriter.WriteBulkStringOrNull(output, target);
        RespWriter.WriteInteger(output, count);
    }

    private void Publish(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects)
    {
        var receivers = _hub.Publish(args[1], args[2]);
        _stats.AddPublishedMessage();
        RespWriter.WriteInteger(output, receivers);
    }
}
