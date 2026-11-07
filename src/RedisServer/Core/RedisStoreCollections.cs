namespace RedisServer.Core;

public enum ZAddMode
{
    None,
    OnlyNew,      // NX
    OnlyExisting, // XX
    GreaterThan,  // GT
    LessThan      // LT
}

/// <summary>Outcome of a ZADD: members added, members whose score changed, and the INCR result when requested.</summary>
public readonly record struct ZAddResult(long Added, long Changed, double? IncrementScore);

/// <summary>One key's full state, deep-copied — the unit the snapshot file and the AOF rewrite work with.</summary>
public sealed record ExportedEntry(
    byte[] Key,
    RedisType Type,
    object Value,
    DateTimeOffset? ExpiresAt);

public sealed partial class RedisStore
{
    internal sealed class ZSetValue
    {
        public Dictionary<byte[], double> Scores { get; } = new(ByteArrayComparer.Instance);
        public SortedSet<ZSetNode> Sorted { get; } = new(ZSetNodeComparer.Instance);
    }

    internal readonly record struct ZSetNode(double Score, byte[] Member);

    private sealed class ZSetNodeComparer : IComparer<ZSetNode>
    {
        public static readonly ZSetNodeComparer Instance = new();

        public int Compare(ZSetNode x, ZSetNode y)
        {
            var byScore = x.Score.CompareTo(y.Score);
            return byScore != 0 ? byScore : ByteArrayComparer.Instance.Compare(x.Member, y.Member);
        }
    }

    // ------------------------------------------------------------------
    // Sets
    // ------------------------------------------------------------------

    public long SetAdd(byte[] key, IReadOnlyList<byte[]> members)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, RedisType.Set, () => new HashSet<byte[]>(ByteArrayComparer.Instance));
            var set = (HashSet<byte[]>)entry.Value;
            long added = 0;
            foreach (var member in members)
            {
                if (set.Add(member))
                {
                    _usedMemory += CollectionElementSize(member);
                    added++;
                }
            }
            return added;
        }
    }

    public long SetRemove(byte[] key, IReadOnlyList<byte[]> members)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Set);
            if (entry is null)
                return 0;
            var set = (HashSet<byte[]>)entry.Value;
            long removed = 0;
            foreach (var member in members)
            {
                if (set.Remove(member))
                {
                    _usedMemory -= CollectionElementSize(member);
                    removed++;
                }
            }
            RemoveIfEmpty(key, entry);
            return removed;
        }
    }

    public bool SetIsMember(byte[] key, byte[] member)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Set);
            return entry is not null && ((HashSet<byte[]>)entry.Value).Contains(member);
        }
    }

    public long SetCard(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Set);
            return entry is null ? 0 : ((HashSet<byte[]>)entry.Value).Count;
        }
    }

    public IReadOnlyList<byte[]> SetMembers(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Set);
            if (entry is null)
                return Array.Empty<byte[]>();
            var members = new List<byte[]>(((HashSet<byte[]>)entry.Value));
            members.Sort(ByteArrayComparer.Instance); // Redis order is unspecified; sorted keeps replies testable
            return members;
        }
    }

    // ------------------------------------------------------------------
    // Hashes
    // ------------------------------------------------------------------

    public long HashSet(byte[] key, IReadOnlyList<KeyValuePair<byte[], byte[]>> pairs)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, RedisType.Hash, () => new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance));
            var hash = (Dictionary<byte[], byte[]>)entry.Value;
            long added = 0;
            foreach (var (field, value) in pairs)
            {
                if (hash.TryGetValue(field, out var old))
                {
                    _usedMemory += value.Length - old.Length;
                }
                else
                {
                    _usedMemory += CollectionElementSize(field) + value.Length;
                    added++;
                }
                hash[field] = value;
            }
            return added;
        }
    }

    public byte[]? HashGet(byte[] key, byte[] field)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Hash);
            if (entry is null)
                return null;
            return ((Dictionary<byte[], byte[]>)entry.Value).GetValueOrDefault(field);
        }
    }

    public long HashDelete(byte[] key, IReadOnlyList<byte[]> fields)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Hash);
            if (entry is null)
                return 0;
            var hash = (Dictionary<byte[], byte[]>)entry.Value;
            long removed = 0;
            foreach (var field in fields)
            {
                if (hash.TryGetValue(field, out var old) && hash.Remove(field))
                {
                    _usedMemory -= CollectionElementSize(field) + old.Length;
                    removed++;
                }
            }
            RemoveIfEmpty(key, entry);
            return removed;
        }
    }

    public IReadOnlyList<KeyValuePair<byte[], byte[]>> HashGetAll(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Hash);
            if (entry is null)
                return Array.Empty<KeyValuePair<byte[], byte[]>>();
            var pairs = new List<KeyValuePair<byte[], byte[]>>(((Dictionary<byte[], byte[]>)entry.Value));
            pairs.Sort((a, b) => ByteArrayComparer.Instance.Compare(a.Key, b.Key));
            return pairs;
        }
    }

    public long HashLength(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.Hash);
            return entry is null ? 0 : ((Dictionary<byte[], byte[]>)entry.Value).Count;
        }
    }

    // ------------------------------------------------------------------
    // Sorted sets
    // ------------------------------------------------------------------

    public ZAddResult ZAdd(byte[] key, ZAddMode mode, bool countChanged, bool increment,
        IReadOnlyList<(double Score, byte[] Member)> items, bool requireExisting = false)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, RedisType.ZSet, () => new ZSetValue());
            var zset = (ZSetValue)entry.Value;

            if (increment)
            {
                var (delta, member) = items[0];
                var exists = zset.Scores.TryGetValue(member, out var current);
                if (!exists && (mode == ZAddMode.OnlyExisting || requireExisting))
                {
                    // A declined INCR on a missing key must not leave the
                    // just-created empty entry behind (real Redis creates
                    // nothing); the non-increment path already cleans up.
                    RemoveIfEmpty(key, entry);
                    return new ZAddResult(0, 0, null);
                }
                if (exists && mode == ZAddMode.OnlyNew)
                    return new ZAddResult(0, 0, null);

                var next = (exists ? current : 0) + delta;
                if (double.IsNaN(next))
                    throw new ValueErrorException("ERR resulting score is not a number (NaN)");
                if (exists && mode == ZAddMode.GreaterThan && next <= current)
                    return new ZAddResult(0, 0, null);
                if (exists && mode == ZAddMode.LessThan && next >= current)
                    return new ZAddResult(0, 0, null);

                ApplyZScore(zset, member, next);
                if (!exists)
                    _usedMemory += ZSetElementSize(member);
                return new ZAddResult(exists ? 0 : 1, exists ? 1 : 0, next);
            }

            long added = 0, changed = 0;
            foreach (var (score, member) in items)
            {
                if (double.IsNaN(score))
                    throw ValueErrorException.NotAFloat();
                var exists = zset.Scores.TryGetValue(member, out var current);
                if (!exists)
                {
                    if (mode == ZAddMode.OnlyExisting || requireExisting)
                        continue;
                    ApplyZScore(zset, member, score);
                    _usedMemory += ZSetElementSize(member);
                    added++;
                    continue;
                }
                if (mode == ZAddMode.OnlyNew)
                    continue;
                if (mode == ZAddMode.GreaterThan && score <= current)
                    continue;
                if (mode == ZAddMode.LessThan && score >= current)
                    continue;
                if (score != current)
                {
                    ApplyZScore(zset, member, score);
                    changed++;
                }
            }
            RemoveIfEmpty(key, entry);
            return new ZAddResult(countChanged ? added + changed : added, changed, null);
        }
    }

    private static void ApplyZScore(ZSetValue zset, byte[] member, double score)
    {
        if (zset.Scores.TryGetValue(member, out var old))
            zset.Sorted.Remove(new ZSetNode(old, member));
        zset.Scores[member] = score;
        zset.Sorted.Add(new ZSetNode(score, member));
    }

    public double? ZScore(byte[] key, byte[] member)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.ZSet);
            if (entry is null)
                return null;
            return ((ZSetValue)entry.Value).Scores.TryGetValue(member, out var score) ? score : null;
        }
    }

    public IReadOnlyList<(byte[] Member, double Score)> ZRange(byte[] key, long start, long stop)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.ZSet);
            if (entry is null)
                return Array.Empty<(byte[], double)>();
            var zset = (ZSetValue)entry.Value;
            if (!NormaliseRange(zset.Scores.Count, ref start, ref stop))
                return Array.Empty<(byte[], double)>();
            var result = new List<(byte[], double)>((int)(stop - start + 1));
            long i = 0;
            foreach (var node in zset.Sorted)
            {
                if (i > stop)
                    break;
                if (i >= start)
                    result.Add((node.Member, node.Score));
                i++;
            }
            return result;
        }
    }

    public long ZRemove(byte[] key, IReadOnlyList<byte[]> members)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.ZSet);
            if (entry is null)
                return 0;
            var zset = (ZSetValue)entry.Value;
            long removed = 0;
            foreach (var member in members)
            {
                if (zset.Scores.TryGetValue(member, out var score))
                {
                    zset.Scores.Remove(member);
                    zset.Sorted.Remove(new ZSetNode(score, member));
                    _usedMemory -= ZSetElementSize(member);
                    removed++;
                }
            }
            RemoveIfEmpty(key, entry);
            return removed;
        }
    }

    public long ZCard(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.ZSet);
            return entry is null ? 0 : ((ZSetValue)entry.Value).Scores.Count;
        }
    }

    // ------------------------------------------------------------------
    // Whole-state export / import (snapshot file, AOF rewrite, loading)
    // ------------------------------------------------------------------

    /// <summary>
    /// Deep-copies the whole keyspace in deterministic (sorted key) order.
    /// Value shapes: string → byte[]; list → List&lt;byte[]&gt; head-to-tail;
    /// set → List&lt;byte[]&gt; sorted; hash → field/value pairs sorted by
    /// field; zset → (member, score) pairs in rank order.
    /// </summary>
    public IReadOnlyList<ExportedEntry> ExportState()
    {
        lock (_gate)
        {
            PurgeExpiredLocked();
            var keys = new List<byte[]>(_data.Keys);
            keys.Sort(ByteArrayComparer.Instance);
            var result = new List<ExportedEntry>(keys.Count);
            foreach (var key in keys)
            {
                var entry = _data[key];
                object value = entry.Type switch
                {
                    RedisType.String => (byte[])((byte[])entry.Value).Clone(),
                    RedisType.List => new List<byte[]>((LinkedList<byte[]>)entry.Value),
                    RedisType.Set => SortedCopy((HashSet<byte[]>)entry.Value),
                    RedisType.Hash => SortedPairs((Dictionary<byte[], byte[]>)entry.Value),
                    RedisType.ZSet => ((ZSetValue)entry.Value).Sorted
                        .Select(n => new KeyValuePair<byte[], double>(n.Member, n.Score)).ToList(),
                    _ => throw new InvalidOperationException($"Unknown entry type {entry.Type}")
                };
                result.Add(new ExportedEntry(key, entry.Type, value, entry.ExpiresAt));
            }
            return result;
        }
    }

    private static List<byte[]> SortedCopy(HashSet<byte[]> set)
    {
        var list = new List<byte[]>(set);
        list.Sort(ByteArrayComparer.Instance);
        return list;
    }

    private static List<KeyValuePair<byte[], byte[]>> SortedPairs(Dictionary<byte[], byte[]> hash)
    {
        var list = new List<KeyValuePair<byte[], byte[]>>(hash);
        list.Sort((a, b) => ByteArrayComparer.Instance.Compare(a.Key, b.Key));
        return list;
    }

    /// <summary>Loads one exported entry back (snapshot load path). Entries already past their deadline are dropped.</summary>
    public void ImportEntry(ExportedEntry exported)
    {
        lock (_gate)
        {
            if (exported.ExpiresAt is { } expiry && expiry <= _clock.GetUtcNow())
                return;
            object value = exported.Type switch
            {
                RedisType.String => (byte[])((byte[])exported.Value).Clone(),
                RedisType.List => new LinkedList<byte[]>((List<byte[]>)exported.Value),
                RedisType.Set => new HashSet<byte[]>((List<byte[]>)exported.Value, ByteArrayComparer.Instance),
                RedisType.Hash => new Dictionary<byte[], byte[]>((List<KeyValuePair<byte[], byte[]>>)exported.Value, ByteArrayComparer.Instance),
                RedisType.ZSet => BuildZSet((List<KeyValuePair<byte[], double>>)exported.Value),
                _ => throw new InvalidOperationException($"Unknown entry type {exported.Type}")
            };
            if (_data.TryGetValue(exported.Key, out var replaced))
                _usedMemory -= EstimateEntrySize(exported.Key, replaced);
            var entry = new RedisEntry { Type = exported.Type, Value = value, ExpiresAt = exported.ExpiresAt };
            _data[exported.Key] = entry;
            _usedMemory += EstimateEntrySize(exported.Key, entry);
        }
    }

    private static ZSetValue BuildZSet(List<KeyValuePair<byte[], double>> pairs)
    {
        var zset = new ZSetValue();
        foreach (var (member, score) in pairs)
            ApplyZScore(zset, member, score);
        return zset;
    }
}
