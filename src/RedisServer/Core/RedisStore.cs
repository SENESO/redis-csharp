using System.Globalization;
using System.Text;

namespace RedisServer.Core;

public enum RedisType
{
    None,
    String,
    List,
    Hash,
    Set,
    ZSet
}

public enum SetCondition
{
    Always,
    IfAbsent,
    IfPresent
}

public enum SetOutcome
{
    Stored,
    NotStored
}

/// <summary>
/// The database: one dictionary of typed, optionally expiring entries.
///
/// Concurrency design — deliberately the Redis model. Redis executes
/// commands on a single thread, which is what makes multi-key commands,
/// transactions and snapshots trivially consistent there. This store takes
/// the equivalent route: <em>every</em> public operation runs under one
/// lock (<see cref="_gate"/>), so individual commands are atomic, and a
/// caller that needs several operations to happen as one unit (EXEC of a
/// transaction, a snapshot export, an AOF rewrite) holds the lock across
/// them with <see cref="EnterScope"/>. Connection handling, protocol
/// parsing and reply I/O stay fully concurrent — only data access
/// serialises, exactly like the real server. The lock is a plain
/// re-entrant <see cref="Monitor"/>, so nested use is safe.
///
/// Expiry is lazy (checked on every access, through
/// <see cref="TimeProvider"/> so tests drive it with a fake clock) plus an
/// active sweep driven by <see cref="RunMaintenance"/>.
/// </summary>
/// <summary>Eviction policies for the --maxmemory ceiling (the LFU family is not implemented).</summary>
public enum MaxMemoryPolicy
{
    NoEviction,
    AllKeysLru,
    AllKeysRandom,
    VolatileLru,
    VolatileRandom,
    VolatileTtl
}

public sealed partial class RedisStore
{
    private readonly Dictionary<byte[], RedisEntry> _data = new(ByteArrayComparer.Instance);
    private readonly object _gate = new();
    private readonly TimeProvider _clock;
    private readonly RedisStats _stats;

    // ------------------------------------------------------------------
    // Memory accounting (approximate, like Redis's own zmalloc-based
    // figure, but data-only: this server tracks no allocator overhead).
    // Every key carries a flat charge for its dictionary slot, entry
    // object and expiry slot; payload bytes are counted exactly and each
    // collection element carries a small flat charge. The counter is
    // maintained incrementally at every mutation and can be recounted
    // from scratch with RecalculateUsedMemory.
    // ------------------------------------------------------------------

    private const long KeyOverheadBytes = 64;
    private const long ElementOverheadBytes = 16;

    private long _usedMemory;
    private long _accessSeq;
    private readonly Random _evictRandom = new();

    private static long CollectionElementSize(byte[] element) => ElementOverheadBytes + element.Length;

    private static long ZSetElementSize(byte[] member) => ElementOverheadBytes + member.Length + sizeof(double);

    private static long EstimateEntrySize(byte[] key, RedisEntry entry)
    {
        long size = KeyOverheadBytes + key.Length;
        switch (entry.Type)
        {
            case RedisType.String:
                size += ((byte[])entry.Value).Length;
                break;
            case RedisType.List:
                foreach (var item in (LinkedList<byte[]>)entry.Value)
                    size += CollectionElementSize(item);
                break;
            case RedisType.Set:
                foreach (var member in (HashSet<byte[]>)entry.Value)
                    size += CollectionElementSize(member);
                break;
            case RedisType.Hash:
                foreach (var (field, value) in (Dictionary<byte[], byte[]>)entry.Value)
                    size += CollectionElementSize(field) + value.Length;
                break;
            case RedisType.ZSet:
                foreach (var member in ((ZSetValue)entry.Value).Scores.Keys)
                    size += ZSetElementSize(member);
                break;
        }
        return size;
    }

    /// <summary>The incrementally maintained memory estimate, in bytes.</summary>
    public long UsedMemory
    {
        get { lock (_gate) return _usedMemory; }
    }

    /// <summary>Full O(n) recount of the estimate — diagnostics and tests; normal operation uses the counter.</summary>
    public long RecalculateUsedMemory()
    {
        lock (_gate)
        {
            long total = 0;
            foreach (var (key, entry) in _data)
                total += EstimateEntrySize(key, entry);
            return total;
        }
    }

    /// <summary>
    /// Evicts one key chosen by approximate sampling, Redis-style: a
    /// reservoir of up to five candidates drawn from a bounded scan of
    /// the keyspace (at most 1024 entries per call), then the policy's
    /// victim — least recently used (access order is a store-wide
    /// sequence stamped on every live access), soonest to expire, or the
    /// first sample for the random policies. Volatile policies consider
    /// only keys carrying a TTL. Returns the evicted key, or null when
    /// no candidate exists. Runs entirely under the store gate; the
    /// caller (the command gate) appends the DEL effect.
    /// </summary>
    public byte[]? TryEvictOne(MaxMemoryPolicy policy)
    {
        lock (_gate)
        {
            if (_data.Count == 0)
                return null;
            var volatileOnly = policy is MaxMemoryPolicy.VolatileLru or MaxMemoryPolicy.VolatileRandom
                or MaxMemoryPolicy.VolatileTtl;
            var sample = new (byte[] Key, RedisEntry Entry)[5];
            var sampled = 0;
            var seen = 0;
            foreach (var (key, entry) in _data)
            {
                if (volatileOnly && entry.ExpiresAt is null)
                    continue;
                seen++;
                if (sampled < sample.Length)
                    sample[sampled++] = (key, entry);
                else
                {
                    var j = _evictRandom.Next(seen);
                    if (j < sample.Length)
                        sample[j] = (key, entry);
                }
                if (seen >= 1024 && sampled == sample.Length)
                    break;
            }
            if (sampled == 0)
                return null;

            var victim = sample[0];
            for (var i = 1; i < sampled; i++)
            {
                var better = policy switch
                {
                    MaxMemoryPolicy.AllKeysLru or MaxMemoryPolicy.VolatileLru =>
                        sample[i].Entry.LastAccess < victim.Entry.LastAccess,
                    MaxMemoryPolicy.VolatileTtl => sample[i].Entry.ExpiresAt < victim.Entry.ExpiresAt,
                    _ => false
                };
                if (better)
                    victim = sample[i];
            }

            _data.Remove(victim.Key);
            _usedMemory -= EstimateEntrySize(victim.Key, victim.Entry);
            _stats.AddEvictedKey();
            return victim.Key;
        }
    }

    public RedisStore(TimeProvider clock, RedisStats stats)
    {
        _clock = clock;
        _stats = stats;
    }

    private sealed class RedisEntry
    {
        public required RedisType Type;
        public required object Value;
        public DateTimeOffset? ExpiresAt;

        /// <summary>Store-wide access sequence stamp, the basis of approximate-LRU eviction.</summary>
        public long LastAccess;
    }

    /// <summary>Holds the store lock across a composite operation. Dispose to release.</summary>
    public IDisposable EnterScope()
    {
        Monitor.Enter(_gate);
        return new Scope(this);
    }

    private sealed class Scope(RedisStore store) : IDisposable
    {
        public void Dispose() => Monitor.Exit(store._gate);
    }

    // ------------------------------------------------------------------
    // Entry plumbing (callers already hold _gate)
    // ------------------------------------------------------------------

    private RedisEntry? GetLiveEntry(byte[] key)
    {
        if (!_data.TryGetValue(key, out var entry))
            return null;
        if (entry.ExpiresAt is { } expiry && expiry <= _clock.GetUtcNow())
        {
            _data.Remove(key);
            _usedMemory -= EstimateEntrySize(key, entry);
            _stats.AddExpiredKey();
            return null;
        }
        entry.LastAccess = ++_accessSeq;
        return entry;
    }

    private RedisEntry GetOrCreate(byte[] key, RedisType type, Func<object> factory)
    {
        var entry = GetLiveEntry(key);
        if (entry is null)
        {
            entry = new RedisEntry { Type = type, Value = factory() };
            entry.LastAccess = ++_accessSeq;
            _data[key] = entry;
            _usedMemory += EstimateEntrySize(key, entry);
            return entry;
        }
        if (entry.Type != type)
            throw new WrongTypeException();
        return entry;
    }

    private RedisEntry? GetTyped(byte[] key, RedisType type)
    {
        var entry = GetLiveEntry(key);
        if (entry is null)
            return null;
        if (entry.Type != type)
            throw new WrongTypeException();
        return entry;
    }

    private void RemoveIfEmpty(byte[] key, RedisEntry entry)
    {
        // Redis invariant: a collection key disappears when it becomes empty.
        var empty = entry.Type switch
        {
            RedisType.List => ((LinkedList<byte[]>)entry.Value).Count == 0,
            RedisType.Set => ((HashSet<byte[]>)entry.Value).Count == 0,
            RedisType.Hash => ((Dictionary<byte[], byte[]>)entry.Value).Count == 0,
            RedisType.ZSet => ((ZSetValue)entry.Value).Scores.Count == 0,
            _ => false
        };
        if (empty)
        {
            _data.Remove(key);
            _usedMemory -= KeyOverheadBytes + key.Length;
        }
    }

    private int PurgeExpiredLocked()
    {
        var now = _clock.GetUtcNow();
        List<byte[]>? dead = null;
        foreach (var (key, entry) in _data)
        {
            if (entry.ExpiresAt is { } expiry && expiry <= now)
                (dead ??= new List<byte[]>()).Add(key);
        }
        if (dead is null)
            return 0;
        foreach (var key in dead)
        {
            _usedMemory -= EstimateEntrySize(key, _data[key]);
            _data.Remove(key);
            _stats.AddExpiredKey();
        }
        return dead.Count;
    }

    /// <summary>Active expiry sweep: reclaims entries whose deadline passed without being touched.</summary>
    public void RunMaintenance()
    {
        lock (_gate)
            PurgeExpiredLocked();
    }

    // ------------------------------------------------------------------
    // Strings
    // ------------------------------------------------------------------

    public SetOutcome Set(byte[] key, byte[] value, SetCondition condition, TimeSpan? ttl, bool keepTtl)
    {
        lock (_gate)
        {
            var existing = GetLiveEntry(key);
            if (condition == SetCondition.IfAbsent && existing is not null)
                return SetOutcome.NotStored;
            if (condition == SetCondition.IfPresent && existing is null)
                return SetOutcome.NotStored;

            var expiresAt = keepTtl && existing is not null
                ? existing.ExpiresAt
                : ttl is { } span
                    ? _clock.GetUtcNow() + span
                    : null;

            if (existing is not null)
                _usedMemory -= EstimateEntrySize(key, existing);
            var entry = new RedisEntry { Type = RedisType.String, Value = value, ExpiresAt = expiresAt };
            entry.LastAccess = ++_accessSeq;
            _data[key] = entry;
            _usedMemory += EstimateEntrySize(key, entry);
            return SetOutcome.Stored;
        }
    }

    public byte[]? Get(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.String);
            return entry is null ? null : (byte[])entry.Value;
        }
    }

    public long Append(byte[] key, byte[] suffix)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, RedisType.String, () => Array.Empty<byte>());
            var current = (byte[])entry.Value;
            var combined = new byte[current.Length + suffix.Length];
            current.CopyTo(combined, 0);
            suffix.CopyTo(combined, current.Length);
            entry.Value = combined;
            _usedMemory += suffix.Length;
            return combined.Length;
        }
    }

    public long StrLen(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.String);
            return entry is null ? 0 : ((byte[])entry.Value).Length;
        }
    }

    /// <summary>Shared INCR/DECR/INCRBY core: strict integer parsing, checked arithmetic, TTL preserved.</summary>
    public long ApplyDelta(byte[] key, long delta)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, RedisType.String, () => "0"u8.ToArray());
            if (!TryParseStrictLong((byte[])entry.Value, out var current))
                throw ValueErrorException.NotAnInteger();

            long next;
            try
            {
                next = checked(current + delta);
            }
            catch (OverflowException)
            {
                throw ValueErrorException.Overflow();
            }

            var bytes = Encoding.ASCII.GetBytes(next.ToString(CultureInfo.InvariantCulture));
            _usedMemory += bytes.Length - ((byte[])entry.Value).Length;
            entry.Value = bytes;
            return next;
        }
    }

    /// <summary>
    /// Redis's integer grammar: an optional minus, then digits with no
    /// leading zero unless the value is exactly "0" — no spaces, no plus.
    /// </summary>
    internal static bool TryParseStrictLong(ReadOnlySpan<byte> text, out long value)
    {
        value = 0;
        if (text.IsEmpty)
            return false;
        var i = 0;
        var negative = false;
        if (text[0] == (byte)'-')
        {
            negative = true;
            i = 1;
            if (text.Length == 1)
                return false;
        }
        if (text[i] == (byte)'0' && text.Length != i + 1)
            return false; // leading zero

        long acc = 0;
        for (; i < text.Length; i++)
        {
            var c = text[i];
            if (c < (byte)'0' || c > (byte)'9')
                return false;
            try
            {
                acc = checked(acc * 10 - (c - (byte)'0')); // accumulate negative to reach long.MinValue
            }
            catch (OverflowException)
            {
                return false;
            }
        }

        if (!negative)
        {
            if (acc == long.MinValue)
                return false;
            acc = -acc;
        }
        value = acc;
        return true;
    }

    internal static bool TryParseDouble(ReadOnlySpan<byte> text, out double value)
    {
        value = 0;
        var s = Encoding.ASCII.GetString(text).Trim();
        switch (s.ToLowerInvariant())
        {
            case "inf":
            case "+inf":
            case "infinity":
            case "+infinity":
                value = double.PositiveInfinity;
                return true;
            case "-inf":
            case "-infinity":
                value = double.NegativeInfinity;
                return true;
        }
        if (!double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return false;
        if (double.IsInfinity(value))
            return false; // overflow such as 1e309: only the literal inf spellings above are valid
        return !double.IsNaN(value); // Redis rejects NaN scores outright
    }

    internal static string FormatDouble(double value)
    {
        if (double.IsPositiveInfinity(value))
            return "inf";
        if (double.IsNegativeInfinity(value))
            return "-inf";
        if (value == Math.Truncate(value) && Math.Abs(value) < 1e17)
            return ((long)value).ToString(CultureInfo.InvariantCulture);
        return value.ToString("G17", CultureInfo.InvariantCulture);
    }

    // ------------------------------------------------------------------
    // Keys
    // ------------------------------------------------------------------

    public long Delete(IReadOnlyList<byte[]> keys)
    {
        lock (_gate)
        {
            long removed = 0;
            foreach (var key in keys)
            {
                if (GetLiveEntry(key) is { } entry)
                {
                    _usedMemory -= EstimateEntrySize(key, entry);
                    _data.Remove(key);
                    removed++;
                }
            }
            return removed;
        }
    }

    public long Exists(IReadOnlyList<byte[]> keys)
    {
        lock (_gate)
        {
            long count = 0;
            foreach (var key in keys)
            {
                if (GetLiveEntry(key) is not null)
                    count++;
            }
            return count;
        }
    }

    public RedisType TypeOf(byte[] key)
    {
        lock (_gate)
            return GetLiveEntry(key)?.Type ?? RedisType.None;
    }

    /// <summary>
    /// Sets an absolute expiry. A deadline in the past deletes the key, as in
    /// Redis (reported through <paramref name="removedKey"/>). False when
    /// the key does not exist.
    /// </summary>
    public bool ExpireAt(byte[] key, DateTimeOffset when, out bool removedKey)
    {
        removedKey = false;
        lock (_gate)
        {
            var entry = GetLiveEntry(key);
            if (entry is null)
                return false;
            if (when <= _clock.GetUtcNow())
            {
                _usedMemory -= EstimateEntrySize(key, entry);
                _data.Remove(key);
                removedKey = true;
                return true;
            }
            entry.ExpiresAt = when;
            return true;
        }
    }

    /// <summary>Reads a key's current deadline without touching it. False when the key does not exist.</summary>
    public bool TryGetExpiry(byte[] key, out DateTimeOffset? expiry)
    {
        lock (_gate)
        {
            var entry = GetLiveEntry(key);
            expiry = entry?.ExpiresAt;
            return entry is not null;
        }
    }

    /// <summary>Remaining TTL in milliseconds: -2 when the key is missing, -1 when it has no expiry.</summary>
    public long TtlMilliseconds(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetLiveEntry(key);
            if (entry is null)
                return -2;
            if (entry.ExpiresAt is not { } expiry)
                return -1;
            var remaining = expiry - _clock.GetUtcNow();
            return remaining <= TimeSpan.Zero ? 0 : (long)remaining.TotalMilliseconds;
        }
    }

    public bool Persist(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetLiveEntry(key);
            if (entry is null || entry.ExpiresAt is null)
                return false;
            entry.ExpiresAt = null;
            return true;
        }
    }

    public long Count
    {
        get
        {
            lock (_gate)
            {
                PurgeExpiredLocked();
                return _data.Count;
            }
        }
    }

    public long ExpiresCount
    {
        get
        {
            lock (_gate)
            {
                PurgeExpiredLocked();
                long count = 0;
                foreach (var entry in _data.Values)
                {
                    if (entry.ExpiresAt is not null)
                        count++;
                }
                return count;
            }
        }
    }

    public IReadOnlyList<byte[]> Keys(byte[] pattern)
    {
        lock (_gate)
        {
            PurgeExpiredLocked();
            var result = new List<byte[]>();
            foreach (var key in _data.Keys)
            {
                if (Glob.IsMatch(pattern, key))
                    result.Add(key);
            }
            result.Sort(ByteArrayComparer.Instance);
            return result;
        }
    }

    /// <summary>All live keys, sorted — the deterministic basis for SCAN snapshots, snapshot files and AOF rewrites.</summary>
    public IReadOnlyList<byte[]> AllKeysSorted()
    {
        lock (_gate)
        {
            PurgeExpiredLocked();
            var keys = new List<byte[]>(_data.Keys);
            keys.Sort(ByteArrayComparer.Instance);
            return keys;
        }
    }

    public void FlushAll()
    {
        lock (_gate)
        {
            _data.Clear();
            _usedMemory = 0;
        }
    }

    // ------------------------------------------------------------------
    // Lists (LinkedList: O(1) at both ends, which is all LPUSH/RPOP need;
    // LINDEX/LRANGE walk, as does Redis's quicklist for random access)
    // ------------------------------------------------------------------

    public long LeftPush(byte[] key, IReadOnlyList<byte[]> values)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, RedisType.List, () => new LinkedList<byte[]>());
            var list = (LinkedList<byte[]>)entry.Value;
            foreach (var value in values)
            {
                list.AddFirst(value);
                _usedMemory += CollectionElementSize(value);
            }
            return list.Count;
        }
    }

    public long RightPush(byte[] key, IReadOnlyList<byte[]> values)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, RedisType.List, () => new LinkedList<byte[]>());
            var list = (LinkedList<byte[]>)entry.Value;
            foreach (var value in values)
            {
                list.AddLast(value);
                _usedMemory += CollectionElementSize(value);
            }
            return list.Count;
        }
    }

    public byte[]? LeftPop(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.List);
            if (entry is null)
                return null;
            var list = (LinkedList<byte[]>)entry.Value;
            var value = list.First!.Value;
            list.RemoveFirst();
            _usedMemory -= CollectionElementSize(value);
            RemoveIfEmpty(key, entry);
            return value;
        }
    }

    public byte[]? RightPop(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.List);
            if (entry is null)
                return null;
            var list = (LinkedList<byte[]>)entry.Value;
            var value = list.Last!.Value;
            list.RemoveLast();
            _usedMemory -= CollectionElementSize(value);
            RemoveIfEmpty(key, entry);
            return value;
        }
    }

    public IReadOnlyList<byte[]> LeftPop(byte[] key, long count)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.List);
            if (entry is null)
                return Array.Empty<byte[]>();
            var list = (LinkedList<byte[]>)entry.Value;
            var take = (int)Math.Min(count, list.Count);
            var result = new List<byte[]>(take);
            for (var i = 0; i < take; i++)
            {
                result.Add(list.First!.Value);
                _usedMemory -= CollectionElementSize(list.First!.Value);
                list.RemoveFirst();
            }
            RemoveIfEmpty(key, entry);
            return result;
        }
    }

    public IReadOnlyList<byte[]> RightPop(byte[] key, long count)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.List);
            if (entry is null)
                return Array.Empty<byte[]>();
            var list = (LinkedList<byte[]>)entry.Value;
            var take = (int)Math.Min(count, list.Count);
            var result = new List<byte[]>(take);
            for (var i = 0; i < take; i++)
            {
                result.Add(list.Last!.Value);
                _usedMemory -= CollectionElementSize(list.Last!.Value);
                list.RemoveLast();
            }
            RemoveIfEmpty(key, entry);
            return result;
        }
    }

    public long ListLength(byte[] key)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.List);
            return entry is null ? 0 : ((LinkedList<byte[]>)entry.Value).Count;
        }
    }

    public byte[]? ListIndex(byte[] key, long index)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.List);
            if (entry is null)
                return null;
            var list = (LinkedList<byte[]>)entry.Value;
            var i = index < 0 ? index + list.Count : index;
            if (i < 0 || i >= list.Count)
                return null;
            var node = list.First;
            for (long n = 0; n < i; n++)
                node = node!.Next;
            return node!.Value;
        }
    }

    public IReadOnlyList<byte[]> ListRange(byte[] key, long start, long stop)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, RedisType.List);
            if (entry is null)
                return Array.Empty<byte[]>();
            var list = (LinkedList<byte[]>)entry.Value;
            if (!NormaliseRange(list.Count, ref start, ref stop))
                return Array.Empty<byte[]>();
            var result = new List<byte[]>((int)(stop - start + 1));
            var node = list.First;
            for (long i = 0; i <= stop; i++)
            {
                if (i >= start)
                    result.Add(node!.Value);
                node = node!.Next;
            }
            return result;
        }
    }

    /// <summary>Redis range math: negative indexes count from the end; returns false for an empty window.</summary>
    internal static bool NormaliseRange(long length, ref long start, ref long stop)
    {
        if (start < 0)
            start += length;
        if (stop < 0)
            stop += length;
        if (start < 0)
            start = 0;
        if (stop >= length)
            stop = length - 1;
        return start <= stop && start < length;
    }
}
