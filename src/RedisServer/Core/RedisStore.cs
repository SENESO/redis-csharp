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
/// <summary>Eviction policies for the --maxmemory ceiling.</summary>
public enum MaxMemoryPolicy
{
    NoEviction,
    AllKeysLru,
    AllKeysRandom,
    AllKeysLfu,
    VolatileLru,
    VolatileRandom,
    VolatileTtl,
    VolatileLfu
}

public sealed partial class RedisStore
{
    /// <summary>The number of logical databases (SELECT's range), as in Redis's default build.</summary>
    public const int DatabaseCount = 16;

    private readonly Dictionary<byte[], RedisEntry>[] _dbs = CreateDatabases();

    private static Dictionary<byte[], RedisEntry>[] CreateDatabases()
    {
        var dbs = new Dictionary<byte[], RedisEntry>[DatabaseCount];
        for (var i = 0; i < dbs.Length; i++)
            dbs[i] = new Dictionary<byte[], RedisEntry>(ByteArrayComparer.Instance);
        return dbs;
    }

    /// <summary>The keyspace table of one logical database. Callers hold the store gate.</summary>
    private Dictionary<byte[], RedisEntry> Table(int db) => _dbs[db];
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
            foreach (var table in _dbs)
                foreach (var (key, entry) in table)
                    total += EstimateEntrySize(key, entry);
            return total;
        }
    }

    /// <summary>
    /// Evicts one key chosen by approximate sampling, Redis-style: a
    /// reservoir of up to five candidates drawn from a bounded scan of
    /// the keyspace (at most 1024 entries per call, across all logical
    /// databases — the memory ceiling is global), then the policy's
    /// victim — least recently used (access order is a store-wide
    /// sequence stamped on every live access), least frequently used,
    /// soonest to expire, or the first sample for the random policies.
    /// Volatile policies consider only keys carrying a TTL. Returns the
    /// evicted key and its database, or null when no candidate exists.
    /// Runs entirely under the store gate; the caller (the command gate)
    /// appends the DEL effect in the victim's database.
    /// </summary>
    public (int Db, byte[] Key)? TryEvictOne(MaxMemoryPolicy policy)
    {
        lock (_gate)
        {
            var volatileOnly = policy is MaxMemoryPolicy.VolatileLru or MaxMemoryPolicy.VolatileRandom
                or MaxMemoryPolicy.VolatileTtl or MaxMemoryPolicy.VolatileLfu;
            var nowMinute = policy is MaxMemoryPolicy.AllKeysLfu or MaxMemoryPolicy.VolatileLfu
                ? CurrentMinute()
                : 0;
            var sample = new (int Db, byte[] Key, RedisEntry Entry)[5];
            var sampled = 0;
            var seen = 0;
            for (var db = 0; db < _dbs.Length; db++)
            {
                foreach (var (key, entry) in _dbs[db])
                {
                    if (volatileOnly && entry.ExpiresAt is null)
                        continue;
                    seen++;
                    if (sampled < sample.Length)
                        sample[sampled++] = (db, key, entry);
                    else
                    {
                        var j = _evictRandom.Next(seen);
                        if (j < sample.Length)
                            sample[j] = (db, key, entry);
                    }
                    if (seen >= 1024 && sampled == sample.Length)
                        break;
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
                    MaxMemoryPolicy.AllKeysLfu or MaxMemoryPolicy.VolatileLfu =>
                        DecayedCounter(sample[i].Entry, nowMinute) < DecayedCounter(victim.Entry, nowMinute),
                    MaxMemoryPolicy.VolatileTtl => sample[i].Entry.ExpiresAt < victim.Entry.ExpiresAt,
                    _ => false
                };
                if (better)
                    victim = sample[i];
            }

            Table(victim.Db).Remove(victim.Key);
            _usedMemory -= EstimateEntrySize(victim.Key, victim.Entry);
            _stats.AddEvictedKey();
            return (victim.Db, victim.Key);
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

        /// <summary>
        /// LFU access counter (0–255), Redis's probabilistic scheme: new
        /// entries start at 5, each access raises it with probability
        /// 1/((counter-5)*logFactor + 1), and it decays linearly with
        /// idle time. See <see cref="RedisStore.TouchLfu"/>.
        /// </summary>
        public int FreqCounter;

        /// <summary>The minute (on the store clock) of the last LFU decay step.</summary>
        public long FreqMinute;
    }

    // ------------------------------------------------------------------
    // LFU configuration (Redis's --lfu-log-factor / --lfu-decay-time).
    // ------------------------------------------------------------------

    /// <summary>Logarithmic factor of the LFU counter increment probability. Default 10, as in Redis.</summary>
    public int LfuLogFactor { get; set; } = 10;

    /// <summary>Minutes of idleness per unit of LFU counter decay. Default 1, as in Redis.</summary>
    public int LfuDecayTimeMinutes { get; set; } = 1;

    private const int LfuInitialCounter = 5;

    private long CurrentMinute() => _clock.GetUtcNow().ToUnixTimeSeconds() / 60;

    /// <summary>
    /// The decayed LFU counter for an entry as of <paramref name="nowMinute"/>:
    /// one counter point per full decay period of idleness, floored at 0.
    /// Pure — used both when an access mutates the counter and when
    /// eviction compares candidates.
    /// </summary>
    private int DecayedCounter(RedisEntry entry, long nowMinute)
    {
        var idlePeriods = (nowMinute - entry.FreqMinute) / Math.Max(1, LfuDecayTimeMinutes);
        if (idlePeriods <= 0)
            return entry.FreqCounter;
        var decayed = entry.FreqCounter - idlePeriods;
        return decayed > 0 ? (int)decayed : 0;
    }

    /// <summary>
    /// Records an access for LFU purposes: decay the counter for the
    /// idle time since the last touch, then raise it probabilistically —
    /// the increment probability is 1/((counter-5)*logFactor + 1) based
    /// on the decayed value, so hot keys approach Redis's "very likely
    /// to stay" regime while cold keys cost almost nothing. A log
    /// factor of 0 makes the probability 1 (Redis's own tests use it).
    /// </summary>
    private void TouchLfu(RedisEntry entry)
    {
        var now = CurrentMinute();
        var counter = DecayedCounter(entry, now);
        entry.FreqMinute = now;
        var baseValue = counter > LfuInitialCounter ? counter - LfuInitialCounter : 0;
        var probability = 1.0 / (baseValue * (double)LfuLogFactor + 1.0);
        if (counter < 255 && _evictRandom.NextDouble() < probability)
            counter++;
        entry.FreqCounter = counter;
    }

    /// <summary>
    /// The current (decayed) LFU counter of a live key, or null when
    /// the key does not exist. Diagnostics and tests — reads the entry
    /// without recording an access (unlike the normal command path).
    /// </summary>
    public int? GetFreqCounter(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            if (!Table(db).TryGetValue(key, out var entry))
                return null;
            if (entry.ExpiresAt is { } expiry && expiry <= _clock.GetUtcNow())
                return null;
            return DecayedCounter(entry, CurrentMinute());
        }
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

    private RedisEntry? GetLiveEntry(byte[] key, int db)
    {
        if (!Table(db).TryGetValue(key, out var entry))
            return null;
        if (entry.ExpiresAt is { } expiry && expiry <= _clock.GetUtcNow())
        {
            Table(db).Remove(key);
            _usedMemory -= EstimateEntrySize(key, entry);
            _stats.AddExpiredKey();
            return null;
        }
        entry.LastAccess = ++_accessSeq;
        TouchLfu(entry);
        return entry;
    }

    private RedisEntry GetOrCreate(byte[] key, int db, RedisType type, Func<object> factory)
    {
        var entry = GetLiveEntry(key, db);
        if (entry is null)
        {
            entry = new RedisEntry { Type = type, Value = factory() };
            entry.LastAccess = ++_accessSeq;
            entry.FreqCounter = LfuInitialCounter;
            entry.FreqMinute = CurrentMinute();
            Table(db)[key] = entry;
            _usedMemory += EstimateEntrySize(key, entry);
            return entry;
        }
        if (entry.Type != type)
            throw new WrongTypeException();
        return entry;
    }

    private RedisEntry? GetTyped(byte[] key, int db, RedisType type)
    {
        var entry = GetLiveEntry(key, db);
        if (entry is null)
            return null;
        if (entry.Type != type)
            throw new WrongTypeException();
        return entry;
    }

    private void RemoveIfEmpty(byte[] key, int db, RedisEntry entry)
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
            Table(db).Remove(key);
            _usedMemory -= KeyOverheadBytes + key.Length;
        }
    }

    private int PurgeExpiredLocked()
    {
        var now = _clock.GetUtcNow();
        var purged = 0;
        foreach (var table in _dbs)
        {
            List<byte[]>? dead = null;
            foreach (var (key, entry) in table)
            {
                if (entry.ExpiresAt is { } expiry && expiry <= now)
                    (dead ??= new List<byte[]>()).Add(key);
            }
            if (dead is null)
                continue;
            foreach (var key in dead)
            {
                _usedMemory -= EstimateEntrySize(key, table[key]);
                table.Remove(key);
                _stats.AddExpiredKey();
            }
            purged += dead.Count;
        }
        return purged;
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

    public SetOutcome Set(byte[] key, byte[] value, SetCondition condition, TimeSpan? ttl, bool keepTtl, int db = 0)
    {
        lock (_gate)
        {
            var existing = GetLiveEntry(key, db);
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
            entry.FreqCounter = LfuInitialCounter;
            entry.FreqMinute = CurrentMinute();
            Table(db)[key] = entry;
            _usedMemory += EstimateEntrySize(key, entry);
            return SetOutcome.Stored;
        }
    }

    public byte[]? Get(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.String);
            return entry is null ? null : (byte[])entry.Value;
        }
    }

    public long Append(byte[] key, byte[] suffix, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, db, RedisType.String, () => Array.Empty<byte>());
            var current = (byte[])entry.Value;
            var combined = new byte[current.Length + suffix.Length];
            current.CopyTo(combined, 0);
            suffix.CopyTo(combined, current.Length);
            entry.Value = combined;
            _usedMemory += suffix.Length;
            return combined.Length;
        }
    }

    public long StrLen(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.String);
            return entry is null ? 0 : ((byte[])entry.Value).Length;
        }
    }

    /// <summary>Shared INCR/DECR/INCRBY core: strict integer parsing, checked arithmetic, TTL preserved.</summary>
    public long ApplyDelta(byte[] key, long delta, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, db, RedisType.String, () => "0"u8.ToArray());
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

    public long Delete(IReadOnlyList<byte[]> keys, int db = 0)
    {
        lock (_gate)
        {
            long removed = 0;
            foreach (var key in keys)
            {
                if (GetLiveEntry(key, db) is { } entry)
                {
                    _usedMemory -= EstimateEntrySize(key, entry);
                    Table(db).Remove(key);
                    removed++;
                }
            }
            return removed;
        }
    }

    public long Exists(IReadOnlyList<byte[]> keys, int db = 0)
    {
        lock (_gate)
        {
            long count = 0;
            foreach (var key in keys)
            {
                if (GetLiveEntry(key, db) is not null)
                    count++;
            }
            return count;
        }
    }

    public RedisType TypeOf(byte[] key, int db = 0)
    {
        lock (_gate)
            return GetLiveEntry(key, db)?.Type ?? RedisType.None;
    }

    /// <summary>
    /// Sets an absolute expiry. A deadline in the past deletes the key, as in
    /// Redis (reported through <paramref name="removedKey"/>). False when
    /// the key does not exist.
    /// </summary>
    public bool ExpireAt(byte[] key, DateTimeOffset when, out bool removedKey, int db = 0)
    {
        removedKey = false;
        lock (_gate)
        {
            var entry = GetLiveEntry(key, db);
            if (entry is null)
                return false;
            if (when <= _clock.GetUtcNow())
            {
                _usedMemory -= EstimateEntrySize(key, entry);
                Table(db).Remove(key);
                removedKey = true;
                return true;
            }
            entry.ExpiresAt = when;
            return true;
        }
    }

    /// <summary>Reads a key's current deadline without touching it. False when the key does not exist.</summary>
    public bool TryGetExpiry(byte[] key, out DateTimeOffset? expiry, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetLiveEntry(key, db);
            expiry = entry?.ExpiresAt;
            return entry is not null;
        }
    }

    /// <summary>Remaining TTL in milliseconds: -2 when the key is missing, -1 when it has no expiry.</summary>
    public long TtlMilliseconds(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetLiveEntry(key, db);
            if (entry is null)
                return -2;
            if (entry.ExpiresAt is not { } expiry)
                return -1;
            var remaining = expiry - _clock.GetUtcNow();
            return remaining <= TimeSpan.Zero ? 0 : (long)remaining.TotalMilliseconds;
        }
    }

    public bool Persist(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetLiveEntry(key, db);
            if (entry is null || entry.ExpiresAt is null)
                return false;
            entry.ExpiresAt = null;
            return true;
        }
    }

    /// <summary>Total live keys across all logical databases.</summary>
    public long Count
    {
        get
        {
            lock (_gate)
            {
                PurgeExpiredLocked();
                long total = 0;
                foreach (var table in _dbs)
                    total += table.Count;
                return total;
            }
        }
    }

    /// <summary>Live keys in one logical database (DBSIZE's number).</summary>
    public long CountInDb(int db)
    {
        lock (_gate)
        {
            PurgeExpiredLocked();
            return Table(db).Count;
        }
    }

    /// <summary>Total keys carrying an expiry, across all logical databases.</summary>
    public long ExpiresCount
    {
        get
        {
            lock (_gate)
            {
                PurgeExpiredLocked();
                long count = 0;
                foreach (var table in _dbs)
                    foreach (var entry in table.Values)
                    {
                        if (entry.ExpiresAt is not null)
                            count++;
                    }
                return count;
            }
        }
    }

    /// <summary>Keys carrying an expiry in one logical database (INFO keyspace's per-DB figure).</summary>
    public long ExpiresInDb(int db)
    {
        lock (_gate)
        {
            PurgeExpiredLocked();
            long count = 0;
            foreach (var entry in Table(db).Values)
            {
                if (entry.ExpiresAt is not null)
                    count++;
            }
            return count;
        }
    }

    public IReadOnlyList<byte[]> Keys(byte[] pattern, int db = 0)
    {
        lock (_gate)
        {
            PurgeExpiredLocked();
            var result = new List<byte[]>();
            foreach (var key in Table(db).Keys)
            {
                if (Glob.IsMatch(pattern, key))
                    result.Add(key);
            }
            result.Sort(ByteArrayComparer.Instance);
            return result;
        }
    }

    /// <summary>All live keys of one database, sorted — the deterministic basis for SCAN snapshots, snapshot files and AOF rewrites.</summary>
    public IReadOnlyList<byte[]> AllKeysSorted(int db = 0)
    {
        lock (_gate)
        {
            PurgeExpiredLocked();
            var keys = new List<byte[]>(Table(db).Keys);
            keys.Sort(ByteArrayComparer.Instance);
            return keys;
        }
    }

    /// <summary>Empties every logical database.</summary>
    public void FlushAll()
    {
        lock (_gate)
        {
            foreach (var table in _dbs)
                table.Clear();
            _usedMemory = 0;
        }
    }

    /// <summary>Empties one logical database (FLUSHDB); the memory counter is recounted for that table.</summary>
    public void FlushDb(int db)
    {
        lock (_gate)
        {
            var table = Table(db);
            foreach (var (key, entry) in table)
                _usedMemory -= EstimateEntrySize(key, entry);
            table.Clear();
        }
    }

    /// <summary>
    /// MOVE's store half: relocates a live entry between databases,
    /// preserving its value, expiry and access history. False when the
    /// source key does not exist or the target database already holds
    /// the key (a live one — an expired target does not block the move).
    /// The global memory estimate is untouched: the entry only changes
    /// tables.
    /// </summary>
    public bool MoveEntry(byte[] key, int fromDb, int toDb)
    {
        lock (_gate)
        {
            var entry = GetLiveEntry(key, fromDb);
            if (entry is null)
                return false;
            if (GetLiveEntry(key, toDb) is not null)
                return false;
            Table(fromDb).Remove(key);
            Table(toDb)[key] = entry;
            return true;
        }
    }

    /// <summary>SWAPDB's store half: exchanges two databases' keyspaces wholesale.</summary>
    public void SwapDbs(int first, int second)
    {
        lock (_gate)
            (_dbs[first], _dbs[second]) = (_dbs[second], _dbs[first]);
    }

    // ------------------------------------------------------------------
    // Lists (LinkedList: O(1) at both ends, which is all LPUSH/RPOP need;
    // LINDEX/LRANGE walk, as does Redis's quicklist for random access)
    // ------------------------------------------------------------------

    public long LeftPush(byte[] key, IReadOnlyList<byte[]> values, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, db, RedisType.List, () => new LinkedList<byte[]>());
            var list = (LinkedList<byte[]>)entry.Value;
            foreach (var value in values)
            {
                list.AddFirst(value);
                _usedMemory += CollectionElementSize(value);
            }
            return list.Count;
        }
    }

    public long RightPush(byte[] key, IReadOnlyList<byte[]> values, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetOrCreate(key, db, RedisType.List, () => new LinkedList<byte[]>());
            var list = (LinkedList<byte[]>)entry.Value;
            foreach (var value in values)
            {
                list.AddLast(value);
                _usedMemory += CollectionElementSize(value);
            }
            return list.Count;
        }
    }

    public byte[]? LeftPop(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.List);
            if (entry is null)
                return null;
            var list = (LinkedList<byte[]>)entry.Value;
            var value = list.First!.Value;
            list.RemoveFirst();
            _usedMemory -= CollectionElementSize(value);
            RemoveIfEmpty(key, db, entry);
            return value;
        }
    }

    public byte[]? RightPop(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.List);
            if (entry is null)
                return null;
            var list = (LinkedList<byte[]>)entry.Value;
            var value = list.Last!.Value;
            list.RemoveLast();
            _usedMemory -= CollectionElementSize(value);
            RemoveIfEmpty(key, db, entry);
            return value;
        }
    }

    public IReadOnlyList<byte[]> LeftPop(byte[] key, long count, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.List);
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
            RemoveIfEmpty(key, db, entry);
            return result;
        }
    }

    public IReadOnlyList<byte[]> RightPop(byte[] key, long count, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.List);
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
            RemoveIfEmpty(key, db, entry);
            return result;
        }
    }

    public long ListLength(byte[] key, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.List);
            return entry is null ? 0 : ((LinkedList<byte[]>)entry.Value).Count;
        }
    }

    public byte[]? ListIndex(byte[] key, long index, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.List);
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

    public IReadOnlyList<byte[]> ListRange(byte[] key, long start, long stop, int db = 0)
    {
        lock (_gate)
        {
            var entry = GetTyped(key, db, RedisType.List);
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
