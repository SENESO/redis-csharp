using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Persistence;

namespace RedisServer.Tests;

/// <summary>
/// --maxmemory and eviction. The estimator is deterministic (64 bytes
/// per key plus payload bytes; 16 bytes per collection element), so the
/// tests size their workloads exactly: a 1-byte key with a 400-byte
/// value accounts for 465 bytes. Error text and the deny-oom /
/// volatile-scope rules were probed against redis-server 7.0.15 first.
/// </summary>
public sealed class MaxMemoryTests
{
    private const string Oom = "-OOM command not allowed when used memory > 'maxmemory'.\r\n";
    private static readonly string Big = new('v', 400); // 400-byte value

    [Fact]
    public async Task NoEviction_WritesAreRefusedWithOom_ReadsStillServed()
    {
        // Cap 1000, checked before each command runs (as in Redis): two
        // 465-byte keys fit (930), the third still runs (930 is not over)
        // and lands at 1395 — every write after that is refused.
        await using var h = SessionHarness.Start(maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.NoEviction);
        var c = h.Connect();

        await c.SendCommandAsync("SET", "a", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "b", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "c", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "d", Big);
        await c.ExpectAsync(Oom);
        // Deny-oom covers growth of existing keys and counters too.
        await c.SendCommandAsync("INCR", "n");
        await c.ExpectAsync(Oom);
        await c.SendCommandAsync("APPEND", "a", "x");
        await c.ExpectAsync(Oom);
        // Reads, deletes and everything else keep working.
        await c.SendCommandAsync("GET", "a");
        Assert.Equal(Big, await c.ReadReplyAsync());
        await c.SendCommandAsync("DEL", "a");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("SET", "d", Big);
        await c.ExpectAsync("+OK\r\n");
        Assert.Equal(0, h.Stats.EvictedKeys);
    }

    [Fact]
    public async Task AllKeysLru_EvictsTheLeastRecentlyUsed_BeforeTheWrite()
    {
        // Eviction is attempted before every command while the store is
        // over the cap (Redis's freeMemoryIfNeeded does the same), so
        // each step below names its eviction exactly: with at most four
        // keys live, the five-candidate sample always sees them all and
        // the LRU victim is deterministic.
        await using var h = SessionHarness.Start(maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.AllKeysLru);
        var c = h.Connect();

        await c.SendCommandAsync("SET", "a", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "b", Big);
        await c.ExpectAsync("+OK\r\n");
        // Touch a: b is now the least recently used of the two.
        await c.SendCommandAsync("GET", "a");
        Assert.Equal(Big, await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "c", Big); // lands over the cap at 1395
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "d", Big); // evicts b first, then writes
        await c.ExpectAsync("+OK\r\n");
        Assert.Equal(1, h.Stats.EvictedKeys);
        await c.SendCommandAsync("SET", "e", Big); // evicts a (next-oldest), then writes
        await c.ExpectAsync("+OK\r\n");
        Assert.Equal(2, h.Stats.EvictedKeys);
        // Reading e evicts c (the last over-cap key), settling at 930.
        await c.SendCommandAsync("GET", "e");
        Assert.Equal(Big, await c.ReadReplyAsync());
        Assert.Equal(3, h.Stats.EvictedKeys);
        Assert.Equal(930, h.Store.UsedMemory);

        await c.SendCommandAsync("GET", "b");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("GET", "a");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("GET", "c");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("EXISTS", "d", "e");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("DBSIZE");
        await c.ExpectAsync(":2\r\n");
    }

    [Fact]
    public async Task VolatilePolicies_NeverTouchKeysWithoutTtl()
    {
        await using var h = SessionHarness.Start(maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.VolatileLru);
        var c = h.Connect();

        await c.SendCommandAsync("SET", "plain-a", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "plain-b", Big);
        await c.ExpectAsync("+OK\r\n");
        // Over the cap with no volatile key in sight: OOM, plains intact.
        await c.SendCommandAsync("SET", "plain-c", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "plain-d", Big);
        await c.ExpectAsync(Oom);
        await c.SendCommandAsync("EXISTS", "plain-a", "plain-b", "plain-c");
        await c.ExpectAsync(":3\r\n");
        Assert.Equal(0, h.Stats.EvictedKeys);
    }

    [Fact]
    public async Task VolatileTtl_EvictsTheSoonestToExpire()
    {
        // Sizes pin the victim by arithmetic: late = 64+4+100 = 168,
        // soon = 468, plain = 469, plain2 = 470. After SET plain the
        // store is at 1105; SET plain2 must evict ONE volatile key to
        // proceed — the result 1105-468+470 = 1107 proves the victim
        // was "soon" (the earliest deadline), not "late" (which would
        // leave 1407).
        await using var h = SessionHarness.Start(maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.VolatileTtl);
        var c = h.Connect();
        var small = new string('v', 100);

        await c.SendCommandAsync("SET", "late", small, "PX", "60000");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "soon", Big, "PX", "5000");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "plain", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "plain2", Big);
        await c.ExpectAsync("+OK\r\n");
        Assert.Equal(1, h.Stats.EvictedKeys);
        Assert.Equal(1107, h.Store.UsedMemory);

        // Still over the cap: the next command evicts "late" (the only
        // volatile key left) before running — plain keys are never
        // candidates under a volatile policy.
        await c.SendCommandAsync("EXISTS", "soon");
        await c.ExpectAsync(":0\r\n");
        Assert.Equal(2, h.Stats.EvictedKeys);
        await c.SendCommandAsync("EXISTS", "plain", "plain2");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("DBSIZE");
        await c.ExpectAsync(":2\r\n");
    }

    [Fact]
    public async Task AllKeysRandom_KeepsTheDatasetBoundedAndCountsEvictions()
    {
        await using var h = SessionHarness.Start(maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.AllKeysRandom);
        var c = h.Connect();

        for (var i = 0; i < 40; i++)
        {
            await c.SendCommandAsync("SET", "k" + i, Big);
            await c.ExpectAsync("+OK\r\n");
        }

        Assert.True(h.Stats.EvictedKeys >= 30, $"expected many evictions, got {h.Stats.EvictedKeys}");
        await c.SendCommandAsync("DBSIZE");
        var size = Assert.IsType<long>(await c.ReadReplyAsync());
        Assert.True(size <= 4, $"dataset should stay within one write of the cap, got {size} keys");
        Assert.True(h.Store.UsedMemory <= 1000 + 465);
    }

    [Fact]
    public async Task Evictions_AreAppendedToTheAofAsDelEffects()
    {
        await using var h = SessionHarness.Start(appendOnly: true,
            maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.AllKeysLru);
        var c = h.Connect();

        await c.SendCommandAsync("SET", "a", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "b", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("GET", "a");
        Assert.Equal(Big, await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "c", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "d", Big); // evicts b; its DEL must reach the AOF
        await c.ExpectAsync("+OK\r\n");

        var aofPath = Path.Combine(h.DataDirectory, "appendonly.aof");
        var commands = AppendOnlyFile.ReadCommands(aofPath, out var truncated);
        Assert.False(truncated);
        var dels = commands.Where(cmd => cmd.Length == 2 &&
            System.Text.Encoding.ASCII.GetString(cmd[0]) == "DEL").ToList();
        var del = Assert.Single(dels);
        Assert.Equal("b", System.Text.Encoding.ASCII.GetString(del[1]));
    }

    [Fact]
    public async Task UsedMemoryCounter_MatchesAFullRecountAfterAMixedWorkload()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("SET", "s", "abc");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("APPEND", "s", "defgh");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("INCRBY", "n", "12345");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("LPUSH", "l", "a", "bb", "ccc");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("LPOP", "l", "2");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("SADD", "set", "x", "yy", "x");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("SREM", "set", "x");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("HSET", "hash", "f1", "v1", "f2", "longer-value");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("HSET", "hash", "f1", "v1-extended");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("HDEL", "hash", "f2");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("ZADD", "z", "1.5", "m1", "2", "m2");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("ZADD", "z", "INCR", "0.5", "m1");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("ZREM", "z", "m2");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("SET", "gone", "x");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("DEL", "gone");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("SET", "ttl", "x", "PX", "60000");
        await c.ReadReplyAsync();

        Assert.Equal(h.Store.RecalculateUsedMemory(), h.Store.UsedMemory);
        Assert.True(h.Store.UsedMemory > 0);
    }

    [Fact]
    public async Task Info_ReportsTheMemorySectionAndEvictedKeys()
    {
        await using var h = SessionHarness.Start(maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.AllKeysLru);
        var c = h.Connect();

        await c.SendCommandAsync("SET", "a", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("INFO", "memory");
        var info = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.Contains("# Memory", info);
        Assert.Contains("used_memory:465", info);
        Assert.Contains("maxmemory:1000", info);
        Assert.Contains("maxmemory_policy:allkeys-lru", info);
        await c.SendCommandAsync("INFO", "stats");
        info = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.Contains("evicted_keys:0", info);
    }

    [Fact]
    public void MemorySizeAndPolicyParsing_FollowRedisSuffixRules()
    {
        Assert.Equal(1024, ServerOptions.ParseMemorySize("1024"));
        Assert.Equal(1000, ServerOptions.ParseMemorySize("1k"));
        Assert.Equal(1024, ServerOptions.ParseMemorySize("1kb"));
        Assert.Equal(2_000_000, ServerOptions.ParseMemorySize("2m"));
        Assert.Equal(2_097_152, ServerOptions.ParseMemorySize("2mb"));
        Assert.Equal(1_000_000_000, ServerOptions.ParseMemorySize("1g"));
        Assert.Equal(1_073_741_824, ServerOptions.ParseMemorySize("1gb"));
        Assert.Equal(0, ServerOptions.ParseMemorySize("0"));
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseMemorySize("lots"));
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseMemorySize("-5"));

        Assert.Equal(MaxMemoryPolicy.AllKeysLru, ServerOptions.ParseMaxMemoryPolicy("allkeys-lru"));
        Assert.Equal(MaxMemoryPolicy.VolatileTtl, ServerOptions.ParseMaxMemoryPolicy("volatile-ttl"));
        Assert.Equal(MaxMemoryPolicy.NoEviction, ServerOptions.ParseMaxMemoryPolicy("noeviction"));
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseMaxMemoryPolicy("allkeys-fifo"));
    }
}
