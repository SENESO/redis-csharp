using System.Text;
using RedisServer.Core;
using RedisServer.Options;

namespace RedisServer.Tests;

/// <summary>
/// LFU eviction: Redis's probabilistic counter (initial value 5,
/// increment probability 1/((counter-5)*logFactor + 1)) and its linear
/// time decay, under the allkeys-lfu and volatile-lfu policies. A log
/// factor of 0 makes every increment certain — the same device Redis's
/// own test suite uses — so the counter arithmetic is exact here.
/// </summary>
public class LfuTests
{
    private readonly FakeTimeProvider _clock = new();
    private readonly RedisStats _stats = new();

    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);
    private static readonly string Big = new('x', 400); // ~465 bytes per entry with a 1-char key

    private RedisStore NewStore(int logFactor = 0, int decayMinutes = 1)
    {
        var store = new RedisStore(_clock, _stats) { LfuLogFactor = logFactor, LfuDecayTimeMinutes = decayMinutes };
        return store;
    }

    [Fact]
    public void NewKey_StartsAtTheInitialCounter_AndEachAccessAddsOne_WhenFactorIsZero()
    {
        var store = NewStore();
        store.Set(B("k"), B("v"), SetCondition.Always, null, false);
        Assert.Equal(5, store.GetFreqCounter(B("k")));
        store.Get(B("k"));
        store.Get(B("k"));
        store.Get(B("k"));
        Assert.Equal(8, store.GetFreqCounter(B("k")));
        Assert.Null(store.GetFreqCounter(B("ghost")));
    }

    [Fact]
    public void Counter_IsCappedAt255()
    {
        var store = NewStore();
        store.Set(B("k"), B("v"), SetCondition.Always, null, false);
        for (var i = 0; i < 400; i++)
            store.Get(B("k"));
        Assert.Equal(255, store.GetFreqCounter(B("k")));
    }

    [Fact]
    public void DefaultLogFactor_MakesIncrementsProbabilisticAndSublinear()
    {
        var store = NewStore(logFactor: 10);
        store.Set(B("k"), B("v"), SetCondition.Always, null, false);
        for (var i = 0; i < 200; i++)
            store.Get(B("k"));
        var counter = store.GetFreqCounter(B("k"));
        // The first increment is certain (probability 1 at counter 5);
        // afterwards the probability collapses — 200 touches land far
        // below the deterministic 205 of a factor-0 counter.
        Assert.True(counter > 5 && counter < 60, $"counter was {counter}");
    }

    [Fact]
    public void Decay_SubtractsOnePointPerDecayPeriodOfIdleness()
    {
        var store = NewStore();
        store.Set(B("k"), B("v"), SetCondition.Always, null, false);
        for (var i = 0; i < 10; i++)
            store.Get(B("k"));
        Assert.Equal(15, store.GetFreqCounter(B("k")));

        _clock.Advance(TimeSpan.FromMinutes(10));
        Assert.Equal(5, store.GetFreqCounter(B("k")));
        _clock.Advance(TimeSpan.FromMinutes(60));
        Assert.Equal(0, store.GetFreqCounter(B("k"))); // floored at zero
    }

    [Fact]
    public void DecayTime_ScalesTheDecayRate()
    {
        var store = NewStore(decayMinutes: 5);
        store.Set(B("k"), B("v"), SetCondition.Always, null, false);
        for (var i = 0; i < 15; i++)
            store.Get(B("k"));
        Assert.Equal(20, store.GetFreqCounter(B("k")));
        _clock.Advance(TimeSpan.FromMinutes(12)); // two full 5-minute periods
        Assert.Equal(18, store.GetFreqCounter(B("k")));
    }

    [Fact]
    public void TryEvictOne_AllKeysLfu_PicksTheColdestSample()
    {
        var store = NewStore();
        store.Set(B("hot"), B("v"), SetCondition.Always, null, false);
        store.Set(B("cold"), B("v"), SetCondition.Always, null, false);
        for (var i = 0; i < 20; i++)
            store.Get(B("hot"));
        var victim = store.TryEvictOne(MaxMemoryPolicy.AllKeysLfu);
        Assert.Equal("cold", Encoding.ASCII.GetString(victim!.Value.Key));
        Assert.Equal(0, victim.Value.Db);
    }

    [Fact]
    public void TryEvictOne_VolatileLfu_OnlyConsidersKeysWithTtl()
    {
        var store = NewStore();
        store.Set(B("plain"), B("v"), SetCondition.Always, null, false);
        store.Set(B("timed"), B("v"), SetCondition.Always, null, false);
        store.ExpireAt(B("timed"), _clock.GetUtcNow() + TimeSpan.FromMinutes(30), out _);
        for (var i = 0; i < 20; i++)
            store.Get(B("plain")); // hottest key in the store, but no TTL
        var victim = store.TryEvictOne(MaxMemoryPolicy.VolatileLfu);
        Assert.Equal("timed", Encoding.ASCII.GetString(victim!.Value.Key));
    }

    [Fact]
    public async Task AllKeysLfu_EvictsTheColdestKey_BeforeTheWrite()
    {
        await using var h = SessionHarness.Start(maxMemory: 1000,
            maxMemoryPolicy: MaxMemoryPolicy.AllKeysLfu, lfuLogFactor: 0);
        var c = h.Connect();

        await c.SendCommandAsync("SET", "a", Big);
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "b", Big);
        await c.ExpectAsync("+OK\r\n");
        // a becomes hot (counter 15), c lukewarm, b stays at the initial 5.
        for (var i = 0; i < 10; i++)
        {
            await c.SendCommandAsync("GET", "a");
            Assert.Equal(Big, await c.ReadReplyAsync());
        }
        await c.SendCommandAsync("SET", "c", Big); // over the cap at 1395
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("GET", "c");
        Assert.Equal(Big, await c.ReadReplyAsync()); // c: counter 6
        await c.SendCommandAsync("SET", "d", Big); // evicts b (coldest) first
        await c.ExpectAsync("+OK\r\n");
        Assert.Equal(1, h.Stats.EvictedKeys);

        await c.SendCommandAsync("GET", "a"); // over the cap: evicts d (5), the new coldest
        Assert.Equal(Big, await c.ReadReplyAsync());
        Assert.Equal(2, h.Stats.EvictedKeys);
        await c.SendCommandAsync("EXISTS", "a", "c");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("GET", "b");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("GET", "d");
        await c.ExpectAsync("$-1\r\n");
    }

    [Fact]
    public async Task Info_ReportsTheLfuPolicyName()
    {
        await using var h = SessionHarness.Start(maxMemory: 1000,
            maxMemoryPolicy: MaxMemoryPolicy.VolatileLfu);
        var c = h.Connect();
        await c.SendCommandAsync("INFO", "memory");
        var info = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.Contains("maxmemory_policy:volatile-lfu", info);
    }

    [Fact]
    public void Options_ParseTheLfuPoliciesAndKnobs()
    {
        Assert.Equal(MaxMemoryPolicy.AllKeysLfu, ServerOptions.ParseMaxMemoryPolicy("allkeys-lfu"));
        Assert.Equal(MaxMemoryPolicy.VolatileLfu, ServerOptions.ParseMaxMemoryPolicy("volatile-lfu"));
        var options = ServerOptions.FromArgs(new[] { "--lfu-log-factor", "3", "--lfu-decay-time", "7" });
        Assert.Equal(3, options.LfuLogFactor);
        Assert.Equal(7, options.LfuDecayTime);
        var defaults = ServerOptions.FromArgs(Array.Empty<string>());
        Assert.Equal(10, defaults.LfuLogFactor);
        Assert.Equal(1, defaults.LfuDecayTime);
    }
}
