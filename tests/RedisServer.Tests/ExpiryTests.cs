using System.Text;
using RedisServer.Core;

namespace RedisServer.Tests;

/// <summary>Expiry behaviour — lazy on access, swept by maintenance, all on a fake clock.</summary>
public class ExpiryTests
{
    private readonly FakeTimeProvider _clock = new();
    private readonly RedisStats _stats = new();

    private RedisStore NewStore() => new(_clock, _stats);

    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);

    [Fact]
    public void Set_WithTtl_ExpiresLazilyOnAccess()
    {
        var store = NewStore();
        store.Set(B("k"), B("v"), SetCondition.Always, TimeSpan.FromSeconds(10), false);
        Assert.NotNull(store.Get(B("k")));

        _clock.Advance(TimeSpan.FromSeconds(11));
        Assert.Null(store.Get(B("k")));
        Assert.Equal(RedisType.None, store.TypeOf(B("k")));
    }

    [Fact]
    public void Set_WithoutTtl_ClearsPreviousDeadline_KeepTtlPreservesIt()
    {
        var store = NewStore();
        store.Set(B("k"), B("v1"), SetCondition.Always, TimeSpan.FromSeconds(10), false);
        store.Set(B("k"), B("v2"), SetCondition.Always, null, keepTtl: false);
        _clock.Advance(TimeSpan.FromSeconds(30));
        Assert.NotNull(store.Get(B("k"))); // deadline cleared by the plain SET

        store.Set(B("k"), B("v3"), SetCondition.Always, TimeSpan.FromSeconds(10), false);
        store.Set(B("k"), B("v4"), SetCondition.Always, null, keepTtl: true);
        _clock.Advance(TimeSpan.FromSeconds(11));
        Assert.Null(store.Get(B("k"))); // deadline kept, now passed
    }

    [Fact]
    public void Mutations_PreserveTtl()
    {
        var store = NewStore();
        store.Set(B("n"), B("1"), SetCondition.Always, TimeSpan.FromSeconds(10), false);
        store.ApplyDelta(B("n"), 1);
        store.Append(B("n"), B(""));
        _clock.Advance(TimeSpan.FromSeconds(11));
        Assert.Null(store.Get(B("n")));
    }

    [Fact]
    public void Ttl_ReportsMinusTwoForMissing_MinusOneForNoExpiry()
    {
        var store = NewStore();
        Assert.Equal(-2, store.TtlMilliseconds(B("ghost")));
        store.Set(B("k"), B("v"), SetCondition.Always, null, false);
        Assert.Equal(-1, store.TtlMilliseconds(B("k")));
        store.Set(B("t"), B("v"), SetCondition.Always, TimeSpan.FromSeconds(30), false);
        var ttl = store.TtlMilliseconds(B("t"));
        Assert.InRange(ttl, 29_000, 30_000);
    }

    [Fact]
    public void ExpireAt_InThePast_DeletesImmediately()
    {
        var store = NewStore();
        store.Set(B("k"), B("v"), SetCondition.Always, null, false);
        var applied = store.ExpireAt(B("k"), _clock.GetUtcNow() - TimeSpan.FromSeconds(1), out var removedKey);
        Assert.True(applied);
        Assert.True(removedKey);
        Assert.Equal(RedisType.None, store.TypeOf(B("k")));
        Assert.False(store.ExpireAt(B("ghost"), _clock.GetUtcNow().AddMinutes(1), out _));
    }

    [Fact]
    public void Persist_RemovesDeadline_OnlyWhenOneExists()
    {
        var store = NewStore();
        store.Set(B("k"), B("v"), SetCondition.Always, TimeSpan.FromSeconds(10), false);
        Assert.True(store.Persist(B("k")));
        Assert.False(store.Persist(B("k")));
        Assert.False(store.Persist(B("ghost")));
        _clock.Advance(TimeSpan.FromSeconds(60));
        Assert.NotNull(store.Get(B("k")));
    }

    [Fact]
    public void Maintenance_SweepPurgesUntouchedExpiredKeys()
    {
        var store = NewStore();
        store.Set(B("a"), B("1"), SetCondition.Always, TimeSpan.FromSeconds(5), false);
        store.Set(B("b"), B("2"), SetCondition.Always, TimeSpan.FromSeconds(5), false);
        store.Set(B("c"), B("3"), SetCondition.Always, null, false);
        _clock.Advance(TimeSpan.FromSeconds(6));
        store.RunMaintenance();
        Assert.Equal(1, store.Count);
        Assert.Equal(2, _stats.ExpiredKeys);
        Assert.Equal(0, store.ExpiresCount); // only "c" remains, and it has no expiry
    }
}
