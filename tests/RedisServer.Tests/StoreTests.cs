using System.Text;
using RedisServer.Core;

namespace RedisServer.Tests;

/// <summary>Store semantics for strings and keys, driven directly against <see cref="RedisStore"/>.</summary>
public class StoreTests
{
    private readonly FakeTimeProvider _clock = new();
    private readonly RedisStats _stats = new();

    private RedisStore NewStore() => new(_clock, _stats);

    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);
    private static string S(byte[]? bytes) => bytes is null ? "<null>" : Encoding.ASCII.GetString(bytes);

    [Fact]
    public void Set_ThenGet_RoundTrips()
    {
        var store = NewStore();
        Assert.Equal(SetOutcome.Stored, store.Set(B("k"), B("hello"), SetCondition.Always, null, false));
        Assert.Equal("hello", S(store.Get(B("k"))));
        Assert.Equal(RedisType.String, store.TypeOf(B("k")));
    }

    [Fact]
    public void Get_MissingKey_IsNull_AndTypeIsNone()
    {
        var store = NewStore();
        Assert.Null(store.Get(B("ghost")));
        Assert.Equal(RedisType.None, store.TypeOf(B("ghost")));
    }

    [Fact]
    public void Set_NX_OnlyStoresWhenAbsent_XX_OnlyWhenPresent()
    {
        var store = NewStore();
        Assert.Equal(SetOutcome.Stored, store.Set(B("k"), B("v1"), SetCondition.IfAbsent, null, false));
        Assert.Equal(SetOutcome.NotStored, store.Set(B("k"), B("v2"), SetCondition.IfAbsent, null, false));
        Assert.Equal("v1", S(store.Get(B("k"))));

        Assert.Equal(SetOutcome.Stored, store.Set(B("k"), B("v3"), SetCondition.IfPresent, null, false));
        Assert.Equal(SetOutcome.NotStored, store.Set(B("other"), B("v"), SetCondition.IfPresent, null, false));
        Assert.Equal("v3", S(store.Get(B("k"))));
    }

    [Fact]
    public void Set_OverwritesAnyType()
    {
        var store = NewStore();
        store.LeftPush(B("k"), new[] { B("x") });
        Assert.Equal(RedisType.List, store.TypeOf(B("k")));
        store.Set(B("k"), B("now-a-string"), SetCondition.Always, null, false);
        Assert.Equal(RedisType.String, store.TypeOf(B("k")));
        Assert.Equal("now-a-string", S(store.Get(B("k"))));
    }

    [Fact]
    public void WrongType_Access_Throws()
    {
        var store = NewStore();
        store.LeftPush(B("list"), new[] { B("x") });
        Assert.Throws<WrongTypeException>(() => store.Get(B("list")));
        Assert.Throws<WrongTypeException>(() => store.SetMembers(B("list")));
        store.Set(B("s"), B("v"), SetCondition.Always, null, false);
        Assert.Throws<WrongTypeException>(() => store.LeftPush(B("s"), new[] { B("x") }));
    }

    [Fact]
    public void Append_CreatesAndExtends_PreservingTtl()
    {
        var store = NewStore();
        Assert.Equal(5, store.Append(B("k"), B("hello")));
        Assert.Equal(11, store.Append(B("k"), B(" world")));
        Assert.Equal("hello world", S(store.Get(B("k"))));
        Assert.Equal(11, store.StrLen(B("k")));
        Assert.Equal(0, store.StrLen(B("missing")));
    }

    [Fact]
    public void IncrDecr_FollowRedisSemantics()
    {
        var store = NewStore();
        Assert.Equal(1, store.ApplyDelta(B("n"), 1));       // missing starts at 0
        Assert.Equal(11, store.ApplyDelta(B("n"), 10));
        Assert.Equal(6, store.ApplyDelta(B("n"), -5));
        Assert.Equal("6", S(store.Get(B("n"))));
    }

    [Fact]
    public void Incr_OnNonInteger_Throws_NotAnInteger()
    {
        var store = NewStore();
        store.Set(B("s"), B("abc"), SetCondition.Always, null, false);
        Assert.Throws<ValueErrorException>(() => store.ApplyDelta(B("s"), 1));
        store.Set(B("f"), B("1.5"), SetCondition.Always, null, false);
        Assert.Throws<ValueErrorException>(() => store.ApplyDelta(B("f"), 1));
        store.Set(B("lz"), B("007"), SetCondition.Always, null, false);
        Assert.Throws<ValueErrorException>(() => store.ApplyDelta(B("lz"), 1)); // leading zero is not an integer per Redis
    }

    [Fact]
    public void Incr_Overflow_Throws_AndKeepsValue()
    {
        var store = NewStore();
        store.Set(B("max"), B(long.MaxValue.ToString()), SetCondition.Always, null, false);
        var ex = Assert.Throws<ValueErrorException>(() => store.ApplyDelta(B("max"), 1));
        Assert.Contains("overflow", ex.Message);
        Assert.Equal(long.MaxValue.ToString(), S(store.Get(B("max"))));

        store.Set(B("min"), B(long.MinValue.ToString()), SetCondition.Always, null, false);
        Assert.Throws<ValueErrorException>(() => store.ApplyDelta(B("min"), -1));
    }

    [Fact]
    public void Delete_CountsActualRemovals_Exists_CountsDuplicates()
    {
        var store = NewStore();
        store.Set(B("a"), B("1"), SetCondition.Always, null, false);
        store.Set(B("b"), B("2"), SetCondition.Always, null, false);
        Assert.Equal(3, store.Exists(new[] { B("a"), B("b"), B("a"), B("ghost") })); // duplicates count, as in Redis >= 3.0.3
        Assert.Equal(2, store.Delete(new[] { B("a"), B("b"), B("ghost") }));
        Assert.Equal(0, store.Delete(new[] { B("a") }));
    }

    [Fact]
    public void FlushAll_ClearsEverything()
    {
        var store = NewStore();
        store.Set(B("a"), B("1"), SetCondition.Always, null, false);
        store.LeftPush(B("l"), new[] { B("x") });
        store.FlushAll();
        Assert.Equal(0, store.Count);
        Assert.Equal(RedisType.None, store.TypeOf(B("a")));
    }

    [Fact]
    public void Keys_MatchesGlobPatterns()
    {
        var store = NewStore();
        store.Set(B("user:1"), B("x"), SetCondition.Always, null, false);
        store.Set(B("user:2"), B("x"), SetCondition.Always, null, false);
        store.Set(B("session:9"), B("x"), SetCondition.Always, null, false);
        Assert.Equal(2, store.Keys(B("user:*")).Count);
        Assert.Single(store.Keys(B("session:?")));
        Assert.Equal(3, store.Keys(B("*")).Count);
        Assert.Empty(store.Keys(B("order:*")));
    }

    [Fact]
    public void Glob_ClassesRangesAndEscapes()
    {
        Assert.True(Glob.IsMatch("h?llo", "hello"));
        Assert.True(Glob.IsMatch("h[ae]llo", "hallo"));
        Assert.False(Glob.IsMatch("h[ae]llo", "hillo"));
        Assert.True(Glob.IsMatch("[a-z]ey", "key"));
        Assert.True(Glob.IsMatch("[^a]ey", "key"));
        Assert.False(Glob.IsMatch("[^a]ey", "aey"));
        Assert.True(Glob.IsMatch(@"a\*b", "a*b"));
        Assert.True(Glob.IsMatch("*", ""));
        Assert.False(Glob.IsMatch("?", ""));
    }
}
