using System.Text;
using RedisServer.Core;

namespace RedisServer.Tests;

/// <summary>Store semantics for lists, sets, hashes and sorted sets.</summary>
public class CollectionTests
{
    private readonly FakeTimeProvider _clock = new();
    private readonly RedisStats _stats = new();

    private RedisStore NewStore() => new(_clock, _stats);

    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);
    private static List<string> Strings(IEnumerable<byte[]> values) =>
        values.Select(v => Encoding.ASCII.GetString(v)).ToList();

    // ---------------- Lists ----------------

    [Fact]
    public void List_PushPop_Ordering()
    {
        var store = NewStore();
        Assert.Equal(3, store.LeftPush(B("l"), new[] { B("a"), B("b"), B("c") })); // list: c b a
        Assert.Equal(4, store.RightPush(B("l"), new[] { B("d") }));               // list: c b a d
        Assert.Equal(new[] { "c", "b", "a", "d" }, Strings(store.ListRange(B("l"), 0, -1)));
        Assert.Equal("c", Encoding.ASCII.GetString(store.LeftPop(B("l"))!));
        Assert.Equal("d", Encoding.ASCII.GetString(store.RightPop(B("l"))!));
        Assert.Equal(2, store.ListLength(B("l")));
    }

    [Fact]
    public void List_RangeAndIndex_NegativeIndexes()
    {
        var store = NewStore();
        store.RightPush(B("l"), new[] { B("a"), B("b"), B("c"), B("d") });
        Assert.Equal(new[] { "b", "c" }, Strings(store.ListRange(B("l"), 1, 2)));
        Assert.Equal(new[] { "c", "d" }, Strings(store.ListRange(B("l"), -2, -1)));
        Assert.Equal(new[] { "a", "b", "c", "d" }, Strings(store.ListRange(B("l"), 0, 100)));
        Assert.Empty(store.ListRange(B("l"), 5, 9));
        Assert.Equal("a", Encoding.ASCII.GetString(store.ListIndex(B("l"), 0)!));
        Assert.Equal("d", Encoding.ASCII.GetString(store.ListIndex(B("l"), -1)!));
        Assert.Null(store.ListIndex(B("l"), 4));
        Assert.Null(store.ListIndex(B("missing"), 0));
    }

    [Fact]
    public void List_PopWithCount_AndKeyDisappearsWhenEmpty()
    {
        var store = NewStore();
        store.RightPush(B("l"), new[] { B("a"), B("b"), B("c") });
        Assert.Equal(new[] { "a", "b" }, Strings(store.LeftPop(B("l"), 2)));
        Assert.Equal(new[] { "c" }, Strings(store.RightPop(B("l"), 5)));
        Assert.Equal(RedisType.None, store.TypeOf(B("l"))); // empty collection keys are removed
        Assert.Null(store.LeftPop(B("l")));
        Assert.Empty(store.LeftPop(B("l"), 3));
    }

    // ---------------- Sets ----------------

    [Fact]
    public void Set_AddRemoveMembership()
    {
        var store = NewStore();
        Assert.Equal(3, store.SetAdd(B("s"), new[] { B("a"), B("b"), B("c"), B("a") }));
        Assert.Equal(0, store.SetAdd(B("s"), new[] { B("a") }));
        Assert.True(store.SetIsMember(B("s"), B("b")));
        Assert.False(store.SetIsMember(B("s"), B("z")));
        Assert.Equal(3, store.SetCard(B("s")));
        Assert.Equal(1, store.SetRemove(B("s"), new[] { B("b"), B("z") }));
        Assert.Equal(new[] { "a", "c" }, Strings(store.SetMembers(B("s"))));
        Assert.Equal(2, store.SetRemove(B("s"), new[] { B("a"), B("c") }));
        Assert.Equal(RedisType.None, store.TypeOf(B("s")));
        Assert.Equal(0, store.SetCard(B("missing")));
    }

    // ---------------- Hashes ----------------

    [Fact]
    public void Hash_SetGetDeleteAll()
    {
        var store = NewStore();
        var added = store.HashSet(B("h"), new[]
        {
            new KeyValuePair<byte[], byte[]>(B("name"), B("eslam")),
            new KeyValuePair<byte[], byte[]>(B("lang"), B("csharp"))
        });
        Assert.Equal(2, added);
        Assert.Equal(0, store.HashSet(B("h"), new[] { new KeyValuePair<byte[], byte[]>(B("name"), B("ashraf")) }));
        Assert.Equal("ashraf", Encoding.ASCII.GetString(store.HashGet(B("h"), B("name"))!));
        Assert.Null(store.HashGet(B("h"), B("ghost")));
        Assert.Equal(2, store.HashLength(B("h")));
        var all = store.HashGetAll(B("h"));
        Assert.Equal(2, all.Count);
        Assert.Equal(1, store.HashDelete(B("h"), new[] { B("lang"), B("ghost") }));
        Assert.Equal(1, store.HashDelete(B("h"), new[] { B("name") }));
        Assert.Equal(RedisType.None, store.TypeOf(B("h")));
    }

    // ---------------- Sorted sets ----------------

    [Fact]
    public void ZSet_AddScoreRange_OrdersByScoreThenMember()
    {
        var store = NewStore();
        var result = store.ZAdd(B("z"), ZAddMode.None, false, false, new[] { (2.0, B("b")), (1.0, B("a")), (2.0, B("c")) });
        Assert.Equal(3, result.Added);
        Assert.Equal(1.0, store.ZScore(B("z"), B("a")));
        Assert.Null(store.ZScore(B("z"), B("ghost")));
        var range = store.ZRange(B("z"), 0, -1);
        Assert.Equal(new[] { "a", "b", "c" }, range.Select(r => Encoding.ASCII.GetString(r.Member)).ToList()); // 2.0 tie: b before c lexicographically
        Assert.Equal(3, store.ZCard(B("z")));
    }

    [Fact]
    public void ZSet_Range_NegativeIndexes()
    {
        var store = NewStore();
        store.ZAdd(B("z"), ZAddMode.None, false, false, new[] { (1.0, B("a")), (2.0, B("b")), (3.0, B("c")) });
        var last2 = store.ZRange(B("z"), -2, -1);
        Assert.Equal(new[] { "b", "c" }, last2.Select(r => Encoding.ASCII.GetString(r.Member)).ToList());
        Assert.Empty(store.ZRange(B("z"), 5, 9));
    }

    [Fact]
    public void ZSet_UpdateModes_NX_XX_GT_LT_CH()
    {
        var store = NewStore();
        store.ZAdd(B("z"), ZAddMode.None, false, false, new[] { (5.0, B("m")) });

        // NX never updates
        Assert.Equal(0, store.ZAdd(B("z"), ZAddMode.OnlyNew, false, false, new[] { (9.0, B("m")) }).Added);
        Assert.Equal(5.0, store.ZScore(B("z"), B("m")));
        // XX never adds
        Assert.Equal(0, store.ZAdd(B("z"), ZAddMode.OnlyExisting, false, false, new[] { (1.0, B("new")) }).Added);
        Assert.Null(store.ZScore(B("z"), B("new")));
        // GT only raises
        store.ZAdd(B("z"), ZAddMode.GreaterThan, false, false, new[] { (4.0, B("m")) });
        Assert.Equal(5.0, store.ZScore(B("z"), B("m")));
        store.ZAdd(B("z"), ZAddMode.GreaterThan, false, false, new[] { (7.0, B("m")) });
        Assert.Equal(7.0, store.ZScore(B("z"), B("m")));
        // LT only lowers
        store.ZAdd(B("z"), ZAddMode.LessThan, false, false, new[] { (8.0, B("m")) });
        Assert.Equal(7.0, store.ZScore(B("z"), B("m")));
        // CH counts the change
        var ch = store.ZAdd(B("z"), ZAddMode.None, countChanged: true, false, new[] { (6.0, B("m")), (1.0, B("n")) });
        Assert.Equal(2, ch.Added); // 1 changed + 1 added
        var noCh = store.ZAdd(B("z"), ZAddMode.None, countChanged: false, false, new[] { (3.0, B("m")) });
        Assert.Equal(0, noCh.Added); // update only: not counted without CH
    }

    [Fact]
    public void ZSet_Increment()
    {
        var store = NewStore();
        var first = store.ZAdd(B("z"), ZAddMode.None, false, increment: true, new[] { (2.5, B("m")) });
        Assert.Equal(2.5, first.IncrementScore);
        var second = store.ZAdd(B("z"), ZAddMode.None, false, increment: true, new[] { (1.5, B("m")) });
        Assert.Equal(4.0, second.IncrementScore);
        var blocked = store.ZAdd(B("z"), ZAddMode.OnlyExisting, false, increment: true, new[] { (1.0, B("ghost")) });
        Assert.Null(blocked.IncrementScore);
        Assert.Equal(1, store.ZCard(B("z")));
    }

    [Fact]
    public void ZSet_Remove_AndEmptyKeyDisappears()
    {
        var store = NewStore();
        store.ZAdd(B("z"), ZAddMode.None, false, false, new[] { (1.0, B("a")), (2.0, B("b")) });
        Assert.Equal(1, store.ZRemove(B("z"), new[] { B("a"), B("ghost") }));
        Assert.Equal(1, store.ZRemove(B("z"), new[] { B("b") }));
        Assert.Equal(RedisType.None, store.TypeOf(B("z")));
    }
}
