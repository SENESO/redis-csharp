using System.Text;
using RedisServer.Core;
using RedisServer.Options;
using Xunit;

namespace RedisServer.Tests;

/// <summary>
/// Cluster mode over the static slot map: real CRC16 slot math (the
/// vectors were probed against redis-server 7.0.15's CLUSTER KEYSLOT),
/// MOVED/CROSSSLOT/CLUSTERDOWN wire shapes, the CLUSTER subcommands,
/// and the commands cluster mode refuses.
/// </summary>
public sealed class ClusterTests
{
    private const string Map =
        "0-5460@127.0.0.1:7401,5461-10922@127.0.0.1:7402,10923-16383@127.0.0.1:7403";

    private static SessionHarness Cluster() =>
        SessionHarness.Start(clusterEnabled: true, clusterSlots: Map, clusterPort: 7401);

    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);

    /// <summary>Finds a key whose slot falls in this node's range (setup only; the math itself is pinned by the vector test).</summary>
    private static string OwnKey(int n = 0)
    {
        for (var i = n; ; i++)
        {
            var key = $"ownkey{i}";
            if (ClusterSlot.OfKey(B(key)) <= 5460)
                return key;
        }
    }

    [Fact]
    public void SlotMath_MatchesProbedVectors()
    {
        Assert.Equal((ushort)0x31C3, ClusterSlot.Crc16(B("123456789")));
        Assert.Equal(12739, ClusterSlot.OfKey(B("123456789")));
        Assert.Equal(12182, ClusterSlot.OfKey(B("foo")));
        Assert.Equal(5061, ClusterSlot.OfKey(B("{bar}x")));
        // Hash tags: only the tagged part is hashed.
        Assert.Equal(ClusterSlot.OfKey(B("user1000")), ClusterSlot.OfKey(B("{user1000}.followers")));
        Assert.Equal(ClusterSlot.OfKey(B("{u}1")), ClusterSlot.OfKey(B("{u}2")));
        // An empty tag is ignored: the whole key is hashed.
        Assert.Equal(ClusterSlot.Crc16(B("a{}b")) % 16384, ClusterSlot.OfKey(B("a{}b")));
        // An unbalanced brace is ignored too.
        Assert.Equal(ClusterSlot.Crc16(B("a{bc")) % 16384, ClusterSlot.OfKey(B("a{bc")));
    }

    [Fact]
    public void SlotMapParsing_ValidatesAndSorts()
    {
        var map = ServerOptions.ParseClusterSlots("5461-10922@127.0.0.1:7402,0-5460@127.0.0.1:7401");
        Assert.Equal(2, map.Count);
        Assert.Equal(0, map[0].Start);
        Assert.Equal("127.0.0.1", map[1].Host);
        Assert.Equal(7402, map[1].Port);
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseClusterSlots("0-99999@127.0.0.1:7401"));
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseClusterSlots("10-5@127.0.0.1:7401"));
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseClusterSlots("0-10@127.0.0.1"));
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseClusterSlots("0-10@127.0.0.1:0"));
        Assert.Throws<ArgumentException>(() => ServerOptions.ParseClusterSlots("nonsense"));
    }

    [Fact]
    public async Task OwnSlotKeys_AreServedNormally()
    {
        await using var h = Cluster();
        var c = h.Connect();
        var key = OwnKey();
        await c.SendCommandAsync("SET", key, "v");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", key);
        Assert.Equal("v", await c.ReadReplyAsync());
        await c.SendCommandAsync("DBSIZE");
        Assert.Equal(1L, await c.ReadReplyAsync());
    }

    [Fact]
    public async Task ForeignSlotKeys_GetFollowableMovedReplies()
    {
        await using var h = Cluster();
        var c = h.Connect();
        // "123456789" hashes to slot 12739, owned by the 7403 node.
        await c.SendCommandAsync("SET", "123456789", "v");
        Assert.Equal(new TestClient.RespError("MOVED 12739 127.0.0.1:7403"), await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "123456789");
        Assert.Equal(new TestClient.RespError("MOVED 12739 127.0.0.1:7403"), await c.ReadReplyAsync());
        // Nothing was written locally by the redirected write.
        await c.SendCommandAsync("DBSIZE");
        Assert.Equal(0L, await c.ReadReplyAsync());
    }

    [Fact]
    public async Task MultiKeyCommands_MustShareOneSlot()
    {
        await using var h = Cluster();
        var c = h.Connect();
        // Same hash tag: one slot, and it is this node's only if the tag
        // says so. Find a tag that lands here, then use it for both keys.
        string? tag = null;
        for (var i = 0; i < 1000 && tag is null; i++)
            if (ClusterSlot.OfKey(B($"{{t{i}}}x")) <= 5460)
                tag = $"t{i}";
        Assert.NotNull(tag);
        await c.SendCommandAsync("SET", $"{{{tag}}}a", "1");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", $"{{{tag}}}b", "2");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("DEL", $"{{{tag}}}a", $"{{{tag}}}b");
        Assert.Equal(2L, await c.ReadReplyAsync());
        // Two keys in different slots: CROSSSLOT, nothing runs.
        await c.SendCommandAsync("DEL", OwnKey(), "123456789");
        Assert.Equal(
            new TestClient.RespError("CROSSSLOT Keys in request don't hash to the same slot"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task UnservedSlot_GetsClusterDown()
    {
        await using var h = SessionHarness.Start(
            clusterEnabled: true, clusterSlots: "0-100@127.0.0.1:7401", clusterPort: 7401);
        var c = h.Connect();
        await c.SendCommandAsync("SET", "123456789", "v");
        Assert.Equal(new TestClient.RespError("CLUSTERDOWN Hash slot not served"), await c.ReadReplyAsync());
        await c.SendCommandAsync("CLUSTER", "INFO");
        var info = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.Contains("cluster_state:fail", info);
        Assert.Contains("cluster_slots_assigned:101", info);
    }

    [Fact]
    public async Task ClusterSubcommands_AnswerFromTheMap()
    {
        await using var h = Cluster();
        var c = h.Connect();
        await c.SendCommandAsync("CLUSTER", "KEYSLOT", "123456789");
        Assert.Equal(12739L, await c.ReadReplyAsync());

        var key = OwnKey();
        await c.SendCommandAsync("SET", key, "v");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("CLUSTER", "COUNTKEYSINSLOT", ClusterSlot.OfKey(B(key)).ToString());
        Assert.Equal(1L, await c.ReadReplyAsync());
        await c.SendCommandAsync("CLUSTER", "COUNTKEYSINSLOT", "99999");
        Assert.Equal(new TestClient.RespError("ERR Slot out of range"), await c.ReadReplyAsync());

        await c.SendCommandAsync("CLUSTER", "INFO");
        var info = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.Contains("cluster_state:ok", info);
        Assert.Contains("cluster_slots_assigned:16384", info);
        Assert.Contains("cluster_known_nodes:3", info);

        await c.SendCommandAsync("CLUSTER", "SLOTS");
        var slots = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(3, slots.Count);
        var first = Assert.IsType<List<object?>>(slots[0]);
        Assert.Equal(0L, first[0]);
        Assert.Equal(5460L, first[1]);
        var node = Assert.IsType<List<object?>>(first[2]);
        Assert.Equal("127.0.0.1", node[0]);
        Assert.Equal(7401L, node[1]);

        await c.SendCommandAsync("CLUSTER", "NODES");
        var nodes = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.Equal(3, nodes.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length);
        Assert.Contains("myself,master", nodes);
        Assert.Contains("127.0.0.1:7402@17402", nodes);

        await c.SendCommandAsync("CLUSTER", "FROB");
        Assert.Equal(
            new TestClient.RespError("ERR unknown subcommand 'FROB'. Try CLUSTER HELP."),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task ClusterCommand_WhenDisabled_IsRefused()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("CLUSTER", "INFO");
        Assert.Equal(
            new TestClient.RespError("ERR This instance has cluster support disabled"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task DatabaseCommands_AreRefusedInClusterMode()
    {
        await using var h = Cluster();
        var c = h.Connect();
        await c.SendCommandAsync("SELECT", "1");
        Assert.Equal(
            new TestClient.RespError("ERR SELECT is not allowed in cluster mode"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("MOVE", "k", "1");
        Assert.Equal(
            new TestClient.RespError("ERR MOVE is not allowed in cluster mode"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("SWAPDB", "0", "1");
        Assert.Equal(
            new TestClient.RespError("ERR SWAPDB is not allowed in cluster mode"),
            await c.ReadReplyAsync());
        // INFO carries the Cluster section.
        await c.SendCommandAsync("INFO", "cluster");
        Assert.Equal("# Cluster\r\ncluster_enabled:1\r\n",
            Assert.IsType<string>(await c.ReadReplyAsync()));
    }

    [Fact]
    public async Task Scripts_AreSlotCheckedByTheirKeys()
    {
        await using var h = Cluster();
        var c = h.Connect();
        await c.SendCommandAsync("EVAL", "return redis.call('SET', KEYS[1], 'v')", "1", "123456789");
        Assert.Equal(new TestClient.RespError("MOVED 12739 127.0.0.1:7403"), await c.ReadReplyAsync());
        await c.SendCommandAsync("EVAL", "return 1", "0");
        Assert.Equal(1L, await c.ReadReplyAsync());
    }
}
