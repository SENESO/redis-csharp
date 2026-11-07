using System.Buffers;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Persistence;
using RedisServer.Protocol;

namespace RedisServer.Tests;

/// <summary>
/// Persistence end to end: snapshots (round-trip, corruption, expiry) and
/// the AOF (replay, rewrite compaction, FLUSHALL, torn-tail recovery).
/// A "restart" here means a brand-new store + manager over the same
/// directory, loading exactly the way the server does at boot.
/// </summary>
public class PersistenceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "redis-csharp-persistence", Guid.NewGuid().ToString("N"));

    private sealed record Node(
        FakeTimeProvider Clock,
        RedisStore Store,
        PersistenceManager Persistence,
        CommandProcessor Processor);

    private Node Boot(bool appendOnly)
    {
        var clock = new FakeTimeProvider();
        return BootAt(clock, appendOnly);
    }

    private Node BootAt(FakeTimeProvider clock, bool appendOnly)
    {
        var stats = new RedisStats();
        var store = new RedisStore(clock, stats);
        var options = new ServerOptions
        {
            Port = 0,
            DataDirectory = _dir,
            AppendOnly = appendOnly,
            AppendFsync = AofFsyncPolicy.Always
        };
        var persistence = new PersistenceManager(options, NullLogger<PersistenceManager>.Instance);
        var processor = new CommandProcessor(store, stats, clock, persistence, new PubSubHub(), port: 0);
        persistence.Load(store, processor.ExecuteReplay);
        return new Node(clock, store, persistence, processor);
    }

    private static string Exec(Node node, params string[] args)
    {
        var output = new ArrayBufferWriter<byte>();
        node.Processor.Execute(args.Select(a => Encoding.ASCII.GetBytes(a)).ToArray(),
            new ClientContext(), output);
        return Encoding.ASCII.GetString(output.WrittenSpan);
    }

    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);
    private static string S(byte[]? bytes) => bytes is null ? "<null>" : Encoding.ASCII.GetString(bytes);

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dir))
                Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            // Scratch cleanup is best-effort.
        }
        GC.SuppressFinalize(this);
    }

    // ---------------- Snapshot ----------------

    [Fact]
    public void Snapshot_RoundTripsEveryType()
    {
        var node = Boot(appendOnly: false);
        Exec(node, "SET", "s", "hello");
        Exec(node, "RPUSH", "l", "a", "b", "c");
        Exec(node, "SADD", "set", "x", "y");
        Exec(node, "HSET", "h", "f1", "v1", "f2", "v2");
        Exec(node, "ZADD", "z", "1.5", "m1", "2.5", "m2");
        Exec(node, "SET", "ttl", "v", "EX", "3600");
        node.Persistence.SaveSnapshot(node.Store);
        node.Persistence.Dispose();

        var restarted = Boot(appendOnly: false);
        Assert.Equal("hello", S(restarted.Store.Get(B("s"))));
        Assert.Equal(new[] { "a", "b", "c" },
            restarted.Store.ListRange(B("l"), 0, -1).Select(v => S(v)).ToList());
        Assert.Equal(2, restarted.Store.SetCard(B("set")));
        Assert.Equal("v2", S(restarted.Store.HashGet(B("h"), B("f2"))));
        Assert.Equal(2.5, restarted.Store.ZScore(B("z"), B("m2")));
        Assert.InRange(restarted.Store.TtlMilliseconds(B("ttl")), 3_500_000, 3_600_000);
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Snapshot_CorruptFile_IsIgnoredAndServerStartsEmpty()
    {
        var node = Boot(appendOnly: false);
        Exec(node, "SET", "s", "hello");
        node.Persistence.SaveSnapshot(node.Store);
        node.Persistence.Dispose();

        var path = Path.Combine(_dir, "dump.rcdb");
        var bytes = File.ReadAllBytes(path);
        bytes[bytes.Length / 2] ^= 0xFF; // damage the payload: CRC must catch it
        File.WriteAllBytes(path, bytes);

        var status = SnapshotFile.TryRead(path, out _, out var error);
        Assert.Equal(SnapshotLoadStatus.Corrupt, status);
        Assert.NotNull(error);

        var restarted = Boot(appendOnly: false); // must not throw
        Assert.Equal(0, restarted.Store.Count);
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Snapshot_ExpiredEntries_AreDroppedOnLoad()
    {
        var node = Boot(appendOnly: false);
        Exec(node, "SET", "short", "v", "EX", "30");
        Exec(node, "SET", "long", "v");
        node.Persistence.SaveSnapshot(node.Store);
        node.Persistence.Dispose();

        var later = new FakeTimeProvider();
        later.Advance(TimeSpan.FromSeconds(31));
        var restarted = BootAt(later, appendOnly: false);
        Assert.Null(restarted.Store.Get(B("short")));
        Assert.Equal("v", S(restarted.Store.Get(B("long"))));
        restarted.Persistence.Dispose();
    }

    // ---------------- AOF ----------------

    [Fact]
    public void Aof_Replay_ReconstructsState_IncludingCountersAndExpiry()
    {
        var node = Boot(appendOnly: true);
        Exec(node, "SET", "name", "eslam");
        Exec(node, "INCR", "counter");
        Exec(node, "INCR", "counter");
        Exec(node, "INCR", "counter");
        Exec(node, "LPUSH", "jobs", "a", "b");
        Exec(node, "SADD", "tags", "dotnet", "redis");
        Exec(node, "HSET", "cfg", "mode", "fast");
        Exec(node, "ZADD", "board", "10", "p1");
        Exec(node, "SET", "session", "abc", "EX", "3600");
        Exec(node, "SET", "gone", "x");
        Exec(node, "DEL", "gone");
        node.Persistence.Dispose();

        var restarted = Boot(appendOnly: true);
        Assert.Equal("eslam", S(restarted.Store.Get(B("name"))));
        Assert.Equal("3", S(restarted.Store.Get(B("counter"))));
        Assert.Equal(2, restarted.Store.ListLength(B("jobs")));
        Assert.True(restarted.Store.SetIsMember(B("tags"), B("redis")));
        Assert.Equal("fast", S(restarted.Store.HashGet(B("cfg"), B("mode"))));
        Assert.Equal(10.0, restarted.Store.ZScore(B("board"), B("p1")));
        Assert.True(restarted.Store.TryGetExpiry(B("session"), out var expiry) && expiry is not null);
        Assert.Null(restarted.Store.Get(B("gone")));
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Aof_SetWithRelativeExpiry_ReplaysAsAbsoluteDeadline()
    {
        var node = Boot(appendOnly: true);
        Exec(node, "SET", "temp", "v", "EX", "30");
        node.Persistence.Dispose();

        // Restart after the deadline: had the relative "EX 30" been logged
        // verbatim, the key would wrongly come back to life for 30 more
        // seconds. The normalised PEXPIREAT keeps it dead.
        var later = new FakeTimeProvider();
        later.Advance(TimeSpan.FromSeconds(31));
        var restarted = BootAt(later, appendOnly: true);
        Assert.Null(restarted.Store.Get(B("temp")));
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Aof_Rewrite_CompactsAndPreservesFinalState()
    {
        var node = Boot(appendOnly: true);
        for (var i = 0; i < 200; i++)
            Exec(node, "SET", "hot", $"value-{i}");
        for (var i = 0; i < 200; i++)
            Exec(node, "INCR", "counter");
        Exec(node, "RPUSH", "list", "only");
        var aofPath = Path.Combine(_dir, "appendonly.aof");
        var sizeBefore = new FileInfo(aofPath).Length;

        node.Persistence.RewriteAof(node.Store);
        var sizeAfter = new FileInfo(aofPath).Length;
        Assert.True(sizeAfter < sizeBefore, $"rewrite should shrink the AOF ({sizeAfter} !< {sizeBefore})");

        Exec(node, "SET", "after", "rewrite"); // appends continue onto the new file
        node.Persistence.Dispose();

        var restarted = Boot(appendOnly: true);
        Assert.Equal("value-199", S(restarted.Store.Get(B("hot"))));
        Assert.Equal("200", S(restarted.Store.Get(B("counter"))));
        Assert.Equal("only", S(restarted.Store.ListIndex(B("list"), 0)));
        Assert.Equal("rewrite", S(restarted.Store.Get(B("after"))));
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Aof_FlushAll_ReplaysToEmpty()
    {
        var node = Boot(appendOnly: true);
        Exec(node, "SET", "a", "1");
        Exec(node, "SET", "b", "2");
        Exec(node, "FLUSHALL");
        Exec(node, "SET", "c", "3");
        node.Persistence.Dispose();

        var restarted = Boot(appendOnly: true);
        Assert.Null(restarted.Store.Get(B("a")));
        Assert.Equal("3", S(restarted.Store.Get(B("c"))));
        Assert.Equal(1, restarted.Store.Count);
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Aof_TornTail_KeepsEveryCompleteCommand()
    {
        var node = Boot(appendOnly: true);
        Exec(node, "SET", "a", "1");
        Exec(node, "SET", "b", "2");
        node.Persistence.Dispose();

        // Simulate a crash mid-append: half of a RESP array lands on disk.
        var aofPath = Path.Combine(_dir, "appendonly.aof");
        using (var stream = new FileStream(aofPath, FileMode.Append, FileAccess.Write))
        {
            var torn = Encoding.ASCII.GetBytes("*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r");
            stream.Write(torn);
        }

        var commands = AppendOnlyFile.ReadCommands(aofPath, out var truncated);
        Assert.True(truncated);
        Assert.Equal(2, commands.Count);

        var restarted = Boot(appendOnly: true);
        Assert.Equal("1", S(restarted.Store.Get(B("a"))));
        Assert.Equal("2", S(restarted.Store.Get(B("b"))));
        Assert.Null(restarted.Store.Get(B("c")));
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void BgRewriteAof_ViaProcessor_WhenDisabled_Errors()
    {
        var node = Boot(appendOnly: false);
        var reply = Exec(node, "BGREWRITEAOF");
        Assert.StartsWith("-ERR", reply);
        node.Persistence.Dispose();
    }
}
