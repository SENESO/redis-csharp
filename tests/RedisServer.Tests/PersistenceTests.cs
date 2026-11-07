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

    [Fact]
    public void Snapshot_PreservesExpiry_ForEveryType()
    {
        var node = Boot(appendOnly: false);
        Exec(node, "SET", "str", "v");
        Exec(node, "RPUSH", "list", "a");
        Exec(node, "SADD", "set", "m");
        Exec(node, "HSET", "hash", "f", "v");
        Exec(node, "ZADD", "zset", "1", "m");
        foreach (var key in new[] { "str", "list", "set", "hash", "zset" })
            Assert.Equal(":1\r\n", Exec(node, "EXPIRE", key, "3600"));
        node.Persistence.SaveSnapshot(node.Store);
        node.Persistence.Dispose();

        var restarted = Boot(appendOnly: false);
        Assert.Equal(5, restarted.Store.Count);
        foreach (var key in new[] { "str", "list", "set", "hash", "zset" })
            Assert.InRange(restarted.Store.TtlMilliseconds(B(key)), 3_500_000, 3_600_000);
        Assert.Equal("a", S(restarted.Store.ListIndex(B("list"), 0)));
        Assert.Equal("v", S(restarted.Store.HashGet(B("hash"), B("f"))));
        Assert.Equal(1.0, restarted.Store.ZScore(B("zset"), B("m")));

        // And the deadlines still bite after the restart.
        restarted.Clock.Advance(TimeSpan.FromSeconds(3601));
        restarted.Store.RunMaintenance();
        Assert.Equal(0, restarted.Store.Count);
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Aof_PreservesExpiry_OnCollectionKeys()
    {
        var node = Boot(appendOnly: true);
        Exec(node, "RPUSH", "list", "a", "b");
        Assert.Equal(":1\r\n", Exec(node, "EXPIRE", "list", "3600"));
        Exec(node, "HSET", "hash", "f", "v");
        var deadlineMs = node.Clock.GetUtcNow().AddHours(2).ToUnixTimeMilliseconds();
        Assert.Equal(":1\r\n", Exec(node, "PEXPIREAT", "hash", deadlineMs.ToString()));
        node.Persistence.Dispose();

        var restarted = Boot(appendOnly: true);
        Assert.Equal(2, restarted.Store.ListLength(B("list")));
        Assert.True(restarted.Store.TryGetExpiry(B("list"), out var listExpiry) && listExpiry is not null);
        Assert.True(restarted.Store.TryGetExpiry(B("hash"), out var hashExpiry) && hashExpiry is not null);
        Assert.Equal("v", S(restarted.Store.HashGet(B("hash"), B("f"))));
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Aof_WinsOverSnapshot_WhenBothPresent()
    {
        // State 1: only a snapshot exists.
        var first = Boot(appendOnly: false);
        Exec(first, "SET", "snap-only", "1");
        Assert.Equal("+OK\r\n", Exec(first, "SAVE"));
        first.Persistence.Dispose();

        // State 2: booted with the AOF enabled (the snapshot loads, since
        // no AOF exists yet), then a write lands in the AOF.
        var second = Boot(appendOnly: true);
        Exec(second, "SET", "aof-only", "2");
        second.Persistence.Dispose();

        // With the AOF enabled it is the source of truth: it replays from
        // empty, so the snapshot's key does not come back.
        var third = Boot(appendOnly: true);
        Assert.Equal("2", S(third.Store.Get(B("aof-only"))));
        Assert.Null(third.Store.Get(B("snap-only")));
        Assert.Equal(1, third.Store.Count);
        third.Persistence.Dispose();

        // Without the AOF, the snapshot is still there and still loads.
        var fourth = Boot(appendOnly: false);
        Assert.Equal("1", S(fourth.Store.Get(B("snap-only"))));
        Assert.Null(fourth.Store.Get(B("aof-only")));
        fourth.Persistence.Dispose();
    }

    [Fact]
    public void Aof_Rewrite_PreservesExpiries()
    {
        var node = Boot(appendOnly: true);
        Exec(node, "SET", "temp", "v", "EX", "3600");
        Exec(node, "SET", "stable", "v");
        var reply = Exec(node, "BGREWRITEAOF");
        Assert.StartsWith("+Background append only file rewriting started", reply);
        node.Persistence.Dispose();

        var restarted = Boot(appendOnly: true);
        Assert.Equal("v", S(restarted.Store.Get(B("temp"))));
        Assert.InRange(restarted.Store.TtlMilliseconds(B("temp")), 3_500_000, 3_600_000);
        Assert.Equal("v", S(restarted.Store.Get(B("stable"))));
        Assert.Equal(-1, restarted.Store.TtlMilliseconds(B("stable")));
        restarted.Persistence.Dispose();
    }

    [Fact]
    public void Save_AndBgSave_ViaCommands_WriteTheSnapshotFile()
    {
        var node = Boot(appendOnly: false);
        Exec(node, "SET", "k", "v");
        var path = Path.Combine(_dir, "dump.rcdb");

        Assert.Equal("+OK\r\n", Exec(node, "SAVE"));
        Assert.True(File.Exists(path));

        File.Delete(path);
        Assert.Equal("+Background saving started\r\n", Exec(node, "BGSAVE"));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!File.Exists(path) && DateTime.UtcNow < deadline)
            Thread.Sleep(50);
        Assert.True(File.Exists(path));
        node.Persistence.Dispose();
    }

    [Fact]
    public async Task Aof_Rewrite_UnderConcurrentWrites_ReplaysToExactState()
    {
        var clock = new FakeTimeProvider();
        var stats = new RedisStats();
        var store = new RedisStore(clock, stats);
        var options = new ServerOptions
        {
            Port = 0,
            DataDirectory = _dir,
            AppendOnly = true,
            AppendFsync = AofFsyncPolicy.Always
        };
        var persistence = new PersistenceManager(options, NullLogger<PersistenceManager>.Instance);
        var processor = new CommandProcessor(store, stats, clock, persistence, new PubSubHub(), port: 0);

        const int workers = 4, perWorker = 150;
        var tasks = Enumerable.Range(0, workers).Select(w => Task.Run(() =>
        {
            var ctx = new ClientContext();
            var sink = new ArrayBufferWriter<byte>();
            for (var i = 0; i < perWorker; i++)
            {
                processor.Execute(new[] { B("SET"), B($"w{w}:k{i}"), B($"v{i}") }, ctx, sink);
                processor.Execute(new[] { B("INCR"), B("shared") }, ctx, sink);
            }
        })).ToArray();

        // Rewrite repeatedly while the writers are mid-flight: appends
        // racing the file swap must not be lost or duplicated.
        var rewrites = 0;
        while (!Task.WhenAll(tasks).IsCompleted && rewrites < 200)
        {
            var sink = new ArrayBufferWriter<byte>();
            processor.Execute(new[] { B("BGREWRITEAOF") }, new ClientContext(), sink);
            rewrites++;
            await Task.Delay(1);
        }
        await Task.WhenAll(tasks);
        Assert.True(rewrites > 0);
        persistence.Dispose();

        var stats2 = new RedisStats();
        var store2 = new RedisStore(clock, stats2);
        var persistence2 = new PersistenceManager(options, NullLogger<PersistenceManager>.Instance);
        var processor2 = new CommandProcessor(store2, stats2, clock, persistence2, new PubSubHub(), port: 0);
        persistence2.Load(store2, processor2.ExecuteReplay);
        persistence2.Dispose();

        Assert.Equal((workers * perWorker).ToString(), S(store2.Get(B("shared"))));
        Assert.Equal(workers * perWorker + 1, store2.Count);
        for (var w = 0; w < workers; w++)
            Assert.Equal($"v{perWorker - 1}", S(store2.Get(B($"w{w}:k{perWorker - 1}"))));
    }
}
