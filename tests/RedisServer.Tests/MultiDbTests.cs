using System.Buffers;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Persistence;
using RedisServer.Protocol;
using RedisServer.Replication;

namespace RedisServer.Tests;

/// <summary>
/// Logical databases: SELECT isolation, per-DB DBSIZE, scoped FLUSHDB,
/// MOVE / SWAPDB, and the multi-DB contracts of the durable surfaces —
/// the effect stream frames effects with SELECT (byte-identical for
/// database-0-only workloads), snapshots are format version 2 with
/// version 1 still loadable, and replication applies per-DB effects to
/// the same database on the replica, partial resyncs included.
/// Error texts probed against redis-server 7.0.15.
/// </summary>
public class MultiDbTests
{
    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);

    [Fact]
    public async Task Select_IsolatesKeyspaces_PerConnection()
    {
        await using var h = SessionHarness.Start();
        var a = h.Connect();
        var b = h.Connect();
        await a.SendCommandAsync("SET", "k", "in-zero");
        Assert.Equal("OK", await a.ReadReplyAsync());
        await b.SendCommandAsync("SELECT", "5");
        Assert.Equal("OK", await b.ReadReplyAsync());
        await b.SendCommandAsync("GET", "k");
        Assert.Null(await b.ReadReplyAsync());
        await b.SendCommandAsync("SET", "k", "in-five");
        Assert.Equal("OK", await b.ReadReplyAsync());
        await a.SendCommandAsync("GET", "k");
        Assert.Equal("in-zero", await a.ReadReplyAsync());
        await b.SendCommandAsync("GET", "k");
        Assert.Equal("in-five", await b.ReadReplyAsync());

        await a.SendCommandAsync("DBSIZE");
        Assert.Equal(1L, await a.ReadReplyAsync());
        await b.SendCommandAsync("DBSIZE");
        Assert.Equal(1L, await b.ReadReplyAsync());
        await b.SendCommandAsync("SELECT", "6");
        Assert.Equal("OK", await b.ReadReplyAsync());
        await b.SendCommandAsync("DBSIZE");
        Assert.Equal(0L, await b.ReadReplyAsync());
    }

    [Fact]
    public async Task Select_IndexErrors_MatchRedis()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SELECT", "16");
        Assert.Equal(new TestClient.RespError("ERR DB index is out of range"), await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "-1");
        Assert.Equal(new TestClient.RespError("ERR DB index is out of range"), await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "abc");
        Assert.Equal(new TestClient.RespError("ERR value is not an integer or out of range"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "15");
        Assert.Equal("OK", await c.ReadReplyAsync());
    }

    [Fact]
    public async Task Collections_WorkInNonZeroDatabases()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SELECT", "4");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("RPUSH", "l", "a", "b", "c");
        Assert.Equal(3L, await c.ReadReplyAsync());
        await c.SendCommandAsync("RPOP", "l");
        Assert.Equal("c", await c.ReadReplyAsync());
        await c.SendCommandAsync("LPOP", "l");
        Assert.Equal("a", await c.ReadReplyAsync());
        await c.SendCommandAsync("HSET", "h", "f", "v");
        Assert.Equal(1L, await c.ReadReplyAsync());
        await c.SendCommandAsync("HGET", "h", "f");
        Assert.Equal("v", await c.ReadReplyAsync());
        await c.SendCommandAsync("SADD", "s", "m");
        Assert.Equal(1L, await c.ReadReplyAsync());
        await c.SendCommandAsync("ZADD", "z", "1.5", "m");
        Assert.Equal(1L, await c.ReadReplyAsync());
        await c.SendCommandAsync("ZSCORE", "z", "m");
        Assert.Equal("1.5", await c.ReadReplyAsync());
        // None of it leaked into database 0.
        await c.SendCommandAsync("SELECT", "0");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("DBSIZE");
        Assert.Equal(0L, await c.ReadReplyAsync());
    }

    [Fact]
    public async Task FlushDb_IsScoped_FlushAll_IsGlobal()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "a", "1");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "3");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "b", "2");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("FLUSHDB");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("DBSIZE");
        Assert.Equal(0L, await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "0");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "a");
        Assert.Equal("1", await c.ReadReplyAsync());
        await c.SendCommandAsync("FLUSHALL");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("DBSIZE");
        Assert.Equal(0L, await c.ReadReplyAsync());
    }

    [Fact]
    public async Task Move_Semantics()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "k", "v", "PX", "60000");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("MOVE", "k", "0");
        Assert.Equal(new TestClient.RespError("ERR source and destination objects are the same"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("MOVE", "ghost", "4");
        Assert.Equal(0L, await c.ReadReplyAsync());
        await c.SendCommandAsync("MOVE", "k", "4");
        Assert.Equal(1L, await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "k");
        Assert.Null(await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "4");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "k");
        Assert.Equal("v", await c.ReadReplyAsync());
        // The expiry travels with the entry.
        await c.SendCommandAsync("PTTL", "k");
        var ttl = Assert.IsType<long>(await c.ReadReplyAsync());
        Assert.True(ttl > 0 && ttl <= 60000, $"ttl was {ttl}");
        // An occupied target refuses the move.
        await c.SendCommandAsync("SELECT", "0");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "k", "other");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("MOVE", "k", "4");
        Assert.Equal(0L, await c.ReadReplyAsync());
        await c.SendCommandAsync("MOVE", "k", "99");
        Assert.Equal(new TestClient.RespError("ERR DB index is out of range"), await c.ReadReplyAsync());
    }

    [Fact]
    public async Task SwapDb_ExchangesKeyspaces()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "k", "zero");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "2");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "k", "two");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SWAPDB", "0", "2");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "k"); // still in db 2, which now holds db 0's data
        Assert.Equal("zero", await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "0");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "k");
        Assert.Equal("two", await c.ReadReplyAsync());
        await c.SendCommandAsync("SWAPDB", "0", "16");
        Assert.Equal(new TestClient.RespError("ERR DB index is out of range"), await c.ReadReplyAsync());
    }

    [Fact]
    public async Task Info_Keyspace_ListsOnlyNonEmptyDbs()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "a", "1");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "7");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "b", "2", "PX", "60000");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("INFO", "keyspace");
        var info = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.Contains("db0:keys=1,expires=0", info);
        Assert.Contains("db7:keys=1,expires=1", info);
        Assert.DoesNotContain("db3:", info);
    }

    [Fact]
    public void Aof_FramesNonZeroDbEffectsWithSelect_AndReloadLandsThemRight()
    {
        var dir = Path.Combine(Path.GetTempPath(), "redis-csharp-multidb", Guid.NewGuid().ToString("N"));
        try
        {
            var first = Boot(dir);
            // Execute with explicit contexts: one write in database 5,
            // one in database 0.
            var output = new ArrayBufferWriter<byte>();
            first.Processor.Execute(new[] { B("SET"), B("deep"), B("5") },
                new ClientContext { SelectedDb = 5 }, output);
            first.Processor.Execute(new[] { B("SET"), B("root"), B("0") },
                new ClientContext(), new ArrayBufferWriter<byte>());
            first.Persistence.Dispose();

            var aof = AppendOnlyFile.ReadCommands(Path.Combine(dir, "appendonly.aof"), out var truncated);
            Assert.False(truncated);
            var names = aof.Select(cmd => Encoding.ASCII.GetString(cmd[0])).ToList();
            Assert.Contains("SELECT", names);

            var second = Boot(dir);
            Assert.Equal("5", Encoding.ASCII.GetString(second.Store.Get(B("deep"), 5)!));
            Assert.Null(second.Store.Get(B("deep"), 0));
            Assert.Equal("0", Encoding.ASCII.GetString(second.Store.Get(B("root"), 0)!));
            second.Persistence.Dispose();
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Snapshot_V2_RoundTripsDatabases_V1_LoadsAsDatabaseZero()
    {
        var clock = new FakeTimeProvider();
        var store = new RedisStore(clock, new RedisStats());
        store.Set(B("a"), B("1"), SetCondition.Always, null, false, db: 0);
        store.Set(B("b"), B("2"), SetCondition.Always, null, false, db: 9);
        var bytes = SnapshotFile.Serialize(store.ExportState());

        Assert.Equal(SnapshotLoadStatus.Loaded, SnapshotFile.TryParse(bytes, out var entries, out _));
        Assert.Equal(0, entries.Single(e => Encoding.ASCII.GetString(e.Key) == "a").Db);
        Assert.Equal(9, entries.Single(e => Encoding.ASCII.GetString(e.Key) == "b").Db);

        var restored = new RedisStore(clock, new RedisStats());
        foreach (var entry in entries)
            restored.ImportEntry(entry);
        Assert.Equal("2", Encoding.ASCII.GetString(restored.Get(B("b"), 9)!));
        Assert.Null(restored.Get(B("b"), 0));

        // A hand-built version-1 snapshot (no per-entry database byte)
        // still parses, with every entry in database 0.
        var v1 = BuildV1Snapshot(B("old"), B("oldval"));
        Assert.Equal(SnapshotLoadStatus.Loaded, SnapshotFile.TryParse(v1, out var v1Entries, out _));
        var only = Assert.Single(v1Entries);
        Assert.Equal(0, only.Db);
        Assert.Equal("old", Encoding.ASCII.GetString(only.Key));
        Assert.Equal("oldval", Encoding.ASCII.GetString((byte[])only.Value));
    }

    [Fact]
    public async Task Replication_PerDbEffects_LandInTheSameDb_PartialResyncIncluded()
    {
        await using var primary = SessionHarness.Start();
        await using var replica = SessionHarness.Start();
        var c = primary.Connect();
        await c.SendCommandAsync("SELECT", "5");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "k", "five");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "0");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "z", "zero");
        Assert.Equal("OK", await c.ReadReplyAsync());

        // Full stream from offset 0: SELECT 5, SET k, SELECT 0, SET z.
        Assert.Equal(SyncKind.Continue,
            primary.Replication.DecideSync(primary.Replication.ReplId, 0, out var slice));
        var frames = slice!;
        Assert.Equal(4, frames.Count);
        // The replica applies only the first two frames, then "drops".
        long appliedBytes = 0;
        foreach (var frame in frames.Take(2))
        {
            Assert.True(replica.Processor.ApplyReplicationFrame(ParseFrame(frame), frame));
            appliedBytes += frame.Length;
        }
        Assert.Equal("five", Encoding.ASCII.GetString(replica.Store.Get(B("k"), 5)!));
        Assert.Equal(5, replica.Replication.FollowDb);

        // More primary writes in a third database, then a partial resync
        // from the replica's exact position.
        await c.SendCommandAsync("SELECT", "9");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "q", "nine");
        Assert.Equal("OK", await c.ReadReplyAsync());
        Assert.Equal(SyncKind.Continue,
            primary.Replication.DecideSync(primary.Replication.ReplId, appliedBytes, out var rest));
        foreach (var frame in rest!)
            Assert.True(replica.Processor.ApplyReplicationFrame(ParseFrame(frame), frame));

        Assert.Equal("zero", Encoding.ASCII.GetString(replica.Store.Get(B("z"), 0)!));
        Assert.Equal("nine", Encoding.ASCII.GetString(replica.Store.Get(B("q"), 9)!));
        Assert.Null(replica.Store.Get(B("q"), 0));
        Assert.Null(replica.Store.Get(B("k"), 0));
        Assert.Equal(9, replica.Replication.FollowDb);
    }

    [Fact]
    public async Task Eviction_RemovesTheVictimFromItsOwnDatabase()
    {
        await using var h = SessionHarness.Start(appendOnly: true,
            maxMemory: 1000, maxMemoryPolicy: MaxMemoryPolicy.AllKeysLru);
        var big = new string('x', 400); // ~465 bytes per entry
        var c = h.Connect();
        await c.SendCommandAsync("SELECT", "7");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "victim", big);
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SELECT", "0");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "a", big);
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SET", "b", big); // lands over the cap at ~1405 bytes
        Assert.Equal("OK", await c.ReadReplyAsync());
        // Eviction runs before the next command: it evicts the LRU key,
        // which is db 7's victim (written first, never touched since).
        await c.SendCommandAsync("GET", "a");
        Assert.Equal(big, await c.ReadReplyAsync());
        Assert.True(h.Stats.EvictedKeys >= 1);
        Assert.Null(h.Store.Get(B("victim"), 7));
        // The eviction DEL is framed into database 7 in the AOF, ahead of
        // the write that triggered it.
        var aof = AppendOnlyFile.ReadCommands(
            Path.Combine(h.DataDirectory, "appendonly.aof"), out _);
        var delIndex = aof.ToList().FindIndex(cmd => Encoding.ASCII.GetString(cmd[0]) == "DEL");
        Assert.True(delIndex > 0);
        Assert.Equal("SELECT", Encoding.ASCII.GetString(aof[delIndex - 1][0]));
        Assert.Equal("7", Encoding.ASCII.GetString(aof[delIndex - 1][1]));
        Assert.Equal("victim", Encoding.ASCII.GetString(aof[delIndex][1]));
    }

    // ------------------------------------------------------------------

    private sealed record Node(RedisStore Store, PersistenceManager Persistence, CommandProcessor Processor);

    private static Node Boot(string dir)
    {
        var clock = new FakeTimeProvider();
        var stats = new RedisStats();
        var store = new RedisStore(clock, stats);
        var options = new ServerOptions
        {
            Port = 0,
            DataDirectory = dir,
            AppendOnly = true,
            AppendFsync = AofFsyncPolicy.Always
        };
        var persistence = new PersistenceManager(options, NullLogger<PersistenceManager>.Instance);
        var processor = new CommandProcessor(store, stats, clock, persistence, new PubSubHub(), port: 0);
        persistence.Load(store, processor.ExecuteReplay);
        return new Node(store, persistence, processor);
    }

    private static byte[][] ParseFrame(byte[] frame)
    {
        var sequence = new ReadOnlySequence<byte>(frame);
        Assert.True(RespParser.TryReadCommand(ref sequence, out var args, out var error), error);
        return args!;
    }

    private static byte[] BuildV1Snapshot(byte[] key, byte[] value)
    {
        using var buffer = new MemoryStream();
        using (var writer = new BinaryWriter(buffer, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write("RCDB"u8.ToArray());
            writer.Write(1u); // format version 1: no database byte
            writer.Write((byte)1); // RedisType.String tag
            writer.Write(false); // no expiry
            writer.Write(key.Length);
            writer.Write(key);
            writer.Write(value.Length);
            writer.Write(value);
            writer.Write((byte)0xFF);
        }
        var payload = buffer.ToArray();
        var crc = Crc32.Compute(payload);
        var bytes = new byte[payload.Length + 4];
        payload.CopyTo(bytes, 0);
        BitConverter.GetBytes(crc).CopyTo(bytes, payload.Length);
        return bytes;
    }
}
