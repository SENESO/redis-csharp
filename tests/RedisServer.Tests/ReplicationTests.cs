using System.Text;
using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Persistence;
using RedisServer.Protocol;
using RedisServer.Replication;

namespace RedisServer.Tests;

/// <summary>
/// Replication, as far as one process can prove it: the backlog/offset
/// bookkeeping and the full-vs-partial decision at the manager level;
/// PSYNC framing over real (in-memory) sessions — the FULLRESYNC header,
/// the snapshot payload, CONTINUE with exactly the missed frames;
/// read-only enforcement with Redis's exact READONLY text across the
/// true write set; and the replica-side apply path (a propagated frame
/// lands in the store, in the replica's own AOF, and in its served
/// stream). The two-process live proof runs on the owner's machine
/// (tools/replica-proof.py) — outbound TCP is blocked in the sandbox
/// these tests run in, so the sync loop itself is exercised there.
/// </summary>
public sealed class ReplicationTests
{
    private static byte[] Frame(int size) => new byte[size];

    // ------------------------------------------------------------------
    // Backlog and the sync decision
    // ------------------------------------------------------------------

    [Fact]
    public void Backlog_OffsetAdvancesByFrameBytes_AndTrimsPastTheCap()
    {
        var manager = new ReplicationManager(backlogSizeBytes: 1024);
        manager.AppendServedFrame(Frame(600));
        Assert.Equal(600, manager.Offset);
        Assert.Equal(0, manager.BacklogFirstByteOffset);

        manager.AppendServedFrame(Frame(600));
        // 1200 > 1024: the oldest frame is dropped, the window slides.
        Assert.Equal(1200, manager.Offset);
        Assert.Equal(600, manager.BacklogFirstByteOffset);
        Assert.Equal(600, manager.BacklogHistoryLength);

        manager.AppendServedFrame(Frame(600));
        Assert.Equal(1800, manager.Offset);
        Assert.Equal(1200, manager.BacklogFirstByteOffset);
    }

    [Fact]
    public void DecideSync_PartialOnlyInsideTheBacklogWindow()
    {
        var manager = new ReplicationManager(backlogSizeBytes: 1024);
        var first = Frame(600);
        var second = Frame(600);
        var third = Frame(600);
        manager.AppendServedFrame(first);
        manager.AppendServedFrame(second);
        manager.AppendServedFrame(third); // window now starts at 1200

        // Inside the window: exactly the frames ending after the offset.
        Assert.Equal(SyncKind.Continue, manager.DecideSync(manager.ReplId, 1200, out var slice));
        Assert.NotNull(slice);
        Assert.Single(slice);
        Assert.Same(third, slice[0]);

        // Fully caught up: Continue with nothing to send.
        Assert.Equal(SyncKind.Continue, manager.DecideSync(manager.ReplId, 1800, out var empty));
        Assert.NotNull(empty);
        Assert.Empty(empty);

        // Below the window, ahead of the stream, a foreign id, and the
        // unknown-identity form all fall back to a full sync.
        Assert.Equal(SyncKind.Full, manager.DecideSync(manager.ReplId, 1199, out _));
        Assert.Equal(SyncKind.Full, manager.DecideSync(manager.ReplId, 1801, out _));
        Assert.Equal(SyncKind.Full, manager.DecideSync("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1800, out _));
        Assert.Equal(SyncKind.Full, manager.DecideSync("?", -1, out _));
    }

    [Fact]
    public void AdoptSyncedStream_ResetsTheBacklogAtTheAdoptedOffset()
    {
        var manager = new ReplicationManager(backlogSizeBytes: 4096);
        manager.AppendServedFrame(Frame(100));
        manager.AdoptSyncedStream("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 5000);

        Assert.Equal("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", manager.ReplId);
        Assert.Equal(5000, manager.Offset);
        Assert.Equal(5000, manager.BacklogFirstByteOffset);
        Assert.Equal(SyncKind.Continue, manager.DecideSync(manager.ReplId, 5000, out var slice));
        Assert.Empty(slice!);
        // The pre-adoption stream is gone: its offsets mean nothing now.
        Assert.Equal(SyncKind.Full, manager.DecideSync(manager.ReplId, 100, out _));
    }

    // ------------------------------------------------------------------
    // Options
    // ------------------------------------------------------------------

    [Fact]
    public void Options_ReplicationFlags_Parse()
    {
        var options = ServerOptions.FromArgs(new[]
        {
            "--replicaof", "primary.internal", "6390",
            "--masterauth", "s3cret",
            "--repl-backlog-size", "2mb"
        });
        Assert.Equal("primary.internal", options.ReplicaOfHost);
        Assert.Equal(6390, options.ReplicaOfPort);
        Assert.Equal("s3cret", options.MasterAuth);
        Assert.Equal(2L * 1024 * 1024, options.ReplBacklogSizeBytes);
    }

    [Fact]
    public void Options_ReplicaOf_WithoutAPort_FailsLoudly()
    {
        Assert.Throws<ArgumentException>(() => ServerOptions.FromArgs(new[] { "--replicaof", "only-a-host" }));
        Assert.Throws<ArgumentException>(() => ServerOptions.FromArgs(new[] { "--replicaof", "host", "99999" }));
    }

    // ------------------------------------------------------------------
    // PSYNC over sessions
    // ------------------------------------------------------------------

    [Fact]
    public async Task Psync_FullSync_ShipsTheSnapshotAtTheCurrentOffset_ThenLiveFrames()
    {
        await using var h = SessionHarness.Start();
        var writer = h.Connect();
        await writer.SendCommandAsync("SET", "a", "1");
        await writer.ExpectAsync("+OK\r\n");
        await writer.SendCommandAsync("LPUSH", "l", "x", "y");
        await writer.ExpectAsync(":2\r\n");
        var offsetBefore = h.Replication.Offset;
        Assert.True(offsetBefore > 0);

        var link = h.Connect();
        await link.SendCommandAsync("REPLCONF", "listening-port", "6399");
        await link.ExpectAsync("+OK\r\n");
        await link.SendCommandAsync("PSYNC", "?", "-1");

        var header = await link.ReadLineAsync();
        Assert.Equal($"+FULLRESYNC {h.Replication.ReplId} {offsetBefore}", header);
        var bulkHeader = await link.ReadLineAsync();
        Assert.StartsWith("$", bulkHeader);
        var length = int.Parse(bulkHeader[1..]);
        var snapshot = await link.ReadBytesAsync(length);
        Assert.Equal(SnapshotLoadStatus.Loaded, SnapshotFile.TryParse(snapshot, out var entries, out _));
        Assert.Equal(2, entries.Count);
        Assert.Contains(entries, e => Encoding.ASCII.GetString(e.Key) == "a");
        Assert.Contains(entries, e => Encoding.ASCII.GetString(e.Key) == "l");

        // Writes after the sync propagate as frames, in order.
        await writer.SendCommandAsync("SET", "b", "2");
        await writer.ExpectAsync("+OK\r\n");
        var frame = await link.ReadReplyAsync() as List<object?>;
        Assert.Equal(new List<object?> { "SET", "b", "2" }, frame);
    }

    [Fact]
    public async Task Psync_Info_CountsTheLink()
    {
        await using var h = SessionHarness.Start();
        var writer = h.Connect();
        var link = h.Connect();
        await link.SendCommandAsync("PSYNC", "?", "-1");
        _ = await link.ReadLineAsync(); // FULLRESYNC header
        var bulkHeader = await link.ReadLineAsync();
        _ = await link.ReadBytesAsync(int.Parse(bulkHeader[1..]));

        await writer.SendCommandAsync("INFO", "replication");
        var info = (string)(await writer.ReadReplyAsync())!;
        Assert.Contains("role:master", info);
        Assert.Contains("connected_slaves:1", info);
        Assert.Contains($"master_replid:{h.Replication.ReplId}", info);
        Assert.Contains($"master_repl_offset:{h.Replication.Offset}", info);
    }

    [Fact]
    public async Task Psync_PartialResync_SendsExactlyTheMissedFrames()
    {
        await using var h = SessionHarness.Start();
        var writer = h.Connect();
        await writer.SendCommandAsync("SET", "k1", "v1");
        await writer.ExpectAsync("+OK\r\n");
        var caughtUp = h.Replication.Offset;

        // A replica that is fully caught up gets CONTINUE and silence.
        var link1 = h.Connect();
        await link1.SendCommandAsync("PSYNC", h.Replication.ReplId, caughtUp.ToString());
        Assert.Equal("+CONTINUE", await link1.ReadLineAsync());
        await link1.ExpectNoReplyAsync(300);

        await writer.SendCommandAsync("SET", "k2", "v2");
        await writer.ExpectAsync("+OK\r\n");
        await writer.SendCommandAsync("INCR", "n");
        await writer.ExpectAsync(":1\r\n");

        // A second replica reconnecting at the old offset gets exactly
        // the two frames it missed, in order, and nothing else.
        var link2 = h.Connect();
        await link2.SendCommandAsync("PSYNC", h.Replication.ReplId, caughtUp.ToString());
        Assert.Equal("+CONTINUE", await link2.ReadLineAsync());
        Assert.Equal(new List<object?> { "SET", "k2", "v2" }, await link2.ReadReplyAsync() as List<object?>);
        Assert.Equal(new List<object?> { "INCR", "n" }, await link2.ReadReplyAsync() as List<object?>);
        await link2.ExpectNoReplyAsync(300);
    }

    [Fact]
    public async Task Psync_OffsetBelowTheBacklogWindow_GetsAFullSync()
    {
        await using var h = SessionHarness.Start(replBacklogSizeBytes: 1024);
        var writer = h.Connect();
        for (var i = 0; i < 8; i++)
        {
            await writer.SendCommandAsync("SET", $"big{i}", new string('v', 200));
            await writer.ExpectAsync("+OK\r\n");
        }
        Assert.True(h.Replication.BacklogFirstByteOffset > 0);

        var link = h.Connect();
        await link.SendCommandAsync("PSYNC", h.Replication.ReplId, "0");
        var header = await link.ReadLineAsync();
        Assert.StartsWith("+FULLRESYNC", header);
    }

    [Fact]
    public async Task Propagation_StreamMatchesTheAofEffectStream_InOrder()
    {
        await using var h = SessionHarness.Start(appendOnly: true);
        var link = h.Connect();
        await link.SendCommandAsync("PSYNC", "?", "-1");
        _ = await link.ReadLineAsync();
        var bulkHeader = await link.ReadLineAsync();
        _ = await link.ReadBytesAsync(int.Parse(bulkHeader[1..]));

        var writer = h.Connect();
        await writer.SendCommandAsync("SET", "s", "v");
        await writer.ExpectAsync("+OK\r\n");
        await writer.SendCommandAsync("INCR", "counter");
        await writer.ExpectAsync(":1\r\n");
        await writer.SendCommandAsync("LPUSH", "jobs", "a", "b");
        await writer.ExpectAsync(":2\r\n");
        await writer.SendCommandAsync("SET", "temp", "x", "EX", "600");
        await writer.ExpectAsync("+OK\r\n");
        await writer.SendCommandAsync("DEL", "s");
        await writer.ExpectAsync(":1\r\n");

        var aof = AppendOnlyFile.ReadCommands(
            Path.Combine(h.DataDirectory, "appendonly.aof"), out var truncated);
        Assert.False(truncated);
        Assert.Equal(6, aof.Count); // SET, INCR, LPUSH, SET, PEXPIREAT, DEL

        foreach (var expected in aof)
        {
            var frame = await link.ReadReplyAsync() as List<object?>;
            Assert.NotNull(frame);
            Assert.Equal(expected.Length, frame.Count);
            for (var i = 0; i < expected.Length; i++)
                Assert.Equal(Encoding.ASCII.GetString(expected[i]), frame[i]);
        }
        await link.ExpectNoReplyAsync(300);
    }

    // ------------------------------------------------------------------
    // Read-only enforcement
    // ------------------------------------------------------------------

    [Fact]
    public async Task Replica_RefusesWritesWithTheExactReadonlyError_ButServesReads()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "kept", "v");
        await c.ExpectAsync("+OK\r\n");

        await c.SendCommandAsync("REPLICAOF", "127.0.0.1", "6399");
        await c.ExpectAsync("+OK\r\n");

        const string readonlyError = "-READONLY You can't write against a read only replica.\r\n";
        // The true write set — including commands the deny-OOM set does
        // not contain (DEL, LPOP, the EXPIRE family, PERSIST, FLUSHALL).
        foreach (var command in new[]
                 {
                     new[] { "SET", "x", "1" }, new[] { "INCR", "n" }, new[] { "DEL", "kept" },
                     new[] { "LPUSH", "l", "e" }, new[] { "LPOP", "l" }, new[] { "SREM", "s", "m" },
                     new[] { "HDEL", "h", "f" }, new[] { "ZREM", "z", "m" },
                     new[] { "EXPIRE", "kept", "10" }, new[] { "PEXPIREAT", "kept", "1" },
                     new[] { "PERSIST", "kept" }, new[] { "FLUSHALL" }
                 })
        {
            await c.SendCommandAsync(command);
            await c.ExpectAsync(readonlyError);
        }

        // Reads and admin commands are untouched by the refusal.
        await c.SendCommandAsync("GET", "kept");
        await c.ExpectAsync("$1\r\nv\r\n");
        await c.SendCommandAsync("PUBLISH", "chan", "msg");
        await c.ExpectAsync(":0\r\n");
        await c.SendCommandAsync("SAVE");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("INFO", "replication");
        var info = (string)(await c.ReadReplyAsync())!;
        Assert.Contains("role:slave", info);
        Assert.Contains("master_host:127.0.0.1", info);
        Assert.Contains("master_port:6399", info);
        Assert.Contains("master_link_status:down", info);

        // A write queued in a transaction is judged at EXEC, inside the array.
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "x", "1");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("EXEC");
        var execReply = await c.ReadReplyAsync() as List<object?>;
        Assert.NotNull(execReply);
        Assert.Single(execReply);
        var error = Assert.IsType<TestClient.RespError>(execReply[0]);
        Assert.Equal("READONLY You can't write against a read only replica.", error.Text);

        // Promotion restores writability immediately.
        await c.SendCommandAsync("REPLICAOF", "NO", "ONE");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "x", "1");
        await c.ExpectAsync("+OK\r\n");
    }

    [Fact]
    public async Task ReplicaOf_ValidationErrors()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("REPLICAOF", "example.com", "notaport");
        await c.ExpectAsync("-ERR Invalid port\r\n");
        await c.SendCommandAsync("REPLICAOF", "example.com", "0");
        await c.ExpectAsync("-ERR Invalid port\r\n");
        // NO ONE on a primary is a no-op success, as in Redis.
        await c.SendCommandAsync("REPLICAOF", "NO", "ONE");
        await c.ExpectAsync("+OK\r\n");
        Assert.False(h.Replication.IsReplica);
    }

    // ------------------------------------------------------------------
    // The replica-side apply path (driven directly: no network needed)
    // ------------------------------------------------------------------

    [Fact]
    public async Task ApplyReplicationFrame_LandsInTheStore_TheReplicaAof_AndTheServedStream()
    {
        await using var h = SessionHarness.Start(appendOnly: true);
        var args = new[] { SessionHarness.B("SET"), SessionHarness.B("synced"), SessionHarness.B("yes") };
        var raw = RespWriter.EncodeCommand(args);

        Assert.True(h.Processor.ApplyReplicationFrame(args, raw));
        Assert.Equal("yes", Encoding.ASCII.GetString(h.Store.Get(SessionHarness.B("synced"))!));
        Assert.Equal(raw.Length, h.Replication.Offset);
        Assert.Equal(raw.Length, h.Replication.FollowOffset);

        // The persistence decision, distinguishable on disk: the frame is
        // in the replica's own AOF verbatim, so a replica restart keeps it.
        var aof = AppendOnlyFile.ReadCommands(
            Path.Combine(h.DataDirectory, "appendonly.aof"), out var truncated);
        Assert.False(truncated);
        var only = Assert.Single(aof);
        Assert.Equal(3, only.Length);
        Assert.Equal("SET", Encoding.ASCII.GetString(only[0]));
        Assert.Equal("synced", Encoding.ASCII.GetString(only[1]));

        // And it is served onward: a downstream PSYNC at offset 0 (still
        // inside the backlog) is a CONTINUE, not a full sync.
        Assert.Equal(SyncKind.Continue,
            h.Replication.DecideSync(h.Replication.ReplId, 0, out var slice));
        Assert.Single(slice!);
    }

    [Fact]
    public async Task ApplyReplicationFrame_ThatErrors_RecordsNothing()
    {
        await using var h = SessionHarness.Start(appendOnly: true);
        var seed = new[] { SessionHarness.B("SET"), SessionHarness.B("s"), SessionHarness.B("abc") };
        Assert.True(h.Processor.ApplyReplicationFrame(seed, RespWriter.EncodeCommand(seed)));
        var offsetAfterSeed = h.Replication.Offset;

        // INCR against a non-integer string errors on any server; the
        // frame must not advance the offset or reach the AOF.
        var bad = new[] { SessionHarness.B("INCR"), SessionHarness.B("s") };
        Assert.False(h.Processor.ApplyReplicationFrame(bad, RespWriter.EncodeCommand(bad)));
        Assert.Equal(offsetAfterSeed, h.Replication.Offset);
        Assert.Equal(offsetAfterSeed, h.Replication.FollowOffset);
        var aof = AppendOnlyFile.ReadCommands(
            Path.Combine(h.DataDirectory, "appendonly.aof"), out _);
        Assert.Single(aof);
    }

    [Fact]
    public async Task LoadReplicationSnapshot_ReplacesTheKeyspace_AndResetsTheAof()
    {
        await using var h = SessionHarness.Start(appendOnly: true);
        var stale = new[] { SessionHarness.B("SET"), SessionHarness.B("stale"), SessionHarness.B("x") };
        Assert.True(h.Processor.ApplyReplicationFrame(stale, RespWriter.EncodeCommand(stale)));

        // Build a snapshot the way a primary would: from another store's state.
        var source = new RedisStore(h.Clock, h.Stats);
        source.Set(SessionHarness.B("fresh"), SessionHarness.B("1"),
            SetCondition.Always, null, keepTtl: false);
        var snapshot = SnapshotFile.Serialize(source.ExportState());

        h.Processor.LoadReplicationSnapshot(snapshot);
        Assert.Null(h.Store.Get(SessionHarness.B("stale")));
        Assert.Equal("1", Encoding.ASCII.GetString(h.Store.Get(SessionHarness.B("fresh"))!));

        // The AOF now describes the synced dataset, not the pre-sync one:
        // replaying it (what a restart does) yields exactly the snapshot.
        var aof = AppendOnlyFile.ReadCommands(
            Path.Combine(h.DataDirectory, "appendonly.aof"), out _);
        Assert.Single(aof);
        Assert.Equal("fresh", Encoding.ASCII.GetString(aof[0][1]));
    }

    [Fact]
    public void SnapshotSerialize_TryParse_RoundTripsIncludingEmpty()
    {
        var stats = new RedisStats();
        var clock = new FakeTimeProvider();
        var store = new RedisStore(clock, stats);
        var empty = SnapshotFile.Serialize(store.ExportState());
        Assert.Equal(SnapshotLoadStatus.Loaded, SnapshotFile.TryParse(empty, out var noEntries, out _));
        Assert.Empty(noEntries);

        store.Set(SessionHarness.B("k"), SessionHarness.B("v"), SetCondition.Always,
            TimeSpan.FromMinutes(5), keepTtl: false);
        var bytes = SnapshotFile.Serialize(store.ExportState());
        Assert.Equal(SnapshotLoadStatus.Loaded, SnapshotFile.TryParse(bytes, out var entries, out _));
        var entry = Assert.Single(entries);
        Assert.Equal("k", Encoding.ASCII.GetString(entry.Key));
        Assert.NotNull(entry.ExpiresAt);
    }
}
