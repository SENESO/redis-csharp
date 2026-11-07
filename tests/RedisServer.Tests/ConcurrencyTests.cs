using System.Buffers;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging.Abstractions;
using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Persistence;
using RedisServer.Protocol;

namespace RedisServer.Tests;

/// <summary>
/// Concurrency stress: parallel clients and workers against shared state.
/// The command gate must keep every composite operation atomic — counters
/// exact, collections intact, AOF replayable — no matter the interleaving.
/// </summary>
public class ConcurrencyTests
{
    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);

    [Fact]
    public async Task ParallelIncr_OnOneCounter_IsExact()
    {
        var store = new RedisStore(new FakeTimeProvider(), new RedisStats());
        const int workers = 8, perWorker = 250;

        var tasks = Enumerable.Range(0, workers).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < perWorker; i++)
                store.ApplyDelta(B("counter"), 1);
        })).ToArray();
        await Task.WhenAll(tasks);

        Assert.Equal((workers * perWorker).ToString(),
            Encoding.ASCII.GetString(store.Get(B("counter"))!));
    }

    [Fact]
    public async Task MixedLoad_WithExpirySweepRunning_StaysConsistent()
    {
        var clock = new FakeTimeProvider();
        var store = new RedisStore(clock, new RedisStats());
        using var stop = new CancellationTokenSource();

        var sweeper = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
                store.RunMaintenance();
        });

        var workers = Enumerable.Range(0, 8).Select(w => Task.Run(() =>
        {
            for (var i = 0; i < 500; i++)
            {
                var key = B($"w{w}:k{i % 25}");
                switch (i % 5)
                {
                    case 0: store.Set(key, B("v"), SetCondition.Always, TimeSpan.FromMilliseconds(50), false); break;
                    case 1: store.LeftPush(B($"w{w}:list"), new[] { B("x") }); break;
                    case 2: store.HashSet(B($"w{w}:hash"), new[] { new KeyValuePair<byte[], byte[]>(B("f"), B("v")) }); break;
                    case 3: store.ZAdd(B($"w{w}:z"), ZAddMode.None, false, false, new[] { ((double)i, B("m")) }); break;
                    default: store.Delete(new[] { key }); break;
                }
            }
        })).ToArray();
        await Task.WhenAll(workers);
        await stop.CancelAsync();
        await sweeper;

        // Every list/hash/zset the workers touched is readable and typed as
        // expected — no torn state from racing the sweep.
        for (var w = 0; w < 8; w++)
        {
            Assert.True(store.ListLength(B($"w{w}:list")) > 0);
            Assert.Equal(RedisType.Hash, store.TypeOf(B($"w{w}:hash")));
            Assert.Equal(RedisType.ZSet, store.TypeOf(B($"w{w}:z")));
        }
    }

    [Fact]
    public async Task ParallelWireClients_PipelinedIncr_PerClientKeys()
    {
        await using var h = SessionHarness.Start();
        const int clients = 8, perClient = 250;

        async Task RunClient(int id)
        {
            var c = h.Connect();
            var key = $"counter:{id}";
            var payload = new StringBuilder();
            for (var i = 0; i < perClient; i++)
                payload.Append($"*2\r\n$4\r\nINCR\r\n${key.Length}\r\n{key}\r\n");
            payload.Append($"*2\r\n$3\r\nGET\r\n${key.Length}\r\n{key}\r\n");
            await c.SendAsync(payload.ToString());
            var text = await c.CollectUntilAsync($"$3\r\n{perClient}\r\n");
            Assert.Equal(perClient, Regex.Matches(text, @":\d+\r\n").Count);
        }

        await Task.WhenAll(Enumerable.Range(0, clients).Select(RunClient));

        var check = h.Connect();
        await check.SendCommandAsync("DBSIZE");
        await check.ExpectAsync($":{clients}\r\n");
    }

    [Fact]
    public async Task ParallelCommands_WithAof_ReplayToExactState()
    {
        var dir = Path.Combine(Path.GetTempPath(), "redis-csharp-concurrency", Guid.NewGuid().ToString("N"));
        try
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

            const int workers = 8, perWorker = 200;
            var tasks = Enumerable.Range(0, workers).Select(w => Task.Run(() =>
            {
                var ctx = new ClientContext();
                var sink = new ArrayBufferWriter<byte>();
                for (var i = 0; i < perWorker; i++)
                {
                    processor.Execute(new[] { B("SET"), B($"w{w}:k{i}"), B("v") }, ctx, sink);
                    processor.Execute(new[] { B("INCR"), B("shared") }, ctx, sink);
                }
            })).ToArray();
            await Task.WhenAll(tasks);
            persistence.Dispose();

            // Restart: replay must reproduce every write exactly once.
            var stats2 = new RedisStats();
            var store2 = new RedisStore(clock, stats2);
            var persistence2 = new PersistenceManager(options, NullLogger<PersistenceManager>.Instance);
            var processor2 = new CommandProcessor(store2, stats2, clock, persistence2, new PubSubHub(), port: 0);
            persistence2.Load(store2, processor2.ExecuteReplay);
            persistence2.Dispose();

            Assert.Equal((workers * perWorker).ToString(), Encoding.ASCII.GetString(store2.Get(B("shared"))!));
            Assert.Equal(workers * perWorker + 1, store2.Count);
        }
        finally
        {
            try
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
            }
            catch (IOException)
            {
                // Scratch cleanup is best-effort.
            }
        }
    }
}
