using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Persistence;
using RedisServer.Protocol;

namespace RedisServer.Tests;

/// <summary>
/// Streaming snapshot load: the RCDB file is parsed entry by entry in
/// bounded memory (with the CRC verified incrementally over the
/// stream), instead of being read whole and copied twice more before
/// parsing. The A/B test measures managed allocations of the legacy
/// whole-buffer path (still used for replication's wire payloads)
/// against the streaming path over the same large file, and both must
/// produce byte-identical resulting state.
/// </summary>
public class SnapshotStreamingTests
{
    private static byte[] B(string text) => Encoding.ASCII.GetBytes(text);

    private static string TempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "redis-csharp-snapstream", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static RedisStore BuildLargeStore(int keys, int valueBytes)
    {
        var store = new RedisStore(new FakeTimeProvider(), new RedisStats());
        var value = new byte[valueBytes];
        for (var i = 0; i < keys; i++)
        {
            Array.Fill(value, (byte)(i % 251));
            store.Set(B($"key:{i}"), (byte[])value.Clone(), SetCondition.Always, null, false, db: i % 4);
        }
        return store;
    }

    [Fact]
    public void StreamingLoad_AllocatesFarLessThanTheWholeBufferPath_AndStateIsExact()
    {
        var dir = TempDir();
        try
        {
            // ~24 MB of snapshot: 12,000 keys x ~2 KB values across 4 DBs.
            var source = BuildLargeStore(keys: 12000, valueBytes: 2000);
            var expected = SnapshotFile.Serialize(source.ExportState());
            var path = Path.Combine(dir, "dump.rcdb");
            File.WriteAllBytes(path, expected);
            Assert.True(new FileInfo(path).Length > 20_000_000);

            // Whole-buffer path (what TryParse does for wire payloads):
            // file bytes + payload copy + parse copy, then the entries.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var beforeWhole = GC.GetAllocatedBytesForCurrentThread();
            var wholeStatus = SnapshotFile.TryParse(File.ReadAllBytes(path), out var wholeEntries, out _);
            var wholeAllocated = GC.GetAllocatedBytesForCurrentThread() - beforeWhole;
            Assert.Equal(SnapshotLoadStatus.Loaded, wholeStatus);

            // Streaming path over the same file.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var streamed = new List<ExportedEntry>();
            var beforeStream = GC.GetAllocatedBytesForCurrentThread();
            var streamStatus = SnapshotFile.StreamRead(path, streamed.Add, out _);
            var streamAllocated = GC.GetAllocatedBytesForCurrentThread() - beforeStream;
            Assert.Equal(SnapshotLoadStatus.Loaded, streamStatus);

            Assert.True(streamAllocated < wholeAllocated * 0.6,
                $"streaming allocated {streamAllocated:N0} vs whole-buffer {wholeAllocated:N0}");

            // Final state exact: re-serialising the streamed entries in
            // export order reproduces the file byte for byte.
            var restored = new RedisStore(new FakeTimeProvider(), new RedisStats());
            foreach (var entry in streamed)
                restored.ImportEntry(entry);
            Assert.Equal(expected, SnapshotFile.Serialize(restored.ExportState()));
            Assert.Equal(12000, restored.Count);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void StreamingLoad_CorruptCrc_ReportsCorrupt_AfterDeliveringAPrefix()
    {
        var dir = TempDir();
        try
        {
            var store = BuildLargeStore(keys: 100, valueBytes: 100);
            var path = Path.Combine(dir, "dump.rcdb");
            SnapshotFile.Write(path, store.ExportState());
            // Flip a byte in the middle of the payload: parsing still
            // succeeds structurally, the CRC at the end condemns it.
            var bytes = File.ReadAllBytes(path);
            bytes[bytes.Length / 2] ^= 0xFF;
            File.WriteAllBytes(path, bytes);

            long delivered = 0;
            var status = SnapshotFile.StreamRead(path, _ => delivered++, out var error);
            Assert.Equal(SnapshotLoadStatus.Corrupt, status);
            Assert.Contains("CRC", error);
            Assert.True(delivered > 0);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void StartupLoad_OfACorruptSnapshot_StartsEmpty_DespiteTheStreamedPrefix()
    {
        var dir = TempDir();
        try
        {
            var store = BuildLargeStore(keys: 50, valueBytes: 100);
            var snapshotPath = Path.Combine(dir, "dump.rcdb");
            SnapshotFile.Write(snapshotPath, store.ExportState());
            var bytes = File.ReadAllBytes(snapshotPath);
            bytes[bytes.Length / 2] ^= 0xFF;
            File.WriteAllBytes(snapshotPath, bytes);

            var options = new ServerOptions { Port = 0, DataDirectory = dir, AppendOnly = false };
            var persistence = new PersistenceManager(options, NullLogger<PersistenceManager>.Instance);
            var clock = new FakeTimeProvider();
            var stats = new RedisStats();
            var fresh = new RedisStore(clock, stats);
            var processor = new CommandProcessor(fresh, stats, clock, persistence, new PubSubHub(), port: 0);
            persistence.Load(fresh, processor.ExecuteReplay);
            Assert.Equal(0, fresh.Count);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void StreamingLoad_TruncatedFile_IsCorrupt_NotAPartialLoad()
    {
        var dir = TempDir();
        try
        {
            var store = BuildLargeStore(keys: 100, valueBytes: 100);
            var path = Path.Combine(dir, "dump.rcdb");
            SnapshotFile.Write(path, store.ExportState());
            var bytes = File.ReadAllBytes(path);
            File.WriteAllBytes(path, bytes[..(bytes.Length / 2)]);

            var status = SnapshotFile.StreamRead(path, _ => { }, out _);
            Assert.Equal(SnapshotLoadStatus.Corrupt, status);

            var missing = SnapshotFile.StreamRead(Path.Combine(dir, "nope.rcdb"), _ => { }, out _);
            Assert.Equal(SnapshotLoadStatus.Missing, missing);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
