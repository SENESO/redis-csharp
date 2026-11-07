using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using RedisServer.Core;
using RedisServer.Options;

namespace RedisServer.Persistence;

/// <summary>
/// Owns the on-disk state: the snapshot file and (when enabled) the AOF.
///
/// Startup loading follows Redis's precedence: with the AOF enabled it is
/// the source of truth — it replays from empty (a rewritten AOF embeds the
/// full state, an unrewritten one the full history). Without an AOF, the
/// snapshot is loaded. A corrupt snapshot is reported and ignored (the
/// server starts empty rather than crash-looping on a damaged file); a torn
/// AOF tail is replayed up to the last complete command.
/// </summary>
public sealed class PersistenceManager : IDisposable
{
    /// <summary>Elements per reconstructed command when an AOF rewrite emits a big collection.</summary>
    private const int RewriteChunkSize = 1000;

    private readonly ServerOptions _options;
    private readonly ILogger _logger;
    private readonly AppendOnlyFile? _aof;

    public PersistenceManager(ServerOptions options, ILogger<PersistenceManager> logger)
    {
        _options = options;
        _logger = logger;
        Directory.CreateDirectory(options.DataDirectory);
        if (options.AppendOnly)
            _aof = new AppendOnlyFile(AofPath, options.AppendFsync);
    }

    public string SnapshotPath => Path.Combine(_options.DataDirectory, _options.SnapshotFileName);
    public string AofPath => Path.Combine(_options.DataDirectory, _options.AppendFileName);
    public bool AofEnabled => _aof is not null;
    public string? LastSaveError { get; private set; }
    public DateTimeOffset? LastSaveTime { get; private set; }

    /// <summary>Records the effects of one executed command (already normalised by the processor).</summary>
    public void AppendEffects(IReadOnlyList<byte[][]> commands)
    {
        if (_aof is null)
            return;
        foreach (var command in commands)
            _aof.Append(command);
    }

    public void FsyncTick() => _aof?.FsyncTick();

    /// <summary>Writes a snapshot of the current state. Returns the file size in bytes.</summary>
    public long SaveSnapshot(RedisStore store)
    {
        try
        {
            var entries = store.ExportState();
            SnapshotFile.Write(SnapshotPath, entries);
            LastSaveTime = DateTimeOffset.UtcNow;
            LastSaveError = null;
            return new FileInfo(SnapshotPath).Length;
        }
        catch (Exception ex)
        {
            LastSaveError = ex.Message;
            throw;
        }
    }

    /// <summary>
    /// Rewrites the AOF from the current state as a minimal command stream
    /// (one command family per key, chunked), then keeps appending to it.
    /// </summary>
    public void RewriteAof(RedisStore store)
    {
        if (_aof is null)
            throw new InvalidOperationException("AOF is not enabled.");
        var commands = new List<byte[][]>();
        foreach (var entry in store.ExportState())
        {
            commands.AddRange(CommandsForEntry(entry));
            if (entry.ExpiresAt is { } expiry)
            {
                commands.Add(new[]
                {
                    "PEXPIREAT"u8.ToArray(), entry.Key,
                    Encoding.ASCII.GetBytes(expiry.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture))
                });
            }
        }
        _aof.Rewrite(commands);
    }

    private static IEnumerable<byte[][]> CommandsForEntry(ExportedEntry entry)
    {
        switch (entry.Type)
        {
            case RedisType.String:
                yield return new[] { "SET"u8.ToArray(), entry.Key, (byte[])entry.Value };
                break;
            case RedisType.List:
                foreach (var chunk in Chunk((List<byte[]>)entry.Value))
                    yield return Command("RPUSH", entry.Key, chunk);
                break;
            case RedisType.Set:
                foreach (var chunk in Chunk((List<byte[]>)entry.Value))
                    yield return Command("SADD", entry.Key, chunk);
                break;
            case RedisType.Hash:
                var pairs = (List<KeyValuePair<byte[], byte[]>>)entry.Value;
                foreach (var chunk in Chunk(pairs))
                {
                    var args = new List<byte[]> { "HSET"u8.ToArray(), entry.Key };
                    foreach (var (field, value) in chunk)
                    {
                        args.Add(field);
                        args.Add(value);
                    }
                    yield return args.ToArray();
                }
                break;
            case RedisType.ZSet:
                var members = (List<KeyValuePair<byte[], double>>)entry.Value;
                foreach (var chunk in Chunk(members))
                {
                    var args = new List<byte[]> { "ZADD"u8.ToArray(), entry.Key };
                    foreach (var (member, score) in chunk)
                    {
                        args.Add(Encoding.ASCII.GetBytes(RedisStore.FormatDouble(score)));
                        args.Add(member);
                    }
                    yield return args.ToArray();
                }
                break;
        }
    }

    private static byte[][] Command(string name, byte[] key, List<byte[]> items)
    {
        var args = new byte[items.Count + 2][];
        args[0] = Encoding.ASCII.GetBytes(name);
        args[1] = key;
        items.CopyTo(args, 2);
        return args;
    }

    private static IEnumerable<List<T>> Chunk<T>(IReadOnlyList<T> items)
    {
        for (var i = 0; i < items.Count; i += RewriteChunkSize)
        {
            var chunk = new List<T>(Math.Min(RewriteChunkSize, items.Count - i));
            for (var j = i; j < Math.Min(i + RewriteChunkSize, items.Count); j++)
                chunk.Add(items[j]);
            yield return chunk;
        }
    }

    /// <summary>
    /// Loads persisted state into the store. Replay commands are handed to
    /// <paramref name="replayCommand"/> (the command processor in replay
    /// mode: no replies, no AOF re-append).
    /// </summary>
    public void Load(RedisStore store, Func<byte[][], bool> replayCommand)
    {
        if (_aof is not null && File.Exists(AofPath) && new FileInfo(AofPath).Length > 0)
        {
            // Streamed: commands are parsed and replayed one at a time
            // in bounded memory, however large the file has grown.
            long applied = 0;
            var read = AppendOnlyFile.StreamCommands(AofPath, command =>
            {
                if (replayCommand(command))
                    applied++;
            }, out var truncatedTail);
            _logger.LogInformation("Loaded {Count} commands from the AOF ({Applied} applied).", read, applied);
            if (truncatedTail)
            {
                _logger.LogWarning(
                    "The AOF ends with an incomplete command (torn write from a crash); everything before it was recovered.");
            }
            return;
        }

        var status = SnapshotFile.TryRead(SnapshotPath, out var entries, out var error);
        switch (status)
        {
            case SnapshotLoadStatus.Loaded:
                foreach (var entry in entries)
                    store.ImportEntry(entry);
                _logger.LogInformation("Loaded {Count} keys from the snapshot.", entries.Count);
                break;
            case SnapshotLoadStatus.Missing:
                _logger.LogInformation("No persisted state found; starting empty.");
                break;
            case SnapshotLoadStatus.Corrupt:
                _logger.LogError("The snapshot file is corrupt ({Error}); starting empty.", error);
                break;
        }
    }

    public void Dispose() => _aof?.Dispose();
}
