using System.Buffers;
using RedisServer.Protocol;

namespace RedisServer.Persistence;

public enum AofFsyncPolicy
{
    Always,
    EverySec,
    No
}

/// <summary>
/// The append-only file: every mutating command, encoded as a RESP array
/// exactly as a client would send it, appended in execution order. Startup
/// replays the file through the normal command path, so recovery needs no
/// second implementation of any command.
///
/// Crash consistency: appends happen <em>after</em> the command executed in
/// memory, under the same ordering the command processor serialises
/// execution with, so the file is always a prefix of the executed history.
/// A crash can tear the final append; replay tolerates exactly that — it
/// stops at the first incomplete command and reports the truncation, having
/// applied every complete command before it. Older bytes can never be torn:
/// appends are sequential writes to one stream.
/// </summary>
public sealed class AppendOnlyFile : IDisposable
{
    private readonly object _gate = new();
    private readonly AofFsyncPolicy _fsyncPolicy;
    private FileStream _stream;
    private bool _dirty;
    private bool _disposed;

    public AppendOnlyFile(string path, AofFsyncPolicy fsyncPolicy)
    {
        Path = path;
        _fsyncPolicy = fsyncPolicy;
        _stream = OpenStream(path);
    }

    public string Path { get; }

    private static FileStream OpenStream(string path) =>
        new(path, FileMode.Append, FileAccess.Write, FileShare.Read);

    /// <summary>Appends one executed command. Callers serialise execution, so appends arrive in history order.</summary>
    public void Append(IReadOnlyList<byte[]> args)
    {
        var frame = RespWriter.EncodeCommand(args);
        lock (_gate)
        {
            if (_disposed)
                return;
            _stream.Write(frame);
            _dirty = true;
            switch (_fsyncPolicy)
            {
                case AofFsyncPolicy.Always:
                    _stream.Flush(flushToDisk: true);
                    _dirty = false;
                    break;
                case AofFsyncPolicy.EverySec:
                    _stream.Flush(); // to the OS now; fsync happens on the per-second tick
                    break;
                case AofFsyncPolicy.No:
                    break;
            }
        }
    }

    /// <summary>The everysec tick (driven by the maintenance service): fsync when data is outstanding.</summary>
    public void FsyncTick()
    {
        lock (_gate)
        {
            if (_disposed || _fsyncPolicy != AofFsyncPolicy.EverySec || !_dirty)
                return;
            _stream.Flush(flushToDisk: true);
            _dirty = false;
        }
    }

    /// <summary>
    /// Atomically replaces the AOF with the given commands: written to a
    /// temporary file, fsynced, then moved over the live one, and appends
    /// continue onto the new file. The caller holds the command gate, so no
    /// append can interleave with the swap.
    /// </summary>
    public void Rewrite(IReadOnlyList<byte[][]> commands)
    {
        var tempPath = Path + ".tmp";
        lock (_gate)
        {
            using (var temp = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                foreach (var command in commands)
                    temp.Write(RespWriter.EncodeCommand(command));
                temp.Flush(flushToDisk: true);
            }

            _stream.Dispose();
            File.Move(tempPath, Path, overwrite: true);
            _stream = OpenStream(Path);
            _dirty = false;
        }
    }

    /// <summary>
    /// Streams the file's commands one at a time for replay, in bounded
    /// memory: the file is read in chunks through a pipe and each parsed
    /// command is handed to <paramref name="onCommand"/> and released —
    /// a hundreds-of-megabytes AOF loads without ever being resident.
    /// A torn tail (crash mid-append) ends the stream quietly with
    /// <paramref name="truncatedTail"/> set; every complete command
    /// before it has been delivered. Returns the number delivered.
    /// </summary>
    public static long StreamCommands(string path, Action<byte[][]> onCommand, out bool truncatedTail)
    {
        truncatedTail = false;
        if (!File.Exists(path))
            return 0;

        long delivered = 0;
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 64 * 1024);
        var reader = System.IO.Pipelines.PipeReader.Create(stream);
        try
        {
            while (true)
            {
                var result = reader.ReadAsync().AsTask().GetAwaiter().GetResult();
                var buffer = result.Buffer;
                while (RespParser.TryReadCommand(ref buffer, out var args, out _))
                {
                    if (args is { Length: > 0 })
                    {
                        onCommand(args);
                        delivered++;
                    }
                }
                if (result.IsCompleted)
                {
                    // Whatever remains unparsed is an incomplete or
                    // malformed tail from a torn final write.
                    truncatedTail = !buffer.IsEmpty;
                    reader.AdvanceTo(buffer.End);
                    break;
                }
                reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        finally
        {
            reader.Complete();
        }
        return delivered;
    }

    /// <summary>
    /// Reads the whole file as a command list. Convenience for tests and
    /// tooling — startup replay uses <see cref="StreamCommands"/> so the
    /// file is never held in memory whole.
    /// </summary>
    public static IReadOnlyList<byte[][]> ReadCommands(string path, out bool truncatedTail)
    {
        var commands = new List<byte[][]>();
        StreamCommands(path, commands.Add, out truncatedTail);
        return commands;
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            try
            {
                _stream.Flush(flushToDisk: true);
            }
            catch (IOException)
            {
                // Shutting down; a failed final fsync loses at most the tail,
                // which replay already tolerates.
            }
            _stream.Dispose();
        }
    }
}
