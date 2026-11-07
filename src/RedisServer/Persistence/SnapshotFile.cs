using RedisServer.Core;

namespace RedisServer.Persistence;

public enum SnapshotLoadStatus
{
    Loaded,
    Missing,
    Corrupt
}

/// <summary>
/// The snapshot file: a compact binary dump of the whole keyspace with an
/// optional deadline per key, sealed with a CRC-32. Format (all integers
/// little-endian):
/// <code>
///   "RCDB" magic, uint32 format version (= 2)
///   per entry:
///     byte  logical database index (version 2+; version 1 files have no
///           such byte and every entry belongs to database 0)
///     byte  type tag (the RedisType values: 1 string, 2 list, 3 hash, 4 set, 5 zset)
///     byte  expiry flag (0/1); when 1, int64 expiry as Unix milliseconds
///     key:  int32 length + raw bytes
///     payload by type:
///       string: int32 length + bytes
///       list:   int32 count + per element int32 length + bytes
///       set:    int32 count + per member int32 length + bytes
///       hash:   int32 count + per pair (field, value), each length-prefixed
///       zset:   int32 count + per member (length-prefixed member, int64 score bits)
///   byte 0xFF end marker
///   uint32 CRC-32 over every preceding byte
/// </code>
/// Writes go to a temporary file that is fsynced and then atomically moved
/// over the previous snapshot, so a crash mid-SAVE can never leave a torn
/// file behind — the old snapshot survives untouched.
/// </summary>
public static class SnapshotFile
{
    private const uint FormatVersion = 2;
    private const byte EndMarker = 0xFF;

    /// <summary>
    /// Serialises entries to the exact bytes <see cref="Write"/> puts on
    /// disk (payload + CRC trailer). Replication's full sync ships these
    /// bytes verbatim, so a replica's snapshot load is the same code path
    /// as a file load (<see cref="TryParse"/>).
    /// </summary>
    public static byte[] Serialize(IReadOnlyList<ExportedEntry> entries)
    {
        using var buffer = new MemoryStream();
        using (var writer = new BinaryWriter(buffer, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            writer.Write("RCDB"u8.ToArray());
            writer.Write(FormatVersion);
            foreach (var entry in entries)
                WriteEntry(writer, entry);
            writer.Write(EndMarker);
        }

        var payload = buffer.ToArray();
        var crc = Crc32.Compute(payload);
        var bytes = new byte[payload.Length + 4];
        payload.CopyTo(bytes, 0);
        BitConverter.GetBytes(crc).CopyTo(bytes, payload.Length);
        return bytes;
    }

    public static void Write(string path, IReadOnlyList<ExportedEntry> entries)
    {
        var bytes = Serialize(entries);
        var tempPath = path + ".tmp";
        using (var file = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            file.Write(bytes);
            file.Flush(flushToDisk: true);
        }
        File.Move(tempPath, path, overwrite: true);
    }

    private static void WriteEntry(BinaryWriter writer, ExportedEntry entry)
    {
        writer.Write((byte)entry.Db);
        writer.Write((byte)entry.Type);
        writer.Write(entry.ExpiresAt is not null);
        if (entry.ExpiresAt is { } expiry)
            writer.Write(expiry.ToUnixTimeMilliseconds());
        WriteBlob(writer, entry.Key);
        switch (entry.Type)
        {
            case RedisType.String:
                WriteBlob(writer, (byte[])entry.Value);
                break;
            case RedisType.List:
            case RedisType.Set:
                var items = (List<byte[]>)entry.Value;
                writer.Write(items.Count);
                foreach (var item in items)
                    WriteBlob(writer, item);
                break;
            case RedisType.Hash:
                var pairs = (List<KeyValuePair<byte[], byte[]>>)entry.Value;
                writer.Write(pairs.Count);
                foreach (var (field, value) in pairs)
                {
                    WriteBlob(writer, field);
                    WriteBlob(writer, value);
                }
                break;
            case RedisType.ZSet:
                var members = (List<KeyValuePair<byte[], double>>)entry.Value;
                writer.Write(members.Count);
                foreach (var (member, score) in members)
                {
                    WriteBlob(writer, member);
                    writer.Write(BitConverter.DoubleToInt64Bits(score));
                }
                break;
            default:
                throw new InvalidOperationException($"Cannot snapshot entry of type {entry.Type}");
        }
    }

    private static void WriteBlob(BinaryWriter writer, byte[] blob)
    {
        writer.Write(blob.Length);
        writer.Write(blob);
    }

    public static SnapshotLoadStatus TryRead(string path, out IReadOnlyList<ExportedEntry> entries, out string? error)
    {
        var collected = new List<ExportedEntry>();
        var status = StreamRead(path, collected.Add, out error);
        entries = status == SnapshotLoadStatus.Loaded ? collected : Array.Empty<ExportedEntry>();
        return status;
    }

    /// <summary>
    /// Streams a snapshot file in bounded memory, delivering each entry
    /// to <paramref name="onEntry"/> as it is parsed — the file is never
    /// held whole (unlike <see cref="TryParse"/>, which serves the
    /// replication wire path where the bytes already arrived as one
    /// payload). The CRC is verified incrementally over the stream; a
    /// failure can therefore surface after some entries were delivered,
    /// and the caller owns discarding them (the startup load flushes
    /// the store, keeping "corrupt ⇒ start empty" exact).
    /// </summary>
    public static SnapshotLoadStatus StreamRead(string path, Action<ExportedEntry> onEntry, out string? error)
    {
        error = null;
        if (!File.Exists(path))
            return SnapshotLoadStatus.Missing;

        try
        {
            // ReadWrite | Delete sharing, for the same Windows reason as
            // the AOF reader: File.ReadAllBytes shares for read only,
            // which both fails against a handle holding the file open and
            // blocks the atomic move-overwrite that replaces a snapshot.
            using var raw = new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            using var window = new CrcWindowStream(raw);
            using var reader = new BinaryReader(window);
            var magic = reader.ReadBytes(4);
            if (!magic.AsSpan().SequenceEqual("RCDB"u8))
            {
                error = "malformed snapshot: bad magic";
                return SnapshotLoadStatus.Corrupt;
            }
            var version = reader.ReadUInt32();
            if (version is not (1 or 2))
            {
                error = $"malformed snapshot: unsupported format version {version}";
                return SnapshotLoadStatus.Corrupt;
            }
            ReadEntries(reader, version, onEntry);

            // The 4 trailer bytes are the stored CRC; reading them
            // through the window pushes the last payload bytes into the
            // computed CRC, completing it.
            var stored = reader.ReadBytes(4);
            if (stored.Length < 4)
            {
                error = "malformed snapshot: truncated CRC trailer";
                return SnapshotLoadStatus.Corrupt;
            }
            if (window.Crc != BitConverter.ToUInt32(stored))
            {
                error = "CRC mismatch — the file is damaged";
                return SnapshotLoadStatus.Corrupt;
            }
            if (raw.ReadByte() != -1)
            {
                error = "malformed snapshot: trailing bytes after the CRC";
                return SnapshotLoadStatus.Corrupt;
            }
            return SnapshotLoadStatus.Loaded;
        }
        catch (EndOfStreamException)
        {
            error = "malformed snapshot: unexpected end of file";
            return SnapshotLoadStatus.Corrupt;
        }
        catch (InvalidDataException ex)
        {
            error = $"malformed snapshot: {ex.Message}";
            return SnapshotLoadStatus.Corrupt;
        }
        catch (Exception ex)
        {
            error = $"could not read the file: {ex.Message}";
            return SnapshotLoadStatus.Corrupt;
        }
    }

    /// <summary>
    /// A read-only stream wrapper that computes the CRC-32 of every
    /// byte read except the most recent four — the snapshot's trailer
    /// is a 4-byte CRC over everything before it, so the trailer bytes
    /// themselves must stay out of the sum until they are known not to
    /// be payload. Reading the trailer through the wrapper completes
    /// the payload CRC.
    /// </summary>
    private sealed class CrcWindowStream : Stream
    {
        private const int WindowSize = 4;

        private readonly Stream _inner;
        private readonly byte[] _tail = new byte[WindowSize];
        private int _tailCount;
        private Crc32.Accumulator _crc = new();

        public CrcWindowStream(Stream inner) => _inner = inner;

        public uint Crc => _crc.Value;

        /// <summary>
        /// Feeds one read chunk into the CRC, holding back only the
        /// newest <see cref="WindowSize"/> bytes overall: everything
        /// older than the window is certainly payload.
        /// </summary>
        private void Feed(ReadOnlySpan<byte> chunk)
        {
            if (chunk.Length >= WindowSize)
            {
                // The whole held tail is payload now, plus all of this
                // chunk except its last WindowSize bytes.
                _crc = _crc.Append(_tail.AsSpan(0, _tailCount));
                _crc = _crc.Append(chunk[..^WindowSize]);
                chunk[^WindowSize..].CopyTo(_tail);
                _tailCount = WindowSize;
            }
            else if (_tailCount + chunk.Length > WindowSize)
            {
                // Part of the held tail overflows into the payload.
                var overflow = _tailCount + chunk.Length - WindowSize;
                _crc = _crc.Append(_tail.AsSpan(0, overflow));
                _tail.AsSpan(overflow, _tailCount - overflow).CopyTo(_tail);
                chunk.CopyTo(_tail.AsSpan(_tailCount - overflow));
                _tailCount = WindowSize;
            }
            else
            {
                chunk.CopyTo(_tail.AsSpan(_tailCount));
                _tailCount += chunk.Length;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = _inner.Read(buffer, offset, count);
            if (read > 0)
                Feed(buffer.AsSpan(offset, read));
            return read;
        }

        public override int Read(Span<byte> buffer)
        {
            var read = _inner.Read(buffer);
            if (read > 0)
                Feed(buffer[..read]);
            return read;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>
    /// Validates and parses snapshot bytes (the form <see cref="Serialize"/>
    /// produces and <see cref="TryRead"/> loads from disk). Replication's
    /// full sync parses the bytes it received over the wire through here,
    /// so a damaged stream is rejected exactly like a damaged file.
    /// </summary>
    public static SnapshotLoadStatus TryParse(byte[] all, out IReadOnlyList<ExportedEntry> entries, out string? error)
    {
        entries = Array.Empty<ExportedEntry>();
        error = null;

        // Smallest legal file: header (8) + end marker (1) + CRC (4).
        if (all.Length < 13)
        {
            error = "file is too short to be a snapshot";
            return SnapshotLoadStatus.Corrupt;
        }

        var payload = all.AsSpan(0, all.Length - 4);
        var storedCrc = BitConverter.ToUInt32(all, all.Length - 4);
        if (Crc32.Compute(payload) != storedCrc)
        {
            error = "CRC mismatch — the file is damaged";
            return SnapshotLoadStatus.Corrupt;
        }

        try
        {
            entries = Parse(payload);
            return SnapshotLoadStatus.Loaded;
        }
        catch (Exception ex)
        {
            error = $"malformed snapshot: {ex.Message}";
            entries = Array.Empty<ExportedEntry>();
            return SnapshotLoadStatus.Corrupt;
        }
    }

    private static List<ExportedEntry> Parse(ReadOnlySpan<byte> payload)
    {
        using var stream = new MemoryStream(payload.ToArray());
        using var reader = new BinaryReader(stream);
        var magic = reader.ReadBytes(4);
        if (!magic.AsSpan().SequenceEqual("RCDB"u8))
            throw new InvalidDataException("bad magic");
        var version = reader.ReadUInt32();
        if (version is not (1 or 2))
            throw new InvalidDataException($"unsupported format version {version}");

        var entries = new List<ExportedEntry>();
        ReadEntries(reader, version, entries.Add);

        if (stream.Position != stream.Length)
            throw new InvalidDataException("trailing bytes after the end marker");
        return entries;
    }

    /// <summary>
    /// Reads entries until the end marker, delivering each one. Shared
    /// by the whole-buffer parse and the streaming file read. Version 2
    /// prefixes each entry with its database index; in version 1 the
    /// first byte is the type tag itself and the database is 0.
    /// </summary>
    private static void ReadEntries(BinaryReader reader, uint version, Action<ExportedEntry> sink)
    {
        while (true)
        {
            var first = reader.ReadByte();
            if (first == EndMarker)
                break;
            int db;
            byte tag;
            if (version >= 2)
            {
                db = first;
                tag = reader.ReadByte();
            }
            else
            {
                db = 0;
                tag = first;
            }
            var type = (RedisType)tag;
            var hasExpiry = reader.ReadBoolean();
            DateTimeOffset? expiry = hasExpiry
                ? DateTimeOffset.FromUnixTimeMilliseconds(reader.ReadInt64())
                : null;
            var key = ReadBlob(reader);
            object value = type switch
            {
                RedisType.String => ReadBlob(reader),
                RedisType.List or RedisType.Set => ReadBlobList(reader),
                RedisType.Hash => ReadPairs(reader),
                RedisType.ZSet => ReadZSet(reader),
                _ => throw new InvalidDataException($"unknown type tag {tag}")
            };
            sink(new ExportedEntry(key, type, value, expiry, db));
        }
    }

    private static byte[] ReadBlob(BinaryReader reader)
    {
        var length = reader.ReadInt32();
        if (length < 0)
            throw new InvalidDataException("negative blob length");
        var blob = reader.ReadBytes(length);
        if (blob.Length != length)
            throw new EndOfStreamException("blob runs past the end of the file");
        return blob;
    }

    private static List<byte[]> ReadBlobList(BinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count < 0)
            throw new InvalidDataException("negative element count");
        var list = new List<byte[]>(count);
        for (var i = 0; i < count; i++)
            list.Add(ReadBlob(reader));
        return list;
    }

    private static List<KeyValuePair<byte[], byte[]>> ReadPairs(BinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count < 0)
            throw new InvalidDataException("negative pair count");
        var pairs = new List<KeyValuePair<byte[], byte[]>>(count);
        for (var i = 0; i < count; i++)
            pairs.Add(new KeyValuePair<byte[], byte[]>(ReadBlob(reader), ReadBlob(reader)));
        return pairs;
    }

    private static List<KeyValuePair<byte[], double>> ReadZSet(BinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count < 0)
            throw new InvalidDataException("negative member count");
        var members = new List<KeyValuePair<byte[], double>>(count);
        for (var i = 0; i < count; i++)
        {
            var member = ReadBlob(reader);
            var score = BitConverter.Int64BitsToDouble(reader.ReadInt64());
            members.Add(new KeyValuePair<byte[], double>(member, score));
        }
        return members;
    }
}
