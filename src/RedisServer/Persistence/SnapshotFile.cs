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
///   "RCDB" magic, uint32 format version (= 1)
///   per entry:
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
    private const uint FormatVersion = 1;
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
        entries = Array.Empty<ExportedEntry>();
        error = null;
        if (!File.Exists(path))
            return SnapshotLoadStatus.Missing;

        byte[] all;
        try
        {
            // ReadWrite | Delete sharing, for the same Windows reason as
            // the AOF reader: File.ReadAllBytes shares for read only,
            // which both fails against a handle holding the file open and
            // blocks the atomic move-overwrite that replaces a snapshot.
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            using var copy = new MemoryStream();
            stream.CopyTo(copy);
            all = copy.ToArray();
        }
        catch (Exception ex)
        {
            error = $"could not read the file: {ex.Message}";
            return SnapshotLoadStatus.Corrupt;
        }

        return TryParse(all, out entries, out error);
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
        if (version != FormatVersion)
            throw new InvalidDataException($"unsupported format version {version}");

        var entries = new List<ExportedEntry>();
        while (true)
        {
            var tag = reader.ReadByte();
            if (tag == EndMarker)
                break;
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
            entries.Add(new ExportedEntry(key, type, value, expiry));
        }

        if (stream.Position != stream.Length)
            throw new InvalidDataException("trailing bytes after the end marker");
        return entries;
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
