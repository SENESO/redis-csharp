namespace RedisServer.Core;

/// <summary>
/// Cluster hash slots: CRC16/XMODEM of the key's hash tag (the part
/// between the first '{' and the next '}' when non-empty), modulo
/// 16384 — the same function Redis Cluster routes by, so slot numbers
/// here are the slot numbers a real cluster would compute
/// (CRC16("123456789") = 0x31C3, slot 12739).
/// </summary>
public static class ClusterSlot
{
    public const int SlotCount = 16384;

    private static readonly ushort[] Table = BuildTable();

    private static ushort[] BuildTable()
    {
        var table = new ushort[256];
        for (var i = 0; i < 256; i++)
        {
            var crc = (ushort)(i << 8);
            for (var bit = 0; bit < 8; bit++)
                crc = (ushort)((crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1);
            table[i] = crc;
        }
        return table;
    }

    public static ushort Crc16(ReadOnlySpan<byte> data)
    {
        ushort crc = 0;
        foreach (var b in data)
            crc = (ushort)((crc << 8) ^ Table[((crc >> 8) ^ b) & 0xFF]);
        return crc;
    }

    /// <summary>The slot a key hashes to, applying the hash-tag rule.</summary>
    public static int OfKey(ReadOnlySpan<byte> key)
    {
        var hashPart = key;
        var open = key.IndexOf((byte)'{');
        if (open >= 0)
        {
            var close = key[(open + 1)..].IndexOf((byte)'}');
            if (close > 0)
                hashPart = key.Slice(open + 1, close);
        }
        return Crc16(hashPart) % SlotCount;
    }
}

/// <summary>One contiguous slot range owned by one node in the static cluster map.</summary>
public sealed record ClusterNodeRange(int Start, int End, string Host, int Port);
