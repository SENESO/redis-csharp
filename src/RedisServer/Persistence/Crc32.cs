namespace RedisServer.Persistence;

/// <summary>
/// CRC-32 (IEEE 802.3 polynomial, the zip/Ethernet variant) used to seal
/// snapshot files. Table-driven, with an incremental accumulator so a file
/// can be checksummed while it is written.
/// </summary>
public static class Crc32
{
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var c = i;
            for (var k = 0; k < 8; k++)
                c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    public static uint Compute(ReadOnlySpan<byte> data) => new Accumulator().Append(data).Value;

    public struct Accumulator
    {
        private uint _crc;

        public Accumulator() => _crc = 0xFFFFFFFFu;

        public Accumulator Append(ReadOnlySpan<byte> data)
        {
            var crc = _crc;
            foreach (var b in data)
                crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
            _crc = crc;
            return this;
        }

        public readonly uint Value => _crc ^ 0xFFFFFFFFu;
    }
}
