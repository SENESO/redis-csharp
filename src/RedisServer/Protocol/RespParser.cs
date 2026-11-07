using System.Buffers;
using System.Globalization;
using System.Text;

namespace RedisServer.Protocol;

/// <summary>
/// Incremental RESP reader: given the not-yet-consumed bytes of a
/// connection, it either produces one complete command (an array of bulk
/// strings, or an inline command split into words), reports that more bytes
/// are needed, or reports a protocol error. Nothing is consumed on the
/// "need more" path, so commands split across packets and pipelined
/// commands both work without copying — the pattern Kestrel-style pipeline
/// readers are built on.
/// </summary>
public static class RespParser
{
    /// <summary>proto-max-bulk-len: a single bulk string may not exceed 512 MB.</summary>
    public const long MaxBulkLength = 512L * 1024 * 1024;

    /// <summary>Upper bound for a header/inline line; protects against unbounded buffering.</summary>
    public const int MaxLineLength = 64 * 1024;

    private const int MaxArrayElements = 1024 * 1024;

    /// <summary>
    /// Attempts to read one command from the front of <paramref name="buffer"/>.
    /// On success <paramref name="buffer"/> is advanced past the command and
    /// <paramref name="args"/> holds it. Returns false when more bytes are
    /// needed (<paramref name="protocolError"/> null) or the stream is
    /// malformed (<paramref name="protocolError"/> set).
    /// </summary>
    public static bool TryReadCommand(ref ReadOnlySequence<byte> buffer, out byte[][]? args, out string? protocolError)
    {
        args = null;
        protocolError = null;
        if (buffer.IsEmpty)
            return false;

        var first = buffer.FirstSpan[0];
        if (first != (byte)'*')
        {
            if (first is (byte)'+' or (byte)'-' or (byte)':' or (byte)'$')
            {
                protocolError = $"expected '*', got '{(char)first}'";
                return false;
            }
            return TryReadInline(ref buffer, out args, out protocolError);
        }

        var reader = new SequenceReader<byte>(buffer);
        reader.Advance(1); // consume the '*'
        if (!TryReadLine(ref reader, out var header))
            return false;
        if (!long.TryParse(header, NumberStyles.Integer, CultureInfo.InvariantCulture, out var count))
        {
            protocolError = "invalid multibulk length";
            return false;
        }
        if (count < 0)
        {
            // A null array is not a command; consume and surface nothing.
            buffer = buffer.Slice(reader.Position);
            args = Array.Empty<byte[]>();
            return true;
        }
        if (count > MaxArrayElements)
        {
            protocolError = "invalid multibulk length";
            return false;
        }

        var items = new byte[count][];
        for (long i = 0; i < count; i++)
        {
            if (!reader.TryPeek(out var typeByte))
                return false;
            if (typeByte != (byte)'$')
            {
                protocolError = $"expected '$', got '{(char)typeByte}'";
                return false;
            }
            reader.Advance(1);
            if (!TryReadLine(ref reader, out var lengthText))
                return false;
            if (!long.TryParse(lengthText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var length))
            {
                protocolError = "invalid bulk length";
                return false;
            }
            if (length < 0 || length > MaxBulkLength)
            {
                protocolError = "invalid bulk length";
                return false;
            }
            if (reader.Remaining < length + 2)
                return false;

            var data = buffer.Slice(reader.Position, length);
            items[i] = data.ToArray();
            reader.Advance(length);
            if (!reader.TryRead(out var cr) || cr != (byte)'\r' ||
                !reader.TryRead(out var lf) || lf != (byte)'\n')
            {
                protocolError = "bulk string is not terminated by CRLF";
                return false;
            }
        }

        buffer = buffer.Slice(reader.Position);
        args = items;
        return true;
    }

    /// <summary>
    /// Inline commands (the telnet convenience form): one line, split into
    /// words with Redis's quoting rules (double quotes support \-escapes and
    /// \xNN hex, single quotes only \').
    /// </summary>
    private static bool TryReadInline(ref ReadOnlySequence<byte> buffer, out byte[][]? args, out string? protocolError)
    {
        args = null;
        protocolError = null;
        var newline = buffer.PositionOf((byte)'\n');
        if (newline is null)
        {
            if (buffer.Length > MaxLineLength)
                protocolError = "inline command line is too long";
            return false;
        }

        var line = buffer.Slice(buffer.Start, newline.Value);
        buffer = buffer.Slice(buffer.GetPosition(1, newline.Value));
        if (line.Length > 0 && line.Slice(line.Length - 1, 1).FirstSpan[0] == (byte)'\r')
            line = line.Slice(0, line.Length - 1);

        var words = SplitInline(line.ToArray(), out protocolError);
        if (words is null)
            return false;
        args = words.ToArray();
        return true;
    }

    private static List<byte[]>? SplitInline(byte[] line, out string? error)
    {
        error = null;
        var words = new List<byte[]>();
        var current = new List<byte>();
        var inWord = false;
        var i = 0;

        void EndWord()
        {
            if (inWord)
                words.Add(current.ToArray());
            current = new List<byte>();
            inWord = false;
        }

        while (i < line.Length)
        {
            var c = line[i];
            if (c is (byte)' ' or (byte)'\t')
            {
                EndWord();
                i++;
                continue;
            }
            inWord = true;
            if (c == (byte)'"')
            {
                i++;
                var closed = false;
                while (i < line.Length)
                {
                    var d = line[i];
                    if (d == (byte)'"')
                    {
                        closed = true;
                        i++;
                        break;
                    }
                    if (d == (byte)'\\' && i + 1 < line.Length)
                    {
                        var e = line[i + 1];
                        if (e == (byte)'x' && i + 3 < line.Length &&
                            TryHex(line[i + 2], out var hi) && TryHex(line[i + 3], out var lo))
                        {
                            current.Add((byte)(hi * 16 + lo));
                            i += 4;
                            continue;
                        }
                        current.Add(e switch
                        {
                            (byte)'n' => (byte)'\n',
                            (byte)'r' => (byte)'\r',
                            (byte)'t' => (byte)'\t',
                            (byte)'a' => (byte)7,
                            (byte)'b' => (byte)8,
                            _ => e
                        });
                        i += 2;
                        continue;
                    }
                    current.Add(d);
                    i++;
                }
                if (!closed)
                {
                    error = "unbalanced quotes in inline command";
                    return null;
                }
            }
            else if (c == (byte)'\'')
            {
                i++;
                var closed = false;
                while (i < line.Length)
                {
                    var d = line[i];
                    if (d == (byte)'\'')
                    {
                        closed = true;
                        i++;
                        break;
                    }
                    if (d == (byte)'\\' && i + 1 < line.Length && line[i + 1] == (byte)'\'')
                    {
                        current.Add((byte)'\'');
                        i += 2;
                        continue;
                    }
                    current.Add(d);
                    i++;
                }
                if (!closed)
                {
                    error = "unbalanced quotes in inline command";
                    return null;
                }
            }
            else
            {
                current.Add(c);
                i++;
            }
        }
        EndWord();
        return words;
    }

    private static bool TryHex(byte c, out int value)
    {
        value = c switch
        {
            >= (byte)'0' and <= (byte)'9' => c - (byte)'0',
            >= (byte)'a' and <= (byte)'f' => c - (byte)'a' + 10,
            >= (byte)'A' and <= (byte)'F' => c - (byte)'A' + 10,
            _ => -1
        };
        return value >= 0;
    }

    /// <summary>Reads one CRLF-terminated line as text. False when the line is incomplete.</summary>
    private static bool TryReadLine(ref SequenceReader<byte> reader, out ReadOnlySpan<byte> line)
    {
        line = default;
        if (!reader.TryReadTo(out ReadOnlySequence<byte> sequence, (byte)'\n'))
            return false;
        if (sequence.Length > 0 && sequence.Slice(sequence.Length - 1, 1).FirstSpan[0] == (byte)'\r')
            sequence = sequence.Slice(0, sequence.Length - 1);
        line = sequence.IsSingleSegment ? sequence.FirstSpan : sequence.ToArray();
        return true;
    }

    public static string Describe(byte[] arg) => Encoding.ASCII.GetString(arg);
}
