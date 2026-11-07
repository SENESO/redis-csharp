using System.Buffers;
using System.Globalization;
using System.Text;

namespace RedisServer.Protocol;

/// <summary>
/// Renders RESP2 replies into a caller-owned buffer (so a session can flush
/// a whole pipelined batch in one write), and encodes commands back into
/// RESP arrays — the form the AOF stores and pub/sub frames are built in.
/// </summary>
public static class RespWriter
{
    private static readonly byte[] CrLf = "\r\n"u8.ToArray();

    public static void WriteSimpleString(IBufferWriter<byte> output, string text)
    {
        output.Write("+"u8);
        output.Write(Encoding.ASCII.GetBytes(text));
        output.Write(CrLf);
    }

    public static void WriteError(IBufferWriter<byte> output, string text)
    {
        output.Write("-"u8);
        output.Write(Encoding.ASCII.GetBytes(text));
        output.Write(CrLf);
    }

    public static void WriteInteger(IBufferWriter<byte> output, long value)
    {
        output.Write(":"u8);
        Span<byte> digits = stackalloc byte[20];
        value.TryFormat(digits, out var written, provider: CultureInfo.InvariantCulture);
        output.Write(digits[..written]);
        output.Write(CrLf);
    }

    public static void WriteBulkString(IBufferWriter<byte> output, ReadOnlySpan<byte> value)
    {
        output.Write("$"u8);
        WriteLength(output, value.Length);
        output.Write(value);
        output.Write(CrLf);
    }

    public static void WriteBulkString(IBufferWriter<byte> output, string text) =>
        WriteBulkString(output, Encoding.ASCII.GetBytes(text));

    public static void WriteNullBulkString(IBufferWriter<byte> output) => output.Write("$-1\r\n"u8);

    public static void WriteBulkStringOrNull(IBufferWriter<byte> output, byte[]? value)
    {
        if (value is null)
            WriteNullBulkString(output);
        else
            WriteBulkString(output, value);
    }

    public static void WriteArrayHeader(IBufferWriter<byte> output, long count)
    {
        output.Write("*"u8);
        WriteLength(output, count);
    }

    public static void WriteNullArray(IBufferWriter<byte> output) => output.Write("*-1\r\n"u8);

    private static void WriteLength(IBufferWriter<byte> output, long value)
    {
        Span<byte> digits = stackalloc byte[20];
        value.TryFormat(digits, out var written, provider: CultureInfo.InvariantCulture);
        output.Write(digits[..written]);
        output.Write(CrLf);
    }

    /// <summary>Encodes one command (name + arguments) as a RESP array of bulk strings.</summary>
    public static byte[] EncodeCommand(IReadOnlyList<byte[]> args)
    {
        var buffer = new ArrayBufferWriter<byte>();
        WriteArrayHeader(buffer, args.Count);
        foreach (var arg in args)
            WriteBulkString(buffer, arg);
        return buffer.WrittenSpan.ToArray();
    }
}
