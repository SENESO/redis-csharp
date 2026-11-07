using System.Buffers;
using System.Globalization;
using System.Text;
using RedisServer.Core;

namespace RedisServer.Protocol;

/// <summary>
/// Renders replies into a caller-owned buffer (so a session can flush a
/// whole pipelined batch in one write), and encodes commands back into
/// RESP arrays — the form the AOF stores and pub/sub frames are built in.
/// The plain writers are RESP2; the protocol-aware writers take the
/// connection's negotiated version and emit the RESP3 form (map / set /
/// push headers, the single-character null, doubles, verbatim strings)
/// when it is 3 — RESP2 output stays byte-identical to what it always was.
/// </summary>
public static class RespWriter
{
    private static readonly byte[] CrLf = "\r\n"u8.ToArray();
    private static readonly byte[] OkReply = "+OK\r\n"u8.ToArray();
    private static readonly byte[] PongReply = "+PONG\r\n"u8.ToArray();
    private static readonly byte[] QueuedReply = "+QUEUED\r\n"u8.ToArray();

    public static void WriteSimpleString(IBufferWriter<byte> output, string text)
    {
        // The three hottest replies are pre-encoded; everything else takes
        // the general path.
        switch (text)
        {
            case "OK": output.Write(OkReply); return;
            case "PONG": output.Write(PongReply); return;
            case "QUEUED": output.Write(QueuedReply); return;
        }
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

    // ------------------------------------------------------------------
    // RESP3 (protocol-aware) writers
    // ------------------------------------------------------------------

    private static readonly byte[] Resp3Null = "_\r\n"u8.ToArray();

    /// <summary>A null bulk-string reply: <c>$-1</c> in RESP2, the RESP3 null (<c>_</c>) in RESP3.</summary>
    public static void WriteNull(IBufferWriter<byte> output, int protocolVersion)
    {
        if (protocolVersion >= 3)
            output.Write(Resp3Null);
        else
            WriteNullBulkString(output);
    }

    /// <summary>A null reply in a position where RESP2 uses a null array (<c>*-1</c>); RESP3 uses <c>_</c> for both.</summary>
    public static void WriteNullArrayValue(IBufferWriter<byte> output, int protocolVersion)
    {
        if (protocolVersion >= 3)
            output.Write(Resp3Null);
        else
            WriteNullArray(output);
    }

    /// <summary>Bulk string or the protocol's null when <paramref name="value"/> is null.</summary>
    public static void WriteBulkStringOrNull(IBufferWriter<byte> output, byte[]? value, int protocolVersion)
    {
        if (value is null)
            WriteNull(output, protocolVersion);
        else
            WriteBulkString(output, value);
    }

    /// <summary>RESP3 map header (<c>%</c>). Only valid on a RESP3 connection.</summary>
    public static void WriteMapHeader(IBufferWriter<byte> output, long pairCount)
    {
        output.Write("%"u8);
        WriteLength(output, pairCount);
    }

    /// <summary>RESP3 set header (<c>~</c>). Only valid on a RESP3 connection.</summary>
    public static void WriteSetHeader(IBufferWriter<byte> output, long count)
    {
        output.Write("~"u8);
        WriteLength(output, count);
    }

    /// <summary>
    /// The header of a pub/sub frame: an array (<c>*</c>) in RESP2, a push
    /// (<c>&gt;</c>) in RESP3 — the one structural change RESP3 makes to
    /// subscription acknowledgements and delivered messages.
    /// </summary>
    public static void WritePushOrArrayHeader(IBufferWriter<byte> output, long count, int protocolVersion)
    {
        if (protocolVersion >= 3)
        {
            output.Write(">"u8);
            WriteLength(output, count);
        }
        else
        {
            WriteArrayHeader(output, count);
        }
    }

    /// <summary>
    /// A RESP3 double (<c>,</c>), formatted exactly as this server formats
    /// scores in bulk strings, so 2 prints as <c>,2</c> and infinity as
    /// <c>,inf</c> — the same spellings real Redis uses.
    /// </summary>
    public static void WriteDouble(IBufferWriter<byte> output, double value)
    {
        output.Write(","u8);
        output.Write(Encoding.ASCII.GetBytes(RedisStore.FormatDouble(value)));
        output.Write(CrLf);
    }

    /// <summary>A double or the protocol's null (RESP3 <c>_</c>); in RESP2 the caller renders a bulk string instead.</summary>
    public static void WriteDoubleOrNull(IBufferWriter<byte> output, double? value, int protocolVersion)
    {
        if (value is { } number)
            WriteDouble(output, number);
        else
            WriteNull(output, protocolVersion);
    }

    /// <summary>
    /// A RESP3 verbatim string (<c>=</c>) with the <c>txt:</c> format
    /// prefix — the shape INFO takes in RESP3. Only valid on a RESP3
    /// connection; RESP2 callers use a bulk string.
    /// </summary>
    public static void WriteVerbatimString(IBufferWriter<byte> output, string text)
    {
        var payloadLength = 4 + Encoding.ASCII.GetByteCount(text);
        output.Write("="u8);
        WriteLength(output, payloadLength);
        output.Write("txt:"u8);
        output.Write(Encoding.ASCII.GetBytes(text));
        output.Write(CrLf);
    }

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
