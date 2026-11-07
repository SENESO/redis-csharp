using System.Diagnostics;
using System.Net.Sockets;
using System.Text;

// A small, honest loopback benchmark: one connection, fixed-size pipelined
// batches, replies fully parsed and counted. It measures this client +
// loopback + server together, which is exactly what the README reports —
// nothing is extrapolated.
//
//   dotnet run --project tools/RedisBenchmark -c Release -- [--port 6379] [--requests 50000]

var port = 6379;
var requests = 50_000;
for (var i = 0; i < args.Length - 1; i++)
{
    if (args[i] == "--port")
        port = int.Parse(args[i + 1]);
    if (args[i] == "--requests")
        requests = int.Parse(args[i + 1]);
}

Console.WriteLine($"Benchmarking 127.0.0.1:{port}, {requests} requests per test, one connection");
Console.WriteLine($"{"test",-10} {"pipeline",8} {"requests",10} {"seconds",9} {"ops/sec",12}");
foreach (var test in new[] { "SET", "GET", "INCR", "LPUSH" })
{
    foreach (var pipeline in new[] { 1, 50 })
    {
        var (count, seconds) = Run(test, pipeline, requests, port);
        Console.WriteLine($"{test,-10} {pipeline,8} {count,10} {seconds,9:F3} {count / seconds,12:N0}");
    }
}

static (long Count, double Seconds) Run(string test, int pipeline, int requests, int port)
{
    using var client = new TcpClient();
    client.NoDelay = true;
    client.Connect("127.0.0.1", port);
    var stream = client.GetStream();
    var reader = new RespReader(stream);

    void Command(params string[] command)
    {
        WriteCommand(stream, command);
        reader.ReadReply();
    }

    Command("FLUSHALL");
    if (test == "GET")
        Command("SET", "bench:get", "value");

    // Warmup: untimed, same shape as the measured loop.
    Drive(test, pipeline, 1_000, stream, reader);

    var watch = Stopwatch.StartNew();
    Drive(test, pipeline, requests, stream, reader);
    watch.Stop();
    return (requests, watch.Elapsed.TotalSeconds);
}

static void Drive(string test, int pipeline, int total, NetworkStream stream, RespReader reader)
{
    var sent = 0;
    while (sent < total)
    {
        var batch = Math.Min(pipeline, total - sent);
        for (var i = 0; i < batch; i++)
        {
            switch (test)
            {
                case "SET": WriteCommand(stream, "SET", "bench:set", "value"); break;
                case "GET": WriteCommand(stream, "GET", "bench:get"); break;
                case "INCR": WriteCommand(stream, "INCR", "bench:counter"); break;
                case "LPUSH": WriteCommand(stream, "LPUSH", "bench:list", "item"); break;
            }
        }
        for (var i = 0; i < batch; i++)
            reader.ReadReply();
        sent += batch;
    }
}

static void WriteCommand(NetworkStream stream, params string[] args)
{
    var builder = new StringBuilder();
    builder.Append('*').Append(args.Length).Append("\r\n");
    foreach (var arg in args)
        builder.Append('$').Append(Encoding.ASCII.GetByteCount(arg)).Append("\r\n").Append(arg).Append("\r\n");
    var bytes = Encoding.ASCII.GetBytes(builder.ToString());
    stream.Write(bytes, 0, bytes.Length);
}

/// <summary>Minimal RESP reply reader: parses and discards one reply, whatever its shape.</summary>
internal sealed class RespReader
{
    private readonly NetworkStream _stream;
    private readonly byte[] _buffer = new byte[64 * 1024];
    private int _start;
    private int _end;

    public RespReader(NetworkStream stream) => _stream = stream;

    private int ReadByte()
    {
        if (_start >= _end)
        {
            _end = _stream.Read(_buffer, 0, _buffer.Length);
            _start = 0;
            if (_end == 0)
                throw new IOException("Connection closed by the server.");
        }
        return _buffer[_start++];
    }

    private string ReadLine()
    {
        var builder = new StringBuilder();
        while (true)
        {
            var b = ReadByte();
            if (b == '\r')
            {
                ReadByte(); // '\n'
                return builder.ToString();
            }
            builder.Append((char)b);
        }
    }

    private void ReadExact(int count)
    {
        var remaining = count + 2; // payload + CRLF
        while (remaining > 0)
        {
            if (_start < _end)
            {
                var take = Math.Min(remaining, _end - _start);
                _start += take;
                remaining -= take;
            }
            else
            {
                ReadByte();
                remaining--;
            }
        }
    }

    public void ReadReply()
    {
        var prefix = (char)ReadByte();
        switch (prefix)
        {
            case '+':
            case '-':
            case ':':
                ReadLine();
                break;
            case '$':
                var length = int.Parse(ReadLine());
                if (length >= 0)
                    ReadExact(length);
                break;
            case '*':
                var count = int.Parse(ReadLine());
                for (var i = 0; i < count; i++)
                    ReadReply();
                break;
            default:
                throw new InvalidDataException($"Unexpected reply prefix '{prefix}'.");
        }
    }
}
