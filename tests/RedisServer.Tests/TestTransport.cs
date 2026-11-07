using System.IO.Pipelines;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Persistence;
using RedisServer.Protocol;

namespace RedisServer.Tests;

/// <summary>
/// In-memory full-duplex transport: two <see cref="Pipe"/>s cross-connected,
/// so protocol tests exercise the real session/framing code end to end
/// without a socket — fast, deterministic, and hermetic in any environment.
/// </summary>
internal sealed class TestDuplexPipe(PipeReader input, PipeWriter output) : IDuplexPipe
{
    public PipeReader Input { get; } = input;
    public PipeWriter Output { get; } = output;
}

/// <summary>The client end of a test connection: sends raw protocol text, asserts exact replies.</summary>
internal sealed class TestClient
{
    private readonly IDuplexPipe _pipe;
    private readonly List<byte> _received = new();
    private bool _closed;

    public TestClient(IDuplexPipe pipe) => _pipe = pipe;

    public async Task SendAsync(string text)
    {
        await _pipe.Output.WriteAsync(Encoding.ASCII.GetBytes(text));
    }

    /// <summary>Sends one command as a RESP array of bulk strings.</summary>
    public async Task SendCommandAsync(params string[] args)
    {
        var builder = new StringBuilder();
        builder.Append('*').Append(args.Length).Append("\r\n");
        foreach (var arg in args)
            builder.Append('$').Append(Encoding.ASCII.GetByteCount(arg)).Append("\r\n").Append(arg).Append("\r\n");
        await SendAsync(builder.ToString());
    }

    /// <summary>Reads until the next bytes exactly match <paramref name="expected"/>, then consumes them.</summary>
    public Task ExpectAsync(string expected, int timeoutMs = 5000) =>
        ExpectBytesAsync(Encoding.ASCII.GetBytes(expected), timeoutMs, expected);

    public async Task ExpectBytesAsync(byte[] expectedBytes, int timeoutMs = 5000, string? display = null)
    {
        var expected = display ?? $"{expectedBytes.Length} bytes";
        using var timeout = new CancellationTokenSource(timeoutMs);
        while (!StartsWith(expectedBytes))
        {
            if (_received.Count >= expectedBytes.Length)
                throw new Xunit.Sdk.XunitException(
                    $"Expected \"{Escape(expected)}\" but the reply starts with \"{Escape(CurrentText())}\".");
            if (_closed)
                throw new Xunit.Sdk.XunitException(
                    $"The session closed the connection; received: \"{Escape(CurrentText())}\".");
            try
            {
                await FillAsync(timeout.Token);
            }
            catch (OperationCanceledException)
            {
                throw new Xunit.Sdk.XunitException(
                    $"Timed out waiting for \"{Escape(expected)}\"; received so far: \"{Escape(CurrentText())}\".");
            }
        }
        _received.RemoveRange(0, expectedBytes.Length);
    }

    /// <summary>An error reply, distinguished from a simple-string reply.</summary>
    public sealed record RespError(string Text);

    /// <summary>
    /// Parses and returns one complete RESP reply from the stream:
    /// simple strings and bulk strings as <see cref="string"/>, errors as
    /// <see cref="RespError"/>, integers as <see cref="long"/>, arrays as
    /// <see cref="List{T}"/> of the same, null bulk/array as null.
    /// </summary>
    public async Task<object?> ReadReplyAsync(int timeoutMs = 5000)
    {
        using var timeout = new CancellationTokenSource(timeoutMs);
        while (true)
        {
            if (TryParseReply(0, out var value, out var length))
            {
                _received.RemoveRange(0, length);
                return value;
            }
            if (_closed)
                throw new Xunit.Sdk.XunitException(
                    $"The session closed before a full reply arrived; received: \"{Escape(CurrentText())}\".");
            try
            {
                await FillAsync(timeout.Token);
            }
            catch (OperationCanceledException)
            {
                throw new Xunit.Sdk.XunitException(
                    $"Timed out waiting for a complete reply; received so far: \"{Escape(CurrentText())}\".");
            }
        }
    }

    private bool TryParseReply(int start, out object? value, out int consumed)
    {
        value = null;
        consumed = 0;
        if (_received.Count <= start)
            return false;
        var type = (char)_received[start];
        switch (type)
        {
            case '+':
            case '-':
            {
                if (!TryReadLine(start + 1, out var line, out var next))
                    return false;
                value = type == '+' ? line : new RespError(line);
                consumed = next - start;
                return true;
            }
            case ':':
            {
                if (!TryReadLine(start + 1, out var line, out var next))
                    return false;
                value = long.Parse(line);
                consumed = next - start;
                return true;
            }
            case '$':
            {
                if (!TryReadLine(start + 1, out var line, out var next))
                    return false;
                var length = int.Parse(line);
                if (length < 0)
                {
                    value = null;
                    consumed = next - start;
                    return true;
                }
                if (_received.Count < next + length + 2)
                    return false;
                value = Encoding.ASCII.GetString(_received.GetRange(next, length).ToArray());
                consumed = next + length + 2 - start;
                return true;
            }
            case '*':
            {
                if (!TryReadLine(start + 1, out var line, out var next))
                    return false;
                var count = int.Parse(line);
                if (count < 0)
                {
                    value = null;
                    consumed = next - start;
                    return true;
                }
                var items = new List<object?>(count);
                var cursor = next;
                for (var i = 0; i < count; i++)
                {
                    if (!TryParseReply(cursor, out var item, out var itemLength))
                        return false;
                    items.Add(item);
                    cursor += itemLength;
                }
                value = items;
                consumed = cursor - start;
                return true;
            }
            default:
                throw new Xunit.Sdk.XunitException($"Unexpected reply byte '{type}'.");
        }
    }

    private bool TryReadLine(int start, out string line, out int next)
    {
        line = string.Empty;
        next = start;
        for (var i = start; i + 1 < _received.Count; i++)
        {
            if (_received[i] == (byte)'\r' && _received[i + 1] == (byte)'\n')
            {
                line = Encoding.ASCII.GetString(_received.GetRange(start, i - start).ToArray());
                next = i + 2;
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Accumulates everything received until <paramref name="marker"/>
    /// appears, then returns the whole text and clears the buffer. For
    /// pipelined bursts whose individual replies are not asserted one by one.
    /// </summary>
    public async Task<string> CollectUntilAsync(string marker, int timeoutMs = 10000)
    {
        using var timeout = new CancellationTokenSource(timeoutMs);
        while (!CurrentText().Contains(marker, StringComparison.Ordinal))
        {
            if (_closed)
                break;
            try
            {
                await FillAsync(timeout.Token);
            }
            catch (OperationCanceledException)
            {
                throw new Xunit.Sdk.XunitException(
                    $"Timed out waiting for the received text to contain \"{Escape(marker)}\"; received: \"{Escape(CurrentText())}\".");
            }
        }
        var text = CurrentText();
        _received.Clear();
        return text;
    }

    public async Task ExpectNoReplyAsync(int quietMs = 400)
    {
        using var timeout = new CancellationTokenSource(quietMs);
        try
        {
            while (!_closed)
                await FillAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            // Nothing arrived within the window — the expected outcome.
        }
        Assert.Empty(_received);
    }

    private async Task FillAsync(CancellationToken cancellationToken)
    {
        var result = await _pipe.Input.ReadAsync(cancellationToken);
        foreach (var segment in result.Buffer)
            _received.AddRange(segment.ToArray());
        _pipe.Input.AdvanceTo(result.Buffer.End);
        if (result.IsCompleted)
            _closed = true;
    }

    private bool StartsWith(byte[] prefix)
    {
        if (_received.Count < prefix.Length)
            return false;
        for (var i = 0; i < prefix.Length; i++)
        {
            if (_received[i] != prefix[i])
                return false;
        }
        return true;
    }

    private string CurrentText() => Encoding.ASCII.GetString(_received.ToArray());

    private static string Escape(string text) => text.Replace("\r", "\\r").Replace("\n", "\\n");
}

/// <summary>
/// One server core (store + processor + hub + persistence in a scratch
/// directory) with as many simultaneous client connections as a test needs.
/// </summary>
internal sealed class SessionHarness : IAsyncDisposable
{
    private readonly List<(CancellationTokenSource Shutdown, Task Task)> _sessions = new();
    private readonly string _dataDirectory;

    private SessionHarness(RedisStore store, RedisStats stats, FakeTimeProvider clock,
        CommandProcessor processor, PubSubHub hub, PersistenceManager persistence, string dataDirectory)
    {
        Store = store;
        Stats = stats;
        Clock = clock;
        Processor = processor;
        Hub = hub;
        Persistence = persistence;
        _dataDirectory = dataDirectory;
    }

    public RedisStore Store { get; }
    public RedisStats Stats { get; }
    public FakeTimeProvider Clock { get; }
    public CommandProcessor Processor { get; }
    public PubSubHub Hub { get; }
    public PersistenceManager Persistence { get; }
    public string DataDirectory => _dataDirectory;

    public static SessionHarness Start(bool appendOnly = false)
    {
        var clock = new FakeTimeProvider();
        var stats = new RedisStats();
        var store = new RedisStore(clock, stats);
        var directory = Path.Combine(Path.GetTempPath(), "redis-csharp-tests", Guid.NewGuid().ToString("N"));
        var options = new ServerOptions
        {
            Port = 0,
            DataDirectory = directory,
            AppendOnly = appendOnly,
            AppendFsync = AofFsyncPolicy.Always
        };
        var persistence = new PersistenceManager(options, NullLogger<PersistenceManager>.Instance);
        var hub = new PubSubHub();
        var processor = new CommandProcessor(store, stats, clock, persistence, hub, port: 0);
        return new SessionHarness(store, stats, clock, processor, hub, persistence, directory);
    }

    /// <summary>Opens one more client connection against the shared server core.</summary>
    public TestClient Connect()
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        var clientPipe = new TestDuplexPipe(serverToClient.Reader, clientToServer.Writer);
        var serverPipe = new TestDuplexPipe(clientToServer.Reader, serverToClient.Writer);

        var shutdown = new CancellationTokenSource();
        var session = new RedisSession(serverPipe, Processor, Hub, Stats, NullLogger.Instance);
        var task = session.RunAsync(shutdown.Token);
        _sessions.Add((shutdown, task));
        return new TestClient(clientPipe);
    }

    /// <summary>Helper for the common case: bytes for string arguments in store-level tests.</summary>
    public static byte[] B(string text) => Encoding.ASCII.GetBytes(text);

    public async ValueTask DisposeAsync()
    {
        foreach (var (shutdown, task) in _sessions)
        {
            await shutdown.CancelAsync();
            try
            {
                await task.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (TimeoutException)
            {
                // In-memory pipes: a lingering session task cannot outlive the test process meaningfully.
            }
            shutdown.Dispose();
        }
        Persistence.Dispose();
        try
        {
            if (Directory.Exists(_dataDirectory))
                Directory.Delete(_dataDirectory, recursive: true);
        }
        catch (IOException)
        {
            // Scratch directory cleanup is best-effort.
        }
    }
}
