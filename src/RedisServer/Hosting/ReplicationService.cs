using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using RedisServer.Options;
using RedisServer.Protocol;
using RedisServer.Replication;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace RedisServer.Hosting;

/// <summary>
/// The replica side of replication: while this server's role is replica,
/// keep a sync connection to the primary and apply its effect stream.
///
/// One cycle: connect, AUTH (when --masterauth is configured), REPLCONF,
/// PSYNC with the last known stream identity. The primary answers
/// +CONTINUE (the backlog covers our offset — the dataset is kept and
/// the missed frames follow) or +FULLRESYNC (a snapshot replaces the
/// dataset wholesale, then live frames follow). Either way the cycle
/// ends in the apply loop, which runs until the link fails; then the
/// loop retries with exponential backoff (1 s doubling to 30 s, reset
/// whenever a sync completes). Reads keep serving from the local dataset
/// throughout — a replica whose primary is down is stale, never broken.
///
/// Role changes (REPLICAOF to another target, or NO ONE) cancel the
/// current cycle through the manager's change signal; promotion simply
/// ends the loop's reason to run.
/// </summary>
public sealed class ReplicationService : BackgroundService
{
    private readonly ReplicationManager _replication;
    private readonly CommandProcessor _processor;
    private readonly ServerOptions _options;
    private readonly ILogger<ReplicationService> _logger;

    public ReplicationService(ReplicationManager replication, CommandProcessor processor,
        ServerOptions options, ILogger<ReplicationService> logger)
    {
        _replication = replication;
        _processor = processor;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var target = _replication.FollowTarget;
            if (target is null)
            {
                try
                {
                    await _replication.WaitForChangeAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                continue;
            }

            using var cycle = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var syncTask = SyncLoopAsync(target.Value.Host, target.Value.Port, cycle.Token);
            var changeTask = _replication.WaitForChangeAsync(stoppingToken);
            await Task.WhenAny(syncTask, changeTask);
            cycle.Cancel();
            try
            {
                await syncTask;
            }
            catch (OperationCanceledException)
            {
                // Role changed or the host is stopping.
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Replication cycle ended with an error.");
            }
        }
    }

    private async Task SyncLoopAsync(string host, int port, CancellationToken ct)
    {
        var backoff = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await SyncOnceAsync(host, port, ct, () => backoff = TimeSpan.FromSeconds(1));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Replication from {Host}:{Port} failed: {Message}", host, port, ex.Message);
            }
            _replication.SetMasterLinkUp(false);
            _replication.SetSyncInProgress(false);

            try
            {
                await Task.Delay(backoff, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            backoff = TimeSpan.FromMilliseconds(Math.Min(backoff.TotalMilliseconds * 2, 30_000));
        }
    }

    private async Task SyncOnceAsync(string host, int port, CancellationToken ct, Action onSyncEstablished)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(host, port, ct);
        var stream = client.GetStream();
        var reader = PipeReader.Create(stream);
        try
        {
            if (_options.MasterAuth is { } password)
            {
                await SendCommandAsync(stream, new[] { Ascii("AUTH"), Encoding.UTF8.GetBytes(password) }, ct);
                var authReply = await ReadLineAsync(reader, ct);
                if (!authReply.StartsWith('+'))
                    throw new InvalidOperationException($"The primary refused replica authentication: {authReply}");
            }

            await SendCommandAsync(stream, new[]
            {
                Ascii("REPLCONF"), Ascii("listening-port"),
                Ascii(_options.Port.ToString(CultureInfo.InvariantCulture))
            }, ct);
            _ = await ReadLineAsync(reader, ct); // +OK; the content is not acted on.

            var (followId, followOffset) = _replication.FollowIdentity;
            await SendCommandAsync(stream, new[]
            {
                Ascii("PSYNC"), Ascii(followId),
                Ascii(followOffset.ToString(CultureInfo.InvariantCulture))
            }, ct);
            var reply = await ReadLineAsync(reader, ct);
            if (reply.StartsWith("+FULLRESYNC", StringComparison.Ordinal))
            {
                var parts = reply.Split(' ');
                if (parts.Length != 3 ||
                    !long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var syncOffset))
                    throw new InvalidOperationException($"Malformed FULLRESYNC reply: {reply}");
                var snapshot = await ReadBulkPayloadAsync(reader, ct);
                _replication.SetSyncInProgress(true);
                try
                {
                    _processor.LoadReplicationSnapshot(snapshot);
                }
                finally
                {
                    _replication.SetSyncInProgress(false);
                }
                _replication.AdoptSyncedStream(parts[1], syncOffset);
                _logger.LogInformation(
                    "Full sync from {Host}:{Port} complete at offset {Offset}.", host, port, syncOffset);
            }
            else if (!reply.StartsWith("+CONTINUE", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"The primary refused PSYNC: {reply}");
            }

            _replication.SetMasterLinkUp(true);
            onSyncEstablished();
            await ApplyStreamAsync(reader, ct);
        }
        finally
        {
            _replication.SetMasterLinkUp(false);
            reader.Complete();
        }
    }

    /// <summary>
    /// The steady state of a sync cycle: parse propagation frames off the
    /// link and apply them, in order, until the link ends. Any ending —
    /// clean close, reset, protocol damage — throws, and the caller
    /// treats it as a dropped link to retry.
    /// </summary>
    private async Task ApplyStreamAsync(PipeReader reader, CancellationToken ct)
    {
        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var cursor = buffer;
            while (true)
            {
                var before = cursor.Length;
                if (!RespParser.TryReadCommand(ref cursor, out var args, out var protocolError))
                {
                    if (protocolError is not null)
                        throw new IOException($"Protocol error in the replication stream: {protocolError}");
                    break; // incomplete frame — wait for more bytes
                }
                if (args is not { Length: > 0 })
                    continue;
                var frameLength = (int)(before - cursor.Length);
                var rawFrame = buffer.Slice(buffer.Length - before, frameLength).ToArray();
                if (!_processor.ApplyReplicationFrame(args, rawFrame))
                {
                    _logger.LogWarning(
                        "A propagated frame did not apply cleanly: {Command}", RespParser.Describe(args[0]));
                }
            }
            reader.AdvanceTo(cursor.Start, buffer.End);
            if (result.IsCompleted)
                throw new IOException("The primary closed the replication link.");
        }
    }

    private static async Task SendCommandAsync(NetworkStream stream, byte[][] args, CancellationToken ct) =>
        await stream.WriteAsync(RespWriter.EncodeCommand(args), ct);

    /// <summary>Reads one CRLF-terminated line from the handshake phase of the link.</summary>
    private static async Task<string> ReadLineAsync(PipeReader reader, CancellationToken ct)
    {
        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var newline = buffer.PositionOf((byte)'\n');
            if (newline is not null)
            {
                var line = Encoding.ASCII.GetString(buffer.Slice(0, newline.Value).ToArray()).TrimEnd('\r');
                reader.AdvanceTo(buffer.GetPosition(1, newline.Value));
                return line;
            }
            reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted)
                throw new IOException("The primary closed the connection during the replication handshake.");
        }
    }

    /// <summary>
    /// Reads the snapshot transfer that follows a +FULLRESYNC line: a
    /// bulk-string header, then exactly that many raw bytes (no trailing
    /// CRLF — the propagation stream resumes immediately after them).
    /// </summary>
    private static async Task<byte[]> ReadBulkPayloadAsync(PipeReader reader, CancellationToken ct)
    {
        var header = await ReadLineAsync(reader, ct);
        if (!header.StartsWith('$') ||
            !long.TryParse(header.AsSpan(1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var length) ||
            length < 0 || length > int.MaxValue)
            throw new InvalidOperationException($"Malformed snapshot header in the replication stream: {header}");

        var payload = new byte[(int)length];
        var filled = 0;
        while (filled < payload.Length)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var take = (int)Math.Min(buffer.Length, payload.Length - filled);
            if (take > 0)
            {
                buffer.Slice(0, take).CopyTo(payload.AsSpan(filled));
                filled += take;
                reader.AdvanceTo(buffer.GetPosition(take));
                continue;
            }
            reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted)
                throw new IOException("The primary closed the connection during the snapshot transfer.");
        }
        return payload;
    }

    private static byte[] Ascii(string text) => Encoding.ASCII.GetBytes(text);
}
