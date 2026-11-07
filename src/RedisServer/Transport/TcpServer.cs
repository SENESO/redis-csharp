using System.Net;
using System.Net.Sockets;
using RedisServer.Core;
using RedisServer.Options;
using RedisServer.Protocol;
using Microsoft.Extensions.Logging;

namespace RedisServer.Transport;

/// <summary>
/// The listening socket and accept loop. Each accepted connection becomes a
/// <see cref="RedisSession"/> running on its own task; this class owns
/// connection accounting and nothing about the protocol.
/// </summary>
public sealed class TcpServer
{
    private readonly ServerOptions _options;
    private readonly CommandProcessor _processor;
    private readonly PubSubHub _hub;
    private readonly RedisStats _stats;
    private readonly ILogger<TcpServer> _logger;

    public TcpServer(ServerOptions options, CommandProcessor processor, PubSubHub hub,
        RedisStats stats, ILogger<TcpServer> logger)
    {
        _options = options;
        _processor = processor;
        _hub = hub;
        _stats = stats;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var listener = new TcpListener(IPAddress.Loopback, _options.Port);
        listener.Start();
        _logger.LogInformation("Redis server listening on 127.0.0.1:{Port}", _options.Port);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(cancellationToken);
                client.NoDelay = true;
                _stats.ConnectionOpened();
                _ = RunSessionAsync(client, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown requested.
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task RunSessionAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var remote = client.Client.RemoteEndPoint;
        _logger.LogDebug("Client connected: {RemoteEndPoint}", remote);
        try
        {
            using (client)
            {
                var session = new RedisSession(
                    new StreamDuplexPipe(client.GetStream()), _processor, _hub, _stats, _logger);
                await session.RunAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Connection from {RemoteEndPoint} failed.", remote);
        }
        finally
        {
            _stats.ConnectionClosed();
            _logger.LogDebug("Client disconnected: {RemoteEndPoint}", remote);
        }
    }
}
