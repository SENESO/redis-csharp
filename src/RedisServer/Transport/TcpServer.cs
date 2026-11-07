using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
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
        var listeners = new List<Task> { RunListenerAsync(_options.Port, certificate: null, cancellationToken) };
        if (_options.TlsPort > 0)
        {
            // Loaded once, up front: a bad certificate path fails the
            // startup loudly instead of the first TLS connection.
            var certificate = LoadServerCertificate(_options);
            listeners.Add(RunListenerAsync(_options.TlsPort, certificate, cancellationToken));
        }
        await Task.WhenAll(listeners);
    }

    /// <summary>
    /// Loads the PKCS#12 server certificate named by the options. Throws
    /// with an actionable message when TLS is configured without a file.
    /// </summary>
    public static X509Certificate2 LoadServerCertificate(ServerOptions options)
    {
        if (string.IsNullOrEmpty(options.TlsCertFile))
            throw new InvalidOperationException(
                "--tls-port is set but --tls-cert-file is not: a TLS listener needs a server certificate (PKCS#12).");
        return new X509Certificate2(options.TlsCertFile, options.TlsCertPassword, X509KeyStorageFlags.Exportable);
    }

    private async Task RunListenerAsync(int port, X509Certificate2? certificate, CancellationToken cancellationToken)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        _logger.LogInformation("Redis server listening on 127.0.0.1:{Port}{Tls}", port,
            certificate is null ? "" : " (TLS)");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(cancellationToken);
                client.NoDelay = true;
                _stats.ConnectionOpened();
                _ = RunSessionAsync(client, certificate, cancellationToken);
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

    private async Task RunSessionAsync(TcpClient client, X509Certificate2? certificate, CancellationToken cancellationToken)
    {
        var remote = client.Client.RemoteEndPoint;
        _logger.LogDebug("Client connected: {RemoteEndPoint}", remote);
        System.Net.Security.SslStream? sslStream = null;
        try
        {
            using (client)
            {
                Stream stream = client.GetStream();
                if (certificate is not null)
                {
                    // The TLS handshake runs before any protocol byte; after
                    // it, the session pipeline is exactly the plaintext one.
                    sslStream = new System.Net.Security.SslStream(stream, leaveInnerStreamOpen: false);
                    await sslStream.AuthenticateAsServerAsync(new System.Net.Security.SslServerAuthenticationOptions
                    {
                        ServerCertificate = certificate,
                        ClientCertificateRequired = false,
                        EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                        CertificateRevocationCheckMode = X509RevocationMode.NoCheck
                    }, cancellationToken);
                    stream = sslStream;
                    _logger.LogDebug("TLS established with {RemoteEndPoint}: {Protocol}", remote, sslStream.SslProtocol);
                }
                var session = new RedisSession(
                    new StreamDuplexPipe(stream), _processor, _hub, _stats, _logger);
                await session.RunAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Connection from {RemoteEndPoint} failed.", remote);
        }
        finally
        {
            sslStream?.Dispose();
            _stats.ConnectionClosed();
            _logger.LogDebug("Client disconnected: {RemoteEndPoint}", remote);
        }
    }
}
