using RedisServer.Core;
using RedisServer.Persistence;
using RedisServer.Protocol;
using RedisServer.Transport;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace RedisServer.Hosting;

/// <summary>
/// Loads persisted state, then runs the TCP listener for the lifetime of
/// the host and stops it gracefully (flushing the AOF on the way out).
/// </summary>
public sealed class RedisHostedService : IHostedService
{
    private readonly TcpServer _server;
    private readonly PersistenceManager _persistence;
    private readonly RedisStore _store;
    private readonly CommandProcessor _processor;
    private readonly ILogger<RedisHostedService> _logger;
    private readonly CancellationTokenSource _shutdown = new();
    private Task? _serverTask;

    public RedisHostedService(TcpServer server, PersistenceManager persistence, RedisStore store,
        CommandProcessor processor, ILogger<RedisHostedService> logger)
    {
        _server = server;
        _persistence = persistence;
        _store = store;
        _processor = processor;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _persistence.Load(_store, _processor.ExecuteReplay);
        _serverTask = _server.RunAsync(_shutdown.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _shutdown.CancelAsync();
        if (_serverTask is not null)
        {
            try
            {
                await _serverTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
        }
        _persistence.Dispose();
        _logger.LogInformation("Redis server stopped.");
    }
}
