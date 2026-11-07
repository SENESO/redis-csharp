using RedisServer.Core;
using RedisServer.Persistence;
using Microsoft.Extensions.Hosting;

namespace RedisServer.Hosting;

/// <summary>
/// Drives periodic housekeeping ten times a second (Redis's default
/// <c>hz</c>): the active expiry sweep — lazy expiry covers correctness on
/// its own, the sweep exists so memory is reclaimed for keys nobody asks
/// about again — and the AOF's everysec fsync tick.
/// </summary>
public sealed class MaintenanceService : BackgroundService
{
    private readonly RedisStore _store;
    private readonly PersistenceManager _persistence;

    public MaintenanceService(RedisStore store, PersistenceManager persistence)
    {
        _store = store;
        _persistence = persistence;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            _store.RunMaintenance();
            _persistence.FsyncTick();
        }
    }
}
