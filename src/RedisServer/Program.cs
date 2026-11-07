using RedisServer.Core;
using RedisServer.Hosting;
using RedisServer.Options;
using RedisServer.Persistence;
using RedisServer.Protocol;
using RedisServer.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Command-line options (-p, --dir, --appendonly, ...) are parsed by
// ServerOptions; the host builder is created without the app args so its
// own command-line configuration never sees them.
var options = ServerOptions.FromArgs(args);

var builder = Host.CreateApplicationBuilder();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<RedisStats>();
builder.Services.AddSingleton(sp => new RedisStore(
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<RedisStats>()));
builder.Services.AddSingleton<PersistenceManager>();
builder.Services.AddSingleton<PubSubHub>();
builder.Services.AddSingleton(sp => new CommandProcessor(
    sp.GetRequiredService<RedisStore>(),
    sp.GetRequiredService<RedisStats>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<PersistenceManager>(),
    sp.GetRequiredService<PubSubHub>(),
    options.Port,
    options.RequirePass,
    options.TlsPort));
builder.Services.AddSingleton<TcpServer>();
builder.Services.AddHostedService<RedisHostedService>();
builder.Services.AddHostedService<MaintenanceService>();

var host = builder.Build();
await host.RunAsync();
