using RedisServer.Persistence;

namespace RedisServer.Options;

/// <summary>
/// Runtime configuration, populated from command-line arguments in
/// <c>Program.cs</c>:
/// <list type="bullet">
/// <item><c>-p &lt;port&gt;</c> — TCP port to listen on (default 6379)</item>
/// <item><c>--dir &lt;path&gt;</c> — directory holding the persistence files (default: current directory)</item>
/// <item><c>--dbfilename &lt;name&gt;</c> — snapshot file name (default <c>dump.rcdb</c>)</item>
/// <item><c>--appendonly yes|no</c> — enable the append-only file (default no)</item>
/// <item><c>--appendfilename &lt;name&gt;</c> — AOF file name (default <c>appendonly.aof</c>)</item>
/// <item><c>--appendfsync always|everysec|no</c> — AOF fsync policy (default everysec)</item>
/// </list>
/// </summary>
public sealed class ServerOptions
{
    public const int DefaultPort = 6379;

    public int Port { get; init; } = DefaultPort;

    public string DataDirectory { get; init; } = ".";

    public string SnapshotFileName { get; init; } = "dump.rcdb";

    public bool AppendOnly { get; init; }

    public string AppendFileName { get; init; } = "appendonly.aof";

    public AofFsyncPolicy AppendFsync { get; init; } = AofFsyncPolicy.EverySec;

    public static ServerOptions FromArgs(string[] args)
    {
        var port = DefaultPort;
        var directory = ".";
        var snapshotFileName = "dump.rcdb";
        var appendOnly = false;
        var appendFileName = "appendonly.aof";
        var appendFsync = AofFsyncPolicy.EverySec;

        for (var i = 0; i < args.Length - 1; i++)
        {
            var value = args[i + 1];
            switch (args[i])
            {
                case "-p" when int.TryParse(value, out var parsedPort):
                    port = parsedPort;
                    break;
                case "--dir":
                    directory = value;
                    break;
                case "--dbfilename":
                    snapshotFileName = value;
                    break;
                case "--appendonly":
                    appendOnly = value.Equals("yes", StringComparison.OrdinalIgnoreCase);
                    break;
                case "--appendfilename":
                    appendFileName = value;
                    break;
                case "--appendfsync":
                    appendFsync = value.ToLowerInvariant() switch
                    {
                        "always" => AofFsyncPolicy.Always,
                        "no" => AofFsyncPolicy.No,
                        _ => AofFsyncPolicy.EverySec
                    };
                    break;
            }
        }

        return new ServerOptions
        {
            Port = port,
            DataDirectory = directory,
            SnapshotFileName = snapshotFileName,
            AppendOnly = appendOnly,
            AppendFileName = appendFileName,
            AppendFsync = appendFsync
        };
    }
}
