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
/// <item><c>--requirepass &lt;password&gt;</c> — when set, connections must <c>AUTH</c> before any
/// other command (default: no password; the <c>REDIS_REQUIREPASS</c> environment variable is the fallback)</item>
/// <item><c>--tls-port &lt;port&gt;</c> — additional TLS listener port (default 0: disabled). The plaintext
/// port keeps serving; TLS is an extra door, not a replacement</item>
/// <item><c>--tls-cert-file &lt;path&gt;</c> — PKCS#12 (.pfx) file holding the server certificate and key,
/// required when <c>--tls-port</c> is set</item>
/// <item><c>--tls-cert-password &lt;password&gt;</c> — password for the certificate file (default: the
/// <c>REDIS_TLS_CERT_PASSWORD</c> environment variable, so the secret need not appear in the process list)</item>
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

    /// <summary>The password connections must present with AUTH, or null for no authentication.</summary>
    public string? RequirePass { get; init; }

    /// <summary>Additional TLS listener port; 0 disables the TLS listener.</summary>
    public int TlsPort { get; init; }

    /// <summary>Path of the PKCS#12 file with the TLS server certificate; required when TlsPort is set.</summary>
    public string? TlsCertFile { get; init; }

    /// <summary>Password for the TLS certificate file, when it has one.</summary>
    public string? TlsCertPassword { get; init; }

    public static ServerOptions FromArgs(string[] args)
    {
        var port = DefaultPort;
        var directory = ".";
        var snapshotFileName = "dump.rcdb";
        var appendOnly = false;
        var appendFileName = "appendonly.aof";
        var appendFsync = AofFsyncPolicy.EverySec;
        var requirePass = Environment.GetEnvironmentVariable("REDIS_REQUIREPASS");
        var tlsPort = 0;
        var tlsCertFile = (string?)null;
        var tlsCertPassword = Environment.GetEnvironmentVariable("REDIS_TLS_CERT_PASSWORD");

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
                case "--requirepass":
                    requirePass = value;
                    break;
                case "--tls-port" when int.TryParse(value, out var parsedTlsPort):
                    tlsPort = parsedTlsPort;
                    break;
                case "--tls-cert-file":
                    tlsCertFile = value;
                    break;
                case "--tls-cert-password":
                    tlsCertPassword = value;
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
            AppendFsync = appendFsync,
            RequirePass = string.IsNullOrEmpty(requirePass) ? null : requirePass,
            TlsPort = tlsPort,
            TlsCertFile = tlsCertFile,
            TlsCertPassword = tlsCertPassword
        };
    }
}
