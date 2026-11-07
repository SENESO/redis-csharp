using RedisServer.Core;
using RedisServer.Persistence;
using RedisServer.Replication;

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
/// <item><c>--maxmemory &lt;bytes&gt;</c> — memory ceiling for the dataset (default 0: no limit). Accepts
/// Redis's size suffixes: k = 1000, kb = 1024, m/mb, g/gb likewise</item>
/// <item><c>--maxmemory-policy &lt;name&gt;</c> — eviction policy at the ceiling: noeviction (default),
/// allkeys-lru, allkeys-random, allkeys-lfu, volatile-lru, volatile-random, volatile-ttl,
/// volatile-lfu</item>
/// <item><c>--lfu-log-factor &lt;n&gt;</c> — LFU counter logarithmic factor (default 10)</item>
/// <item><c>--lfu-decay-time &lt;minutes&gt;</c> — LFU counter decay period in minutes (default 1)</item>
/// <item><c>--replicaof &lt;host&gt; &lt;port&gt;</c> — start as a replica of the given primary
/// (default: standalone primary). The REPLICAOF command changes the target — or promotes with
/// NO ONE — at runtime</item>
/// <item><c>--masterauth &lt;password&gt;</c> — the password this server presents with AUTH when
/// connecting to its primary as a replica (default: none; the <c>REDIS_MASTERAUTH</c> environment
/// variable is the fallback)</item>
/// <item><c>--repl-backlog-size &lt;bytes&gt;</c> — bound of the replication backlog ring, and of
/// each replica link's outbound queue (default 16mb). Accepts the same size suffixes as
/// <c>--maxmemory</c></item>
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

    /// <summary>Dataset memory ceiling in bytes; 0 means no limit.</summary>
    public long MaxMemory { get; init; }

    /// <summary>Eviction policy applied when the dataset is over <see cref="MaxMemory"/>.</summary>
    public MaxMemoryPolicy MaxMemoryPolicy { get; init; } = MaxMemoryPolicy.NoEviction;

    /// <summary>LFU counter logarithmic factor (--lfu-log-factor). Default 10, as in Redis.</summary>
    public int LfuLogFactor { get; init; } = 10;

    /// <summary>LFU counter decay period in minutes (--lfu-decay-time). Default 1, as in Redis.</summary>
    public int LfuDecayTime { get; init; } = 1;

    /// <summary>Cluster mode (--cluster-enabled yes): slot-checked dispatch with MOVED redirection.</summary>
    public bool ClusterEnabled { get; init; }

    /// <summary>
    /// The static cluster slot map (--cluster-slots): every node's
    /// ranges as <c>start-end@host:port</c> entries. This node is the
    /// entry whose port equals <see cref="Port"/>. There is no gossip —
    /// the map is the whole topology, identical on every node.
    /// </summary>
    public IReadOnlyList<ClusterNodeRange> ClusterSlots { get; init; } = Array.Empty<ClusterNodeRange>();

    /// <summary>Parses a --cluster-slots value; malformed entries fail startup loudly.</summary>
    public static IReadOnlyList<ClusterNodeRange> ParseClusterSlots(string text)
    {
        var ranges = new List<ClusterNodeRange>();
        foreach (var entry in text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var at = entry.LastIndexOf('@');
            var colon = entry.LastIndexOf(':');
            if (at < 0 || colon < at)
                throw new ArgumentException($"Invalid --cluster-slots entry '{entry}': expected start-end@host:port.");
            var bounds = entry[..at].Split('-');
            if (bounds.Length != 2 ||
                !int.TryParse(bounds[0], out var start) || !int.TryParse(bounds[1], out var end) ||
                start < 0 || end >= ClusterSlot.SlotCount || start > end)
                throw new ArgumentException($"Invalid --cluster-slots entry '{entry}': slot range must be within 0-{ClusterSlot.SlotCount - 1}.");
            var host = entry[(at + 1)..colon];
            if (host.Length == 0 || !int.TryParse(entry[(colon + 1)..], out var port) || port is < 1 or > 65535)
                throw new ArgumentException($"Invalid --cluster-slots entry '{entry}': host or port is malformed.");
            ranges.Add(new ClusterNodeRange(start, end, host, port));
        }
        if (ranges.Count == 0)
            throw new ArgumentException("--cluster-slots must name at least one range.");
        ranges.Sort((a, b) => a.Start.CompareTo(b.Start));
        return ranges;
    }

    /// <summary>Startup replication target (the REPLICAOF command can change it later); null = primary.</summary>
    public string? ReplicaOfHost { get; init; }

    /// <summary>Port of <see cref="ReplicaOfHost"/>; meaningful only when a host is set.</summary>
    public int ReplicaOfPort { get; init; }

    /// <summary>Password presented to the primary when connecting as a replica, or null.</summary>
    public string? MasterAuth { get; init; }

    /// <summary>Replication backlog bound in bytes; also each replica link's outbound queue bound.</summary>
    public long ReplBacklogSizeBytes { get; init; } = ReplicationManager.DefaultBacklogSizeBytes;

    /// <summary>
    /// Redis's human size grammar: an optional suffix scales the number —
    /// k = 1000, kb = 1024, m = 1000^2, mb = 1024^2, g = 1000^3,
    /// gb = 1024^3; no suffix means bytes.
    /// </summary>
    public static long ParseMemorySize(string text)
    {
        var s = text.Trim().ToLowerInvariant();
        long multiplier = 1;
        if (s.EndsWith("kb")) { multiplier = 1024; s = s[..^2]; }
        else if (s.EndsWith("mb")) { multiplier = 1024L * 1024; s = s[..^2]; }
        else if (s.EndsWith("gb")) { multiplier = 1024L * 1024 * 1024; s = s[..^2]; }
        else if (s.EndsWith("k")) { multiplier = 1000; s = s[..^1]; }
        else if (s.EndsWith("m")) { multiplier = 1000L * 1000; s = s[..^1]; }
        else if (s.EndsWith("g")) { multiplier = 1000L * 1000 * 1000; s = s[..^1]; }
        if (!long.TryParse(s, out var value) || value < 0)
            throw new ArgumentException($"Invalid --maxmemory value '{text}'.");
        return checked(value * multiplier);
    }

    /// <summary>Parses a --maxmemory-policy name; anything outside the implemented set fails startup loudly.</summary>
    public static MaxMemoryPolicy ParseMaxMemoryPolicy(string text) => text.Trim().ToLowerInvariant() switch
    {
        "noeviction" => MaxMemoryPolicy.NoEviction,
        "allkeys-lru" => MaxMemoryPolicy.AllKeysLru,
        "allkeys-random" => MaxMemoryPolicy.AllKeysRandom,
        "allkeys-lfu" => MaxMemoryPolicy.AllKeysLfu,
        "volatile-lru" => MaxMemoryPolicy.VolatileLru,
        "volatile-random" => MaxMemoryPolicy.VolatileRandom,
        "volatile-ttl" => MaxMemoryPolicy.VolatileTtl,
        "volatile-lfu" => MaxMemoryPolicy.VolatileLfu,
        var other => throw new ArgumentException(
            $"Unsupported --maxmemory-policy '{other}'. Supported: noeviction, allkeys-lru, allkeys-random, " +
            "allkeys-lfu, volatile-lru, volatile-random, volatile-ttl, volatile-lfu.")
    };

    public static ServerOptions FromArgs(string[] args)
    {
        var port = DefaultPort;
        var directory = ".";
        var snapshotFileName = "dump.rcdb";
        var appendOnly = false;
        var appendFileName = "appendonly.aof";
        var appendFsync = AofFsyncPolicy.EverySec;
        var requirePass = Environment.GetEnvironmentVariable("REDIS_REQUIREPASS");
        var maxMemory = 0L;
        var maxMemoryPolicy = MaxMemoryPolicy.NoEviction;
        var lfuLogFactor = 10;
        var lfuDecayTime = 1;
        var clusterEnabled = false;
        var clusterSlots = (IReadOnlyList<ClusterNodeRange>)Array.Empty<ClusterNodeRange>();
        var replicaOfHost = (string?)null;
        var replicaOfPort = 0;
        var masterAuth = Environment.GetEnvironmentVariable("REDIS_MASTERAUTH");
        var replBacklogSizeBytes = ReplicationManager.DefaultBacklogSizeBytes;
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
                case "--maxmemory":
                    maxMemory = ParseMemorySize(value);
                    break;
                case "--maxmemory-policy":
                    maxMemoryPolicy = ParseMaxMemoryPolicy(value);
                    break;
                case "--lfu-log-factor" when int.TryParse(value, out var parsedFactor) && parsedFactor >= 0:
                    lfuLogFactor = parsedFactor;
                    break;
                case "--lfu-decay-time" when int.TryParse(value, out var parsedDecay) && parsedDecay >= 1:
                    lfuDecayTime = parsedDecay;
                    break;
                case "--cluster-enabled":
                    clusterEnabled = value.Equals("yes", StringComparison.OrdinalIgnoreCase);
                    break;
                case "--cluster-slots":
                    clusterSlots = ParseClusterSlots(value);
                    break;
                case "--replicaof":
                    // Two values (host, port): the port is the argument
                    // after the host. The pair loop later visits both as
                    // candidate flags; neither matches a case, so they are
                    // simply skipped there.
                    if (i + 2 >= args.Length || !int.TryParse(args[i + 2], out var parsedReplicaPort) ||
                        parsedReplicaPort is < 1 or > 65535)
                        throw new ArgumentException("--replicaof requires a host and a port between 1 and 65535.");
                    replicaOfHost = value;
                    replicaOfPort = parsedReplicaPort;
                    break;
                case "--masterauth":
                    masterAuth = value;
                    break;
                case "--repl-backlog-size":
                    replBacklogSizeBytes = ParseMemorySize(value);
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
            MaxMemory = maxMemory,
            MaxMemoryPolicy = maxMemoryPolicy,
            LfuLogFactor = lfuLogFactor,
            LfuDecayTime = lfuDecayTime,
            ClusterEnabled = clusterEnabled,
            ClusterSlots = clusterSlots,
            ReplicaOfHost = replicaOfHost,
            ReplicaOfPort = replicaOfPort,
            MasterAuth = string.IsNullOrEmpty(masterAuth) ? null : masterAuth,
            ReplBacklogSizeBytes = replBacklogSizeBytes,
            TlsPort = tlsPort,
            TlsCertFile = tlsCertFile,
            TlsCertPassword = tlsCertPassword
        };
    }
}
