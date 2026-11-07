using RedisServer.Core;

namespace RedisServer.Protocol;

/// <summary>
/// Everything the command layer needs to remember about one connection:
/// transaction state and pub/sub subscriptions. Owned by the session,
/// passed into every <see cref="CommandProcessor"/> call — the processor
/// itself stays stateless per client (the Memcached server's shape).
/// </summary>
public sealed class ClientContext
{
    public bool InTransaction { get; set; }

    /// <summary>Set when a command failed validation while queueing; EXEC then aborts with EXECABORT.</summary>
    public bool TransactionAborted { get; set; }

    public List<byte[][]> TransactionQueue { get; } = new();

    public bool CloseRequested { get; set; }

    public HashSet<byte[]> Channels { get; } = new(ByteArrayComparer.Instance);

    public HashSet<byte[]> Patterns { get; } = new(ByteArrayComparer.Instance);

    /// <summary>Session-provided push channel for pub/sub frames (already RESP-encoded). Invoked from any thread.</summary>
    public Action<byte[]>? PostFrame { get; set; }

    public long SubscriptionCount => Channels.Count + Patterns.Count;

    public bool IsSubscribed => SubscriptionCount > 0;
}
