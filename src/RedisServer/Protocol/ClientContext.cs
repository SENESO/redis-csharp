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

    /// <summary>
    /// Server-assigned connection id (the <c>id</c> HELLO reports and
    /// CLIENT ID returns). Assigned by the session when the connection
    /// opens; ids are per-process and increase monotonically.
    /// </summary>
    public long Id { get; set; }

    /// <summary>
    /// The negotiated protocol version: 2 until a HELLO selects 3.
    /// Reply rendering consults it per connection, so RESP2 and RESP3
    /// clients are served side by side.
    /// </summary>
    public int ProtocolVersion { get; set; } = 2;

    /// <summary>The connection's name (HELLO ... SETNAME / CLIENT SETNAME), or null when unset.</summary>
    public byte[]? Name { get; set; }

    /// <summary>Library identity reported with CLIENT SETINFO; recorded, not acted on.</summary>
    public string? LibraryName { get; set; }

    /// <summary>Library version reported with CLIENT SETINFO; recorded, not acted on.</summary>
    public string? LibraryVersion { get; set; }

    /// <summary>
    /// True for server-internal execution that is not a client (AOF
    /// replay): memory-eviction checks are skipped there, because replay
    /// must reproduce history exactly rather than defend a memory cap.
    /// </summary>
    public bool IsServerInternal { get; set; }

    /// <summary>
    /// True once this connection has presented the configured password
    /// (only meaningful when the server runs with one; the processor never
    /// consults it otherwise). Server-internal execution (AOF replay) marks
    /// its throwaway context authenticated — replay is not a client.
    /// </summary>
    public bool IsAuthenticated { get; set; }

    public HashSet<byte[]> Channels { get; } = new(ByteArrayComparer.Instance);

    public HashSet<byte[]> Patterns { get; } = new(ByteArrayComparer.Instance);

    /// <summary>Session-provided push channel for pub/sub frames (already RESP-encoded). Invoked from any thread.</summary>
    public Action<byte[]>? PostFrame { get; set; }

    public long SubscriptionCount => Channels.Count + Patterns.Count;

    public bool IsSubscribed => SubscriptionCount > 0;
}
