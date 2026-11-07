namespace RedisServer.Core;

/// <summary>
/// Thread-safe operational counters, exposed through <c>INFO</c>. All updates
/// use <see cref="Interlocked"/> so hot paths never take a lock just to
/// count something (the same approach as the Memcached server's stats).
/// </summary>
public sealed class RedisStats
{
    private long _totalCommandsProcessed;
    private long _totalConnectionsReceived;
    private long _currConnections;
    private long _keyspaceHits;
    private long _keyspaceMisses;
    private long _expiredKeys;
    private long _publishedMessages;

    public void AddCommand() => Interlocked.Increment(ref _totalCommandsProcessed);
    public void AddKeyspaceHit() => Interlocked.Increment(ref _keyspaceHits);
    public void AddKeyspaceMiss() => Interlocked.Increment(ref _keyspaceMisses);
    public void AddExpiredKey() => Interlocked.Increment(ref _expiredKeys);
    public void AddPublishedMessage() => Interlocked.Increment(ref _publishedMessages);

    public void ConnectionOpened()
    {
        Interlocked.Increment(ref _currConnections);
        Interlocked.Increment(ref _totalConnectionsReceived);
    }

    public void ConnectionClosed() => Interlocked.Decrement(ref _currConnections);

    public long TotalCommandsProcessed => Interlocked.Read(ref _totalCommandsProcessed);
    public long TotalConnectionsReceived => Interlocked.Read(ref _totalConnectionsReceived);
    public long CurrConnections => Interlocked.Read(ref _currConnections);
    public long KeyspaceHits => Interlocked.Read(ref _keyspaceHits);
    public long KeyspaceMisses => Interlocked.Read(ref _keyspaceMisses);
    public long ExpiredKeys => Interlocked.Read(ref _expiredKeys);
    public long PublishedMessages => Interlocked.Read(ref _publishedMessages);
}
