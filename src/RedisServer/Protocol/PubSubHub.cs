using System.Buffers;
using RedisServer.Core;

namespace RedisServer.Protocol;

/// <summary>
/// The pub/sub registry: channel → subscribers and pattern → subscribers,
/// with delivery. Delivery posts an encoded frame to each subscriber's
/// session outbound queue (non-blocking), so a slow subscriber can never
/// stall a publisher — the same reason Redis buffers output per client.
/// </summary>
public sealed class PubSubHub
{
    private readonly object _gate = new();
    private readonly Dictionary<byte[], HashSet<ClientContext>> _channels = new(ByteArrayComparer.Instance);
    private readonly Dictionary<byte[], HashSet<ClientContext>> _patterns = new(ByteArrayComparer.Instance);

    public int ChannelCount
    {
        get { lock (_gate) return _channels.Count; }
    }

    public int PatternCount
    {
        get { lock (_gate) return _patterns.Count; }
    }

    /// <summary>Returns true when this is a new subscription for the client.</summary>
    public bool Subscribe(ClientContext ctx, byte[] channel)
    {
        lock (_gate)
        {
            if (!ctx.Channels.Add(channel))
                return false;
            if (!_channels.TryGetValue(channel, out var subscribers))
                _channels[channel] = subscribers = new HashSet<ClientContext>();
            subscribers.Add(ctx);
            return true;
        }
    }

    public bool SubscribePattern(ClientContext ctx, byte[] pattern)
    {
        lock (_gate)
        {
            if (!ctx.Patterns.Add(pattern))
                return false;
            if (!_patterns.TryGetValue(pattern, out var subscribers))
                _patterns[pattern] = subscribers = new HashSet<ClientContext>();
            subscribers.Add(ctx);
            return true;
        }
    }

    public bool Unsubscribe(ClientContext ctx, byte[] channel)
    {
        lock (_gate)
        {
            if (!ctx.Channels.Remove(channel))
                return false;
            RemoveFromIndex(_channels, channel, ctx);
            return true;
        }
    }

    public bool UnsubscribePattern(ClientContext ctx, byte[] pattern)
    {
        lock (_gate)
        {
            if (!ctx.Patterns.Remove(pattern))
                return false;
            RemoveFromIndex(_patterns, pattern, ctx);
            return true;
        }
    }

    /// <summary>Drops every subscription of a disconnecting client.</summary>
    public void RemoveAll(ClientContext ctx)
    {
        lock (_gate)
        {
            foreach (var channel in ctx.Channels)
                RemoveFromIndex(_channels, channel, ctx);
            foreach (var pattern in ctx.Patterns)
                RemoveFromIndex(_patterns, pattern, ctx);
            ctx.Channels.Clear();
            ctx.Patterns.Clear();
        }
    }

    private static void RemoveFromIndex(Dictionary<byte[], HashSet<ClientContext>> index, byte[] key, ClientContext ctx)
    {
        if (index.TryGetValue(key, out var subscribers))
        {
            subscribers.Remove(ctx);
            if (subscribers.Count == 0)
                index.Remove(key);
        }
    }

    /// <summary>
    /// Delivers a message to direct subscribers and to every pattern
    /// subscriber whose pattern matches. Returns the number of deliveries,
    /// which is what PUBLISH reports (a client reached twice — directly and
    /// by pattern — counts twice, as in Redis).
    /// </summary>
    public int Publish(byte[] channel, byte[] message)
    {
        List<(ClientContext Ctx, byte[] Frame)> deliveries = new();
        lock (_gate)
        {
            if (_channels.TryGetValue(channel, out var direct))
            {
                foreach (var ctx in direct)
                    deliveries.Add((ctx, BuildMessageFrame(null, channel, message)));
            }
            foreach (var (pattern, subscribers) in _patterns)
            {
                if (!Glob.IsMatch(pattern, channel))
                    continue;
                foreach (var ctx in subscribers)
                    deliveries.Add((ctx, BuildMessageFrame(pattern, channel, message)));
            }
        }

        foreach (var (ctx, frame) in deliveries)
            ctx.PostFrame?.Invoke(frame);
        return deliveries.Count;
    }

    private static byte[] BuildMessageFrame(byte[]? pattern, byte[] channel, byte[] message)
    {
        var buffer = new ArrayBufferWriter<byte>();
        if (pattern is null)
        {
            RespWriter.WriteArrayHeader(buffer, 3);
            RespWriter.WriteBulkString(buffer, "message");
        }
        else
        {
            RespWriter.WriteArrayHeader(buffer, 4);
            RespWriter.WriteBulkString(buffer, "pmessage");
            RespWriter.WriteBulkString(buffer, pattern);
        }
        RespWriter.WriteBulkString(buffer, channel);
        RespWriter.WriteBulkString(buffer, message);
        return buffer.WrittenSpan.ToArray();
    }
}
