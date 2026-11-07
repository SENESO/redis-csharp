namespace RedisServer.Tests;

/// <summary>Pub/sub over the wire: channels, patterns, counts, restricted mode.</summary>
public class PubSubTests
{
    [Fact]
    public async Task Subscribe_Publish_MessageRoundTrip()
    {
        await using var h = SessionHarness.Start();
        var sub = h.Connect();
        var pub = h.Connect();

        await sub.SendCommandAsync("SUBSCRIBE", "news");
        await sub.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");

        await pub.SendCommandAsync("PUBLISH", "news", "hello");
        await pub.ExpectAsync(":1\r\n");
        await sub.ExpectAsync("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n");

        await pub.SendCommandAsync("PUBLISH", "sports", "nobody-listens");
        await pub.ExpectAsync(":0\r\n");
    }

    [Fact]
    public async Task PatternSubscription_ReceivesPMessage()
    {
        await using var h = SessionHarness.Start();
        var sub = h.Connect();
        var pub = h.Connect();

        await sub.SendCommandAsync("PSUBSCRIBE", "n*");
        await sub.ExpectAsync("*3\r\n$10\r\npsubscribe\r\n$2\r\nn*\r\n:1\r\n");

        await pub.SendCommandAsync("PUBLISH", "news", "ping");
        await pub.ExpectAsync(":1\r\n");
        await sub.ExpectAsync("*4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$4\r\nping\r\n");

        await sub.SendCommandAsync("PUNSUBSCRIBE", "n*");
        await sub.ExpectAsync("*3\r\n$12\r\npunsubscribe\r\n$2\r\nn*\r\n:0\r\n");
    }

    [Fact]
    public async Task TwoSubscribers_PublishCountsBoth()
    {
        await using var h = SessionHarness.Start();
        var sub1 = h.Connect();
        var sub2 = h.Connect();
        var pub = h.Connect();

        await sub1.SendCommandAsync("SUBSCRIBE", "alerts");
        await sub1.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$6\r\nalerts\r\n:1\r\n");
        await sub2.SendCommandAsync("SUBSCRIBE", "alerts");
        await sub2.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$6\r\nalerts\r\n:1\r\n");

        await pub.SendCommandAsync("PUBLISH", "alerts", "go");
        await pub.ExpectAsync(":2\r\n");
        await sub1.ExpectAsync("*3\r\n$7\r\nmessage\r\n$6\r\nalerts\r\n$2\r\ngo\r\n");
        await sub2.ExpectAsync("*3\r\n$7\r\nmessage\r\n$6\r\nalerts\r\n$2\r\ngo\r\n");
    }

    [Fact]
    public async Task SubscribedMode_RestrictsCommands_AndUnsubscribeRestores()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("SUBSCRIBE", "room");
        await c.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$4\r\nroom\r\n:1\r\n");

        await c.SendCommandAsync("GET", "anything");
        await c.ExpectAsync("-ERR Can't execute 'get': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n");

        await c.SendCommandAsync("PING");
        await c.ExpectAsync("*2\r\n$4\r\npong\r\n$0\r\n\r\n"); // subscribed PING is the array form

        await c.SendCommandAsync("UNSUBSCRIBE", "room");
        await c.ExpectAsync("*3\r\n$11\r\nunsubscribe\r\n$4\r\nroom\r\n:0\r\n");

        await c.SendCommandAsync("GET", "anything");
        await c.ExpectAsync("$-1\r\n"); // normal mode again
    }

    [Fact]
    public async Task UnsubscribeAll_AndUnsubscribeWithNothing()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SUBSCRIBE", "a", "b");
        await c.ExpectAsync(
            "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n" +
            "*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n");
        await c.SendCommandAsync("UNSUBSCRIBE");
        await c.ExpectAsync(
            "*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n" +
            "*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n");

        var fresh = h.Connect();
        await fresh.SendCommandAsync("UNSUBSCRIBE"); // nothing to remove: one null-channel reply
        await fresh.ExpectAsync("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
    }

    [Fact]
    public async Task DirectAndPattern_BothDeliver_AndPublishCountsTwice()
    {
        await using var h = SessionHarness.Start();
        var sub = h.Connect();
        var pub = h.Connect();

        await sub.SendCommandAsync("SUBSCRIBE", "news");
        await sub.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
        await sub.SendCommandAsync("PSUBSCRIBE", "n*");
        await sub.ExpectAsync("*3\r\n$10\r\npsubscribe\r\n$2\r\nn*\r\n:2\r\n");

        await pub.SendCommandAsync("PUBLISH", "news", "ping");
        await pub.ExpectAsync(":2\r\n"); // reached twice: directly and by pattern, as in Redis
        await sub.ExpectAsync("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$4\r\nping\r\n");
        await sub.ExpectAsync("*4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$4\r\nping\r\n");
    }

    [Fact]
    public async Task SubscribeSameChannelTwice_CountStaysOne()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SUBSCRIBE", "dup");
        await c.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$3\r\ndup\r\n:1\r\n");
        await c.SendCommandAsync("SUBSCRIBE", "dup");
        await c.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$3\r\ndup\r\n:1\r\n"); // confirmed again, not double-counted
    }

    [Fact]
    public async Task Unsubscribe_NeverHeldChannel_RepliesWithNameAndCurrentCount()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SUBSCRIBE", "room");
        await c.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$4\r\nroom\r\n:1\r\n");
        await c.SendCommandAsync("UNSUBSCRIBE", "never-held");
        await c.ExpectAsync("*3\r\n$11\r\nunsubscribe\r\n$10\r\nnever-held\r\n:1\r\n");
    }

    [Fact]
    public async Task PunsubscribeAllPatterns_AndPunsubscribeWithNoPatterns()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("PSUBSCRIBE", "x*", "y*");
        await c.ExpectAsync(
            "*3\r\n$10\r\npsubscribe\r\n$2\r\nx*\r\n:1\r\n" +
            "*3\r\n$10\r\npsubscribe\r\n$2\r\ny*\r\n:2\r\n");
        await c.SendCommandAsync("PUNSUBSCRIBE");
        var first = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        var second = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        var seen = new HashSet<string>
        {
            $"{first[1]}:{first[2]}",
            $"{second[1]}:{second[2]}"
        };
        Assert.Equal(new HashSet<string> { "x*:1", "y*:0" }, seen); // order between the two is the hub's

        // No patterns left: one null-pattern reply, and the count still
        // includes the client's channel subscriptions (as in Redis).
        await c.SendCommandAsync("SUBSCRIBE", "room");
        await c.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$4\r\nroom\r\n:1\r\n");
        await c.SendCommandAsync("PUNSUBSCRIBE");
        await c.ExpectAsync("*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:1\r\n");
    }

    [Fact]
    public async Task SubscribedPing_WithMessage_AnswersTheMessage()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SUBSCRIBE", "room");
        await c.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$4\r\nroom\r\n:1\r\n");
        await c.SendCommandAsync("PING", "hello");
        await c.ExpectAsync("*2\r\n$4\r\npong\r\n$5\r\nhello\r\n");
    }
}
