namespace RedisServer.Tests;

/// <summary>
/// End-to-end protocol tests: raw bytes in, exact RESP bytes out, over an
/// in-memory duplex transport running the real session and processor code.
/// </summary>
public class ProtocolTests
{
    [Fact]
    public async Task Ping_AndEcho()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("PING");
        await c.ExpectAsync("+PONG\r\n");
        await c.SendCommandAsync("PING", "hello");
        await c.ExpectAsync("$5\r\nhello\r\n");
        await c.SendCommandAsync("ECHO", "world");
        await c.ExpectAsync("$5\r\nworld\r\n");
    }

    [Fact]
    public async Task SetGet_RespArrays_AndInlineCommands()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "foo", "hello");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("GET", "foo");
        await c.ExpectAsync("$5\r\nhello\r\n");
        await c.SendCommandAsync("GET", "ghost");
        await c.ExpectAsync("$-1\r\n");

        // Inline (telnet-style) commands, including a quoted value.
        await c.SendAsync("SET greeting \"hello world\"\r\n");
        await c.ExpectAsync("+OK\r\n");
        await c.SendAsync("GET greeting\r\n");
        await c.ExpectAsync("$11\r\nhello world\r\n");
    }

    [Fact]
    public async Task PipelinedCommands_AreAnsweredInOrder()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendAsync(
            "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n" +
            "*2\r\n$4\r\nINCR\r\n$1\r\na\r\n" +
            "*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
        await c.ExpectAsync("+OK\r\n:2\r\n$1\r\n2\r\n");
    }

    [Fact]
    public async Task CommandSplitAcrossPackets_IsReassembled()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendAsync("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhel");
        await c.ExpectNoReplyAsync(200);
        await c.SendAsync("lo\r\n");
        await c.ExpectAsync("+OK\r\n");
    }

    [Fact]
    public async Task UnknownCommand_AndWrongArity_AreErrors()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("NOSUCH", "x");
        await c.ExpectAsync("-ERR unknown command 'NOSUCH', with args beginning with: 'x'\r\n");
        await c.SendCommandAsync("GET");
        await c.ExpectAsync("-ERR wrong number of arguments for 'get' command\r\n");
    }

    [Fact]
    public async Task ProtocolError_ClosesTheConnection()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendAsync("$3\r\nfoo\r\n"); // commands must start with '*' or be inline
        await c.ExpectAsync("-ERR Protocol error: expected '*', got '$'\r\n");
        await c.ExpectNoReplyAsync(300); // the session is gone: nothing more, ever
    }

    [Fact]
    public async Task Set_Conditions_AndGetOption_OverTheWire()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "k", "v1", "NX");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "k", "v2", "NX");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("SET", "k", "v2", "XX", "GET");
        await c.ExpectAsync("$2\r\nv1\r\n"); // GET returns the old value
        await c.SendCommandAsync("SET", "k", "v3", "BADOPT");
        await c.ExpectAsync("-ERR syntax error\r\n");
    }

    [Fact]
    public async Task Incr_Errors_OverTheWire()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "s", "abc");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("INCR", "s");
        await c.ExpectAsync("-ERR value is not an integer or out of range\r\n");
        await c.SendCommandAsync("INCRBY", "n", "10");
        await c.ExpectAsync(":10\r\n");
    }

    [Fact]
    public async Task Expiry_OverTheWire_WithFakeClock()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "temp", "v", "EX", "10");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("TTL", "temp");
        await c.ExpectAsync(":10\r\n");
        await c.SendCommandAsync("PTTL", "ghost");
        await c.ExpectAsync(":-2\r\n");
        h.Clock.Advance(TimeSpan.FromSeconds(11));
        await c.SendCommandAsync("GET", "temp");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("TTL", "temp");
        await c.ExpectAsync(":-2\r\n");
    }

    [Fact]
    public async Task Collections_OverTheWire()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("LPUSH", "l", "a", "b", "c");
        await c.ExpectAsync(":3\r\n");
        await c.SendCommandAsync("LRANGE", "l", "0", "-1");
        await c.ExpectAsync("*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");
        await c.SendCommandAsync("TYPE", "l");
        await c.ExpectAsync("+list\r\n");
        await c.SendCommandAsync("GET", "l");
        await c.ExpectAsync("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        await c.SendCommandAsync("HSET", "h", "f2", "v2", "f1", "v1");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("HGETALL", "h");
        await c.ExpectAsync("*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"); // fields sorted
        await c.SendCommandAsync("ZADD", "z", "1.5", "m1", "2", "m2");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("ZRANGE", "z", "0", "-1", "WITHSCORES");
        await c.ExpectAsync("*4\r\n$2\r\nm1\r\n$3\r\n1.5\r\n$2\r\nm2\r\n$1\r\n2\r\n");
    }

    [Fact]
    public async Task Scan_PagesThroughTheWholeKeyspace()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        for (var i = 0; i < 25; i++)
            await c.SendCommandAsync("SET", $"key:{i:D2}", "v");
        for (var i = 0; i < 25; i++)
            await c.ExpectAsync("+OK\r\n");

        var seen = new HashSet<string>();
        var cursor = "0";
        var pages = 0;
        do
        {
            await c.SendCommandAsync("SCAN", cursor, "COUNT", "10");
            var reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
            cursor = Assert.IsType<string>(reply[0]);
            foreach (var key in Assert.IsType<List<object?>>(reply[1]))
                seen.Add(Assert.IsType<string>(key));
            pages++;
        } while (cursor != "0");

        Assert.Equal(25, seen.Count);
        Assert.Equal(3, pages); // 10 + 10 + 5
    }

    [Fact]
    public async Task Scan_MatchAndTypeFilters()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "user:1", "x");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("RPUSH", "user:list", "a");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("SET", "other", "x");
        await c.ExpectAsync("+OK\r\n");

        await c.SendCommandAsync("SCAN", "0", "MATCH", "user:*", "TYPE", "list");
        var reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal("0", Assert.IsType<string>(reply[0]));
        var keys = Assert.IsType<List<object?>>(reply[1]);
        Assert.Equal(new object?[] { "user:list" }, keys.ToArray());
    }

    [Fact]
    public async Task DbSize_Type_Keys_Info_Smoke()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "a", "1");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("DBSIZE");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("KEYS", "*");
        await c.ExpectAsync("*1\r\n$1\r\na\r\n");
        await c.SendCommandAsync("INFO", "stats");
        var info = await c.CollectUntilAsync("total_commands_processed:");
        Assert.Contains("keyspace_hits:", info);
    }
}
