namespace RedisServer.Tests;

/// <summary>
/// HELLO and the RESP3 reply shapes, wire-level. Every expected byte
/// string here was probed against a real redis-server 7.0.15 first:
/// HELLO's flat-array (RESP2) and map (RESP3) handshakes, its error
/// order (version parse, NOPROTO, clause syntax, WRONGPASS, then its
/// own NOAUTH), HGETALL as a map, SMEMBERS as a set, scores as doubles,
/// INFO as a verbatim string, the one-character null everywhere RESP2
/// has $-1 or *-1, pub/sub frames as pushes, and subscribed-mode PING
/// losing its array.
/// </summary>
public sealed class Resp3Tests
{
    private static async Task Hello3(TestClient client)
    {
        await client.SendCommandAsync("HELLO", "3");
        var reply = await client.ReadReplyAsync();
        var pairs = Assert.IsType<List<object?>>(reply);
        Assert.Equal("proto", pairs[4]);
        Assert.Equal(3L, pairs[5]);
    }

    [Fact]
    public async Task Hello_WithoutVersion_ReturnsTheFlatArrayHandshake()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("HELLO");
        var pairs = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(14, pairs.Count);
        Assert.Equal("server", pairs[0]);
        Assert.Equal("redis-csharp", pairs[1]);
        Assert.Equal("version", pairs[2]);
        Assert.Equal("1.0.0", pairs[3]);
        Assert.Equal("proto", pairs[4]);
        Assert.Equal(2L, pairs[5]);
        Assert.Equal("id", pairs[6]);
        Assert.IsType<long>(pairs[7]);
        Assert.Equal("mode", pairs[8]);
        Assert.Equal("standalone", pairs[9]);
        Assert.Equal("role", pairs[10]);
        Assert.Equal("master", pairs[11]);
        Assert.Equal("modules", pairs[12]);
        Assert.Empty(Assert.IsType<List<object?>>(pairs[13]));
    }

    [Fact]
    public async Task Hello3_ReturnsAMap_AndKeepsTheVersionWhenNoVersionFollows()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        // The exact wire prefix of the map handshake (the id value itself
        // is server-assigned, so the tail is parsed value by value).
        await c.SendCommandAsync("HELLO", "3");
        await c.ExpectAsync("%7\r\n$6\r\nserver\r\n$12\r\nredis-csharp\r\n$7\r\nversion\r\n$5\r\n1.0.0\r\n$5\r\nproto\r\n:3\r\n");
        Assert.Equal("id", await c.ReadReplyAsync());
        Assert.IsType<long>(await c.ReadReplyAsync());
        Assert.Equal("mode", await c.ReadReplyAsync());
        Assert.Equal("standalone", await c.ReadReplyAsync());
        Assert.Equal("role", await c.ReadReplyAsync());
        Assert.Equal("master", await c.ReadReplyAsync());
        Assert.Equal("modules", await c.ReadReplyAsync());
        Assert.Empty(Assert.IsType<List<object?>>(await c.ReadReplyAsync()));

        // HELLO without a version reports (and keeps) the current one.
        await c.SendCommandAsync("HELLO");
        var pairs = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(3L, pairs[5]);
    }

    [Fact]
    public async Task Hello_VersionErrors_ComeFirst()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("HELLO", "4");
        await c.ExpectAsync("-NOPROTO unsupported protocol version\r\n");
        await c.SendCommandAsync("HELLO", "0");
        await c.ExpectAsync("-NOPROTO unsupported protocol version\r\n");
        await c.SendCommandAsync("HELLO", "x");
        await c.ExpectAsync("-ERR Protocol version is not an integer or out of range\r\n");
        await c.SendCommandAsync("HELLO", "99999999999999999999");
        await c.ExpectAsync("-ERR Protocol version is not an integer or out of range\r\n");
        // A failed HELLO switches nothing: the next reply is RESP2-shaped.
        await c.SendCommandAsync("SMEMBERS", "ghost");
        await c.ExpectAsync("*0\r\n");
    }

    [Fact]
    public async Task Hello_ClauseErrors_AreSyntaxErrorsNamingTheClause()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("HELLO", "3", "BOGUS", "x");
        await c.ExpectAsync("-ERR Syntax error in HELLO option 'BOGUS'\r\n");
        await c.SendCommandAsync("HELLO", "3", "SETNAME");
        await c.ExpectAsync("-ERR Syntax error in HELLO option 'SETNAME'\r\n");
        await c.SendCommandAsync("HELLO", "3", "AUTH", "onlypassword");
        await c.ExpectAsync("-ERR Syntax error in HELLO option 'AUTH'\r\n");
        await c.SendCommandAsync("HELLO", "3", "SETNAME", "bad name!");
        await c.ExpectAsync("-ERR Client names cannot contain spaces, newlines or special characters.\r\n");
    }

    [Fact]
    public async Task Hello_InsideMulti_ExecutesAtExec()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("HELLO", "3");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("EXEC");
        var exec = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        var hello = Assert.IsType<List<object?>>(Assert.Single(exec));
        Assert.Equal(3L, hello[5]);
        // The version switched at EXEC: replies are RESP3-shaped now.
        await c.SendCommandAsync("GET", "ghost");
        await c.ExpectAsync("_\r\n");
    }

    [Fact]
    public async Task Resp3_CommandShapes()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await Hello3(c);

        await c.SendCommandAsync("HSET", "h", "f1", "v1", "f2", "v2");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("HGETALL", "h");
        await c.ExpectAsync("%2\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n");
        await c.SendCommandAsync("SADD", "s", "b", "a");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("SMEMBERS", "s");
        await c.ExpectAsync("~2\r\n$1\r\na\r\n$1\r\nb\r\n");
        await c.SendCommandAsync("ZADD", "z", "1.5", "m1", "2", "m2");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("ZSCORE", "z", "m1");
        await c.ExpectAsync(",1.5\r\n");
        await c.SendCommandAsync("ZSCORE", "z", "ghost");
        await c.ExpectAsync("_\r\n");
        await c.SendCommandAsync("ZADD", "z", "INCR", "0.5", "m1");
        await c.ExpectAsync(",2\r\n");
        await c.SendCommandAsync("ZRANGE", "z", "0", "-1", "WITHSCORES");
        await c.ExpectAsync("*2\r\n*2\r\n$2\r\nm1\r\n,2\r\n*2\r\n$2\r\nm2\r\n,2\r\n");
        await c.SendCommandAsync("ZRANGE", "z", "0", "-1");
        await c.ExpectAsync("*2\r\n$2\r\nm1\r\n$2\r\nm2\r\n");
    }

    [Fact]
    public async Task Resp3_Nulls_InfoVerbatim_AndSetGet()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await Hello3(c);

        await c.SendCommandAsync("GET", "ghost");
        await c.ExpectAsync("_\r\n");
        await c.SendCommandAsync("LPOP", "ghost");
        await c.ExpectAsync("_\r\n");
        await c.SendCommandAsync("LPOP", "ghost", "3");
        await c.ExpectAsync("_\r\n");
        await c.SendCommandAsync("SET", "nk", "v", "GET");
        await c.ExpectAsync("_\r\n");
        // ZADD INCR: XX on a missing member declines (null); NX on a
        // missing member creates it, exactly as in RESP2.
        await c.SendCommandAsync("ZADD", "zz", "INCR", "XX", "1", "ghost");
        await c.ExpectAsync("_\r\n");
        await c.SendCommandAsync("ZADD", "zz", "INCR", "NX", "1", "ghost");
        await c.ExpectAsync(",1\r\n");
        await c.SendCommandAsync("ZADD", "zz", "INCR", "NX", "1", "ghost");
        await c.ExpectAsync("_\r\n");

        await c.SendCommandAsync("INFO", "server");
        var info = Assert.IsType<string>(await c.ReadReplyAsync());
        Assert.StartsWith("# Server", info);
        Assert.Contains("server_name:redis-csharp", info);

        await c.SendCommandAsync("INFO", "bogussection");
        await c.ExpectAsync("=4\r\ntxt:\r\n");
    }

    [Fact]
    public async Task Resp3_PubSubFramesArePushes_AndSubscribedPingLosesItsArray()
    {
        await using var h = SessionHarness.Start();
        var sub = h.Connect();
        await Hello3(sub);
        var pub = h.Connect();

        await sub.SendCommandAsync("SUBSCRIBE", "news");
        await sub.ExpectAsync(">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
        await sub.SendCommandAsync("PING");
        await sub.ExpectAsync("+PONG\r\n");
        await sub.SendCommandAsync("PING", "yo");
        await sub.ExpectAsync("$2\r\nyo\r\n");

        await pub.SendCommandAsync("PUBLISH", "news", "hello");
        await pub.ExpectAsync(":1\r\n");
        await sub.ExpectAsync(">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n");

        await sub.SendCommandAsync("PSUBSCRIBE", "n*");
        await sub.ExpectAsync(">3\r\n$10\r\npsubscribe\r\n$2\r\nn*\r\n:2\r\n");
        await pub.SendCommandAsync("PUBLISH", "news", "again");
        await pub.ExpectAsync(":2\r\n");
        await sub.ExpectAsync(">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nagain\r\n");
        await sub.ExpectAsync(">4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$5\r\nagain\r\n");
        await sub.SendCommandAsync("UNSUBSCRIBE", "news");
        await sub.ExpectAsync(">3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    }

    [Fact]
    public async Task Resp2AndResp3_SubscribersEachGetTheirOwnFrameShape()
    {
        await using var h = SessionHarness.Start();
        var sub2 = h.Connect();
        var sub3 = h.Connect();
        await Hello3(sub3);
        var pub = h.Connect();

        await sub2.SendCommandAsync("SUBSCRIBE", "mix");
        await sub2.ExpectAsync("*3\r\n$9\r\nsubscribe\r\n$3\r\nmix\r\n:1\r\n");
        await sub3.SendCommandAsync("SUBSCRIBE", "mix");
        await sub3.ExpectAsync(">3\r\n$9\r\nsubscribe\r\n$3\r\nmix\r\n:1\r\n");
        await pub.SendCommandAsync("PUBLISH", "mix", "m");
        await pub.ExpectAsync(":2\r\n");
        await sub2.ExpectAsync("*3\r\n$7\r\nmessage\r\n$3\r\nmix\r\n$1\r\nm\r\n");
        await sub3.ExpectAsync(">3\r\n$7\r\nmessage\r\n$3\r\nmix\r\n$1\r\nm\r\n");
    }

    [Fact]
    public async Task Resp2_ShapesAreUnchanged()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("HSET", "h", "f1", "v1");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("HGETALL", "h");
        await c.ExpectAsync("*2\r\n$2\r\nf1\r\n$2\r\nv1\r\n");
        await c.SendCommandAsync("SADD", "s", "a");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("SMEMBERS", "s");
        await c.ExpectAsync("*1\r\n$1\r\na\r\n");
        await c.SendCommandAsync("ZADD", "z", "1.5", "m1");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("ZSCORE", "z", "m1");
        await c.ExpectAsync("$3\r\n1.5\r\n");
        await c.SendCommandAsync("ZRANGE", "z", "0", "-1", "WITHSCORES");
        await c.ExpectAsync("*2\r\n$2\r\nm1\r\n$3\r\n1.5\r\n");
        await c.SendCommandAsync("LPOP", "ghost", "3");
        await c.ExpectAsync("*-1\r\n");
    }

    [Fact]
    public async Task Hello_WithPassword_AuthClauseRules()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var c = h.Connect();

        // No AUTH clause: HELLO's own, longer NOAUTH text.
        await c.SendCommandAsync("HELLO", "3");
        await c.ExpectAsync("-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time\r\n");
        // A bad password: WRONGPASS, and nothing was switched or unlocked.
        await c.SendCommandAsync("HELLO", "3", "AUTH", "default", "nope");
        await c.ExpectAsync("-WRONGPASS invalid username-password pair or user is disabled.\r\n");
        await c.SendCommandAsync("PING");
        await c.ExpectAsync("-NOAUTH Authentication required.\r\n");
        // Clauses apply in order: SETNAME first, then AUTH.
        await c.SendCommandAsync("HELLO", "3", "SETNAME", "probe", "AUTH", "default", "s3cret");
        var pairs = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(3L, pairs[5]);
        await c.SendCommandAsync("PING");
        await c.ExpectAsync("+PONG\r\n");
    }
}
