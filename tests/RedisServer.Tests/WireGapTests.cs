using RedisServer.Core;

namespace RedisServer.Tests;

/// <summary>
/// Wire-level coverage for the paths the first suite left untested:
/// the WRONGTYPE matrix across type families, arity errors beyond GET,
/// SET/EXPIRE/ZADD option semantics and error texts (each one verified
/// against a real redis-server 7.0), pop/scan edge cases, and QUIT.
/// </summary>
public class WireGapTests
{
    private const string WrongTypeReply =
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    private static async Task<TestClient> SeededAsync(SessionHarness h)
    {
        var c = h.Connect();
        await c.SendCommandAsync("SET", "str", "10");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("RPUSH", "list", "a", "b");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("SADD", "set", "m1", "m2");
        await c.ExpectAsync(":2\r\n");
        await c.SendCommandAsync("HSET", "hash", "f", "v");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("ZADD", "zset", "1", "m");
        await c.ExpectAsync(":1\r\n");
        return c;
    }

    [Fact]
    public async Task WrongType_EveryFamilyAgainstEveryOtherType()
    {
        await using var h = SessionHarness.Start();
        var c = await SeededAsync(h);

        string[][] attacks =
        [
            // String commands against every non-string type
            ["GET", "list"], ["GET", "set"], ["GET", "hash"], ["GET", "zset"],
            ["INCR", "list"], ["INCR", "hash"], ["APPEND", "zset", "x"], ["STRLEN", "set"],
            // List commands against non-lists
            ["LPUSH", "str", "x"], ["LPUSH", "hash", "x"], ["RPOP", "set"], ["LRANGE", "zset", "0", "-1"],
            ["LLEN", "hash"], ["LINDEX", "set", "0"],
            // Set commands against non-sets
            ["SADD", "str", "x"], ["SADD", "zset", "x"], ["SMEMBERS", "list"], ["SCARD", "hash"],
            ["SISMEMBER", "zset", "m"],
            // Hash commands against non-hashes
            ["HSET", "list", "f", "v"], ["HSET", "set", "f", "v"], ["HGET", "str", "f"],
            ["HGETALL", "zset"], ["HLEN", "list"],
            // Sorted-set commands against non-zsets
            ["ZADD", "str", "1", "m"], ["ZADD", "hash", "1", "m"], ["ZSCORE", "list", "m"],
            ["ZRANGE", "set", "0", "-1"], ["ZCARD", "str"],
        ];

        foreach (var attack in attacks)
        {
            await c.SendCommandAsync(attack);
            var reply = Assert.IsType<TestClient.RespError>(await c.ReadReplyAsync());
            Assert.Equal(WrongTypeException.MessageText, reply.Text);
        }

        // Nothing was mutated by the rejected commands.
        await c.SendCommandAsync("GET", "str");
        await c.ExpectAsync("$2\r\n10\r\n");
        await c.SendCommandAsync("LLEN", "list");
        await c.ExpectAsync(":2\r\n");
    }

    [Fact]
    public async Task Arity_Errors_AcrossCommands()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        (string[] Args, string Name)[] cases =
        [
            (["GET", "a", "b"], "get"),
            (["INCR"], "incr"),
            (["INCRBY", "k"], "incrby"),
            (["LPUSH", "k"], "lpush"),
            (["HGET", "h"], "hget"),
            (["SADD", "s"], "sadd"),
            (["TTL"], "ttl"),
            (["SUBSCRIBE"], "subscribe"),
            (["PUBLISH", "chan"], "publish"),
            (["ECHO"], "echo"),
            (["LPOP", "k", "1", "extra"], "lpop"),
            (["PING", "a", "b"], "ping"),
        ];

        foreach (var (args, name) in cases)
        {
            await c.SendCommandAsync(args);
            await c.ExpectAsync($"-ERR wrong number of arguments for '{name}' command\r\n");
        }
    }

    [Fact]
    public async Task Set_GetOption_ReturnsOldValue_EvenWhenConditionDeclines()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "k", "v1");
        await c.ExpectAsync("+OK\r\n");

        // NX declines the write, but GET still answers the old value (real Redis).
        await c.SendCommandAsync("SET", "k", "v2", "NX", "GET");
        await c.ExpectAsync("$2\r\nv1\r\n");
        await c.SendCommandAsync("GET", "k");
        await c.ExpectAsync("$2\r\nv1\r\n");

        // XX on a missing key declines; there is no old value.
        await c.SendCommandAsync("SET", "ghost", "v", "XX", "GET");
        await c.ExpectAsync("$-1\r\n");

        // A stored write with GET answers the previous value as well.
        await c.SendCommandAsync("SET", "k", "v3", "GET");
        await c.ExpectAsync("$2\r\nv1\r\n");
    }

    [Fact]
    public async Task Set_PxExpiry_AndOptionErrors()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("SET", "temp", "v", "PX", "10000");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("PTTL", "temp");
        var pttl = Assert.IsType<long>(await c.ReadReplyAsync());
        Assert.InRange(pttl, 9_000, 10_000);
        h.Clock.Advance(TimeSpan.FromSeconds(11));
        await c.SendCommandAsync("GET", "temp");
        await c.ExpectAsync("$-1\r\n");

        await c.SendCommandAsync("SET", "k", "v", "EX", "0");
        await c.ExpectAsync("-ERR invalid expire time in 'set' command\r\n");
        await c.SendCommandAsync("SET", "k", "v", "PX", "-5");
        await c.ExpectAsync("-ERR invalid expire time in 'set' command\r\n");
        await c.SendCommandAsync("SET", "k", "v", "NX", "XX");
        await c.ExpectAsync("-ERR syntax error\r\n");
        await c.SendCommandAsync("SET", "k", "v", "EX", "10", "KEEPTTL");
        await c.ExpectAsync("-ERR syntax error\r\n");
        await c.SendCommandAsync("SET", "k", "v", "EX", "abc");
        await c.ExpectAsync("-ERR value is not an integer or out of range\r\n");
        await c.SendCommandAsync("SET", "k", "v", "PX");
        await c.ExpectAsync("-ERR syntax error\r\n");

        await c.SendCommandAsync("RPUSH", "alist", "x");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("SET", "alist", "v", "GET");
        await c.ExpectAsync(WrongTypeReply);
    }

    [Fact]
    public async Task ExpireFamily_Conditions_AndErrors_OverTheWire()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("EXPIRE", "ghost", "100");
        await c.ExpectAsync(":0\r\n");

        await c.SendCommandAsync("SET", "k", "v");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("EXPIRE", "k", "100", "NX");
        await c.ExpectAsync(":1\r\n"); // no deadline yet: NX applies
        await c.SendCommandAsync("EXPIRE", "k", "200", "NX");
        await c.ExpectAsync(":0\r\n"); // deadline exists: NX declines
        await c.SendCommandAsync("EXPIRE", "k", "200", "XX");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("EXPIRE", "k", "50", "GT");
        await c.ExpectAsync(":0\r\n"); // not greater than the current 200
        await c.SendCommandAsync("EXPIRE", "k", "300", "GT");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("EXPIRE", "k", "999", "LT");
        await c.ExpectAsync(":0\r\n"); // not less than the current 300
        await c.SendCommandAsync("EXPIRE", "k", "60", "LT");
        await c.ExpectAsync(":1\r\n");

        // A deadline in the past deletes the key and reports 1.
        await c.SendCommandAsync("EXPIRE", "k", "-100");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("EXISTS", "k");
        await c.ExpectAsync(":0\r\n");

        // No deadline at all: GT can never beat infinity, LT always can.
        await c.SendCommandAsync("SET", "plain", "v");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("EXPIRE", "plain", "100", "GT");
        await c.ExpectAsync(":0\r\n");
        await c.SendCommandAsync("EXPIRE", "plain", "100", "XX");
        await c.ExpectAsync(":0\r\n");
        await c.SendCommandAsync("EXPIRE", "plain", "100", "LT");
        await c.ExpectAsync(":1\r\n");

        await c.SendCommandAsync("EXPIRE", "plain", "abc");
        await c.ExpectAsync("-ERR value is not an integer or out of range\r\n");
        await c.SendCommandAsync("EXPIRE", "plain", "100", "BOGUS");
        await c.ExpectAsync("-ERR Unsupported option BOGUS\r\n");

        // The absolute forms accept the same conditions.
        var atMs = h.Clock.GetUtcNow().AddMinutes(30).ToUnixTimeMilliseconds();
        await c.SendCommandAsync("SET", "abs", "v");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("PEXPIREAT", "abs", atMs.ToString(), "NX");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("PTTL", "abs");
        var pttl = Assert.IsType<long>(await c.ReadReplyAsync());
        Assert.InRange(pttl, 29 * 60_000, 30 * 60_000);
        await c.SendCommandAsync("PEXPIREAT", "abs", atMs.ToString(), "NX");
        await c.ExpectAsync(":0\r\n"); // deadline now exists
        await c.SendCommandAsync("PEXPIREAT", "ghost", atMs.ToString());
        await c.ExpectAsync(":0\r\n");

        var atSec = h.Clock.GetUtcNow().AddMinutes(45).ToUnixTimeSeconds();
        await c.SendCommandAsync("SET", "abs2", "v");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("EXPIREAT", "abs2", atSec.ToString());
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("TTL", "abs2");
        var ttl = Assert.IsType<long>(await c.ReadReplyAsync());
        Assert.InRange(ttl, 44 * 60, 45 * 60);
        await c.SendCommandAsync("EXPIREAT", "abs2", "1"); // 1970: long past
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("EXISTS", "abs2");
        await c.ExpectAsync(":0\r\n");
    }

    [Fact]
    public async Task Pop_CountEdgeCases()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("LPOP", "ghost");
        await c.ExpectAsync("$-1\r\n"); // single form: null bulk
        await c.SendCommandAsync("LPOP", "ghost", "3");
        await c.ExpectAsync("*-1\r\n"); // counted form: null array
        await c.SendCommandAsync("RPOP", "ghost", "2");
        await c.ExpectAsync("*-1\r\n");

        await c.SendCommandAsync("RPUSH", "l", "a", "b", "c");
        await c.ExpectAsync(":3\r\n");
        await c.SendCommandAsync("LPOP", "l", "0");
        await c.ExpectAsync("*0\r\n"); // count 0: empty array, list untouched
        await c.SendCommandAsync("LLEN", "l");
        await c.ExpectAsync(":3\r\n");
        await c.SendCommandAsync("LPOP", "l", "-1");
        await c.ExpectAsync("-ERR value is out of range, must be positive\r\n");
        await c.SendCommandAsync("LPOP", "l", "abc");
        await c.ExpectAsync("-ERR value is out of range, must be positive\r\n");
        await c.SendCommandAsync("RPOP", "l", "5");
        await c.ExpectAsync("*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");
        await c.SendCommandAsync("TYPE", "l");
        await c.ExpectAsync("+none\r\n");
    }

    [Fact]
    public async Task Scan_ErrorPaths()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "a", "1");
        await c.ExpectAsync("+OK\r\n");

        await c.SendCommandAsync("SCAN", "xyz");
        await c.ExpectAsync("-ERR invalid cursor\r\n");
        await c.SendCommandAsync("SCAN", "5000000000"); // well-formed but unknown scan state
        await c.ExpectAsync("-ERR invalid cursor\r\n");
        await c.SendCommandAsync("SCAN", "0", "COUNT", "0");
        await c.ExpectAsync("-ERR syntax error\r\n");
        await c.SendCommandAsync("SCAN", "0", "COUNT", "abc");
        await c.ExpectAsync("-ERR value is not an integer or out of range\r\n");
        await c.SendCommandAsync("SCAN", "0", "MATCH");
        await c.ExpectAsync("-ERR syntax error\r\n");
        await c.SendCommandAsync("SCAN", "0", "BOGUS", "x");
        await c.ExpectAsync("-ERR syntax error\r\n");
    }

    [Fact]
    public async Task ZAdd_WireErrors_AndCombinedModes()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("ZADD", "z", "abc", "m");
        await c.ExpectAsync("-ERR value is not a valid float\r\n");
        await c.SendCommandAsync("ZADD", "z", "1e309", "m");
        await c.ExpectAsync("-ERR value is not a valid float\r\n"); // overflow is not infinity
        await c.SendCommandAsync("ZADD", "z", "nan", "m");
        await c.ExpectAsync("-ERR value is not a valid float\r\n");
        await c.SendCommandAsync("ZADD", "z", "1", "m", "2");
        await c.ExpectAsync("-ERR wrong number of arguments for 'zadd' command\r\n");
        await c.SendCommandAsync("ZADD", "z", "NX", "GT", "1", "m");
        await c.ExpectAsync("-ERR GT, LT, and/or NX options at the same time are not compatible\r\n");
        await c.SendCommandAsync("ZADD", "z", "GT", "LT", "1", "m");
        await c.ExpectAsync("-ERR GT, LT, and/or NX options at the same time are not compatible\r\n");
        await c.SendCommandAsync("ZADD", "z", "XX", "NX", "1", "m");
        await c.ExpectAsync("-ERR XX and NX options at the same time are not compatible\r\n");

        // GT alone never blocks a brand-new member…
        await c.SendCommandAsync("ZADD", "z", "GT", "5", "fresh");
        await c.ExpectAsync(":1\r\n");
        // …but XX combined with GT does: XX means the member must exist.
        await c.SendCommandAsync("ZADD", "z", "XX", "GT", "9", "ghost");
        await c.ExpectAsync(":0\r\n");
        await c.SendCommandAsync("ZSCORE", "z", "ghost");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("ZADD", "z", "XX", "GT", "INCR", "4", "ghost");
        await c.ExpectAsync("$-1\r\n");
        // XX GT on an existing member raises the score as usual.
        await c.SendCommandAsync("ZADD", "z", "XX", "GT", "9", "fresh");
        await c.ExpectAsync(":0\r\n");
        await c.SendCommandAsync("ZSCORE", "z", "fresh");
        await c.ExpectAsync("$1\r\n9\r\n");

        // INCR over the wire returns the new score as a bulk string.
        await c.SendCommandAsync("ZADD", "z", "INCR", "1.5", "fresh");
        await c.ExpectAsync("$4\r\n10.5\r\n");

        // Infinite scores are legitimate members.
        await c.SendCommandAsync("ZADD", "z", "inf", "top");
        await c.ExpectAsync(":1\r\n");
        await c.SendCommandAsync("ZSCORE", "z", "top");
        await c.ExpectAsync("$3\r\ninf\r\n");
    }

    [Fact]
    public async Task Delta_AndHash_ArgumentErrors()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("INCRBY", "n", "1.5");
        await c.ExpectAsync("-ERR value is not an integer or out of range\r\n");
        await c.SendCommandAsync("DECRBY", "n", "-9223372036854775808");
        await c.ExpectAsync("-ERR decrement would overflow\r\n");

        await c.SendCommandAsync("SET", "max", "9223372036854775807");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("INCRBY", "max", "1");
        await c.ExpectAsync("-ERR increment or decrement would overflow\r\n");
        await c.SendCommandAsync("GET", "max");
        await c.ExpectAsync("$19\r\n9223372036854775807\r\n"); // unchanged after the failed delta

        await c.SendCommandAsync("HSET", "h", "field");
        await c.ExpectAsync("-ERR wrong number of arguments for 'hset' command\r\n");
    }

    [Fact]
    public async Task FlushModes_AndInfoSections()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("SET", "a", "1");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("FLUSHDB");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("DBSIZE");
        await c.ExpectAsync(":0\r\n");

        await c.SendCommandAsync("SET", "b", "2");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("FLUSHALL", "ASYNC");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("DBSIZE");
        await c.ExpectAsync(":0\r\n");
        await c.SendCommandAsync("FLUSHDB", "BOGUS");
        await c.ExpectAsync("-ERR syntax error\r\n");

        // An unknown INFO section is an empty bulk string in Redis, not an error.
        await c.SendCommandAsync("INFO", "bogussection");
        await c.ExpectAsync("$0\r\n\r\n");
        await c.SendCommandAsync("INFO", "server");
        var info = await c.CollectUntilAsync("tcp_port:");
        Assert.Contains("server_name:redis-csharp", info);
    }

    [Fact]
    public async Task Type_ReportsEveryType()
    {
        await using var h = SessionHarness.Start();
        var c = await SeededAsync(h);
        await c.SendCommandAsync("TYPE", "str");
        await c.ExpectAsync("+string\r\n");
        await c.SendCommandAsync("TYPE", "list");
        await c.ExpectAsync("+list\r\n");
        await c.SendCommandAsync("TYPE", "set");
        await c.ExpectAsync("+set\r\n");
        await c.SendCommandAsync("TYPE", "hash");
        await c.ExpectAsync("+hash\r\n");
        await c.SendCommandAsync("TYPE", "zset");
        await c.ExpectAsync("+zset\r\n");
        await c.SendCommandAsync("TYPE", "ghost");
        await c.ExpectAsync("+none\r\n");
    }

    [Fact]
    public async Task Quit_RepliesOk_ThenClosesTheConnection()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("QUIT");
        await c.ExpectAsync("+OK\r\n");
        var ex = await Assert.ThrowsAsync<Xunit.Sdk.XunitException>(async () => await c.ReadReplyAsync(2000));
        Assert.Contains("closed", ex.Message);
    }
}
