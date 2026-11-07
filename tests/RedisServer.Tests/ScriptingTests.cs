using System.Security.Cryptography;
using System.Text;
using RedisServer.Persistence;

namespace RedisServer.Tests;

/// <summary>
/// Lua scripting (MoonSharp): EVAL / EVALSHA / SCRIPT, redis.call and
/// redis.pcall, KEYS / ARGV, and the RESP conversions. Wire shapes and
/// error texts were probed against redis-server 7.0.15.
/// </summary>
public class ScriptingTests
{
    private static string Sha(string source) =>
        Convert.ToHexString(SHA1.HashData(Encoding.UTF8.GetBytes(source))).ToLowerInvariant();

    private static async Task<object?> Eval(TestClient c, string script, params string[] tail)
    {
        var args = new List<string> { "EVAL", script, ((tail.Length) / 2).ToString() };
        args.AddRange(tail);
        await c.SendCommandAsync(args.ToArray());
        return await c.ReadReplyAsync();
    }

    [Fact]
    public async Task ReturnConversions_FollowRedis()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        Assert.Equal(1L, await Eval(c, "return 1"));
        Assert.Equal("hello", await Eval(c, "return 'hello'"));
        Assert.Null(await Eval(c, "return nil"));
        Assert.Null(await Eval(c, "return false"));
        Assert.Equal(1L, await Eval(c, "return true"));
        Assert.Equal(1L, await Eval(c, "return 1.5"));
        Assert.Equal(-1L, await Eval(c, "return -1.5"));
        Assert.Equal(new List<object?> { 1L, 2L, "x" }, await Eval(c, "return {1, 2, 'x'}"));
        Assert.Equal(new List<object?> { 1L, new List<object?> { 2L, 3L } },
            await Eval(c, "return {1, {2, 3}}"));
        Assert.Equal(new List<object?> { 1L }, await Eval(c, "return {1, nil, 3}"));
        Assert.Equal(new List<object?>(), await Eval(c, "return {}"));
        Assert.Null(await Eval(c, "local x = 1"));
    }

    [Fact]
    public async Task OkAndErrTables_BecomeStatusAndErrorReplies()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("EVAL", "return {ok='done'}", "0");
        await c.ExpectAsync("+done\r\n");
        Assert.Equal(new TestClient.RespError("ERR custom bad"),
            await Eval(c, "return {err='ERR custom bad'}"));
    }

    [Fact]
    public async Task KeysAndArgv_AreExposed()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("EVAL", "return {KEYS[1], KEYS[2], ARGV[1]}", "2", "k1", "k2", "a1");
        Assert.Equal(new List<object?> { "k1", "k2", "a1" }, await c.ReadReplyAsync());
    }

    [Fact]
    public async Task NumKeys_Validation()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("EVAL", "return 1", "-1");
        Assert.Equal(new TestClient.RespError("ERR Number of keys can't be negative"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("EVAL", "return 1", "3", "onlykey");
        Assert.Equal(new TestClient.RespError("ERR Number of keys can't be greater than number of args"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("EVAL", "return 1", "x");
        Assert.Equal(new TestClient.RespError("ERR value is not an integer or out of range"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task RedisCall_RoundTrips_AndWritesLand()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        Assert.Equal("lv", await Eval(c,
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])", "lk", "lv"));
        await c.SendCommandAsync("GET", "lk");
        Assert.Equal("lv", await c.ReadReplyAsync());
        // Status replies surface as ok tables; integer replies as numbers.
        Assert.Equal("OK", await Eval(c, "local v = redis.call('SET', 'nk', 'x'); return v['ok']"));
        Assert.Equal("number", await Eval(c, "return type(redis.call('INCR', 'ctr'))"));
        Assert.Equal("b", await Eval(c,
            "redis.call('RPUSH', 'rl', 'a', 'b'); local v = redis.call('LRANGE', 'rl', 0, -1); return v[2]"));
    }

    [Fact]
    public async Task RedisCall_Errors_AbortWithTheScriptSuffix()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        var unknown = "return redis.call('NOSUCH', 'x')";
        Assert.Equal(new TestClient.RespError(
                $"ERR Unknown Redis command called from script script: {Sha(unknown)}, on @user_script:1."),
            await Eval(c, unknown));
        var arity = "return redis.call('SET', 'onlykey')";
        Assert.Equal(new TestClient.RespError(
                $"ERR Wrong number of args calling Redis command from script script: {Sha(arity)}, on @user_script:1."),
            await Eval(c, arity));
        await c.SendCommandAsync("SET", "s", "abc");
        Assert.Equal("OK", await c.ReadReplyAsync());
        var incr = "return redis.call('INCR', 's')";
        Assert.Equal(new TestClient.RespError(
                $"ERR value is not an integer or out of range script: {Sha(incr)}, on @user_script:1."),
            await Eval(c, incr));
    }

    [Fact]
    public async Task RedisPcall_ReturnsErrorTables_InsteadOfAborting()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        Assert.Equal(new TestClient.RespError("ERR Unknown Redis command called from script"),
            await Eval(c, "return redis.pcall('NOSUCH', 'x')"));
        Assert.Equal(new TestClient.RespError("ERR Wrong number of args calling Redis command from script"),
            await Eval(c, "return redis.pcall('SET', 'onlykey')"));
        await c.SendCommandAsync("SET", "s", "abc");
        Assert.Equal("OK", await c.ReadReplyAsync());
        Assert.Equal("ERR value is not an integer or out of range",
            await Eval(c, "local e = redis.pcall('INCR', 's'); return e['err']"));
        // And execution continues after a pcall error.
        Assert.Equal(7L, await Eval(c, "local e = redis.pcall('INCR', 's'); return 7"));
    }

    [Fact]
    public async Task BannedCommands_AreRefused_InCallAndPcall()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        var multi = "return redis.call('MULTI')";
        Assert.Equal(new TestClient.RespError(
                $"ERR This Redis command is not allowed from script script: {Sha(multi)}, on @user_script:1."),
            await Eval(c, multi));
        Assert.Equal(new TestClient.RespError("ERR This Redis command is not allowed from script"),
            await Eval(c, "return redis.pcall('MULTI')"));
        Assert.Equal(new TestClient.RespError("ERR This Redis command is not allowed from script"),
            await Eval(c, "return redis.pcall('EVAL', 'return 1', '0')"));
    }

    [Fact]
    public async Task RuntimeError_HasTheRedisShape()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        var script = "error('boom')";
        Assert.Equal(new TestClient.RespError(
                $"ERR user_script:1: boom script: {Sha(script)}, on @user_script:1."),
            await Eval(c, script));
        var two = "local x = 1\nerror('line2 boom')";
        Assert.Equal(new TestClient.RespError(
                $"ERR user_script:2: line2 boom script: {Sha(two)}, on @user_script:2."),
            await Eval(c, two));
    }

    [Fact]
    public async Task SyntaxError_HasTheRedisFrame()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        var reply = await Eval(c, "return (");
        var error = Assert.IsType<TestClient.RespError>(reply);
        Assert.StartsWith("ERR Error compiling script (new function): user_script:1:", error.Text);
    }

    [Fact]
    public async Task ScriptCache_LoadExistsFlush_AndEvalSha()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        var source = "return 42";
        await c.SendCommandAsync("EVALSHA", Sha(source), "0");
        Assert.Equal(new TestClient.RespError("NOSCRIPT No matching script. Please use EVAL."),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("SCRIPT", "LOAD", source);
        Assert.Equal(Sha(source), await c.ReadReplyAsync());
        await c.SendCommandAsync("SCRIPT", "EXISTS", Sha(source), Sha("return 43"));
        Assert.Equal(new List<object?> { 1L, 0L }, await c.ReadReplyAsync());
        await c.SendCommandAsync("EVALSHA", Sha(source), "0");
        Assert.Equal(42L, await c.ReadReplyAsync());
        await c.SendCommandAsync("SCRIPT", "FLUSH");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("EVALSHA", Sha(source), "0");
        Assert.Equal(new TestClient.RespError("NOSCRIPT No matching script. Please use EVAL."),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("SCRIPT", "FROB");
        Assert.Equal(new TestClient.RespError("ERR unknown subcommand 'FROB'. Try SCRIPT HELP."),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task SelectInsideScript_AppliesToLaterCalls()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        Assert.Equal("sv", await Eval(c,
            "redis.call('SELECT', 3); redis.call('SET', 'sk', 'sv'); return redis.call('GET', 'sk')"));
        // The shared context keeps the selection after the script, as in Redis.
        await c.SendCommandAsync("GET", "sk");
        Assert.Equal("sv", await c.ReadReplyAsync());
        Assert.Equal("sv", Encoding.ASCII.GetString(h.Store.Get(Encoding.ASCII.GetBytes("sk"), 3)!));
    }

    [Fact]
    public async Task ScriptWrites_PersistExactlyOnce_ToTheAof()
    {
        await using var h = SessionHarness.Start(appendOnly: true);
        var c = h.Connect();
        Assert.Equal("OK", await Eval(c,
            "redis.call('SET', 'from-script', 'yes'); redis.call('INCR', 'counter'); return redis.call('SET', 'second', '2')"));
        var aof = AppendOnlyFile.ReadCommands(
            Path.Combine(h.DataDirectory, "appendonly.aof"), out var truncated);
        Assert.False(truncated);
        var names = aof.Select(cmd => Encoding.ASCII.GetString(cmd[0])).ToList();
        // Exactly the script's three writes — no EVAL frame of its own.
        Assert.Equal(new[] { "SET", "INCR", "SET" }, names);
    }

    [Fact]
    public async Task Eval_OnAReplica_IsRefusedReadonly()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("REPLICAOF", "127.0.0.1", "6399");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("EVAL", "return redis.call('SET', 'x', '1')", "0");
        await c.ExpectAsync("-READONLY You can't write against a read only replica.\r\n");
    }

    [Fact]
    public async Task Resp3_MapReplies_AndSetresp()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendAsync("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n");
        Assert.NotNull(await c.ReadReplyAsync());

        // HGETALL through a script on a RESP3 connection arrives as a map.
        await c.SendCommandAsync("EVAL",
            "redis.call('HSET', 'hh', 'f1', 'v1'); return redis.call('HGETALL', 'hh')", "0");
        await c.ExpectAsync("%1\r\n$2\r\nf1\r\n$2\r\nv1\r\n");

        // On a RESP3 connection scripts default to RESP3 shapes: a
        // missing key is nil. On a RESP2 connection it is boolean false,
        // and redis.setresp(3) upgrades it to nil.
        await c.SendCommandAsync("EVAL", "return type(redis.call('GET', 'nope'))", "0");
        Assert.Equal("nil", await c.ReadReplyAsync());
        var plain = h.Connect();
        await plain.SendCommandAsync("EVAL", "return type(redis.call('GET', 'nope'))", "0");
        Assert.Equal("boolean", await plain.ReadReplyAsync());
        await plain.SendCommandAsync("EVAL", "redis.setresp(3); return type(redis.call('GET', 'nope'))", "0");
        Assert.Equal("nil", await plain.ReadReplyAsync());
    }

    [Fact]
    public async Task Eval_InsideMulti_ExecutesAtExec()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("MULTI");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("EVAL", "return redis.call('SET', 'mk', 'mv')", "0");
        Assert.Equal("QUEUED", await c.ReadReplyAsync());
        await c.SendCommandAsync("EXEC");
        var exec = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal("OK", exec[0]);
        await c.SendCommandAsync("GET", "mk");
        Assert.Equal("mv", await c.ReadReplyAsync());
    }
}
