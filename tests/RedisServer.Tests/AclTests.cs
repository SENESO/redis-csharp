using System.Security.Cryptography;
using System.Text;

namespace RedisServer.Tests;

/// <summary>
/// ACL users: SETUSER / GETUSER / DELUSER / USERS / LIST / WHOAMI / CAT,
/// per-user command and key-pattern enforcement, and the AUTH forms.
/// Wire shapes and error texts were probed against redis-server 7.0.15.
/// </summary>
public class AclTests
{
    private static string Sha256Hex(string text) =>
        Convert.ToHexString(SHA256.HashData(Encoding.ASCII.GetBytes(text))).ToLowerInvariant();

    [Fact]
    public async Task WhoAmI_DefaultUser_AndUserSwitching()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "WHOAMI");
        Assert.Equal("default", await c.ReadReplyAsync());

        await c.SendCommandAsync("ACL", "SETUSER", "alice", "on", ">pw", "allkeys", "allcommands");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("AUTH", "alice", "pw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "WHOAMI");
        Assert.Equal("alice", await c.ReadReplyAsync());
    }

    [Fact]
    public async Task Auth_UnknownUser_DisabledUser_WrongPassword_AllWrongpass()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "SETUSER", "offuser", "off", ">pw", "allkeys", "allcommands");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "SETUSER", "onuser", "on", ">pw", "allkeys", "allcommands");
        Assert.Equal("OK", await c.ReadReplyAsync());

        var stranger = h.Connect();
        await stranger.SendCommandAsync("AUTH", "ghost", "pw");
        Assert.Equal(new TestClient.RespError("WRONGPASS invalid username-password pair or user is disabled."),
            await stranger.ReadReplyAsync());
        await stranger.SendCommandAsync("AUTH", "offuser", "pw");
        Assert.Equal(new TestClient.RespError("WRONGPASS invalid username-password pair or user is disabled."),
            await stranger.ReadReplyAsync());
        await stranger.SendCommandAsync("AUTH", "onuser", "nope");
        Assert.Equal(new TestClient.RespError("WRONGPASS invalid username-password pair or user is disabled."),
            await stranger.ReadReplyAsync());
        // A failed AUTH never revokes: the connection is still default.
        await stranger.SendCommandAsync("ACL", "WHOAMI");
        Assert.Equal("default", await stranger.ReadReplyAsync());
    }

    [Fact]
    public async Task SetUser_ThenGetUser_RoundTripsTheProbedShape()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "SETUSER", "bob", "on", ">secret", "~cached:*", "+get", "+@keyspace", "-scan");
        Assert.Equal("OK", await c.ReadReplyAsync());

        await c.SendCommandAsync("ACL", "GETUSER", "bob");
        var reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal("flags", reply[0]);
        Assert.Equal(new List<object?> { "on" }, reply[1]);
        Assert.Equal("passwords", reply[2]);
        Assert.Equal(new List<object?> { Sha256Hex("secret") }, reply[3]);
        Assert.Equal("commands", reply[4]);
        Assert.Equal("-@all +@keyspace +get -scan", reply[5]);
        Assert.Equal("keys", reply[6]);
        Assert.Equal("~cached:*", reply[7]);
        Assert.Equal("channels", reply[8]);
        Assert.Equal("", reply[9]);
        Assert.Equal("selectors", reply[10]);
        Assert.Equal(new List<object?>(), reply[11]);
    }

    [Fact]
    public async Task GetUser_MissingUser_IsNull_NotAnError()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "GETUSER", "ghost");
        Assert.Null(await c.ReadReplyAsync());
    }

    [Fact]
    public async Task Users_AndList_Rendering()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "SETUSER", "bob", "on", ">secret", "~cached:*", "+get", "+@keyspace", "-scan");
        Assert.Equal("OK", await c.ReadReplyAsync());

        await c.SendCommandAsync("ACL", "USERS");
        Assert.Equal(new List<object?> { "bob", "default" }, await c.ReadReplyAsync());

        await c.SendCommandAsync("ACL", "LIST");
        var list = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(
            $"user bob on #{Sha256Hex("secret")} ~cached:* resetchannels -@all +@keyspace +get -scan",
            list[0]);
        Assert.Equal("user default on nopass ~* &* +@all", list[1]);
    }

    [Fact]
    public async Task NoPerm_Command_AndKey_Texts()
    {
        await using var h = SessionHarness.Start();
        var admin = h.Connect();
        await admin.SendCommandAsync("ACL", "SETUSER", "bob", "on", ">secret", "~cached:*", "+get", "+@keyspace", "-scan");
        Assert.Equal("OK", await admin.ReadReplyAsync());

        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "bob", "secret");
        Assert.Equal("OK", await c.ReadReplyAsync());

        await c.SendCommandAsync("SET", "cached:k", "v");
        Assert.Equal(new TestClient.RespError("NOPERM this user has no permissions to run the 'set' command"),
            await c.ReadReplyAsync());

        await c.SendCommandAsync("GET", "other");
        Assert.Equal(new TestClient.RespError(
                "NOPERM this user has no permissions to access one of the keys used as arguments"),
            await c.ReadReplyAsync());

        // The allowed path still works.
        await c.SendCommandAsync("SET", "cached:k", "v"); // bob cannot SET — admin plants it instead
        Assert.IsType<TestClient.RespError>(await c.ReadReplyAsync());
        await admin.SendCommandAsync("SET", "cached:k", "v");
        Assert.Equal("OK", await admin.ReadReplyAsync());
        await c.SendCommandAsync("GET", "cached:k");
        Assert.Equal("v", await c.ReadReplyAsync());

        // A command removed from a granted category is refused by name.
        await c.SendCommandAsync("SCAN", "0");
        Assert.Equal(new TestClient.RespError("NOPERM this user has no permissions to run the 'scan' command"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task NoPerm_SubcommandName_UsesPipeForm()
    {
        await using var h = SessionHarness.Start();
        var admin = h.Connect();
        await admin.SendCommandAsync("ACL", "SETUSER", "mallory", "on", ">pw", "allkeys", "-@all", "+ping");
        Assert.Equal("OK", await admin.ReadReplyAsync());

        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "mallory", "pw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "WHOAMI");
        Assert.Equal(new TestClient.RespError("NOPERM this user has no permissions to run the 'acl|whoami' command"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("PING");
        Assert.Equal("PONG", await c.ReadReplyAsync());
    }

    [Fact]
    public async Task ArityError_PrecedesNoPerm()
    {
        await using var h = SessionHarness.Start();
        var admin = h.Connect();
        await admin.SendCommandAsync("ACL", "SETUSER", "bob", "on", ">pw", "allkeys", "+get");
        Assert.Equal("OK", await admin.ReadReplyAsync());
        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "bob", "pw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        // SET is not in bob's permissions, but the arity error wins.
        await c.SendCommandAsync("SET", "onlykey");
        Assert.Equal(new TestClient.RespError("ERR wrong number of arguments for 'set' command"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task SetUser_RuleErrors_AreAtomic_AndProbed()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "SETUSER", "u1", "badRule");
        Assert.Equal(new TestClient.RespError("ERR Error in ACL SETUSER modifier 'badRule': Syntax error"),
            await c.ReadReplyAsync());
        // The failed SETUSER created nothing.
        await c.SendCommandAsync("ACL", "GETUSER", "u1");
        Assert.Null(await c.ReadReplyAsync());

        await c.SendCommandAsync("ACL", "SETUSER", "u1", "+@bogus");
        Assert.Equal(new TestClient.RespError(
                "ERR Error in ACL SETUSER modifier '+@bogus': Unknown command or category name in ACL"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "SETUSER", "u1", "#abc");
        Assert.Equal(new TestClient.RespError(
                "ERR Error in ACL SETUSER modifier '#abc': The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters"),
            await c.ReadReplyAsync());

        await c.SendCommandAsync("ACL", "SETUSER", "u1", "on", ">a");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "SETUSER", "u1", "<nosuch");
        Assert.Equal(new TestClient.RespError(
                "ERR Error in ACL SETUSER modifier '<nosuch': The password you are trying to remove from the user does not exist"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task SetUser_PasswordRuleForms()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "SETUSER", "u", "on", ">a", ">b");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "GETUSER", "u");
        var reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(new List<object?> { Sha256Hex("a"), Sha256Hex("b") }, reply[3]);

        // #hash adds a password by hash; !hash removes it.
        await c.SendCommandAsync("ACL", "SETUSER", "u", "#" + Sha256Hex("c"));
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "SETUSER", "u", "!" + Sha256Hex("c"));
        Assert.Equal("OK", await c.ReadReplyAsync());
        // nopass clears every password and accepts anything.
        await c.SendCommandAsync("ACL", "SETUSER", "u", "nopass", "allkeys", "allcommands");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "GETUSER", "u");
        reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(new List<object?>(), reply[3]);
        Assert.Equal(new List<object?> { "on", "nopass" }, reply[1]);
        var guest = h.Connect();
        await guest.SendCommandAsync("AUTH", "u", "whatever");
        Assert.Equal("OK", await guest.ReadReplyAsync());
        // resetpass clears the nopass flag too.
        await c.SendCommandAsync("ACL", "SETUSER", "u", "resetpass");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "GETUSER", "u");
        reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(new List<object?> { "on" }, reply[1]);
    }

    [Fact]
    public async Task SetUser_Reset_RestoresFreshState_AndRulesAppend()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "SETUSER", "carol", "on", ">pw", "+get");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "SETUSER", "carol", "+set");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "GETUSER", "carol");
        var reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal("-@all +get +set", reply[5]);

        await c.SendCommandAsync("ACL", "SETUSER", "carol", "reset", "+set");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "GETUSER", "carol");
        reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(new List<object?> { "off" }, reply[1]);
        Assert.Equal(new List<object?>(), reply[3]);
        Assert.Equal("-@all +set", reply[5]);
        Assert.Equal("", reply[7]);
    }

    [Fact]
    public async Task DelUser_Counts_AndDefaultIsProtected()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "SETUSER", "temp", "on");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "DELUSER", "temp", "ghost");
        Assert.Equal(1L, await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "DELUSER", "temp");
        Assert.Equal(0L, await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "DELUSER", "default");
        Assert.Equal(new TestClient.RespError("ERR The 'default' user cannot be removed"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task Cat_ListsCategories_AndCategoryMembers()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("ACL", "CAT");
        var categories = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Contains("read", categories);
        Assert.Contains("write", categories);
        Assert.Contains("admin", categories);
        Assert.Equal(21, categories.Count);

        await c.SendCommandAsync("ACL", "CAT", "read");
        var read = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Contains("get", read);
        Assert.Contains("hgetall", read);
        Assert.DoesNotContain("set", read);

        await c.SendCommandAsync("ACL", "CAT", "boguscat");
        Assert.Equal(new TestClient.RespError("ERR Unknown category 'boguscat'"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task KeyPatterns_ReadOnly_RefusesWrites()
    {
        await using var h = SessionHarness.Start();
        var admin = h.Connect();
        await admin.SendCommandAsync("ACL", "SETUSER", "reader", "on", ">pw", "%R~ro:*", "+@read", "+set");
        Assert.Equal("OK", await admin.ReadReplyAsync());

        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "reader", "pw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        // SET is command-allowed, but the pattern is read-only.
        await c.SendCommandAsync("SET", "ro:k", "v");
        Assert.Equal(new TestClient.RespError(
                "NOPERM this user has no permissions to access one of the keys used as arguments"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "ro:k");
        Assert.Null(await c.ReadReplyAsync());
    }

    [Fact]
    public async Task MultiKeyCommand_NeedsEveryKeyAllowed()
    {
        await using var h = SessionHarness.Start();
        var admin = h.Connect();
        await admin.SendCommandAsync("SET", "a:1", "x");
        Assert.Equal("OK", await admin.ReadReplyAsync());
        await admin.SendCommandAsync("SET", "b:1", "x");
        Assert.Equal("OK", await admin.ReadReplyAsync());
        await admin.SendCommandAsync("ACL", "SETUSER", "u", "on", ">pw", "~a:*", "+exists", "+del");
        Assert.Equal("OK", await admin.ReadReplyAsync());

        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "u", "pw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("EXISTS", "a:1", "a:2");
        Assert.Equal(1L, await c.ReadReplyAsync());
        await c.SendCommandAsync("EXISTS", "a:1", "b:1");
        Assert.Equal(new TestClient.RespError(
                "NOPERM this user has no permissions to access one of the keys used as arguments"),
            await c.ReadReplyAsync());
        await c.SendCommandAsync("DEL", "b:1");
        Assert.Equal(new TestClient.RespError(
                "NOPERM this user has no permissions to access one of the keys used as arguments"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task RequirePass_BecomesDefaultUserPassword_AndSetUserCanRotateIt()
    {
        await using var h = SessionHarness.Start(requirePass: "bootpw");
        var c = h.Connect();
        await c.SendCommandAsync("PING");
        Assert.Equal(new TestClient.RespError("NOAUTH Authentication required."), await c.ReadReplyAsync());
        await c.SendCommandAsync("AUTH", "bootpw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("ACL", "GETUSER", "default");
        var reply = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(new List<object?> { Sha256Hex("bootpw") }, reply[3]);
        Assert.Equal(new List<object?> { "on" }, reply[1]);

        // Rotate the default user's password at runtime.
        await c.SendCommandAsync("ACL", "SETUSER", "default", "resetpass", ">newpw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        var fresh = h.Connect();
        await fresh.SendCommandAsync("AUTH", "bootpw");
        Assert.Equal(new TestClient.RespError("WRONGPASS invalid username-password pair or user is disabled."),
            await fresh.ReadReplyAsync());
        await fresh.SendCommandAsync("AUTH", "newpw");
        Assert.Equal("OK", await fresh.ReadReplyAsync());
    }

    [Fact]
    public async Task AclCommands_InsideMulti_AreJudgedAtExec()
    {
        await using var h = SessionHarness.Start();
        var admin = h.Connect();
        await admin.SendCommandAsync("ACL", "SETUSER", "u", "on", ">pw", "allkeys", "+multi", "+exec", "+get", "+set");
        Assert.Equal("OK", await admin.ReadReplyAsync());
        await admin.SendCommandAsync("SET", "k", "v1");
        Assert.Equal("OK", await admin.ReadReplyAsync());

        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "u", "pw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("MULTI");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("GET", "k");
        Assert.Equal("QUEUED", await c.ReadReplyAsync());
        await c.SendCommandAsync("DEL", "k");
        Assert.Equal("QUEUED", await c.ReadReplyAsync());
        await c.SendCommandAsync("EXEC");
        var exec = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal("v1", exec[0]);
        Assert.Equal(new TestClient.RespError("NOPERM this user has no permissions to run the 'del' command"),
            exec[1]);
    }

    [Fact]
    public async Task ChannelPatterns_GateSubscriptions()
    {
        await using var h = SessionHarness.Start();
        var admin = h.Connect();
        await admin.SendCommandAsync("ACL", "SETUSER", "sub", "on", ">pw", "allkeys", "allcommands",
            "resetchannels", "&news:*");
        Assert.Equal("OK", await admin.ReadReplyAsync());

        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "sub", "pw");
        Assert.Equal("OK", await c.ReadReplyAsync());
        await c.SendCommandAsync("SUBSCRIBE", "news:tech");
        var ack = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal("subscribe", ack[0]);
        await c.SendCommandAsync("UNSUBSCRIBE");
        await c.ReadReplyAsync();
        await c.SendCommandAsync("SUBSCRIBE", "sports:live");
        Assert.Equal(new TestClient.RespError(
                "NOPERM this user has no permissions to access one of the channels used as arguments"),
            await c.ReadReplyAsync());
    }

    [Fact]
    public async Task GetUser_Resp3_IsAMap()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendAsync("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n");
        var hello = await c.ReadReplyAsync();
        Assert.NotNull(hello);
        await c.SendCommandAsync("ACL", "GETUSER", "default");
        // RESP3 maps are parsed by the test client as a flat list.
        await c.ExpectAsync("%6\r\n");
    }
}
