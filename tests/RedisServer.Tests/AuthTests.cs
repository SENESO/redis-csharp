namespace RedisServer.Tests;

/// <summary>
/// AUTH and the NOAUTH gate, with error texts probed against a real
/// redis-server 7.0.15: every command but AUTH/QUIT is refused before
/// authentication, the password check is per connection, and a failed
/// attempt never revokes an authenticated connection.
/// </summary>
public sealed class AuthTests
{
    private const string NoAuth = "-NOAUTH Authentication required.\r\n";
    private const string WrongPass = "-WRONGPASS invalid username-password pair or user is disabled.\r\n";

    [Fact]
    public async Task EveryCommandButAuthAndQuit_IsRefusedBeforeAuthentication()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var c = h.Connect();

        foreach (var command in new[]
                 {
                     new[] { "PING" }, new[] { "GET", "k" }, new[] { "SET", "k", "v" },
                     new[] { "MULTI" }, new[] { "EXEC" }, new[] { "DISCARD" },
                     new[] { "SUBSCRIBE", "chan" }, new[] { "INFO" }, new[] { "NOSUCH", "x" }
                 })
        {
            await c.SendCommandAsync(command);
            await c.ExpectAsync(NoAuth);
        }
    }

    [Fact]
    public async Task WrongPasswordForms_AreAllWrongPass_AndDoNotAuthenticate()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var c = h.Connect();

        await c.SendCommandAsync("AUTH", "wrong");
        await c.ExpectAsync(WrongPass);
        await c.SendCommandAsync("AUTH", "default", "wrong");
        await c.ExpectAsync(WrongPass);
        // The right password under a nonexistent user is still WRONGPASS.
        await c.SendCommandAsync("AUTH", "bob", "s3cret");
        await c.ExpectAsync(WrongPass);
        // Usernames are case-sensitive: DEFAULT is not the default user.
        await c.SendCommandAsync("AUTH", "DEFAULT", "s3cret");
        await c.ExpectAsync(WrongPass);

        await c.SendCommandAsync("PING");
        await c.ExpectAsync(NoAuth);
    }

    [Fact]
    public async Task CorrectPassword_Authenticates_AndFailedReauthKeepsTheSession()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var c = h.Connect();

        await c.SendCommandAsync("AUTH", "s3cret");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "k", "v");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("GET", "k");
        await c.ExpectAsync("$1\r\nv\r\n");

        // Re-authenticating is fine; a failed attempt does not log us out.
        await c.SendCommandAsync("AUTH", "default", "s3cret");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("AUTH", "wrong");
        await c.ExpectAsync(WrongPass);
        await c.SendCommandAsync("PING");
        await c.ExpectAsync("+PONG\r\n");
    }

    [Fact]
    public async Task AuthenticationState_IsPerConnection()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var first = h.Connect();
        var second = h.Connect();

        await first.SendCommandAsync("AUTH", "s3cret");
        await first.ExpectAsync("+OK\r\n");
        await first.SendCommandAsync("PING");
        await first.ExpectAsync("+PONG\r\n");

        await second.SendCommandAsync("PING");
        await second.ExpectAsync(NoAuth);
    }

    [Fact]
    public async Task Quit_IsAllowedBeforeAuthentication()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var c = h.Connect();
        await c.SendCommandAsync("QUIT");
        await c.ExpectAsync("+OK\r\n");
        var ex = await Assert.ThrowsAsync<Xunit.Sdk.XunitException>(async () => await c.ReadReplyAsync(2000));
        Assert.Contains("closed", ex.Message);
    }

    [Fact]
    public async Task Auth_ArityErrorsApplyBeforeTheGate()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var c = h.Connect();
        await c.SendCommandAsync("AUTH");
        await c.ExpectAsync("-ERR wrong number of arguments for 'auth' command\r\n");
        await c.SendCommandAsync("AUTH", "a", "b", "c");
        await c.ExpectAsync("-ERR wrong number of arguments for 'auth' command\r\n");
    }

    [Fact]
    public async Task WithoutAPassword_AuthHasRedisExactOutcomes()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        // No authentication is needed at all...
        await c.SendCommandAsync("PING");
        await c.ExpectAsync("+PONG\r\n");
        // ...the one-argument form is an error naming the missing password...
        await c.SendCommandAsync("AUTH", "anything");
        await c.ExpectAsync("-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?\r\n");
        // ...the username form against the passwordless default user succeeds...
        await c.SendCommandAsync("AUTH", "default", "anything");
        await c.ExpectAsync("+OK\r\n");
        // ...and any other user does not exist.
        await c.SendCommandAsync("AUTH", "bob", "anything");
        await c.ExpectAsync(WrongPass);
    }

    [Fact]
    public async Task Auth_InsideMulti_ExecutesAtExecTime()
    {
        await using var h = SessionHarness.Start(requirePass: "s3cret");
        var c = h.Connect();
        await c.SendCommandAsync("AUTH", "s3cret");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("AUTH", "s3cret");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("SET", "k", "v");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("EXEC");
        await c.ExpectAsync("*2\r\n+OK\r\n+OK\r\n");
    }
}
