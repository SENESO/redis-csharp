namespace RedisServer.Tests;

/// <summary>
/// CLIENT SETNAME / GETNAME / SETINFO / ID. SETNAME validation and the
/// unknown-subcommand text were probed against redis-server 7.0.15;
/// SETINFO itself is a Redis 7.2 command that client libraries send on
/// connect regardless (redis-py ignores its failure), so it is accepted
/// and the values are kept on the connection. CLIENT ID and the HELLO
/// handshake report the same per-connection id.
/// </summary>
public sealed class ClientTests
{
    [Fact]
    public async Task SetName_GetName_RoundTrip_AndClear()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("CLIENT", "GETNAME");
        await c.ExpectAsync("$-1\r\n");
        await c.SendCommandAsync("CLIENT", "SETNAME", "worker-1");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("CLIENT", "GETNAME");
        await c.ExpectAsync("$8\r\nworker-1\r\n");
        await c.SendCommandAsync("CLIENT", "SETNAME", "");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("CLIENT", "GETNAME");
        await c.ExpectAsync("$-1\r\n");
    }

    [Fact]
    public async Task SetName_RejectsBadCharacters_AndHelloSharesTheState()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("CLIENT", "SETNAME", "bad name");
        await c.ExpectAsync("-ERR Client names cannot contain spaces, newlines or special characters.\r\n");
        await c.SendCommandAsync("HELLO", "3", "SETNAME", "from-hello");
        var pairs = Assert.IsType<List<object?>>(await c.ReadReplyAsync());
        Assert.Equal(3L, pairs[5]);
        await c.SendCommandAsync("CLIENT", "GETNAME");
        await c.ExpectAsync("$10\r\nfrom-hello\r\n");
        // RESP3 null form for a cleared name on a second connection.
        var c2 = h.Connect();
        await c2.SendCommandAsync("HELLO", "3");
        await c2.ReadReplyAsync();
        await c2.SendCommandAsync("CLIENT", "GETNAME");
        await c2.ExpectAsync("_\r\n");
    }

    [Fact]
    public async Task SetInfo_AcceptsLibraryNameAndVersion_RejectsUnknownAttributes()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        await c.SendCommandAsync("CLIENT", "SETINFO", "lib-name", "redis-py");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("CLIENT", "SETINFO", "lib-ver", "8.1.0");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("CLIENT", "SETINFO", "lib", "x");
        await c.ExpectAsync("-ERR Unknown subcommand or wrong number of arguments for 'lib'. Try CLIENT SETINFO HELP.\r\n");
    }

    [Fact]
    public async Task ClientId_MatchesTheHelloId_AndDiffersAcrossConnections()
    {
        await using var h = SessionHarness.Start();
        var c1 = h.Connect();
        var c2 = h.Connect();

        await c1.SendCommandAsync("HELLO");
        var pairs = Assert.IsType<List<object?>>(await c1.ReadReplyAsync());
        var helloId = Assert.IsType<long>(pairs[7]);
        await c1.SendCommandAsync("CLIENT", "ID");
        Assert.Equal(helloId, Assert.IsType<long>(await c1.ReadReplyAsync()));
        await c2.SendCommandAsync("CLIENT", "ID");
        var otherId = Assert.IsType<long>(await c2.ReadReplyAsync());
        Assert.NotEqual(helloId, otherId);
    }

    [Fact]
    public async Task UnknownSubcommand_AndArity()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();

        // The subcommand is echoed exactly as sent.
        await c.SendCommandAsync("CLIENT", "BoGuS");
        await c.ExpectAsync("-ERR unknown subcommand 'BoGuS'. Try CLIENT HELP.\r\n");
        await c.SendCommandAsync("CLIENT", "SETNAME");
        await c.ExpectAsync("-ERR wrong number of arguments for 'client' command\r\n");
        await c.SendCommandAsync("CLIENT");
        await c.ExpectAsync("-ERR wrong number of arguments for 'client' command\r\n");
    }
}
