namespace RedisServer.Tests;

/// <summary>MULTI / EXEC / DISCARD semantics over the wire.</summary>
public class TransactionTests
{
    [Fact]
    public async Task Multi_Exec_RunsQueuedCommandsInOrder()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "a", "1");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("INCR", "a");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("GET", "a");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("EXEC");
        await c.ExpectAsync("*3\r\n+OK\r\n:2\r\n$1\r\n2\r\n");
    }

    [Fact]
    public async Task NothingExecutes_UntilExec()
    {
        await using var h = SessionHarness.Start();
        var watcher = h.Connect();
        var c = h.Connect();
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "pending", "yes");
        await c.ExpectAsync("+QUEUED\r\n");
        await watcher.SendCommandAsync("GET", "pending");
        await watcher.ExpectAsync("$-1\r\n"); // not applied yet
        await c.SendCommandAsync("EXEC");
        await c.ExpectAsync("*1\r\n+OK\r\n");
        await watcher.SendCommandAsync("GET", "pending");
        await watcher.ExpectAsync("$3\r\nyes\r\n");
    }

    [Fact]
    public async Task Discard_DropsTheQueue()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "a", "1");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("DISCARD");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("GET", "a");
        await c.ExpectAsync("$-1\r\n");
    }

    [Fact]
    public async Task ExecAndDiscard_WithoutMulti_Error()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("EXEC");
        await c.ExpectAsync("-ERR EXEC without MULTI\r\n");
        await c.SendCommandAsync("DISCARD");
        await c.ExpectAsync("-ERR DISCARD without MULTI\r\n");
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("-ERR MULTI calls can not be nested\r\n");
        await c.SendCommandAsync("DISCARD");
        await c.ExpectAsync("+OK\r\n");
    }

    [Fact]
    public async Task QueueTimeError_AbortsTheWholeExec()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "a", "1");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("GET"); // arity error at queue time
        await c.ExpectAsync("-ERR wrong number of arguments for 'get' command\r\n");
        await c.SendCommandAsync("EXEC");
        await c.ExpectAsync("-EXECABORT Transaction discarded because of previous errors.\r\n");
        await c.SendCommandAsync("GET", "a");
        await c.ExpectAsync("$-1\r\n"); // nothing ran
    }

    [Fact]
    public async Task RuntimeErrorInsideExec_DoesNotStopLaterCommands()
    {
        await using var h = SessionHarness.Start();
        var c = h.Connect();
        await c.SendCommandAsync("MULTI");
        await c.ExpectAsync("+OK\r\n");
        await c.SendCommandAsync("SET", "x", "abc");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("INCR", "x"); // fails at run time, not queue time
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("SET", "y", "ok");
        await c.ExpectAsync("+QUEUED\r\n");
        await c.SendCommandAsync("EXEC");
        await c.ExpectAsync("*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n+OK\r\n");
        await c.SendCommandAsync("GET", "y");
        await c.ExpectAsync("$2\r\nok\r\n"); // Redis does not roll back — documented behaviour
    }
}
