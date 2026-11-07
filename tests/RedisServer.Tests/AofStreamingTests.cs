using RedisServer.Persistence;
using RedisServer.Protocol;

namespace RedisServer.Tests;

/// <summary>
/// The streaming AOF reader: it must deliver exactly the commands the
/// materialising reader returns (they share one parse path), including
/// the torn-tail rule — a crash mid-append ends the stream quietly with
/// every complete command before it delivered.
/// </summary>
public sealed class AofStreamingTests
{
    [Fact]
    public void StreamCommands_MatchesReadCommands_IncludingTheTornTail()
    {
        var dir = Path.Combine(Path.GetTempPath(), "redis-csharp-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "appendonly.aof");

        var expected = new List<byte[][]>();
        using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write))
        {
            for (var i = 0; i < 500; i++)
            {
                var command = new[]
                {
                    System.Text.Encoding.ASCII.GetBytes("SET"),
                    System.Text.Encoding.ASCII.GetBytes("key" + i),
                    System.Text.Encoding.ASCII.GetBytes(new string('v', i % 97))
                };
                expected.Add(command);
                stream.Write(RespWriter.EncodeCommand(command));
            }
            // A torn final append: half a frame, as a crash leaves it.
            stream.Write(System.Text.Encoding.ASCII.GetBytes("*3\r\n$3\r\nSET\r\n$4\r\nkey5"));
        }

        var streamed = new List<byte[][]>();
        var read = AppendOnlyFile.StreamCommands(path, streamed.Add, out var streamTruncated);
        var listed = AppendOnlyFile.ReadCommands(path, out var listTruncated);

        Assert.Equal(500, read);
        Assert.True(streamTruncated);
        Assert.True(listTruncated);
        Assert.Equal(listed.Count, streamed.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Length, streamed[i].Length);
            for (var j = 0; j < expected[i].Length; j++)
                Assert.Equal(expected[i][j], streamed[i][j]);
        }
    }

    [Fact]
    public void StreamCommands_MissingFile_DeliversNothing()
    {
        var path = Path.Combine(Path.GetTempPath(), "redis-csharp-tests", Guid.NewGuid().ToString("N"), "nope.aof");
        var delivered = AppendOnlyFile.StreamCommands(path, _ => Assert.Fail("no commands expected"),
            out var truncated);
        Assert.Equal(0, delivered);
        Assert.False(truncated);
    }
}
