using System.IO.Pipelines;

namespace RedisServer.Transport;

/// <summary>Adapts a <see cref="Stream"/> (here a network stream) to an <see cref="IDuplexPipe"/> for the session layer.</summary>
internal sealed class StreamDuplexPipe : IDuplexPipe
{
    public StreamDuplexPipe(Stream stream)
    {
        Input = PipeReader.Create(stream);
        Output = PipeWriter.Create(stream);
    }

    public PipeReader Input { get; }

    public PipeWriter Output { get; }
}
