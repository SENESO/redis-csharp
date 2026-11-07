using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;
using RedisServer.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RedisServer.Protocol;

/// <summary>
/// One client connection: frames the byte stream into RESP commands with
/// <see cref="RespParser"/>, dispatches them to the
/// <see cref="CommandProcessor"/>, and writes replies.
///
/// All outbound bytes — command replies produced here and pub/sub frames
/// pushed by other connections — flow through one unbounded channel drained
/// by a single writer loop, so the transport has exactly one writer and a
/// publisher never has to lock or block on this connection's socket.
/// </summary>
public sealed class RedisSession
{
    private readonly IDuplexPipe _transport;
    private readonly CommandProcessor _processor;
    private readonly PubSubHub _hub;
    private readonly RedisStats _stats;
    private readonly ILogger _logger;
    private readonly ClientContext _context = new();
    private readonly Channel<byte[]> _outbound = Channel.CreateUnbounded<byte[]>();

    public RedisSession(IDuplexPipe transport, CommandProcessor processor, PubSubHub hub,
        RedisStats stats, ILogger? logger = null)
    {
        _transport = transport;
        _processor = processor;
        _hub = hub;
        _stats = stats;
        _logger = logger ?? NullLogger.Instance;
        _context.PostFrame = frame => _outbound.Writer.TryWrite(frame);
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var writerTask = PumpOutputAsync(cancellationToken);
        try
        {
            await ReadLoopAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Server shutting down — close quietly.
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Session ended with an error.");
        }
        finally
        {
            _hub.RemoveAll(_context);
            _outbound.Writer.TryComplete();
            try
            {
                // Let the pump flush whatever is still queued (a final reply
                // or protocol error) before the transport is completed.
                await writerTask.WaitAsync(TimeSpan.FromSeconds(2));
            }
            catch (Exception)
            {
                // The peer is gone or we are shutting down; nothing to salvage.
            }
            await _transport.Output.CompleteAsync();
            await _transport.Input.CompleteAsync();
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        var reader = _transport.Input;
        while (true)
        {
            var result = await reader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            var closeAfterFlush = ProcessBuffer(ref buffer);

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (closeAfterFlush || _context.CloseRequested)
                return;
            if (result.IsCompleted)
                return;
            if (buffer.Length > RespParser.MaxLineLength && !HasPartialArrayStart(buffer))
            {
                EnqueueReply(BuildError("ERR Protocol error: too big inline request"));
                return;
            }
        }
    }

    /// <summary>
    /// Processes every complete command in the buffer. All replies produced
    /// from one read are rendered into a single buffer and queued as one
    /// frame, so a pipelined batch costs one outbound write instead of one
    /// per command. Returns true on a protocol error.
    /// </summary>
    private bool ProcessBuffer(ref ReadOnlySequence<byte> buffer)
    {
        // Parse every complete command in this read first, then execute
        // them as one batch under one gate acquisition.
        _pendingBatch.Clear();
        string? protocolError = null;
        while (!buffer.IsEmpty)
        {
            if (!RespParser.TryReadCommand(ref buffer, out var args, out protocolError))
                break; // incomplete (wait for more bytes) or malformed (protocolError set)
            if (args is { Length: > 0 })
                _pendingBatch.Add(args);
            // else: e.g. a null array — not a command, skipped
        }

        _replyBuffer.ResetWrittenCount();
        if (_pendingBatch.Count > 0)
            _processor.ExecuteBatch(_pendingBatch, _context, _replyBuffer);
        if (protocolError is not null)
            RespWriter.WriteError(_replyBuffer, $"ERR Protocol error: {protocolError}");
        if (_replyBuffer.WrittenCount > 0)
            _outbound.Writer.TryWrite(_replyBuffer.WrittenSpan.ToArray());
        return protocolError is not null;
    }

    private static bool HasPartialArrayStart(ReadOnlySequence<byte> buffer) =>
        !buffer.IsEmpty && buffer.FirstSpan[0] == (byte)'*';

    private static byte[] BuildError(string message)
    {
        var buffer = new ArrayBufferWriter<byte>();
        RespWriter.WriteError(buffer, message);
        return buffer.WrittenSpan.ToArray();
    }

    private void EnqueueReply(byte[] frame) => _outbound.Writer.TryWrite(frame);

    private readonly ArrayBufferWriter<byte> _replyBuffer = new();
    private readonly List<byte[][]> _pendingBatch = new();
    private readonly ArrayBufferWriter<byte> _sendBuffer = new();

    /// <summary>Moves every currently queued outbound frame into the send buffer. False when the queue was empty.</summary>
    private bool DrainOutbound(ChannelReader<byte[]> reader)
    {
        _sendBuffer.ResetWrittenCount();
        var any = false;
        while (reader.TryRead(out var frame))
        {
            _sendBuffer.Write(frame);
            any = true;
        }
        return any;
    }

    private async Task PumpOutputAsync(CancellationToken cancellationToken)
    {
        var writer = _transport.Output;
        var reader = _outbound.Reader;
        try
        {
            // Drain every queued frame into one buffer and write once per
            // burst, instead of one flushing write per frame.
            while (true)
            {
                if (!DrainOutbound(reader))
                {
                    if (!await reader.WaitToReadAsync(cancellationToken))
                        break; // channel completed and empty: nothing left
                    continue;
                }
                var result = await writer.WriteAsync(_sendBuffer.WrittenMemory, cancellationToken);
                if (result.IsCompleted)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Output pump ended with an error.");
        }
    }
}
