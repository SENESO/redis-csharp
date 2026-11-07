using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using MoonSharp.Interpreter;
using RedisServer.Core;

namespace RedisServer.Protocol;

/// <summary>
/// Lua scripting for <see cref="CommandProcessor"/>: EVAL / EVALSHA /
/// SCRIPT, executed by MoonSharp (BSD-licensed, see the README's
/// third-party notes) under the command gate — a script therefore runs
/// atomically against the dataset, exactly the guarantee Redis gives.
/// The commands a script issues go through the same dispatch as client
/// commands (same ACL checks, same read-only and memory gates) and
/// append to the same effect list, so a script's writes persist and
/// replicate exactly once, as themselves.
///
/// Wire behaviour probed against redis-server 7.0.15: the NOSCRIPT
/// text, the "script: &lt;sha&gt;, on @user_script:&lt;line&gt;." error
/// suffix, redis.call's abort texts (unknown command, wrong args, not
/// allowed), redis.pcall's error tables, and the RESP conversions —
/// Lua numbers always become integer replies, string-keyed tables lose
/// their string part, RESP3 map replies arrive in Lua as flat arrays
/// and are re-emitted as maps when returned under redis.setresp(3).
/// </summary>
public sealed partial class CommandProcessor
{
    private readonly Dictionary<string, string> _scriptCache = new(StringComparer.Ordinal);

    /// <summary>Commands a script may not run (Redis's noscript set, plus the scripting family itself).</summary>
    private static readonly HashSet<string> ScriptBannedCommands = new(StringComparer.OrdinalIgnoreCase)
    {
        "MULTI", "EXEC", "DISCARD",
        "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE",
        "PSYNC", "REPLCONF", "REPLICAOF",
        "SAVE", "BGSAVE", "BGREWRITEAOF",
        "EVAL", "EVALSHA", "SCRIPT", "ACL", "CLIENT"
    };

    private void Eval(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var source = Encoding.UTF8.GetString(args[1]);
        var sha = ScriptSha(source);
        _scriptCache[sha] = source;
        RunScript(source, sha, args, ctx, output, effects, effectDbs);
    }

    private void EvalSha(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var sha = Encoding.UTF8.GetString(args[1]).ToLowerInvariant();
        if (!_scriptCache.TryGetValue(sha, out var source))
            throw new RedisCommandException("NOSCRIPT No matching script. Please use EVAL.");
        RunScript(source, sha, args, ctx, output, effects, effectDbs);
    }

    private void ScriptCommand(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var sub = NameOf(args[1]);
        switch (sub)
        {
            case "LOAD":
                RequireSubArity(args, 3, "script|load");
                var source = Encoding.UTF8.GetString(args[2]);
                var sha = ScriptSha(source);
                _scriptCache[sha] = source;
                RespWriter.WriteBulkString(output, sha);
                return;
            case "EXISTS":
                RespWriter.WriteArrayHeader(output, args.Length - 2);
                for (var i = 2; i < args.Length; i++)
                    RespWriter.WriteInteger(output,
                        _scriptCache.ContainsKey(Encoding.UTF8.GetString(args[i]).ToLowerInvariant()) ? 1 : 0);
                return;
            case "FLUSH":
                if (args.Length > 3)
                    throw new RedisCommandException(ArityError("script|flush"));
                _scriptCache.Clear();
                RespWriter.WriteSimpleString(output, "OK");
                return;
            default:
                throw new RedisCommandException(
                    $"ERR unknown subcommand '{RespParser.Describe(args[1])}'. Try SCRIPT HELP.");
        }
    }

    private static string ScriptSha(string source) =>
        Convert.ToHexString(SHA1.HashData(Encoding.UTF8.GetBytes(source))).ToLowerInvariant();

    /// <summary>Per-run mutable state shared with the redis.* callbacks.</summary>
    private sealed class ScriptRun
    {
        public required ClientContext Client { get; init; }
        public required List<byte[][]> Effects { get; init; }
        public required List<int> EffectDbs { get; init; }
        public required string Sha { get; init; }
        public int RespVersion { get; set; }
        public string? AbortText { get; set; }
        public HashSet<Table> MapTables { get; } = new();
    }

    private void RunScript(string source, string sha, byte[][] args, ClientContext ctx,
        IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var numKeys = ParseLong(args[2]);
        if (numKeys < 0)
            throw new RedisCommandException("ERR Number of keys can't be negative");
        if (numKeys > args.Length - 3)
            throw new RedisCommandException("ERR Number of keys can't be greater than number of args");

        var run = new ScriptRun
        {
            Client = ctx,
            Effects = effects,
            EffectDbs = effectDbs,
            Sha = sha,
            RespVersion = ctx.ProtocolVersion
        };

        var script = new Script(CoreModules.Preset_SoftSandbox);
        var keys = new Table(script);
        for (var i = 0; i < numKeys; i++)
            keys.Set(i + 1, DynValue.NewString(Encoding.UTF8.GetString(args[3 + i])));
        var argv = new Table(script);
        for (var i = 3 + (int)numKeys; i < args.Length; i++)
            argv.Set(i - 2 - (int)numKeys, DynValue.NewString(Encoding.UTF8.GetString(args[i])));
        script.Globals.Set("KEYS", DynValue.NewTable(keys));
        script.Globals.Set("ARGV", DynValue.NewTable(argv));

        var redis = new Table(script);
        redis.Set("call", DynValue.NewCallback((_, cbArgs) => ScriptCall(run, cbArgs, pcall: false)));
        redis.Set("pcall", DynValue.NewCallback((_, cbArgs) => ScriptCall(run, cbArgs, pcall: true)));
        redis.Set("setresp", DynValue.NewCallback((_, cbArgs) =>
        {
            if (cbArgs.Count > 0 && cbArgs[0].Type == DataType.Number)
                run.RespVersion = (int)cbArgs[0].Number;
            return DynValue.Nil;
        }));
        script.Globals.Set("redis", DynValue.NewTable(redis));

        DynValue result;
        try
        {
            result = script.DoString(source, script.Globals, "user_script");
        }
        catch (SyntaxErrorException ex)
        {
            throw new RedisCommandException(
                $"ERR Error compiling script (new function): {NormalizeLocation(ex.DecoratedMessage ?? ex.Message)}");
        }
        catch (ScriptRuntimeException ex)
        {
            if (run.AbortText is { } abort)
                throw new RedisCommandException($"{abort} script: {sha}, on @user_script:1.");
            var decorated = NormalizeLocation(ex.DecoratedMessage ?? ex.Message);
            throw new RedisCommandException($"ERR {decorated} script: {sha}, on @user_script:{ExtractLine(decorated)}.");
        }

        WriteScriptResult(output, result, run);
    }

    /// <summary>
    /// MoonSharp decorates error locations as "user_script:(line,start-end)";
    /// Redis's form is "user_script:line". Normalises the former to the
    /// latter.
    /// </summary>
    private static string NormalizeLocation(string decorated) =>
        System.Text.RegularExpressions.Regex.Replace(decorated,
            @"user_script:\((\d+),[\d-]+\)", "user_script:$1");

    private static int ExtractLine(string decorated)
    {
        // After normalisation the form is "user_script:<line>: ...".
        var marker = decorated.IndexOf(':');
        if (marker < 0)
            return 1;
        var rest = decorated.AsSpan(marker + 1);
        var end = rest.IndexOf(':');
        return end > 0 && int.TryParse(rest[..end], out var line) ? line : 1;
    }

    /// <summary>
    /// redis.call / redis.pcall: converts the Lua arguments to a
    /// command, dispatches it through ExecuteLocked on the caller's
    /// context and effect list, and converts the reply back to Lua.
    /// </summary>
    private DynValue ScriptCall(ScriptRun run, CallbackArguments cbArgs, bool pcall)
    {
        if (cbArgs.Count == 0 || cbArgs[0].Type != DataType.String)
            return ScriptCallError(run, pcall, "ERR Wrong number of args calling Redis command from script");

        var name = cbArgs[0].String;
        if (ScriptBannedCommands.Contains(name))
            return ScriptCallError(run, pcall, "ERR This Redis command is not allowed from script");

        var cmdArgs = new byte[cbArgs.Count][];
        cmdArgs[0] = Encoding.UTF8.GetBytes(name);
        for (var i = 1; i < cbArgs.Count; i++)
        {
            cmdArgs[i] = cbArgs[i].Type switch
            {
                DataType.String => Encoding.UTF8.GetBytes(cbArgs[i].String),
                DataType.Number => Encoding.ASCII.GetBytes(FormatScriptNumber(cbArgs[i].Number)),
                _ => null!
            };
            if (cmdArgs[i] is null)
                return ScriptCallError(run, pcall,
                    "ERR Lua boolean value or table passed as argument to a Redis command");
        }

        var buffer = new ArrayBufferWriter<byte>();
        var savedProtocol = run.Client.ProtocolVersion;
        run.Client.ProtocolVersion = Math.Clamp(run.RespVersion, 2, 3);
        try
        {
            ExecuteLocked(cmdArgs, run.Client, buffer, run.Effects, run.EffectDbs, inTransactionBody: false);
        }
        finally
        {
            run.Client.ProtocolVersion = savedProtocol;
        }

        var value = RespValue.Parse(buffer.WrittenSpan);
        if (value is RespValue.ErrorValue error)
        {
            var text = error.Text;
            if (text.StartsWith("ERR unknown command", StringComparison.Ordinal))
                text = "ERR Unknown Redis command called from script";
            else if (text.StartsWith("ERR wrong number of arguments for", StringComparison.Ordinal))
                text = "ERR Wrong number of args calling Redis command from script";
            return ScriptCallError(run, pcall, text);
        }
        return RespToLua(value, run);
    }

    private DynValue ScriptCallError(ScriptRun run, bool pcall, string text)
    {
        if (pcall)
        {
            var table = new Table(null);
            table.Set("err", DynValue.NewString(text));
            return DynValue.NewTable(table);
        }
        run.AbortText = text;
        throw new ScriptRuntimeException(text);
    }

    private static string FormatScriptNumber(double value) =>
        value == Math.Truncate(value) && Math.Abs(value) < 1e17
            ? ((long)value).ToString(CultureInfo.InvariantCulture)
            : RedisStore.FormatDouble(value);

    private static DynValue RespToLua(RespValue value, ScriptRun run)
    {
        switch (value)
        {
            case RespValue.StatusValue status:
            {
                var table = new Table(null);
                table.Set("ok", DynValue.NewString(status.Text));
                return DynValue.NewTable(table);
            }
            case RespValue.IntegerValue integer:
                return DynValue.NewNumber(integer.Value);
            case RespValue.BulkValue bulk:
                return DynValue.NewString(Encoding.UTF8.GetString(bulk.Bytes));
            case RespValue.DoubleValue dbl:
                return DynValue.NewNumber(dbl.Value);
            case RespValue.NullValue:
                return run.RespVersion >= 3 ? DynValue.Nil : DynValue.False;
            case RespValue.ArrayValue array:
            {
                var table = new Table(null);
                for (var i = 0; i < array.Items.Length; i++)
                    table.Set(i + 1, RespToLua(array.Items[i], run));
                return DynValue.NewTable(table);
            }
            case RespValue.MapValue map:
            {
                // RESP3 maps arrive as flat alternating arrays, exactly
                // what Redis hands Lua; the table is tagged so a direct
                // return under setresp(3) re-emits it as a map.
                var table = new Table(null);
                for (var i = 0; i < map.Pairs.Length; i++)
                {
                    table.Set(2 * i + 1, RespToLua(map.Pairs[i].Key, run));
                    table.Set(2 * i + 2, RespToLua(map.Pairs[i].Value, run));
                }
                run.MapTables.Add(table);
                return DynValue.NewTable(table);
            }
            default:
                return DynValue.Nil;
        }
    }

    private void WriteScriptResult(IBufferWriter<byte> output, DynValue value, ScriptRun run) =>
        WriteLuaValue(output, value, run);

    private void WriteLuaValue(IBufferWriter<byte> output, DynValue value, ScriptRun run)
    {
        switch (value.Type)
        {
            case DataType.Nil:
            case DataType.Void:
                RespWriter.WriteNull(output, run.RespVersion >= 3 ? 3 : 2);
                return;
            case DataType.Boolean:
                if (value.Boolean)
                    RespWriter.WriteInteger(output, 1);
                else
                    RespWriter.WriteNull(output, run.RespVersion >= 3 ? 3 : 2);
                return;
            case DataType.Number:
                // Lua numbers always become integer replies (truncated
                // toward zero), in RESP2 and RESP3 alike.
                RespWriter.WriteInteger(output, (long)value.Number);
                return;
            case DataType.String:
                RespWriter.WriteBulkString(output, value.String);
                return;
            case DataType.Table:
            {
                var table = value.Table;
                var err = table.Get("err");
                if (err.Type == DataType.String)
                {
                    RespWriter.WriteError(output, err.String);
                    return;
                }
                var ok = table.Get("ok");
                if (ok.Type == DataType.String)
                {
                    RespWriter.WriteSimpleString(output, ok.String);
                    return;
                }
                if (run.RespVersion >= 3 && run.MapTables.Contains(table))
                {
                    var length = TableLength(table);
                    RespWriter.WriteMapHeader(output, length / 2);
                    for (var i = 1; i <= length; i++)
                        WriteLuaValue(output, table.Get(i), run);
                    return;
                }
                var count = TableLength(table);
                RespWriter.WriteArrayHeader(output, count);
                for (var i = 1; i <= count; i++)
                    WriteLuaValue(output, table.Get(i), run);
                return;
            }
            default:
                RespWriter.WriteNull(output, run.RespVersion >= 3 ? 3 : 2);
                return;
        }
    }

    /// <summary>The array-part length of a Lua table: entries from 1 until the first nil.</summary>
    private static int TableLength(Table table)
    {
        var length = 0;
        while (table.Get(length + 1).Type != DataType.Nil)
            length++;
        return length;
    }
}

/// <summary>
/// One parsed RESP reply — the bridge between a nested command's byte
/// reply and Lua values.
/// </summary>
internal abstract record RespValue
{
    public sealed record StatusValue(string Text) : RespValue;
    public sealed record ErrorValue(string Text) : RespValue;
    public sealed record IntegerValue(long Value) : RespValue;
    public sealed record BulkValue(byte[] Bytes) : RespValue;
    public sealed record DoubleValue(double Value) : RespValue;
    public sealed record NullValue : RespValue;
    public sealed record ArrayValue(RespValue[] Items) : RespValue;
    public sealed record MapValue((RespValue Key, RespValue Value)[] Pairs) : RespValue;

    public static RespValue Parse(ReadOnlySpan<byte> bytes)
    {
        var position = 0;
        return ParseAt(bytes, ref position);
    }

    private static RespValue ParseAt(ReadOnlySpan<byte> bytes, ref int position)
    {
        if (position >= bytes.Length)
            return new NullValue();
        var type = (char)bytes[position++];
        switch (type)
        {
            case '+':
                return new StatusValue(ReadLine(bytes, ref position));
            case '-':
                return new ErrorValue(ReadLine(bytes, ref position));
            case ':':
                return new IntegerValue(long.Parse(ReadLine(bytes, ref position), CultureInfo.InvariantCulture));
            case ',':
                return new DoubleValue(double.Parse(ReadLine(bytes, ref position), CultureInfo.InvariantCulture));
            case '_':
                position += 2;
                return new NullValue();
            case '#':
            {
                var flag = (char)bytes[position];
                position += 3;
                return new IntegerValue(flag == 't' ? 1 : 0);
            }
            case '$':
            case '=':
            case '|':
            {
                var length = int.Parse(ReadLine(bytes, ref position), CultureInfo.InvariantCulture);
                if (length < 0)
                    return new NullValue();
                var data = bytes.Slice(position, length).ToArray();
                position += length + 2;
                return new BulkValue(data);
            }
            case '*':
            {
                var count = int.Parse(ReadLine(bytes, ref position), CultureInfo.InvariantCulture);
                if (count < 0)
                    return new NullValue();
                var items = new RespValue[count];
                for (var i = 0; i < count; i++)
                    items[i] = ParseAt(bytes, ref position);
                return new ArrayValue(items);
            }
            case '%':
            {
                var count = int.Parse(ReadLine(bytes, ref position), CultureInfo.InvariantCulture);
                var pairs = new (RespValue, RespValue)[count];
                for (var i = 0; i < count; i++)
                    pairs[i] = (ParseAt(bytes, ref position), ParseAt(bytes, ref position));
                return new MapValue(pairs);
            }
            default:
                return new NullValue();
        }
    }

    private static string ReadLine(ReadOnlySpan<byte> bytes, ref int position)
    {
        var end = bytes[position..].IndexOf("\r\n"u8);
        var text = Encoding.UTF8.GetString(bytes.Slice(position, end));
        position += end + 2;
        return text;
    }
}
