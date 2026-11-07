using System.Buffers;
using System.Text;
using RedisServer.Core;

namespace RedisServer.Protocol;

/// <summary>
/// The ACL command family for <see cref="CommandProcessor"/>:
/// SETUSER / GETUSER / DELUSER / USERS / LIST / WHOAMI / CAT.
///
/// Wire shapes and error texts are probed against redis-server 7.0.15:
/// GETUSER answers a flat field array in RESP2 and a map in RESP3, a
/// missing user answers a plain null (not an error), subcommand arity
/// errors name the subcommand ("acl|setuser"), and SETUSER applies its
/// rules to a copy of the user — a bad rule fails the whole command and
/// leaves the user untouched.
/// </summary>
public sealed partial class CommandProcessor
{
    private void Acl(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        var sub = NameOf(args[1]);
        switch (sub)
        {
            case "WHOAMI":
                RequireSubArity(args, 2, "acl|whoami");
                RespWriter.WriteBulkString(output, ctx.User?.Name ?? "default");
                return;
            case "USERS":
                RequireSubArity(args, 2, "acl|users");
                var names = _acl.Names;
                RespWriter.WriteArrayHeader(output, names.Count);
                foreach (var name in names)
                    RespWriter.WriteBulkString(output, name);
                return;
            case "LIST":
                RequireSubArity(args, 2, "acl|list");
                var all = _acl.Names;
                RespWriter.WriteArrayHeader(output, all.Count);
                foreach (var name in all)
                    RespWriter.WriteBulkString(output, _acl.Find(name)!.DescribeForList());
                return;
            case "GETUSER":
                RequireSubArity(args, 3, "acl|getuser");
                AclGetUser(args, ctx, output);
                return;
            case "DELUSER":
                if (args.Length < 3)
                    throw new RedisCommandException(ArityError("acl|deluser"));
                AclDeleteUsers(args, output);
                return;
            case "SETUSER":
                if (args.Length < 3)
                    throw new RedisCommandException(ArityError("acl|setuser"));
                AclSetUser(args, output);
                return;
            case "CAT":
                if (args.Length is < 2 or > 3)
                    throw new RedisCommandException(ArityError("acl|cat"));
                AclCat(args, output);
                return;
            default:
                throw new RedisCommandException(
                    $"ERR unknown subcommand '{RespParser.Describe(args[1])}'. Try ACL HELP.");
        }
    }

    private static void RequireSubArity(byte[][] args, int expected, string canonical)
    {
        if (args.Length != expected)
            throw new RedisCommandException(ArityError(canonical));
    }

    private void AclGetUser(byte[][] args, ClientContext ctx, IBufferWriter<byte> output)
    {
        var user = _acl.Find(Encoding.UTF8.GetString(args[2]));
        if (user is null)
        {
            // A missing user is a null reply, not an error.
            RespWriter.WriteNull(output, ctx.ProtocolVersion);
            return;
        }

        if (ctx.ProtocolVersion >= 3)
            RespWriter.WriteMapHeader(output, 6);
        else
            RespWriter.WriteArrayHeader(output, 12);

        RespWriter.WriteBulkString(output, "flags");
        var flags = new List<string>();
        flags.Add(user.Enabled ? "on" : "off");
        if (user.NoPass)
            flags.Add("nopass");
        RespWriter.WriteArrayHeader(output, flags.Count);
        foreach (var flag in flags)
            RespWriter.WriteBulkString(output, flag);

        RespWriter.WriteBulkString(output, "passwords");
        RespWriter.WriteArrayHeader(output, user.PasswordHashes.Count);
        foreach (var hash in user.PasswordHashes)
            RespWriter.WriteBulkString(output, hash);

        RespWriter.WriteBulkString(output, "commands");
        RespWriter.WriteBulkString(output, user.DescribeCommands());

        RespWriter.WriteBulkString(output, "keys");
        RespWriter.WriteBulkString(output, user.DescribeKeys());

        RespWriter.WriteBulkString(output, "channels");
        RespWriter.WriteBulkString(output, user.DescribeChannels());

        RespWriter.WriteBulkString(output, "selectors");
        RespWriter.WriteArrayHeader(output, 0);
    }

    private void AclDeleteUsers(byte[][] args, IBufferWriter<byte> output)
    {
        long deleted = 0;
        foreach (var nameArg in Tail(args, 2))
        {
            var name = Encoding.UTF8.GetString(nameArg);
            if (name == "default")
                throw new RedisCommandException("ERR The 'default' user cannot be removed");
            if (_acl.Delete(name))
                deleted++;
        }
        RespWriter.WriteInteger(output, deleted);
    }

    private void AclSetUser(byte[][] args, IBufferWriter<byte> output)
    {
        var name = Encoding.UTF8.GetString(args[1 + 1]);
        var work = _acl.Find(name)?.Clone() ?? FreshUser(name);
        for (var i = 3; i < args.Length; i++)
        {
            var rule = Encoding.UTF8.GetString(args[i]);
            var detail = AclManager.ApplyRule(work, rule);
            if (detail is not null)
                throw new RedisCommandException($"ERR Error in ACL SETUSER modifier '{rule}': {detail}");
        }
        _acl.Commit(work);
        RespWriter.WriteSimpleString(output, "OK");

        static AclUser FreshUser(string name)
        {
            var user = new AclUser { Name = name };
            user.Reset();
            return user;
        }
    }

    private void AclCat(byte[][] args, IBufferWriter<byte> output)
    {
        if (args.Length == 2)
        {
            RespWriter.WriteArrayHeader(output, AclCatalog.Categories.Count);
            foreach (var category in AclCatalog.Categories)
                RespWriter.WriteBulkString(output, category);
            return;
        }

        var requested = RespParser.Describe(args[2]);
        var categoryName = requested.ToLowerInvariant();
        if (!AclCatalog.IsCategory(categoryName))
            throw new RedisCommandException($"ERR Unknown category '{requested}'");
        var commands = AclCatalog.CommandsInCategory(categoryName);
        RespWriter.WriteArrayHeader(output, commands.Count);
        foreach (var command in commands)
            RespWriter.WriteBulkString(output, command);
    }
}
