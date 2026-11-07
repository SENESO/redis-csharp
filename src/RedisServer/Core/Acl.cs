using System.Security.Cryptography;
using System.Text;

namespace RedisServer.Core;

/// <summary>
/// One key pattern from a user's rule set: a glob plus the access it
/// grants. <c>~pat</c> grants read and write; <c>%R~pat</c> read only,
/// <c>%W~pat</c> write only (the <c>%RW~</c> form is the explicit
/// spelling of the plain <c>~</c> rule).
/// </summary>
public sealed record AclKeyPattern(byte[] Glob, bool CanRead, bool CanWrite)
{
    /// <summary>The rule text as GETUSER / ACL LIST render it.</summary>
    public string Render()
    {
        var glob = Encoding.UTF8.GetString(Glob);
        if (CanRead && CanWrite)
            return "~" + glob;
        var flags = (CanRead ? "R" : "") + (CanWrite ? "W" : "");
        return $"%{flags}~{glob}";
    }
}

/// <summary>
/// One ACL user. Command permission is an effective set of canonical
/// command names (see <see cref="AclCatalog"/>), mutated rule by rule in
/// application order — the bitmap model Redis itself uses, so rule order
/// matters exactly the way it does there. Alongside it, the wholesale
/// grants (categories added, commands added/removed on top) are tracked
/// in application order purely so GETUSER can render the commands string
/// the way Redis derives it ("-@all +@keyspace +get -scan").
/// </summary>
public sealed class AclUser
{
    public required string Name { get; init; }

    public bool Enabled { get; set; }
    public bool NoPass { get; set; }

    /// <summary>SHA-256 hex (lowercase) of each password, in the order passwords were added.</summary>
    public List<string> PasswordHashes { get; } = new();

    public bool AllKeys { get; set; }
    public List<AclKeyPattern> KeyPatterns { get; } = new();

    public bool AllChannels { get; set; }
    public List<byte[]> ChannelPatterns { get; } = new();

    /// <summary>The effective permission set (canonical command names).</summary>
    public HashSet<string> AllowedCommands { get; } = new(StringComparer.Ordinal);

    // Rendering bookkeeping (see the class summary).
    internal bool AllCommandsBase { get; set; }
    internal List<string> GrantedCategories { get; } = new();
    internal List<string> RemovedCategories { get; } = new();
    internal List<string> AddedCommands { get; } = new();
    internal List<string> RemovedCommands { get; } = new();

    public bool AllowsCommand(string canonicalName) => AllowedCommands.Contains(canonicalName);

    /// <summary>Checks a presented plaintext password against the stored hashes.</summary>
    public bool CheckPassword(byte[] presented)
    {
        if (NoPass)
            return true;
        var hash = AclManager.HashPassword(presented);
        var presentedBytes = Encoding.ASCII.GetBytes(hash);
        var match = false;
        foreach (var stored in PasswordHashes)
        {
            if (CryptographicOperations.FixedTimeEquals(presentedBytes, Encoding.ASCII.GetBytes(stored)))
                match = true;
        }
        return match;
    }

    /// <summary>Every key must be reachable through some pattern granting the needed access.</summary>
    public bool AllowsKeys(IReadOnlyList<byte[]> keys, bool write)
    {
        if (keys.Count == 0 || AllKeys)
            return true;
        foreach (var key in keys)
        {
            var granted = false;
            foreach (var pattern in KeyPatterns)
            {
                if ((write ? pattern.CanWrite : pattern.CanRead) && Glob.IsMatch(pattern.Glob, key))
                {
                    granted = true;
                    break;
                }
            }
            if (!granted)
                return false;
        }
        return true;
    }

    /// <summary>
    /// Channel access for SUBSCRIBE: with patterns configured, a channel
    /// must match one of them. PSUBSCRIBE checks its pattern argument the
    /// same way (an approximation of Redis's overlap test — a pattern is
    /// admitted when the pattern text itself matches an allowed glob).
    /// </summary>
    public bool AllowsChannel(byte[] channel)
    {
        if (AllChannels)
            return true;
        foreach (var pattern in ChannelPatterns)
        {
            if (Glob.IsMatch(pattern, channel))
                return true;
        }
        return false;
    }

    /// <summary>The GETUSER "commands" string, derived the way Redis derives it.</summary>
    public string DescribeCommands()
    {
        var parts = new List<string> { AllCommandsBase ? "+@all" : "-@all" };
        if (AllCommandsBase)
        {
            foreach (var name in RemovedCommands)
                parts.Add("-" + name);
            foreach (var category in RemovedCategories)
                parts.Add("-@" + category);
            return string.Join(" ", parts);
        }

        var covered = new HashSet<string>(StringComparer.Ordinal);
        foreach (var category in GrantedCategories)
        {
            parts.Add("+@" + category);
            foreach (var name in AclCatalog.CommandsInCategory(category))
                covered.Add(name);
        }
        foreach (var name in AddedCommands)
        {
            if (!covered.Contains(name))
                parts.Add("+" + name);
        }
        foreach (var name in RemovedCommands)
        {
            if (covered.Contains(name) || AllCommandsBase)
                parts.Add("-" + name);
        }
        foreach (var category in RemovedCategories)
            parts.Add("-@" + category);
        return string.Join(" ", parts);
    }

    /// <summary>The GETUSER "keys" string: "~*" for allkeys, else the patterns space-joined.</summary>
    public string DescribeKeys() =>
        AllKeys ? "~*" : string.Join(" ", KeyPatterns.Select(p => p.Render()));

    /// <summary>The GETUSER "channels" string: "&amp;*" for allchannels, else the patterns space-joined.</summary>
    public string DescribeChannels() =>
        AllChannels ? "&*" : string.Join(" ", ChannelPatterns.Select(p => "&" + Encoding.UTF8.GetString(p)));

    /// <summary>
    /// The ACL LIST line: "user &lt;name&gt; &lt;flags&gt; &lt;#hashes&gt;
    /// &lt;keys&gt; &lt;channels&gt; &lt;commands&gt;", with the channels part
    /// rendered as "&amp;*" or as "resetchannels" followed by the patterns.
    /// </summary>
    public string DescribeForList()
    {
        var parts = new List<string> { "user", Name, Enabled ? "on" : "off" };
        if (NoPass)
            parts.Add("nopass");
        foreach (var hash in PasswordHashes)
            parts.Add("#" + hash);
        if (AllKeys)
            parts.Add("~*");
        else if (KeyPatterns.Count > 0)
            parts.Add(DescribeKeys());
        if (AllChannels)
            parts.Add("&*");
        else
        {
            parts.Add("resetchannels");
            foreach (var pattern in ChannelPatterns)
                parts.Add("&" + Encoding.UTF8.GetString(pattern));
        }
        parts.Add(DescribeCommands());
        return string.Join(" ", parts);
    }

    /// <summary>A deep copy — SETUSER applies rules to a copy and commits only on success.</summary>
    public AclUser Clone()
    {
        var copy = new AclUser { Name = Name };
        copy.CopyFrom(this);
        return copy;
    }

    internal void CopyFrom(AclUser other)
    {
        Enabled = other.Enabled;
        NoPass = other.NoPass;
        PasswordHashes.Clear();
        PasswordHashes.AddRange(other.PasswordHashes);
        AllKeys = other.AllKeys;
        KeyPatterns.Clear();
        KeyPatterns.AddRange(other.KeyPatterns);
        AllChannels = other.AllChannels;
        ChannelPatterns.Clear();
        ChannelPatterns.AddRange(other.ChannelPatterns);
        AllowedCommands.Clear();
        AllowedCommands.UnionWith(other.AllowedCommands);
        AllCommandsBase = other.AllCommandsBase;
        GrantedCategories.Clear();
        GrantedCategories.AddRange(other.GrantedCategories);
        RemovedCategories.Clear();
        RemovedCategories.AddRange(other.RemovedCategories);
        AddedCommands.Clear();
        AddedCommands.AddRange(other.AddedCommands);
        RemovedCommands.Clear();
        RemovedCommands.AddRange(other.RemovedCommands);
    }

    /// <summary>The state a brand-new user starts from — and the state "reset" restores.</summary>
    internal void Reset()
    {
        Enabled = false;
        NoPass = false;
        PasswordHashes.Clear();
        AllKeys = false;
        KeyPatterns.Clear();
        AllChannels = false;
        ChannelPatterns.Clear();
        AllowedCommands.Clear();
        AllCommandsBase = false;
        GrantedCategories.Clear();
        RemovedCategories.Clear();
        AddedCommands.Clear();
        RemovedCommands.Clear();
    }
}

/// <summary>
/// The user directory. Owns the <c>default</c> user (seeded from
/// --requirepass at startup: that password becomes the default user's
/// password; without one the default user is passwordless) and every
/// user created with ACL SETUSER. Users are runtime state only — like
/// Redis without an aclfile, a restart forgets them; the AOF and the
/// replication stream never carry user definitions.
///
/// All access happens under the command processor's gate, so no locking
/// lives here.
/// </summary>
public sealed class AclManager
{
    private readonly Dictionary<string, AclUser> _users = new(StringComparer.Ordinal);

    public AclManager(string? requirePass)
    {
        var defaultUser = new AclUser { Name = "default", Enabled = true };
        defaultUser.AllKeys = true;
        defaultUser.AllChannels = true;
        GrantAllCommands(defaultUser);
        if (requirePass is null)
        {
            defaultUser.NoPass = true;
        }
        else
        {
            defaultUser.PasswordHashes.Add(HashPassword(Encoding.UTF8.GetBytes(requirePass)));
        }
        _users.Add("default", defaultUser);
    }

    public AclUser DefaultUser => _users["default"];

    public AclUser? Find(string name) => _users.GetValueOrDefault(name);

    /// <summary>All user names, sorted — the ACL USERS reply.</summary>
    public IReadOnlyList<string> Names
    {
        get
        {
            var names = _users.Keys.ToList();
            names.Sort(StringComparer.Ordinal);
            return names;
        }
    }

    public AclUser GetOrCreate(string name)
    {
        if (_users.TryGetValue(name, out var existing))
            return existing;
        var created = new AclUser { Name = name };
        created.Reset();
        _users.Add(name, created);
        return created;
    }

    /// <summary>Deletes a user. The default user cannot be deleted (the caller reports that).</summary>
    public bool Delete(string name) => name != "default" && _users.Remove(name);

    /// <summary>
    /// Commits a user prepared by SETUSER: an existing user is updated in
    /// place (connections acting as it hold the object, so the new rules
    /// reach them); a new one joins the directory.
    /// </summary>
    public void Commit(AclUser user)
    {
        if (_users.TryGetValue(user.Name, out var existing))
            existing.CopyFrom(user);
        else
            _users.Add(user.Name, user);
    }

    internal static void GrantAllCommands(AclUser user)
    {
        user.AllowedCommands.Clear();
        user.AllowedCommands.UnionWith(AclCatalog.AllCommandNames);
        user.AllCommandsBase = true;
        user.GrantedCategories.Clear();
        user.RemovedCategories.Clear();
        user.AddedCommands.Clear();
        user.RemovedCommands.Clear();
    }

    internal static void RevokeAllCommands(AclUser user)
    {
        user.AllowedCommands.Clear();
        user.AllCommandsBase = false;
        user.GrantedCategories.Clear();
        user.RemovedCategories.Clear();
        user.AddedCommands.Clear();
        user.RemovedCommands.Clear();
    }

    /// <summary>SHA-256 of the password bytes, lowercase hex — the form Redis stores and GETUSER reports.</summary>
    public static string HashPassword(byte[] password) =>
        Convert.ToHexString(SHA256.HashData(password)).ToLowerInvariant();

    /// <summary>A valid stored-hash literal: exactly 64 lowercase hexadecimal characters.</summary>
    public static bool IsValidHash(string text)
    {
        if (text.Length != 64)
            return false;
        foreach (var c in text)
        {
            if (c is not ((>= '0' and <= '9') or (>= 'a' and <= 'f')))
                return false;
        }
        return true;
    }

    /// <summary>
    /// Applies one SETUSER rule to <paramref name="user"/>. Returns null
    /// on success or the error detail (the part after "Error in ACL
    /// SETUSER modifier '...': ") on failure.
    /// </summary>
    public static string? ApplyRule(AclUser user, string rule)
    {
        switch (rule)
        {
            case "":
                return null; // an empty modifier is a no-op in Redis
            case "on":
                user.Enabled = true;
                return null;
            case "off":
                user.Enabled = false;
                return null;
            case "reset":
                user.Reset();
                return null;
            case "nopass":
                user.NoPass = true;
                user.PasswordHashes.Clear();
                return null;
            case "resetpass":
                user.NoPass = false;
                user.PasswordHashes.Clear();
                return null;
            case "allkeys":
                user.AllKeys = true;
                user.KeyPatterns.Clear();
                return null;
            case "resetkeys":
                user.AllKeys = false;
                user.KeyPatterns.Clear();
                return null;
            case "allchannels":
                user.AllChannels = true;
                user.ChannelPatterns.Clear();
                return null;
            case "resetchannels":
                user.AllChannels = false;
                user.ChannelPatterns.Clear();
                return null;
            case "allcommands":
                GrantAllCommands(user);
                return null;
            case "nocommands":
                RevokeAllCommands(user);
                return null;
        }

        if (rule.StartsWith('>'))
        {
            user.PasswordHashes.Add(HashPassword(Encoding.UTF8.GetBytes(rule[1..])));
            return null;
        }
        if (rule.StartsWith('<'))
        {
            var hash = HashPassword(Encoding.UTF8.GetBytes(rule[1..]));
            return user.PasswordHashes.Remove(hash)
                ? null
                : "The password you are trying to remove from the user does not exist";
        }
        if (rule.StartsWith('#'))
        {
            var hash = rule[1..];
            if (!IsValidHash(hash))
                return "The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters";
            if (!user.PasswordHashes.Contains(hash))
                user.PasswordHashes.Add(hash);
            return null;
        }
        if (rule.StartsWith('!'))
        {
            var hash = rule[1..];
            if (!IsValidHash(hash))
                return "The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters";
            return user.PasswordHashes.Remove(hash)
                ? null
                : "The password you are trying to remove from the user does not exist";
        }
        if (rule.StartsWith('~'))
        {
            user.KeyPatterns.Add(new AclKeyPattern(Encoding.UTF8.GetBytes(rule[1..]), CanRead: true, CanWrite: true));
            return null;
        }
        if (rule.StartsWith('%'))
        {
            var tilde = rule.IndexOf('~');
            if (tilde < 0)
                return "Syntax error";
            var flags = rule[1..tilde];
            if (flags.Length == 0 || flags.Any(c => c is not ('R' or 'W')))
                return "Syntax error";
            user.KeyPatterns.Add(new AclKeyPattern(
                Encoding.UTF8.GetBytes(rule[(tilde + 1)..]),
                CanRead: flags.Contains('R'), CanWrite: flags.Contains('W')));
            return null;
        }
        if (rule.StartsWith('&'))
        {
            user.ChannelPatterns.Add(Encoding.UTF8.GetBytes(rule[1..]));
            return null;
        }
        if (rule.StartsWith('+') || rule.StartsWith('-'))
        {
            var grant = rule[0] == '+';
            var target = rule[1..];
            if (target.StartsWith('@'))
            {
                var category = target[1..].ToLowerInvariant();
                if (category == "all")
                {
                    if (grant) GrantAllCommands(user); else RevokeAllCommands(user);
                    return null;
                }
                if (!AclCatalog.IsCategory(category))
                    return "Unknown command or category name in ACL";
                ApplyCategory(user, category, grant);
                return null;
            }
            var names = AclCatalog.ResolveRuleName(target.ToLowerInvariant());
            if (names is null)
                return "Unknown command or category name in ACL";
            ApplyCommands(user, names, grant);
            return null;
        }
        return "Syntax error";
    }

    private static void ApplyCategory(AclUser user, string category, bool grant)
    {
        var members = AclCatalog.CommandsInCategory(category);
        if (grant)
        {
            foreach (var name in members)
                user.AllowedCommands.Add(name);
            user.RemovedCategories.Remove(category);
            if (!user.GrantedCategories.Contains(category))
                user.GrantedCategories.Add(category);
            // A wholesale grant supersedes earlier per-command removals
            // inside the category.
            user.RemovedCommands.RemoveAll(members.Contains);
        }
        else
        {
            foreach (var name in members)
                user.AllowedCommands.Remove(name);
            user.GrantedCategories.Remove(category);
            if (!user.RemovedCategories.Contains(category))
                user.RemovedCategories.Add(category);
            user.AddedCommands.RemoveAll(members.Contains);
        }
    }

    private static void ApplyCommands(AclUser user, IReadOnlyList<string> names, bool grant)
    {
        foreach (var name in names)
        {
            if (grant)
            {
                user.AllowedCommands.Add(name);
                user.RemovedCommands.Remove(name);
                if (!user.AddedCommands.Contains(name))
                    user.AddedCommands.Add(name);
            }
            else
            {
                user.AllowedCommands.Remove(name);
                user.AddedCommands.Remove(name);
                if (!user.RemovedCommands.Contains(name))
                    user.RemovedCommands.Add(name);
            }
        }
    }
}

/// <summary>
/// The command catalogue ACL reasons about: canonical command names
/// (subcommand-ful commands use the "container|subcommand" form Redis
/// uses, e.g. <c>acl|setuser</c>), the category of every command, and
/// the key positions of every command — shared by ACL key checks and by
/// cluster slot checks.
///
/// Category assignments follow Redis's own command flags (a read/write
/// split matching its @read/@write categories; container commands filed
/// where Redis files them).
/// </summary>
public static class AclCatalog
{
    /// <summary>All ACL categories, in the order ACL CAT lists them.</summary>
    public static readonly IReadOnlyList<string> Categories = new[]
    {
        "keyspace", "read", "write", "set", "sortedset", "list", "hash", "string",
        "bitmap", "hyperloglog", "geo", "stream", "pubsub", "admin", "fast", "slow",
        "blocking", "dangerous", "connection", "transaction", "scripting"
    };

    private static readonly Dictionary<string, string[]> CategoryMap = BuildCategoryMap();

    private static Dictionary<string, string[]> BuildCategoryMap()
    {
        var map = new Dictionary<string, string[]>(StringComparer.Ordinal);
        void Add(string name, params string[] categories) => map[name] = categories;

        // Connection
        Add("ping", "connection", "fast");
        Add("echo", "connection", "fast");
        Add("auth", "connection", "fast");
        Add("hello", "connection", "fast");
        Add("quit", "connection", "fast");
        Add("select", "connection", "fast");
        Add("reset", "connection", "fast");
        foreach (var sub in new[] { "setname", "getname", "setinfo", "id" })
            Add("client|" + sub, "connection", "admin", "slow");

        // Server / admin
        Add("info", "admin", "dangerous", "slow");
        Add("dbsize", "keyspace", "read", "fast");
        Add("flushdb", "keyspace", "write", "dangerous", "slow");
        Add("flushall", "keyspace", "write", "dangerous", "slow");
        Add("save", "admin", "dangerous", "slow");
        Add("bgsave", "admin", "dangerous", "slow");
        Add("bgrewriteaof", "admin", "dangerous", "slow");
        Add("replicaof", "admin", "dangerous", "slow");
        Add("psync", "admin");
        Add("replconf", "admin");
        foreach (var sub in new[] { "setuser", "getuser", "deluser", "users", "list", "whoami", "cat" })
            Add("acl|" + sub, "admin", "slow");
        foreach (var sub in new[] { "info", "slots", "nodes", "keyslot", "countkeysinslot" })
            Add("cluster|" + sub, "admin", "slow");

        // Strings
        Add("set", "string", "write");
        Add("get", "string", "read", "fast");
        Add("del", "keyspace", "write");
        Add("exists", "keyspace", "read", "fast");
        Add("incr", "string", "write", "fast");
        Add("decr", "string", "write", "fast");
        Add("incrby", "string", "write", "fast");
        Add("decrby", "string", "write", "fast");
        Add("append", "string", "write");
        Add("strlen", "string", "read", "fast");

        // Keys
        Add("expire", "keyspace", "write", "fast");
        Add("pexpire", "keyspace", "write", "fast");
        Add("expireat", "keyspace", "write", "fast");
        Add("pexpireat", "keyspace", "write", "fast");
        Add("ttl", "keyspace", "read", "fast");
        Add("pttl", "keyspace", "read", "fast");
        Add("persist", "keyspace", "write", "fast");
        Add("type", "keyspace", "read", "fast");
        Add("keys", "keyspace", "read", "dangerous", "slow");
        Add("scan", "keyspace", "read", "slow");
        Add("move", "keyspace", "write", "fast");
        Add("swapdb", "keyspace", "write", "dangerous", "fast");

        // Lists
        Add("lpush", "list", "write", "fast");
        Add("rpush", "list", "write", "fast");
        Add("lpop", "list", "write", "fast");
        Add("rpop", "list", "write", "fast");
        Add("llen", "list", "read", "fast");
        Add("lrange", "list", "read");
        Add("lindex", "list", "read");

        // Sets
        Add("sadd", "set", "write", "fast");
        Add("srem", "set", "write", "fast");
        Add("smembers", "set", "read");
        Add("sismember", "set", "read", "fast");
        Add("scard", "set", "read", "fast");

        // Hashes
        Add("hset", "hash", "write", "fast");
        Add("hget", "hash", "read", "fast");
        Add("hdel", "hash", "write", "fast");
        Add("hgetall", "hash", "read", "fast");
        Add("hlen", "hash", "read", "fast");

        // Sorted sets
        Add("zadd", "sortedset", "write", "fast");
        Add("zincrby", "sortedset", "write", "fast");
        Add("zscore", "sortedset", "read", "fast");
        Add("zrange", "sortedset", "read");
        Add("zrem", "sortedset", "write", "fast");
        Add("zcard", "sortedset", "read", "fast");

        // Transactions
        Add("multi", "transaction");
        Add("exec", "transaction");
        Add("discard", "transaction");

        // Pub/sub
        Add("subscribe", "pubsub", "slow");
        Add("unsubscribe", "pubsub", "slow");
        Add("psubscribe", "pubsub", "slow");
        Add("punsubscribe", "pubsub", "slow");
        Add("publish", "pubsub", "fast");

        // Scripting
        Add("eval", "scripting", "slow");
        Add("evalsha", "scripting", "slow");
        foreach (var sub in new[] { "load", "exists", "flush" })
            Add("script|" + sub, "scripting", "slow");

        return map;
    }

    /// <summary>Every canonical command name — the universe ACL permission sets range over.</summary>
    public static IReadOnlyCollection<string> AllCommandNames => CategoryMap.Keys;

    public static bool IsCategory(string name) => Categories.Contains(name);

    /// <summary>The canonical names in a category, in catalogue order.</summary>
    public static IReadOnlyList<string> CommandsInCategory(string category)
    {
        var result = new List<string>();
        foreach (var (name, categories) in CategoryMap)
        {
            if (categories.Contains(category))
                result.Add(name);
        }
        result.Sort(StringComparer.Ordinal);
        return result;
    }

    /// <summary>
    /// Resolves an ACL rule target ("get", "client", "client|setname",
    /// "acl") to the canonical names it denotes: a container name denotes
    /// every one of its subcommands. Null when the name is unknown.
    /// </summary>
    public static IReadOnlyList<string>? ResolveRuleName(string name)
    {
        if (CategoryMap.ContainsKey(name))
            return new[] { name };
        var prefixed = CategoryMap.Keys
            .Where(k => k.StartsWith(name + "|", StringComparison.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();
        return prefixed.Count > 0 ? prefixed : null;
    }

    /// <summary>
    /// The canonical name a command invocation is permission-checked
    /// under: subcommand-ful containers fold their subcommand in
    /// ("acl|setuser"), as Redis names them in ACL.
    /// </summary>
    public static string CanonicalName(byte[][] args)
    {
        var baseName = Encoding.ASCII.GetString(args[0]).ToLowerInvariant();
        if (args.Length > 1 && baseName is "acl" or "client" or "script" or "cluster")
            return baseName + "|" + Encoding.ASCII.GetString(args[1]).ToLowerInvariant();
        return baseName;
    }

    /// <summary>
    /// Extracts the keys a command operates on, from its recorded key
    /// specification. Commands not listed have no keys. EVAL/EVALSHA are
    /// dynamic: their key count is an argument.
    /// </summary>
    public static IReadOnlyList<byte[]> ExtractKeys(string canonicalName, byte[][] args)
    {
        var baseName = canonicalName.Split('|')[0];
        switch (baseName)
        {
            case "eval":
            case "evalsha":
            {
                // EVAL script numkeys key... — the keys sit between the
                // count and the ARGV tail.
                if (args.Length < 3 ||
                    !long.TryParse(Encoding.ASCII.GetString(args[2]), out var numKeys) || numKeys < 0)
                    return Array.Empty<byte[]>();
                var keys = new List<byte[]>();
                for (long i = 0; i < numKeys && 3 + i < args.Length; i++)
                    keys.Add(args[3 + (int)i]);
                return keys;
            }
            case "del":
            case "exists":
                return args.Skip(1).ToArray();
            case "flushdb":
            case "flushall":
            case "keys":
            case "scan":
                return Array.Empty<byte[]>();
            default:
                // The single-key majority: the key is the first argument.
                if (SingleKeyCommands.Contains(baseName) && args.Length > 1)
                    return new[] { args[1] };
                return Array.Empty<byte[]>();
        }
    }

    private static readonly HashSet<string> SingleKeyCommands = new(StringComparer.Ordinal)
    {
        "set", "get", "incr", "decr", "incrby", "decrby", "append", "strlen",
        "expire", "pexpire", "expireat", "pexpireat", "ttl", "pttl", "persist", "type",
        "move",
        "lpush", "rpush", "lpop", "rpop", "llen", "lrange", "lindex",
        "sadd", "srem", "smembers", "sismember", "scard",
        "hset", "hget", "hdel", "hgetall", "hlen",
        "zadd", "zincrby", "zscore", "zrange", "zrem", "zcard"
    };
}
