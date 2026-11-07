namespace RedisServer.Core;

/// <summary>
/// Equality and ordering for binary-safe keys and values. Redis strings are
/// byte sequences, not text, so every dictionary/set in the store keys on
/// this comparer instead of the default reference equality of
/// <see cref="byte"/> arrays. Ordering is unsigned lexicographic — the same
/// order Redis uses for sorted-set tie-breaking.
/// </summary>
public sealed class ByteArrayComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
{
    public static readonly ByteArrayComparer Instance = new();

    private ByteArrayComparer()
    {
    }

    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y))
            return true;
        if (x is null || y is null)
            return false;
        return x.AsSpan().SequenceEqual(y);
    }

    public int GetHashCode(byte[] obj)
    {
        // FNV-1a over the bytes: cheap, deterministic across processes (the
        // framework's HashCode is randomised per process, which would make
        // snapshot iteration order change between runs for no reason).
        unchecked
        {
            var hash = 2166136261u;
            foreach (var b in obj)
            {
                hash ^= b;
                hash *= 16777619u;
            }
            return (int)hash;
        }
    }

    public int Compare(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y))
            return 0;
        if (x is null)
            return -1;
        if (y is null)
            return 1;
        return x.AsSpan().SequenceCompareTo(y);
    }
}
