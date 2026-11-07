using System.Text;

namespace RedisServer.Core;

/// <summary>
/// Redis-style glob matching, used by <c>KEYS</c>, <c>SCAN MATCH</c> and
/// pattern subscriptions: <c>*</c> matches any run, <c>?</c> one byte,
/// <c>[...]</c> a byte class (with ranges and <c>^</c> negation), and
/// <c>\</c> escapes the next byte. Matching is over raw bytes, like Redis.
/// </summary>
public static class Glob
{
    public static bool IsMatch(ReadOnlySpan<byte> pattern, ReadOnlySpan<byte> value)
    {
        // Iterative matcher with backtracking on '*' (the classic two-pointer
        // glob algorithm; no recursion, no allocation).
        var p = 0;
        var v = 0;
        var starPattern = -1;
        var starValue = -1;

        while (v < value.Length)
        {
            if (p < pattern.Length)
            {
                var pc = pattern[p];
                if (pc == (byte)'*')
                {
                    starPattern = p++;
                    starValue = v;
                    continue;
                }
                if (pc == (byte)'?')
                {
                    p++;
                    v++;
                    continue;
                }
                if (pc == (byte)'[' && TryMatchClass(pattern, p, value[v], out var consumed))
                {
                    if (consumed > 0)
                    {
                        p += consumed;
                        v++;
                        continue;
                    }
                    // Malformed class: fall through and treat '[' literally.
                }
                var literal = pc;
                var literalLength = 1;
                if (pc == (byte)'\\' && p + 1 < pattern.Length)
                {
                    literal = pattern[p + 1];
                    literalLength = 2;
                }
                if (literal == value[v])
                {
                    p += literalLength;
                    v++;
                    continue;
                }
            }

            if (starPattern >= 0)
            {
                p = starPattern + 1;
                v = ++starValue;
                continue;
            }
            return false;
        }

        while (p < pattern.Length && pattern[p] == (byte)'*')
            p++;
        return p == pattern.Length;
    }

    /// <summary>
    /// Matches one byte against a <c>[...]</c> class starting at
    /// <paramref name="start"/>. Returns true when the class is well-formed;
    /// <paramref name="consumed"/> is then the class length in the pattern
    /// when the byte matches, or 0 when it does not.
    /// </summary>
    private static bool TryMatchClass(ReadOnlySpan<byte> pattern, int start, byte value, out int consumed)
    {
        consumed = 0;
        var i = start + 1;
        var negate = false;
        if (i < pattern.Length && pattern[i] == (byte)'^')
        {
            negate = true;
            i++;
        }

        var matched = false;
        var first = true;
        while (i < pattern.Length && (pattern[i] != (byte)']' || first))
        {
            first = false;
            var lo = pattern[i];
            if (lo == (byte)'\\' && i + 1 < pattern.Length)
            {
                lo = pattern[++i];
            }
            if (i + 2 < pattern.Length && pattern[i + 1] == (byte)'-' && pattern[i + 2] != (byte)']')
            {
                var hi = pattern[i + 2];
                if (lo <= value && value <= hi)
                    matched = true;
                i += 3;
            }
            else
            {
                if (lo == value)
                    matched = true;
                i++;
            }
        }

        if (i >= pattern.Length)
            return false; // no closing ']': not a class at all

        if (matched != negate)
            consumed = i - start + 1;
        return true;
    }

    /// <summary>Convenience overload for tests and string patterns.</summary>
    public static bool IsMatch(string pattern, string value) =>
        IsMatch(Encoding.ASCII.GetBytes(pattern), Encoding.ASCII.GetBytes(value));
}
