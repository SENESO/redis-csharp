namespace RedisServer.Core;

/// <summary>Thrown by the store when a command touches a key holding a different type.</summary>
public sealed class WrongTypeException : Exception
{
    public const string MessageText = "WRONGTYPE Operation against a key holding the wrong kind of value";

    public WrongTypeException() : base(MessageText)
    {
    }
}

/// <summary>
/// Thrown when a value-level rule is violated (a non-integer where an
/// integer is required, an overflow, a bad score…). The message is the full
/// RESP error text, ready to be rendered after the leading '-' by the
/// protocol layer.
/// </summary>
public sealed class ValueErrorException : Exception
{
    public ValueErrorException(string message) : base(message)
    {
    }

    public static ValueErrorException NotAnInteger() =>
        new("ERR value is not an integer or out of range");

    public static ValueErrorException NotAFloat() =>
        new("ERR value is not a valid float");

    public static ValueErrorException Overflow() =>
        new("ERR increment or decrement would overflow");
}
