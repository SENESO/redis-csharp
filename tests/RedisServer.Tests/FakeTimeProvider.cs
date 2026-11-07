namespace RedisServer.Tests;

/// <summary>A <see cref="TimeProvider"/> whose clock tests move by hand.</summary>
internal sealed class FakeTimeProvider : TimeProvider
{
    private readonly object _gate = new();
    private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate)
            return _now;
    }

    public void Advance(TimeSpan by)
    {
        lock (_gate)
            _now += by;
    }
}
