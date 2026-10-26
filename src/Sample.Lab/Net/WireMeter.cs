namespace Sample.Lab.Net;

/// <summary>
/// Bytes on the wire per direction, counted on the client's TCP stream: HTTP/2 frames with gzip-compressed gRPC
/// messages, headers and pings included. Up = client → owner, down = owner → client.
/// </summary>
public sealed class WireMeter
{
    private long _up;
    private long _down;
    private readonly object _rateLock = new();
    private (DateTimeOffset At, long Up, long Down) _rateMark = (DateTimeOffset.UtcNow, 0, 0);
    private (double Up, double Down) _rate;

    public long BytesUp => Interlocked.Read(ref _up);
    public long BytesDown => Interlocked.Read(ref _down);

    internal void AddUp(int n) => Interlocked.Add(ref _up, n);

    internal void AddDown(int n) => Interlocked.Add(ref _down, n);

    /// <summary>Bytes per second over the last sampling period (call about once a second).</summary>
    public (double Up, double Down) SampleRate()
    {
        lock (_rateLock)
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            double dt = (now - _rateMark.At).TotalSeconds;
            if (dt >= 0.5)
            {
                _rate = ((BytesUp - _rateMark.Up) / dt, (BytesDown - _rateMark.Down) / dt);
                _rateMark = (now, BytesUp, BytesDown);
            }
            return _rate;
        }
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _up, 0);
        Interlocked.Exchange(ref _down, 0);
        lock (_rateLock)
            _rateMark = (DateTimeOffset.UtcNow, 0, 0);
    }

    public static string Format(double bytes) => bytes switch
    {
        < 1024 => $"{bytes:0} B",
        < 1024 * 1024 => $"{bytes / 1024:0.0} KB",
        _ => $"{bytes / 1024 / 1024:0.00} MB",
    };
}
