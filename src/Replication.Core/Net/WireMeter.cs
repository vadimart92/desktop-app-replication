using System.Collections.Concurrent;

namespace Replication.Net;

/// <summary>
/// Bytes on the wire per direction, counted on the client's TCP stream: HTTP/2 frames with gzip-compressed gRPC
/// messages, headers and pings included. Up = client → owner, down = owner → client.
/// Also keeps the uncompressed size of gRPC messages by type, to compare with what actually went over the wire.
/// </summary>
public sealed class WireMeter
{
    private long _up;
    private long _down;
    private readonly ConcurrentDictionary<string, (long Count, long Bytes)> _messages = new();
    private readonly object _rateLock = new();
    private (DateTimeOffset At, long Up, long Down) _rateMark = (DateTimeOffset.UtcNow, 0, 0);
    private (double Up, double Down) _rate;

    public long BytesUp => Interlocked.Read(ref _up);
    public long BytesDown => Interlocked.Read(ref _down);

    internal void AddUp(int n) => Interlocked.Add(ref _up, n);

    internal void AddDown(int n) => Interlocked.Add(ref _down, n);

    /// <summary>Uncompressed protobuf size of a message, by direction and type (for example "↓ Batch").</summary>
    public void CountMessage(string key, int bytes) =>
        _messages.AddOrUpdate(key, (1, bytes), (_, v) => (v.Count + 1, v.Bytes + bytes));

    // ToArray is atomic on ConcurrentDictionary.
    public IReadOnlyList<KeyValuePair<string, (long Count, long Bytes)>> Messages => _messages.ToArray();

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
        _messages.Clear();
        lock (_rateLock)
            _rateMark = (DateTimeOffset.UtcNow, 0, 0);
    }

    public static string Format(double bytes) => bytes switch
    {
        < 1024 => $"{bytes:0} Б",
        < 1024 * 1024 => $"{bytes / 1024:0.0} КБ",
        _ => $"{bytes / 1024 / 1024:0.00} МБ",
    };
}
