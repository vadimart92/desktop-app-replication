using System.Threading.Channels;

namespace Replication.Net;

/// <summary>
/// Emulated link for a client: one-way latency and bandwidth per direction. Zero means "as is".
/// The design's target network is 2-3 s RTT, 70 kbit/s up, 2 Mbit/s down (16).
/// </summary>
public sealed class NetworkProfile
{
    public TimeSpan Latency { get; set; }
    public long UpBitsPerSecond { get; set; }
    public long DownBitsPerSecond { get; set; }

    public static NetworkProfile Local() => new NetworkProfile();

    /// <summary>1.5 s each way, 70 kbit/s up, 2 Mbit/s down.</summary>
    public static NetworkProfile Target() => new NetworkProfile { Latency = TimeSpan.FromMilliseconds(1500), UpBitsPerSecond = 70_000, DownBitsPerSecond = 2_000_000 };

    public void CopyFrom(NetworkProfile p)
    {
        Latency = p.Latency;
        UpBitsPerSecond = p.UpBitsPerSecond;
        DownBitsPerSecond = p.DownBitsPerSecond;
    }

    public override string ToString() =>
        Latency == TimeSpan.Zero && UpBitsPerSecond == 0 && DownBitsPerSecond == 0
            ? "без обмежень"
            : $"затримка {Latency.TotalMilliseconds:0} мс в кожен бік, ↑ {Kbit(UpBitsPerSecond)}, ↓ {Kbit(DownBitsPerSecond)}";

    private static string Kbit(long bps) => bps == 0 ? "∞" : bps >= 1_000_000 ? $"{bps / 1_000_000.0:0.#} Мбіт/с" : $"{bps / 1000} кбіт/с";
}

/// <summary>
/// Wraps the client's TCP stream: counts bytes per direction and, when the profile says so, delays and paces them.
/// Each chunk gets a delivery time: it waits for the link to be free (bandwidth), then travels for the latency.
/// </summary>
internal sealed class ShapedStream : Stream
{
    private readonly Stream _inner;
    private readonly WireMeter _meter;
    private readonly NetworkProfile _profile;
    private readonly Channel<Chunk> _up = Channel.CreateUnbounded<Chunk>(new UnboundedChannelOptions { SingleReader = true });
    private readonly Channel<Chunk> _down = Channel.CreateUnbounded<Chunk>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private readonly CancellationTokenSource _cts = new();
    private readonly object _upLock = new();
    private DateTimeOffset _upFreeAt = DateTimeOffset.MinValue;
    private DateTimeOffset _downFreeAt = DateTimeOffset.MinValue;
    private Chunk? _current;
    private int _currentOffset;

    private sealed record Chunk(byte[] Data, DateTimeOffset DeliverAt);

    public ShapedStream(Stream inner, WireMeter meter, NetworkProfile profile)
    {
        _inner = inner;
        _meter = meter;
        _profile = profile;
        _ = Task.Run(UpPumpAsync);
        _ = Task.Run(DownPumpAsync);
    }

    private static DateTimeOffset Schedule(ref DateTimeOffset freeAt, int length, long bps, TimeSpan latency)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DateTimeOffset start = freeAt > now ? freeAt : now;
        TimeSpan transmit = bps > 0 ? TimeSpan.FromSeconds(length * 8.0 / bps) : TimeSpan.Zero;
        freeAt = start + transmit;
        return freeAt + latency;
    }

    private static async Task WaitUntil(DateTimeOffset at, CancellationToken ct)
    {
        TimeSpan d = at - DateTimeOffset.UtcNow;
        if (d > TimeSpan.FromMilliseconds(1))
            await Task.Delay(d, ct);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        if (buffer.Length == 0)
            return;
        byte[] copy = buffer.ToArray();
        DateTimeOffset at;
        lock (_upLock)
            at = Schedule(ref _upFreeAt, copy.Length, _profile.UpBitsPerSecond, _profile.Latency);
        await _up.Writer.WriteAsync(new Chunk(copy, at), ct);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
        WriteAsync(buffer.AsMemory(offset, count), ct).AsTask();

    public override void Write(byte[] buffer, int offset, int count) => WriteAsync(buffer, offset, count).GetAwaiter().GetResult();

    private async Task UpPumpAsync()
    {
        try
        {
            await foreach (Chunk c in _up.Reader.ReadAllAsync(_cts.Token))
            {
                await WaitUntil(c.DeliverAt, _cts.Token);
                await _inner.WriteAsync(c.Data, _cts.Token);
                await _inner.FlushAsync(_cts.Token);
                _meter.AddUp(c.Data.Length);
            }
        }
        catch (Exception) when (_cts.IsCancellationRequested) { }
        catch (Exception)
        {
            _cts.Cancel();
        }
    }

    private async Task DownPumpAsync()
    {
        byte[] buf = new byte[16 * 1024];
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                int n = await _inner.ReadAsync(buf, _cts.Token);
                if (n == 0)
                    break;
                DateTimeOffset at = Schedule(ref _downFreeAt, n, _profile.DownBitsPerSecond, _profile.Latency);
                await _down.Writer.WriteAsync(new Chunk(buf[..n], at), _cts.Token);
            }
            _down.Writer.TryComplete();
        }
        catch (Exception e)
        {
            _down.Writer.TryComplete(_cts.IsCancellationRequested ? null : e);
        }
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
    {
        if (_current is null)
        {
            if (!await _down.Reader.WaitToReadAsync(ct))
                return 0;
            if (!_down.Reader.TryRead(out _current))
                return 0;
            _currentOffset = 0;
            await WaitUntil(_current.DeliverAt, ct);
            _meter.AddDown(_current.Data.Length);
        }
        int n = Math.Min(buffer.Length, _current.Data.Length - _currentOffset);
        _current.Data.AsMemory(_currentOffset, n).CopyTo(buffer);
        _currentOffset += n;
        if (_currentOffset >= _current.Data.Length)
            _current = null;
        return n;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
        ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

    public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer, offset, count).GetAwaiter().GetResult();

    public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;

    public override void Flush() { }

    public override bool CanRead => true;
    public override bool CanWrite => true;
    public override bool CanSeek => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _cts.Cancel();
            _up.Writer.TryComplete();
            _inner.Dispose();
        }
        base.Dispose(disposing);
    }
}
