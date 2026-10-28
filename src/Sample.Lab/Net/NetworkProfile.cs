namespace Sample.Lab.Net;

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
            ? "no limits"
            : $"latency {Latency.TotalMilliseconds:0} ms each way, ↑ {Kbit(UpBitsPerSecond)}, ↓ {Kbit(DownBitsPerSecond)}";

    private static string Kbit(long bps) => bps == 0 ? "∞" : bps >= 1_000_000 ? $"{bps / 1_000_000.0:0.#} Mbit/s" : $"{bps / 1000} kbit/s";
}
