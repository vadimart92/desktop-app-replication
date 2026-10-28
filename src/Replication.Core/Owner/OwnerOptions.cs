using System.Collections.Concurrent;

namespace Replication.Owner;

public sealed class OwnerOptions
{
    /// <summary>Schema version checked by the first Subscribe message (6.2).</summary>
    public int SchemaVersion { get; set; } = 1;

    /// <summary>Rows per catch-up batch. The design sizes a batch at ~64 KB or more (6.4); the sample lowers it to show ranges.</summary>
    public int CatchupBatchRows { get; set; } = 500;

    /// <summary>Rows per online batch.</summary>
    public int OnlineBatchRows { get; set; } = 500;

    /// <summary>
    /// An online stream that is still more than this many versions behind the head after a step falls back to catch-up:
    /// open tables first, newest first, with <c>remaining</c> reported.
    /// </summary>
    public long OnlineLagVersions { get; set; } = 1200;

    /// <summary>Online batches are sent at most this often, gathering what appeared meanwhile (6.4, "gluing small batches").</summary>
    public TimeSpan OnlineInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>How often <c>PRAGMA data_version</c> is polled for new commits (6.4, "online").</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>A client seen within this window holds back <c>SyncBase</c> (5.2).</summary>
    public TimeSpan ActivityWindow { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Tombstones and silent clients are forgotten after this (10.2). A client offline for longer finds its cursor below
    /// <c>purged_version</c> and gets a snapshot, which drops its unsent outbox. Only the time offline counts, not the number of changes.
    /// </summary>
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(30);

    public TimeSpan PurgeInterval { get; set; } = TimeSpan.FromHours(1);

    public int SnapshotChunkBytes { get; set; } = 1024 * 1024;

    public TimeSpan SnapshotKeep { get; set; } = TimeSpan.FromHours(3);

    /// <summary>Where VACUUM INTO writes snapshot files. Defaults to a folder next to the database.</summary>
    public string? SnapshotDirectory { get; set; }

    /// <summary>Fault injection for demos and tests. Not used in production paths unless set.</summary>
    public OwnerFaults Faults { get; } = new OwnerFaults();
}

/// <summary>Knobs the sample scenarios use to reproduce timing cases from the demo page.</summary>
public sealed class OwnerFaults
{
    private readonly ConcurrentDictionary<string, (TimeSpan Delay, DateTimeOffset Until)> _streamDelay = new();

    /// <summary>Holds every Subscribe message to <paramref name="clientId"/> for <paramref name="delay"/> while the window is open.</summary>
    public void DelayStream(string clientId, TimeSpan delay, TimeSpan window) =>
        _streamDelay[clientId] = (delay, DateTimeOffset.UtcNow + window);

    internal TimeSpan StreamDelay(string clientId) =>
        _streamDelay.TryGetValue(clientId, out (TimeSpan Delay, DateTimeOffset Until) d) && d.Until > DateTimeOffset.UtcNow ? d.Delay : TimeSpan.Zero;
}
