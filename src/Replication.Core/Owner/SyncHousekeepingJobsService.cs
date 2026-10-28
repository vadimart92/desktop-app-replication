using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Replication.Owner;

/// <summary>Owner background jobs: version file (5.3), floor refresh (5.2) and tombstone cleanup (10.2).</summary>
public sealed class SyncHousekeepingJobsService(OwnerStore store, ILogger<SyncHousekeepingJobsService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        DateTimeOffset lastPurge = DateTimeOffset.UtcNow;
        DateTimeOffset lastFloor = DateTimeOffset.UtcNow;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
                store.WriteVersionFile();
                if (DateTimeOffset.UtcNow - lastFloor > TimeSpan.FromMinutes(1))
                {
                    // A client that left the activity window stops holding the base back.
                    store.RecomputeFloor();
                    lastFloor = DateTimeOffset.UtcNow;
                }
                if (DateTimeOffset.UtcNow - lastPurge > store.Options.PurgeInterval)
                {
                    OwnerStore.PurgeResult r = store.Purge();
                    if (r.Tombstones > 0)
                        log.LogInformation("Purge: deleted {Tombstones} tombstones, purged_version = {PurgedVersion}", r.Tombstones, r.PurgedVersion);
                    store.IncrementalVacuum(256);
                    lastPurge = DateTimeOffset.UtcNow;
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                log.LogError(e, "Background job failed: {Error}", e.Message);
            }
        }
    }

    public override async Task StopAsync(CancellationToken ct)
    {
        await base.StopAsync(ct);
        try
        {
            store.WriteVersionFile();
        }
        catch { }
    }
}
