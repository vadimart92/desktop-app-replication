using Microsoft.EntityFrameworkCore;
using Replication.Client;
using Replication.Owner;
using Sample.Domain;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Design 6.2, 6.3: a replica the owner can no longer catch up is replaced by an empty one that fills from the stream, and subscribers hear about it.</summary>
public class EmptyReplicaTests
{
    [Fact]
    public async Task Empty_replica_fills_from_the_stream_after_tombstones_were_purged()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o => o.CatchupBatchRows = 4);
        await lab.Owner.DeleteItemAsync("Скотч");
        await lab.Owner.DeleteItemAsync("Маркери");
        // No clients yet: every tombstone goes, purged_version > 0.
        OwnerStore.PurgeResult purged = await lab.Owner.PurgeAsync();
        Assert.True(purged.PurgedVersion > 0);

        lab.C2.Agent.Options.SnapshotMode = SnapshotMode.EmptyReplica;
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();
        Assert.Empty(Inspect.Diff(lab.Owner, lab.C2));
    }

    [Fact]
    public async Task Empty_replica_over_an_existing_one_reports_every_table_changed()
    {
        await using Lab lab = await Lab.StartAsync();
        await lab.SyncNowAsync(lab.C1);
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();
        lab.C2.Link = false;
        await Lab.WaitAsync(() => lab.C2.Agent.GetStatus().State == AgentState.Offline, TimeSpan.FromSeconds(30), "Клієнт 2 не вийшов з мережі");

        // The journal empties on the owner, and its tombstones are forgotten while Клієнт 2 is away: no batch will ever touch it.
        await using (SampleDbContext db = lab.Owner.Db())
            await db.Log.ExecuteDeleteAsync(TestContext.Current.CancellationToken);
        await lab.SettleAsync();
        lab.Owner.AdvanceClock(TimeSpan.FromDays(31));
        OwnerStore.PurgeResult purged = await lab.Owner.PurgeAsync();
        Assert.True(purged.PurgedVersion > lab.C2.Agent.CursorOf("Log").Cursor);

        lab.C2.Agent.Options.SnapshotMode = SnapshotMode.EmptyReplica;
        var seen = new HashSet<string>();
        lab.C2.Agent.DataChanged += tables =>
        {
            lock (seen)
                seen.UnionWith(tables);
        };
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();

        lock (seen)
            Assert.Superset(lab.C2.Replication.Model.Tables.Select(t => t.Name).ToHashSet(), seen);
        Assert.Empty(Inspect.Diff(lab.Owner, lab.C2));
    }
}
