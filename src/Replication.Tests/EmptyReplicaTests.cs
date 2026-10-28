using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Client;
using Replication.Owner;
using Replication.Tests.Integration;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Design 6.2, 6.3: with SkipSnapshotOnEmptyReplica a replica the owner can no longer catch up starts empty and fills from the stream, and subscribers hear about it. The flag-only cases are in SkipSnapshotOnEmptyReplicaTests.</summary>
public sealed class EmptyReplicaTests
{
    [Test]
    public async Task Empty_replica_fills_from_the_stream_after_tombstones_were_purged()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o => o.CatchupBatchRows = 4);
        await lab.Owner.DeleteItemAsync("Скотч");
        await lab.Owner.DeleteItemAsync("Маркери");
        // No clients yet: every tombstone goes, purged_version > 0.
        OwnerStore.PurgeResult purged = await lab.Owner.PurgeAsync();
        Assert.That(purged.PurgedVersion, Is.GreaterThan(0));

        lab.C2.Agent.Options.SkipSnapshotOnEmptyReplica = true;
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();
        Assert.That(Inspect.Diff(lab.Owner, lab.C2), Is.Empty);
    }

    [Test]
    public async Task Empty_replica_over_an_existing_one_reports_every_table_changed()
    {
        await using Lab lab = await Lab.StartAsync();
        await lab.SyncNowAsync(lab.C1);
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();
        await lab.GoOfflineAsync(lab.C2);

        // The journal empties on the owner, and its tombstones are forgotten while Клієнт 2 is away: no batch will ever touch it.
        await using (SampleDbContext db = lab.Owner.Db())
            await db.Log.ExecuteDeleteAsync(TestContext.CurrentContext.CancellationToken);
        await lab.SettleAsync();
        lab.Owner.AdvanceClock(TimeSpan.FromDays(31));
        OwnerStore.PurgeResult purged = await lab.Owner.PurgeAsync();
        Assert.That(purged.PurgedVersion, Is.GreaterThan(lab.C2.Agent.CursorOf("Log").Cursor));

        lab.C2.Agent.Options.SkipSnapshotOnEmptyReplica = true;
        var seen = new HashSet<string>();
        lab.C2.Agent.DataChanged += tables =>
        {
            lock (seen)
                seen.UnionWith(tables);
        };
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();

        lock (seen)
            Assert.That(seen, Is.EquivalentTo(lab.C2.Replication.Model.Tables.Select(t => t.Name).ToHashSet()));
        Assert.That(Inspect.Diff(lab.Owner, lab.C2), Is.Empty);
    }
}
