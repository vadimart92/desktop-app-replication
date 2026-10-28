using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Client;
using Replication.Owner;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// A new replica has zero cursors from Identity (6.2, 6.3). By default the owner answers its Start with SnapshotRequired;
/// with <see cref="AgentOptions.SkipSnapshotOnEmptyReplica"/> (<c>Start.skip_snapshot_on_empty_replica</c>) it streams from zero, and a replica the owner
/// cannot catch up is emptied and refilled from the stream instead of a snapshot file.
/// </summary>
[TestFixture]
public sealed class SkipSnapshotOnEmptyReplicaTests : LabTests
{
    [Test]
    public async Task Client_without_a_replica_takes_the_instance_from_Identity_and_fills_from_the_stream()
    {
        Lab lab = await StartAsync(o => o.CatchupBatchRows = 3);
        lab.C2.Agent.Options.SkipSnapshotOnEmptyReplica = true;

        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(lab.C2.Instance, Is.EqualTo(lab.Owner.Store.InstanceId));
            Assert.That(lab.Diff(lab.C2), Is.Empty);
            Assert.That(Rows.All(lab.C2.DbPath, "Item", lab.C2.Instance), Has.Count.EqualTo(9));
            Assert.That(Rows.Scalar(lab.C2.DbPath, "SELECT COUNT(*) FROM _sync_instances WHERE snapshot_version IS NULL"), Is.EqualTo(1L), "no snapshot was installed");
            Assert.That(ClientLog(lab.C2), Has.None.Contains("→ Snapshot"));
        });
    }

    [Test]
    public async Task Without_the_flag_a_client_without_a_replica_gets_a_snapshot()
    {
        Lab lab = await StartAsync();

        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ClientLog(lab.C2), Has.Some.Contains("→ Snapshot: no replica"));
            Assert.That(lab.Diff(lab.C2), Is.Empty);
        });
    }

    [Test]
    public async Task A_change_made_during_catch_up_arrives_over_the_same_stream()
    {
        Lab lab = await StartAsync(o => o.CatchupBatchRows = 2);
        lab.C2.Agent.Options.SkipSnapshotOnEmptyReplica = true;

        await lab.SyncNowAsync(lab.C2);
        await lab.Owner.SetPriceAsync("Палета", 470);
        await lab.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(lab.C2.ReplicaRow("Item", lab.ItemId("Палета"))["Price"], Is.EqualTo(470L));
            Assert.That(lab.Diff(lab.C2), Is.Empty);
        });
    }

    [Test]
    public async Task Flag_refills_a_replica_behind_purged_version_from_the_stream()
    {
        Lab lab = await StartSyncedAsync();
        await lab.GoOfflineAsync(lab.C2);
        await lab.C2.SetPriceAsync("Палета", 470);
        await using (SampleDbContext db = lab.Owner.Db())
            await db.Log.ExecuteDeleteAsync(TestContext.CurrentContext.CancellationToken);
        await lab.SettleAsync();
        lab.Owner.AdvanceClock(TimeSpan.FromDays(31));
        OwnerStore.PurgeResult purged = await lab.Owner.PurgeAsync();
        Assert.That(purged.PurgedVersion, Is.GreaterThan(lab.C2.Agent.CursorOf("Log").Cursor));
        int snapshots = ClientLog(lab.C2).Count(l => l.StartsWith("→ Snapshot", StringComparison.Ordinal));

        lab.C2.Agent.Options.SkipSnapshotOnEmptyReplica = true;
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ClientLog(lab.C2), Has.Some.Contains("SkipSnapshotOnEmptyReplica, replica cleared"));
            Assert.That(lab.C2.Outbox(), Is.Empty, "the queue went with the old replica");
            Assert.That(Inspect.OwnerItem(lab.Owner, "Палета")?.Price, Is.Not.EqualTo(470));
            Assert.That(ClientLog(lab.C2).Count(l => l.StartsWith("→ Snapshot", StringComparison.Ordinal)), Is.EqualTo(snapshots));
            Assert.That(lab.Diff(lab.C2), Is.Empty);
        });
    }
}
