using NUnit.Framework;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Owner to client (6.2-6.4, 7): the first connection takes a snapshot, later changes come over the Subscribe stream,
/// and a client that was offline catches up on reconnect. The replica of an instance is the rows with its <c>InstanceId</c>.
/// </summary>
[TestFixture]
public sealed class OwnerToClientTests : LabTests
{
    [Test]
    public async Task First_connection_takes_a_snapshot_and_the_replica_equals_the_owner()
    {
        Lab lab = await StartAsync();
        long head = lab.Head();

        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(lab.C1.Instance, Is.EqualTo(lab.Owner.Store.InstanceId));
        Assert.That(Rows.All(lab.C1.DbPath, "Category", lab.C1.Instance), Has.Count.EqualTo(3));
        Assert.That(Rows.All(lab.C1.DbPath, "Item", lab.C1.Instance), Has.Count.EqualTo(9));
        Assert.That(Rows.All(lab.C1.DbPath, "Log", lab.C1.Instance), Has.Count.EqualTo(3));
        Assert.That(lab.Diff(lab.C1), Is.Empty, "same ids, SyncVersion and data columns");
        Assert.That(Rows.Scalar(lab.C1.DbPath, "SELECT snapshot_version FROM _sync_instances"), Is.EqualTo(head));
        foreach (string table in new[] { "Category", "Item", "Log" })
            Assert.That(lab.C1.Agent.CursorOf(table).Cursor, Is.EqualTo(head), table);
        // The owner registered the client with the snapshot version, so cleanup keeps the tombstones it will need (6.3).
        await Lab.WaitAsync(() => Rows.Scalar(lab.Owner.DbPath, $"SELECT acked_version FROM _sync_clients WHERE client_id = '{lab.C1.Replication.ClientId}'") is long acked && acked == head,
            TimeSpan.FromSeconds(10), "the client is not registered on the owner");
        Assert.That(Rows.All(lab.C2.DbPath, "Item"), Is.Empty, "Клієнт 2 never connected");
    }

    [Test]
    public async Task Owner_changes_reach_an_online_client_with_the_owners_versions()
    {
        Lab lab = await StartSyncedAsync();
        var changed = new HashSet<string>();
        lab.C1.Agent.DataChanged += tables =>
        {
            lock (changed)
                changed.UnionWith(tables);
        };

        Guid lamp = await lab.Owner.NewItemAsync("Лампа", category: "Склад");
        await lab.Owner.SetPriceAsync("Палета", 470);
        await lab.Owner.DeleteItemAsync("Скотч");
        await lab.SettleAsync();

        Assert.That(lab.C1.ReplicaRow("Item", lamp).Version, Is.EqualTo(lab.OwnerRow("Item", lamp).Version));
        Assert.That(lab.C1.ReplicaRow("Item", lab.ItemId("Палета"))["Price"], Is.EqualTo(470L));
        Assert.That(Rows.All(lab.C1.DbPath, "Item", lab.C1.Instance).Values.Select(r => (string?)r["Name"]), Has.None.EqualTo("Скотч"));
        Assert.That(lab.C1.Agent.MinCursor, Is.EqualTo(lab.Head()));
        lock (changed)
            Assert.That(changed, Does.Contain("Item"), "the app hears which tables to re-read");
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    [Test]
    public async Task Offline_client_catches_up_on_reconnect_from_its_cursors()
    {
        Lab lab = await StartSyncedAsync();
        await lab.GoOfflineAsync(lab.C2);
        long before = lab.C2.Agent.MinCursor;

        await lab.Owner.BurstAsync();
        await lab.Owner.DeleteCategoryAsync("Архів");
        Assert.That(lab.C2.Agent.MinCursor, Is.EqualTo(before), "nothing arrives while offline");
        Assert.That(lab.Diff(lab.C2), Is.Not.Empty);

        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();

        Assert.That(lab.Diff(lab.C2), Is.Empty);
        Assert.That(lab.C2.Agent.MinCursor, Is.EqualTo(lab.Head()));
        Assert.That(lab.C2.Agent.CursorOf("Item").Ranges, Is.Empty, "the received ranges merged into the cursor");
        // A catch-up: the only snapshot is the one of the first connection.
        Assert.That(ClientLog(lab.C2).Count(l => l.StartsWith("→ Snapshot", StringComparison.Ordinal)), Is.EqualTo(1));
    }
}
