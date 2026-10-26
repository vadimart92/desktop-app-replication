using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Client to owner (5.4, 6.5, 8): the app writes the replica through its own DbContext with the WriteRouter interceptor;
/// the edit and its outbox row commit together, the agent sends the outbox with Apply, and the owner's version comes
/// back in the reply instead of as an echo on the stream.
/// </summary>
[TestFixture]
public sealed class ClientToOwnerTests : LabTests
{
    [Test]
    public async Task Edit_of_a_replica_row_is_queued_at_once_and_applied_on_the_owner()
    {
        Lab lab = await StartSyncedAsync();
        Guid stapler = lab.ItemId("Степлер");
        await lab.GoOfflineAsync(lab.C1);

        await lab.C1.SetPriceAsync("Степлер", 150);

        // The replica has the value at once; the outbox says what changed, never the values (5.4).
        Assert.That(lab.C1.ReplicaRow("Item", stapler)["Price"], Is.EqualTo(150L));
        OutboxEntry entry = lab.C1.Outbox().Single();
        Assert.That((entry.Table, entry.Pk, entry.Kind, entry.Sent), Is.EqualTo(("Item", Wire.PkText(stapler), OutboxKind.Patch, OutboxSendState.Waiting)));
        Assert.That(entry.Columns, Is.EquivalentTo(new[] { "Price", "ModifiedOn" }));
        Assert.That(lab.OwnerRow("Item", stapler)["Price"], Is.EqualTo(120L));

        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        RowData owner = lab.OwnerRow("Item", stapler);
        Assert.That(owner["Price"], Is.EqualTo(150L));
        Assert.That(owner.Origin, Is.EqualTo(lab.C1.Replication.ClientId), "the owner remembers whose change this version is (5.2)");
        Assert.That(lab.C1.ReplicaRow("Item", stapler).Version, Is.EqualTo(owner.Version), "the version came back in ApplyReply");
        Assert.That(lab.C1.Outbox(), Is.Empty);
        Assert.That(Rows.Scalar(lab.Owner.DbPath, $"SELECT applied_seq FROM _sync_clients WHERE client_id = '{lab.C1.Replication.ClientId}'"), Is.EqualTo(1L));
        // No echo: the stream skipped the row for Клієнт 1, Клієнт 2 got it.
        Assert.That(OwnerLogAbout(lab.C1), Has.Some.Contains($"skipped as own changes: Item v{owner.Version}"));
        Assert.That(lab.C2.ReplicaRow("Item", stapler)["Price"], Is.EqualTo(150L));
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    [Test]
    public async Task Edits_made_offline_collapse_into_one_action_that_carries_the_last_values()
    {
        Lab lab = await StartSyncedAsync();
        Guid stapler = lab.ItemId("Степлер");
        await lab.GoOfflineAsync(lab.C1);
        long head = lab.Head();

        foreach (long price in new long[] { 121, 130, 140, 150 })
            await lab.C1.SetPriceAsync("Степлер", price);
        await lab.C1.RenameAsync("Степлер", "Степлер №10");

        OutboxEntry entry = lab.C1.Outbox().Single();
        Assert.That(entry.Columns, Is.EquivalentTo(new[] { "Price", "Name", "ModifiedOn" }));

        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        RowData owner = lab.OwnerRow("Item", stapler);
        Assert.That((owner["Name"], owner["Price"]), Is.EqualTo(("Степлер №10", 150L)));
        Assert.That(lab.Head(), Is.EqualTo(head + 1), "five edits, one write on the owner");
        Assert.That(lab.Diff(lab.C1), Is.Empty);
    }

    [Test]
    public async Task Rows_created_on_the_client_get_their_versions_from_the_owner()
    {
        Lab lab = await StartSyncedAsync();
        await lab.GoOfflineAsync(lab.C1);

        Guid news = await lab.C1.CreateCategoryAsync("Новинки");
        Guid drone = await lab.C1.CreateItemAsync("Дрон", 900, "новий", "Новинки");

        Assert.That(lab.C1.ReplicaRow("Item", drone).Version, Is.Zero, "not on the owner yet");
        Assert.That(lab.C1.Outbox().Select(e => (e.Table, e.Kind)), Is.EqualTo(new[] { ("Category", OutboxKind.Create), ("Item", OutboxKind.Create) }));
        Assert.That(lab.OwnerHas("Item", drone), Is.False);

        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(lab.OwnerRow("Item", drone)["CategoryId"], Is.EqualTo(Wire.PkText(news)));
        Assert.That(lab.OwnerRow("Item", drone).Origin, Is.EqualTo(lab.C1.Replication.ClientId));
        Assert.That(lab.C1.ReplicaRow("Item", drone).Version, Is.EqualTo(lab.OwnerRow("Item", drone).Version));
        Assert.That(lab.C1.ReplicaRow("Category", news).Version, Is.EqualTo(lab.OwnerRow("Category", news).Version));
        Assert.That(lab.C2.ReplicaRow("Item", drone)["Name"], Is.EqualTo("Дрон"));
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    [Test]
    public async Task Delete_on_the_client_deletes_on_the_owner_and_the_tombstone_names_the_client()
    {
        Lab lab = await StartSyncedAsync();
        Guid tape = lab.ItemId("Скотч");
        await lab.GoOfflineAsync(lab.C1);

        await lab.C1.DeleteItemAsync("Скотч");

        Assert.That(lab.C1.ReplicaHas("Item", tape), Is.False);
        Assert.That(lab.C1.Outbox().Single().Kind, Is.EqualTo(OutboxKind.Delete));

        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(lab.OwnerHas("Item", tape), Is.False);
        Assert.That(lab.Tombstone("Item", tape)?.Origin, Is.EqualTo(lab.C1.Replication.ClientId));
        Assert.That(lab.C2.ReplicaHas("Item", tape), Is.False, "Клієнт 2 got the tombstone");
        Assert.That(lab.C1.Outbox(), Is.Empty);
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    [Test]
    public async Task Local_rows_live_in_the_same_tables_and_never_reach_the_outbox()
    {
        Lab lab = await StartSyncedAsync();
        var mine = new Category { Name = "Мої нотатки" };
        await using (SampleDbContext db = lab.C1.Db())
        {
            db.Categories.Add(mine);
            db.Items.Add(new Item { Name = "Чернетка", Category = mine });
            await db.SaveChangesAsync();
        }
        await lab.Owner.BurstAsync();
        await lab.SettleAsync();

        Assert.That(lab.C1.Outbox(), Is.Empty);
        Assert.That(lab.OwnerHas("Category", mine.Id), Is.False);
        Assert.That(Rows.All(lab.C1.DbPath, "Item").Values.Where(r => r.Instance is null).Select(r => r["Name"]), Is.EqualTo(new[] { "Чернетка" }), "the client's own rows keep InstanceId NULL");
        Assert.That(lab.Diff(lab.C1), Is.Empty, "sync leaves rows of other InstanceIds alone");
        // One query over every source, as the UI reads (13).
        string instance = lab.C1.Instance;
        await using SampleDbContext reader = lab.C1.Db();
        int visible = await reader.Items.CountAsync(x => EF.Property<string?>(x, SyncColumns.InstanceId) == null || EF.Property<string>(x, SyncColumns.InstanceId) == instance);
        Assert.That(visible, Is.EqualTo(Rows.All(lab.Owner.DbPath, "Item").Count + 1));
    }

    [Test]
    public async Task Bulk_delete_sends_one_predicate_and_spares_the_rows_the_user_did_not_see()
    {
        Lab lab = await StartSyncedAsync();
        await lab.GoOfflineAsync(lab.C1);
        string[] seen = ["Старий принтер", "Факс", "Каталог 2019"];
        Guid[] seenIds = [.. seen.Select(lab.ItemId)];

        Assert.That(await lab.C1.DeleteWhereStatusAsync("архів"), Is.EqualTo(3));
        // The predicate is limited to SyncVersion <= V, the cursor of the table: keys are not sent (8.6).
        OutboxEntry entry = lab.C1.Outbox().Single();
        Assert.That((entry.Kind, entry.Class, entry.Pk), Is.EqualTo((OutboxKind.PredicateDelete, OutboxClass.Bulk, (string?)null)));
        Assert.That(seenIds.Select(id => lab.C1.ReplicaHas("Item", id)), Has.All.False);
        Guid cabinet = await lab.Owner.NewItemAsync("Архівна шафа", status: "архів", category: "Склад");

        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(seenIds.Select(id => lab.OwnerHas("Item", id)), Has.All.False);
        Assert.That(lab.OwnerHas("Item", cabinet), Is.True, "created after V, the user never saw it");
        Assert.That(lab.C1.ReplicaHas("Item", cabinet), Is.True);
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    [Test]
    public async Task Archive_moves_rows_to_the_clients_archive_and_deletes_them_on_the_owner()
    {
        Lab lab = await StartSyncedAsync();
        Guid fax = lab.ItemId("Факс");

        string result = await lab.C1.ArchiveAsync(("Item", "Факс"));
        await lab.SettleAsync();

        Assert.That(result, Does.StartWith("archive:"));
        Assert.That(lab.OwnerHas("Item", fax), Is.False);
        RowData archived = Rows.One(lab.C1.DbPath, "Item", fax, SyncColumns.ArchiveOf(lab.C1.Instance));
        Assert.That(archived["Name"], Is.EqualTo("Факс"));
        Assert.That(lab.C2.ReplicaHas("Item", fax), Is.False, "other clients see an ordinary delete");

        // Sync never touches the archive: the owner's later changes leave it as it was.
        await lab.Owner.BurstAsync();
        await lab.SettleAsync();
        Assert.That(Rows.One(lab.C1.DbPath, "Item", fax, SyncColumns.ArchiveOf(lab.C1.Instance)).Version, Is.EqualTo(archived.Version));
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }
}
