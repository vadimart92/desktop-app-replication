using NUnit.Framework;
using Replication.Client;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Conflicts (9): in one column the change that reached the owner last wins, other columns keep the owner's values,
/// and a delete wins over everything. The user learns what was dropped from the client's log.
/// </summary>
[TestFixture]
public sealed class ConflictTests : LabTests
{
    [Test]
    public async Task Same_column_is_won_by_the_last_change_to_reach_the_owner_and_other_columns_keep_the_owners_values()
    {
        Lab lab = await StartSyncedAsync();
        Guid paper = lab.ItemId("Папір A4");
        await lab.GoOfflineAsync(lab.C1);

        await lab.C1.SetPriceAsync("Папір A4", 260);
        await lab.Owner.SetStatusAsync("Папір A4", "архів");
        await lab.Owner.SetPriceAsync("Папір A4", 199);
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        RowData owner = lab.OwnerRow("Item", paper);
        Assert.That((owner["Price"], owner["Status"]), Is.EqualTo((260L, "архів")));
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);

        // The automation's next pass wins in turn: there is no manual editing on the owner to protect (2).
        await lab.Owner.SetPriceAsync("Папір A4", 205);
        await lab.SettleAsync();
        Assert.That(lab.C1.ReplicaRow("Item", paper)["Price"], Is.EqualTo(205L));
    }

    [Test]
    public async Task Delete_on_the_owner_wins_over_an_edit_made_offline()
    {
        Lab lab = await StartSyncedAsync();
        Guid markers = lab.ItemId("Маркери");
        await lab.GoOfflineAsync(lab.C1);

        await lab.C1.SetPriceAsync("Маркери", 95);
        await lab.Owner.DeleteItemAsync("Маркери");
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(lab.OwnerHas("Item", markers), Is.False, "the edit does not bring the row back (9.1)");
        Assert.That(lab.C1.ReplicaHas("Item", markers), Is.False);
        Assert.That(lab.C1.Outbox(), Is.Empty);
        Assert.That(lab.Notes.Of(lab.C1.Label), Has.Some.Contains("«Маркери»"));
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    [Test]
    public async Task Delete_on_the_client_wins_over_a_change_the_owner_made_meanwhile()
    {
        Lab lab = await StartSyncedAsync();
        Guid tape = lab.ItemId("Скотч");
        await lab.GoOfflineAsync(lab.C1);

        await lab.C1.DeleteItemAsync("Скотч");
        await lab.Owner.SetPriceAsync("Скотч", 45);
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(lab.OwnerHas("Item", tape), Is.False);
        Assert.That(lab.C1.ReplicaHas("Item", tape), Is.False, "the newer row from the owner is not inserted back");
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    [Test]
    public async Task Row_created_under_a_parent_the_owner_deleted_is_rejected_and_removed_from_the_replica()
    {
        Lab lab = await StartSyncedAsync();
        await lab.GoOfflineAsync(lab.C1);

        Guid scanner = await lab.C1.CreateItemAsync("Сканер", 500, "новий", "Архів");
        Guid news = await lab.C1.CreateCategoryAsync("Новинки");
        Guid drone = await lab.C1.CreateItemAsync("Дрон", 900, "новий", "Новинки");
        await lab.Owner.DeleteCategoryAsync("Архів");
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(lab.OwnerHas("Item", scanner), Is.False);
        Assert.That(lab.C1.ReplicaHas("Item", scanner), Is.False);
        Assert.That(lab.Notes.Of(lab.C1.Label), Has.Some.Contains("«Сканер»"));
        // The chain with a live parent goes through in the same batch (8.4).
        Assert.That(lab.OwnerHas("Category", news) && lab.OwnerHas("Item", drone), Is.True);
        Assert.That(lab.C1.Outbox(), Is.Empty);
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
    }

    // Sequential on purpose: nothing here races, so no conflict rule applies; the concurrent cases are the tests above.
    [Test]
    public async Task Edits_to_different_columns_of_one_row_by_two_clients_in_turn_all_reach_the_owner_without_conflict()
    {
        Lab lab = await StartSyncedAsync();
        Guid pallet = lab.ItemId("Палета");

        await lab.C1.SetPriceAsync("Палета", 470);
        await lab.SettleAsync();
        await lab.C2.SetStatusAsync("Палета", "новий");
        await lab.SettleAsync();
        await lab.C1.RenameAsync("Палета", "Палета EUR");
        await lab.SettleAsync();

        RowData owner = lab.OwnerRow("Item", pallet);
        Assert.That((owner["Name"], owner["Price"], owner["Status"]), Is.EqualTo(("Палета EUR", 470L, "новий")));
        foreach (ClientNode c in lab.Clients)
        {
            Assert.That(lab.Diff(c), Is.Empty, c.Label);
            Assert.That(c.Outbox(), Is.Empty, c.Label);
        }
    }
}
