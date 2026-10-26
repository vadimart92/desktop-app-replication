using NUnit.Framework;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Design 5.2, "base and mask": the base moves only to a version every active client has, so a client near the head
/// gets just the changed columns, and the mask accumulates while an active client lags.
/// </summary>
[TestFixture]
public sealed class ColumnMaskTests : LabTests
{
    [Test]
    public async Task Mask_accumulates_while_an_active_client_lags_and_restarts_once_it_caught_up()
    {
        Lab lab = await StartSyncedAsync();
        Guid stapler = lab.ItemId("Степлер");
        long synced = lab.OwnerRow("Item", stapler).Version;
        await lab.GoOfflineAsync(lab.C2);

        // Клієнт 2 is offline but within the activity window, so its cursor is the floor. Both clients have the
        // inserted row, so the first change moves the base to it; Клієнт 2 lacks the first change, so the second one
        // keeps that base and adds its columns to the mask.
        await lab.Owner.SetPriceAsync("Степлер", 150);
        await lab.Owner.SetStatusAsync("Степлер", "архів");
        RowData row = lab.OwnerRow("Item", stapler);
        Assert.That(row.Base, Is.EqualTo(synced));
        Assert.That(row.Mask, Is.EqualTo(lab.MaskOf("Item", "Price", "Status", "ModifiedOn")));

        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();
        Assert.That(lab.Diff(lab.C2), Is.Empty);
        long caughtUp = lab.OwnerRow("Item", stapler).Version;
        // The floor is recomputed on Ack; wait until the owner saw Клієнт 2's cursor pass the row.
        await Lab.WaitAsync(() => (long)Rows.Scalar(lab.Owner.DbPath, "SELECT floor FROM _sync_floor WHERE tbl = 'Item'")! >= caughtUp,
            TimeSpan.FromSeconds(30), "the floor did not move");

        await lab.Owner.SetPriceAsync("Степлер", 160);
        row = lab.OwnerRow("Item", stapler);
        Assert.That(row.Base, Is.EqualTo(caughtUp));
        Assert.That(row.Mask, Is.EqualTo(lab.MaskOf("Item", "Price", "ModifiedOn")));
    }

    [Test]
    public async Task Client_at_the_head_gets_only_the_changed_columns()
    {
        Lab lab = await StartSyncedAsync();
        await lab.Owner.SetPriceAsync("Степлер", 150);
        await lab.SettleAsync();
        long v = lab.OwnerRow("Item", lab.ItemId("Степлер")).Version;

        Assert.That(OwnerLogAbout(lab.C1), Has.Some.Contains($"Item v{v} [ModifiedOn, Price]"));
        Assert.That(OwnerLogAbout(lab.C1), Has.None.Contains($"v{v} full"));
        Assert.That(lab.C1.ReplicaRow("Item", lab.ItemId("Степлер"))["Price"], Is.EqualTo(150L));
        foreach (ClientNode c in lab.Clients)
            Assert.That(lab.Diff(c), Is.Empty);
    }
}
