using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// A complex type (<c>[ComplexType] ObjectSettings</c>) is stored in columns of its owner's table, <c>Settings_Description</c>
/// and <c>Settings_StockLevel</c>. They are data columns like any other: in the mask, the triggers and the wire.
/// </summary>
[TestFixture]
public sealed class ComplexTypeTests : LabTests
{
    [Test]
    public async Task Sync_model_has_a_column_for_each_property_of_the_complex_type()
    {
        Lab lab = await StartAsync();

        Assert.That(lab.Owner.Store.Model["StockObject"].Columns, Is.SupersetOf(new[] { "Settings_Description", "Settings_StockLevel" }));
    }

    [Test]
    public async Task Insert_on_the_owner_is_inserted_into_the_client_with_the_complex_type()
    {
        Lab lab = await StartSyncedAsync();

        Guid id = await lab.Owner.NewStockObjectAsync("Палета", "Верхній ярус", 7);
        await lab.SettleAsync();

        RowData row = lab.C1.ReplicaRow("StockObject", id);
        Assert.That(row["Settings_Description"], Is.EqualTo("Верхній ярус"));
        Assert.That(row["Settings_StockLevel"], Is.EqualTo(7L));
        Assert.That(row.Version, Is.EqualTo(lab.OwnerRow("StockObject", id).Version));
        Assert.That(lab.Diff(lab.C1), Is.Empty);
    }

    [Test]
    public async Task New_replica_gets_the_complex_type_in_its_snapshot()
    {
        Lab lab = await StartAsync();
        Guid id = await lab.Owner.NewStockObjectAsync("Стелаж", null, 3);

        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(ClientLog(lab.C1), Has.Some.Contains("→ Snapshot"), "the row came in a snapshot, not in the stream");
        RowData row = lab.C1.ReplicaRow("StockObject", id);
        Assert.That(row["Settings_Description"], Is.Null);
        Assert.That(row["Settings_StockLevel"], Is.EqualTo(3L));
        Assert.That(lab.Diff(lab.C1), Is.Empty);
    }

    [Test]
    public async Task Owner_edit_of_one_member_reaches_the_client_as_a_partial_row_with_only_that_column()
    {
        Lab lab = await StartAsync();
        Guid id = await lab.Owner.NewStockObjectAsync("Стелаж", "верхній", 3);
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();
        SyncTable stock = lab.Owner.Store.Model["StockObject"];

        await using (SampleDbContext db = lab.Owner.Db())
        {
            StockObject o = await db.StockObjects.SingleAsync(x => x.Id == id, TestContext.CurrentContext.CancellationToken);
            o.Settings = o.Settings with { StockLevel = 9 };
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }
        await lab.SettleAsync();

        RowData owner = lab.OwnerRow("StockObject", id);
        Assert.That(owner.Mask & stock.MaskOf(["Settings_StockLevel"]), Is.Not.Zero);
        Assert.That(owner.Mask & stock.MaskOf(["Settings_Description", "Name"]), Is.Zero, "the untouched members are not in the mask");
        Assert.That(OwnerLogAbout(lab.C1), Has.Some.Contains("Settings_StockLevel]"));
        Assert.That(OwnerLogAbout(lab.C1), Has.None.Contains("Settings_Description"));
        RowData replica = lab.C1.ReplicaRow("StockObject", id);
        Assert.That(replica["Settings_StockLevel"], Is.EqualTo(9L));
        Assert.That(replica["Settings_Description"], Is.EqualTo("верхній"));
        Assert.That(lab.Diff(lab.C1), Is.Empty);
    }

    [Test]
    public async Task Client_edit_of_one_member_queues_only_that_column_and_reaches_the_owner()
    {
        Lab lab = await StartSyncedAsync();
        Guid id = await lab.Owner.NewStockObjectAsync("Стелаж", "верхній", 3);
        await lab.SettleAsync();
        await lab.GoOfflineAsync(lab.C1);

        await using (SampleDbContext db = lab.C1.Db())
        {
            StockObject o = await db.StockObjects.SingleAsync(x => x.Id == id, TestContext.CurrentContext.CancellationToken);
            o.Settings = o.Settings with { StockLevel = 12 };
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        OutboxEntry queued = lab.C1.Outbox().Single();
        Assert.That(queued.Kind, Is.EqualTo(OutboxKind.Patch));
        Assert.That(queued.Columns, Does.Contain("Settings_StockLevel"));
        Assert.That(queued.Columns, Has.None.EqualTo("Settings_Description"));
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();
        RowData owner = lab.OwnerRow("StockObject", id);
        Assert.That(owner["Settings_StockLevel"], Is.EqualTo(12L));
        Assert.That(owner["Settings_Description"], Is.EqualTo("верхній"), "the other member kept the owner's value");
        Assert.That(owner.Origin, Is.EqualTo(lab.C1.Replication.ClientId));
        Assert.That(lab.Diff(lab.C2), Is.Empty);
    }

    [Test]
    public async Task Client_edit_that_clears_a_member_travels_as_NULL_both_ways()
    {
        Lab lab = await StartSyncedAsync();
        Guid id = await lab.Owner.NewStockObjectAsync("Стелаж", "верхній", 3);
        await lab.SettleAsync();

        await using (SampleDbContext db = lab.C1.Db())
        {
            StockObject o = await db.StockObjects.SingleAsync(x => x.Id == id, TestContext.CurrentContext.CancellationToken);
            o.Settings = o.Settings with { Description = null };
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }
        await lab.SettleAsync();

        Assert.That(lab.OwnerRow("StockObject", id)["Settings_Description"], Is.Null);
        Assert.That(lab.C2.ReplicaRow("StockObject", id)["Settings_Description"], Is.Null, "the other client heard the NULL");
        Assert.That(lab.C2.ReplicaRow("StockObject", id)["Settings_StockLevel"], Is.EqualTo(3L));

        await using (SampleDbContext db = lab.Owner.Db())
        {
            StockObject o = await db.StockObjects.SingleAsync(x => x.Id == id, TestContext.CurrentContext.CancellationToken);
            o.Settings = o.Settings with { Description = "знову" };
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }
        await lab.SettleAsync();
        Assert.That(lab.C1.ReplicaRow("StockObject", id)["Settings_Description"], Is.EqualTo("знову"));
    }

    [Test]
    public async Task Object_created_on_the_client_arrives_on_the_owner_with_its_complex_type()
    {
        Lab lab = await StartSyncedAsync();
        var created = new StockObject { Name = "Піддон", Settings = new ObjectSettings("біля воріт", 40) };

        await using (SampleDbContext db = lab.C1.Db())
        {
            db.StockObjects.Add(created).SetInstance(lab.C1.Instance);
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }
        await lab.SettleAsync();

        RowData owner = lab.OwnerRow("StockObject", created.Id);
        Assert.That(owner["Settings_Description"], Is.EqualTo("біля воріт"));
        Assert.That(owner["Settings_StockLevel"], Is.EqualTo(40L));
        Assert.That(owner.Origin, Is.EqualTo(lab.C1.Replication.ClientId));
        Assert.That(lab.Diff(lab.C1), Is.Empty);
        Assert.That(lab.Diff(lab.C2), Is.Empty);
    }
}
