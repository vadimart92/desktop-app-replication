using NUnit.Framework;
using Replication.Model;
using Replication.Tests.Integration;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Design 5.2: the owner's triggers on the edge cases, with raw SQL past EF. No client is registered, so the floor is open.</summary>
public sealed class TriggerEdgeTests
{
    [Test]
    public async Task Insert_of_a_deleted_id_again_removes_its_tombstone_and_gives_a_new_version()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid tape = lab.ItemId("Скотч");
        string pk = Wire.PkText(tape);
        OwnerSql.Exec(lab, $"CREATE TABLE _bak AS SELECT * FROM Item WHERE Id = '{pk}'");
        OwnerSql.Exec(lab, $"DELETE FROM Item WHERE Id = '{pk}'");
        long deletedAt = lab.Tombstone("Item", tape)!.Value.Version;

        OwnerSql.Exec(lab, "INSERT INTO Item SELECT * FROM _bak");

        Assert.That(lab.Tombstone("Item", tape), Is.Null, "a row that exists again has no tombstone");
        RowData row = lab.OwnerRow("Item", tape);
        Assert.That(row.Version, Is.GreaterThan(deletedAt));
        Assert.That(row.Version, Is.EqualTo(lab.Head()));
        Assert.That(row.Mask, Is.EqualTo(lab.Owner.Store.Model["Item"].FullMask), "an insert is a full row");
        Assert.That(row.Base, Is.Zero);
    }

    [Test]
    public async Task Second_delete_of_a_reinserted_id_replaces_the_tombstone_with_the_newer_version()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid tape = lab.ItemId("Скотч");
        string pk = Wire.PkText(tape);
        OwnerSql.Exec(lab, $"CREATE TABLE _bak AS SELECT * FROM Item WHERE Id = '{pk}'");
        OwnerSql.Exec(lab, $"DELETE FROM Item WHERE Id = '{pk}'");
        long first = lab.Tombstone("Item", tape)!.Value.Version;
        OwnerSql.Exec(lab, "INSERT INTO Item SELECT * FROM _bak");

        OwnerSql.Exec(lab, $"DELETE FROM Item WHERE Id = '{pk}'");

        Assert.That(OwnerSql.Scalar(lab, $"SELECT COUNT(*) FROM _sync_tombstones WHERE tbl = 'Item' AND pk = '{pk}'"), Is.EqualTo(1));
        Assert.That(lab.Tombstone("Item", tape)!.Value.Version, Is.GreaterThan(first));
        Assert.That(lab.Tombstone("Item", tape)!.Value.Version, Is.EqualTo(lab.Head()));
    }

    [Test]
    public async Task Update_that_changes_nothing_adds_no_column_to_the_mask()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid stapler = lab.ItemId("Степлер");
        long price = (long)lab.OwnerRow("Item", stapler)["Price"]!;

        OwnerSql.Exec(lab, $"UPDATE Item SET Price = {price} WHERE Id = '{Wire.PkText(stapler)}'");

        RowData row = lab.OwnerRow("Item", stapler);
        Assert.That(row.Mask, Is.Zero, "the floor is open, so the mask restarts and no column changed");
        Assert.That(row["Price"], Is.EqualTo(price));
    }

    [Test]
    public async Task Update_of_two_columns_sets_exactly_their_bits_and_the_base_to_the_previous_version()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid stapler = lab.ItemId("Степлер");
        long before = lab.OwnerRow("Item", stapler).Version;
        SyncTable item = lab.Owner.Store.Model["Item"];

        OwnerSql.Exec(lab, $"UPDATE Item SET Price = Price + 1, Status = 'архів' WHERE Id = '{Wire.PkText(stapler)}'");

        RowData row = lab.OwnerRow("Item", stapler);
        Assert.That(row.Mask, Is.EqualTo(item.MaskOf(["Price", "Status"])));
        Assert.That(row.Base, Is.EqualTo(before));
        Assert.That(row.Version, Is.EqualTo(lab.Head()));
    }

    [Test]
    public async Task Update_of_one_member_of_a_complex_type_sets_only_that_members_bit()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid id = await lab.Owner.NewStockObjectAsync("Стелаж", "верхній", 3);
        SyncTable stock = lab.Owner.Store.Model["StockObject"];

        OwnerSql.Exec(lab, $"UPDATE StockObject SET Settings_StockLevel = 9 WHERE Id = '{Wire.PkText(id)}'");

        Assert.That(lab.OwnerRow("StockObject", id).Mask, Is.EqualTo(stock.MaskOf(["Settings_StockLevel"])));
        OwnerSql.Exec(lab, $"UPDATE StockObject SET Settings_Description = NULL WHERE Id = '{Wire.PkText(id)}'");
        Assert.That(lab.OwnerRow("StockObject", id).Mask, Is.EqualTo(stock.MaskOf(["Settings_Description"])), "NULL is a change like any other");
    }

    [Test]
    public async Task Update_that_does_not_name_the_origin_resets_it_to_the_owner()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid stapler = lab.ItemId("Степлер");
        string where = $"WHERE Id = '{Wire.PkText(stapler)}'";
        OwnerSql.Exec(lab, $"UPDATE Item SET Price = 1, SyncOrigin = 'cl-x' {where}");

        OwnerSql.Exec(lab, $"UPDATE Item SET Price = 2 {where}");

        Assert.That(lab.OwnerRow("Item", stapler).Origin, Is.Null, "a change by the owner goes to every client");
    }

    [Test]
    public async Task Update_that_moves_the_version_itself_does_not_fire_the_trigger_again()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid stapler = lab.ItemId("Степлер");
        long head = lab.Head();
        long version = lab.OwnerRow("Item", stapler).Version;

        OwnerSql.Exec(lab, $"UPDATE Item SET SyncVersion = SyncVersion + 5 WHERE Id = '{Wire.PkText(stapler)}'");

        Assert.That(lab.Head(), Is.EqualTo(head), "the WHEN clause keeps the trigger from firing on its own update");
        Assert.That(lab.OwnerRow("Item", stapler).Version, Is.EqualTo(version + 5));
    }

    [Test]
    public async Task Cascaded_child_tombstones_carry_no_origin_while_the_deleted_parent_carries_the_clients()
    {
        await using Lab lab = await Lab.StartAsync();
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();
        Guid storage = lab.CategoryId("Склад");
        List<Guid> items = [.. Rows.All(lab.Owner.DbPath, "Item").Where(x => (string?)x.Value["CategoryId"] == Wire.PkText(storage)).Select(x => x.Key)];
        Assert.That(items, Is.Not.Empty);

        await lab.C1.DeleteCategoryAsync("Склад");
        await lab.SettleAsync();

        Assert.That(lab.Tombstone("Category", storage)?.Origin, Is.EqualTo(lab.C1.Replication.ClientId));
        foreach (Guid item in items)
        {
            (long Version, string? Origin)? tombstone = lab.Tombstone("Item", item);
            Assert.That(tombstone, Is.Not.Null);
            Assert.That(tombstone!.Value.Origin, Is.Null, "the cascade is the owner's schema at work, not a delete the client sent");
        }
        Assert.That(lab.Diff(lab.C1), Is.Empty);
    }
}
