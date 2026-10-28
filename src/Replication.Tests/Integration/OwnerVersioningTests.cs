using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Model;
using Replication.Owner;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Design 5.2: on the owner the triggers version every write path of the application (SaveChanges, ExecuteUpdate,
/// ExecuteDelete, raw SQL, FK cascades), keep the changed-column mask and write tombstones. No sync code runs here.
/// </summary>
[TestFixture]
public sealed class OwnerVersioningTests
{
    private string _path = "";
    private OwnerStore _store = null!;
    private Category _office = null!;
    private Item _stapler = null!;
    private Item _paper = null!;

    private SyncTable ItemTable => _store.Model["Item"];

    [SetUp]
    public async Task SetUp()
    {
        _path = Path.Combine(TestDb.NewDir(), "owner.db");
        SyncModel model;
        await using (SampleDbContext db = Db())
        {
            await db.Database.EnsureCreatedAsync();
            model = SyncModel.From(db);
        }
        _store = new OwnerStore(_path, model);
        _store.Install();
        _office = new Category { Name = "Офіс" };
        _stapler = new Item { Name = "Степлер", Price = 120, Category = _office };
        _paper = new Item { Name = "Папір A4", Price = 240, Category = _office };
        await using (SampleDbContext db = Db())
        {
            db.Items.AddRange(_stapler, _paper);
            await db.SaveChangesAsync();
        }
    }

    [TearDown]
    public void TearDown() => SqliteConnection.ClearAllPools();

    private SampleDbContext Db() => SampleDbContext.Open(_path);

    private long Head() => (long)Rows.Scalar(_path, "SELECT version FROM _sync_meta")!;

    private RowData Item(Item it) => Rows.One(_path, "Item", it.Id);

    [Test]
    public void Insert_gets_the_next_version_a_full_mask_and_no_origin()
    {
        // One counter for the database: the parent saved in the same SaveChanges gets the lower version (5.3).
        Assert.That(Head(), Is.EqualTo(3));
        Assert.That(Rows.One(_path, "Category", _office.Id).Version, Is.EqualTo(1));
        Assert.That(new[] { Item(_stapler).Version, Item(_paper).Version }, Is.EquivalentTo(new[] { 2L, 3L }));
        RowData row = Item(_stapler);
        Assert.That((row.Base, row.Mask, row.Origin), Is.EqualTo((0L, ItemTable.FullMask, (string?)null)));
    }

    [Test]
    public async Task SaveChanges_update_gets_a_new_version_and_masks_the_changed_columns()
    {
        long before = Item(_stapler).Version;
        long paper = Item(_paper).Version;
        await using (SampleDbContext db = Db())
        {
            (await db.Items.SingleAsync(x => x.Id == _stapler.Id)).Price = 150;
            await db.SaveChangesAsync();
        }

        RowData row = Item(_stapler);
        Assert.That(row.Version, Is.EqualTo(Head()));
        Assert.That(row.Version, Is.GreaterThan(before));
        // No active client holds the base back (floor = ∞), so the base moves to the old version
        // and the mask has only what changed after it: Price, and ModifiedOn set by the app's interceptor.
        Assert.That(row.Base, Is.EqualTo(before));
        Assert.That(row.Mask, Is.EqualTo(ItemTable.MaskOf(["Price", "ModifiedOn"])));
        Assert.That(Item(_paper).Version, Is.EqualTo(paper), "other rows keep their versions");
    }

    [Test]
    public async Task ExecuteUpdate_and_raw_SQL_are_versioned_like_SaveChanges()
    {
        long head = Head();
        await using (SampleDbContext db = Db())
            await db.Items.Where(x => x.Id == _stapler.Id).ExecuteUpdateAsync(u => u.SetProperty(x => x.Status, "архів"));
        Assert.That((Item(_stapler).Version, Item(_stapler).Mask), Is.EqualTo((head + 1, ItemTable.MaskOf(["Status"]))));

        using (SqliteConnection c = _store.Open())
            TestDb.Exec(c, "UPDATE Item SET Name = 'Папір A3', Price = 260 WHERE Name = 'Папір A4'");
        Assert.That((Item(_paper).Version, Item(_paper).Mask), Is.EqualTo((head + 2, ItemTable.MaskOf(["Name", "Price"]))));

        // A set-based update gives each row its own version.
        await using (SampleDbContext db = Db())
            Assert.That(await db.Items.ExecuteUpdateAsync(u => u.SetProperty(x => x.Price, x => x.Price + 1)), Is.EqualTo(2));
        Assert.That(new[] { Item(_stapler).Version, Item(_paper).Version }, Is.EquivalentTo(new[] { head + 3, head + 4 }));
    }

    [Test]
    public async Task Delete_leaves_a_tombstone_and_an_FK_cascade_leaves_one_for_every_child()
    {
        await using (SampleDbContext db = Db())
            await db.Items.Where(x => x.Id == _paper.Id).ExecuteDeleteAsync();
        Assert.That(Tombstones(), Is.EqualTo(new[] { ("Item", _paper.Id, Head(), (string?)null) }));

        await using (SampleDbContext db = Db())
        {
            db.Categories.Remove(await db.Categories.SingleAsync());
            await db.SaveChangesAsync();
        }
        // SQLite's cascade deleted the stapler; the delete trigger fired for it too (5.2).
        Assert.That(Rows.All(_path, "Item"), Is.Empty);
        Assert.That(Tombstones().Select(t => (t.Table, t.Pk)), Is.EquivalentTo(new[] { ("Item", _paper.Id), ("Item", _stapler.Id), ("Category", _office.Id) }));
        Assert.That(Tombstones().Max(t => t.Version), Is.EqualTo(Head()));
    }

    private List<(string Table, Guid Pk, long Version, string? Origin)> Tombstones()
    {
        using SqliteConnection c = _store.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = "SELECT tbl, pk, version, origin FROM _sync_tombstones ORDER BY version";
        using SqliteDataReader r = cmd.ExecuteReader();
        var list = new List<(string, Guid, long, string?)>();
        while (r.Read())
            list.Add((r.GetString(0), Guid.Parse(r.GetString(1)), r.GetInt64(2), r.IsDBNull(3) ? null : r.GetString(3)));
        return list;
    }
}
