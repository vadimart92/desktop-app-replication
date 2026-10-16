using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Replication.Protocol;
using Replication.Tests.Integration.Migrations;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>
/// Catch-up of an open table (6.4): only the open tables go first; the rows they reference by FK come by key, as
/// point ranges (v-1, v] in ordinary batches, so a big parent table does not hold back the first child row and the
/// parents it pulled are not sent again when the parent table's own catch-up comes.
/// </summary>
public sealed class ParentsByKeyTests
{
    private const int Million = 1_000_000;

    [Test]
    public async Task A_million_categories_do_not_hold_back_the_first_item_and_it_comes_with_its_category_once()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o => o.CatchupBatchRows = 10_000);
        OwnerSql.Exec(lab, $"""
            WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {Million})
            INSERT INTO Category(Id, Name, CreatedOn, ModifiedOn)
            SELECT printf('%08X-0000-4000-8000-000000000000', i), 'Категорія ' || i, '2026-01-01 00:00:00', '2026-01-01 00:00:00' FROM n
            """);
        // An item in a category from the middle of the million, and one in a category created after it.
        OwnerSql.Exec(lab, """
            INSERT INTO Category(Id, Name, CreatedOn, ModifiedOn) VALUES ('FFFFFFFF-0000-4000-8000-000000000000', 'Нова', '2026-01-02 00:00:00', '2026-01-02 00:00:00');
            INSERT INTO Item(Id, Name, Price, Status, CategoryId, CreatedOn, ModifiedOn) VALUES
              ('AAAAAAA1-0000-4000-8000-000000000000', 'З середини', 10, 'новий', '0007A120-0000-4000-8000-000000000000', '2026-01-02 00:00:00', '2026-01-02 00:00:00'),
              ('AAAAAAA2-0000-4000-8000-000000000000', 'З нової', 20, 'новий', 'FFFFFFFF-0000-4000-8000-000000000000', '2026-01-02 00:00:00', '2026-01-02 00:00:00');
            """);

        List<SentBatch> batches = await CatchUpAsync(lab.Owner.Address, lab.Owner.Store, "Item");

        int firstItem = batches.FindIndex(b => b.Tbl == "Item");
        Assert.That(firstItem, Is.GreaterThanOrEqualTo(0), "Item не прийшов");
        HashSet<Guid> categoriesFirst = batches.Take(firstItem).Where(b => b.Tbl == "Category").SelectMany(b => b.Pks).ToHashSet();
        Assert.That(batches.Take(firstItem).All(b => b.Tbl == "Category"), Is.True, "до першого Item їхало щось, крім його категорій");
        Assert.That(categoriesFirst, Is.EquivalentTo(Referenced(lab.Owner.Store, "Item", "CategoryId", batches[firstItem].Pks)),
            "перед першим Item мають приїхати рівно категорії його рядків");
        AssertEachRowOnce(batches, lab.Owner.Store);
    }

    [Test]
    public async Task Parents_of_parents_come_first_and_once()
    {
        string dir = TestDb.NewDir();
        string path = Path.Combine(dir, "owner.db");
        await using (ShopDbContext db = ShopDbContext.Open(path, ShopDbContext.Latest))
            db.GetService<IMigrator>().Migrate(ShopDbContext.MigrationOf(ShopDbContext.Latest));
        SyncModel model;
        await using (ShopDbContext db = ShopDbContext.Open(":memory:", ShopDbContext.Latest))
            model = SyncModel.From(db);
        var store = new OwnerStore(path, model, new OwnerOptions { SchemaVersion = ShopDbContext.Latest, CatchupBatchRows = 20 });
        store.Install();
        await using OwnerHost host = await OwnerHost.StartAsync(store);
        using (SqliteConnection c = store.Open())
        {
            // Shelves, products spread over them, then more shelves: the parents of the tags are deep in both tables.
            TestDb.Exec(c, """
                WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 50)
                INSERT INTO Shelf(Id, Name) SELECT printf('%08X-0000-4000-8000-000000000001', i), 'Полиця ' || i FROM n;
                WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200)
                INSERT INTO Product(Id, Name, Price, Discount, ShelfId)
                SELECT printf('%08X-0000-4000-8000-000000000002', i), 'Товар ' || i, i, 0, printf('%08X-0000-4000-8000-000000000001', 1 + i % 50) FROM n;
                WITH RECURSIVE n(i) AS (SELECT 51 UNION ALL SELECT i + 1 FROM n WHERE i < 100)
                INSERT INTO Shelf(Id, Name) SELECT printf('%08X-0000-4000-8000-000000000001', i), 'Полиця ' || i FROM n;
                INSERT INTO Tag(Id, Label, ProductId) VALUES
                  ('00000001-0000-4000-8000-000000000003', 'хіт', '00000007-0000-4000-8000-000000000002'),
                  ('00000002-0000-4000-8000-000000000003', 'новинка', '00000096-0000-4000-8000-000000000002');
                """);
        }

        List<SentBatch> batches = await CatchUpAsync(host.Address, store, "Tag");

        int firstTag = batches.FindIndex(b => b.Tbl == "Tag");
        Assert.That(firstTag, Is.GreaterThanOrEqualTo(0), "Tag не прийшов");
        List<SentBatch> before = batches.Take(firstTag).ToList();
        HashSet<Guid> products = Referenced(store, "Tag", "ProductId", batches[firstTag].Pks);
        HashSet<Guid> shelves = Referenced(store, "Product", "ShelfId", products);
        Assert.That(before.Where(b => b.Tbl == "Product").SelectMany(b => b.Pks), Is.EquivalentTo(products));
        Assert.That(before.Where(b => b.Tbl == "Shelf").SelectMany(b => b.Pks), Is.EquivalentTo(shelves));
        Assert.That(before.FindLastIndex(b => b.Tbl == "Shelf"), Is.LessThan(before.FindIndex(b => b.Tbl == "Product")),
            "полиці мають приїхати раніше за товари, що на них посилаються");
        AssertEachRowOnce(batches, store);
    }

    [Test]
    public async Task A_replica_filled_with_parents_by_key_ends_equal_to_the_owner_with_merged_cursors()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o => o.CatchupBatchRows = 7);
        OwnerSql.Exec(lab, """
            WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300)
            INSERT INTO Category(Id, Name, CreatedOn, ModifiedOn)
            SELECT printf('%08X-0000-4000-8000-000000000000', i), 'Категорія ' || i, '2026-01-01 00:00:00', '2026-01-01 00:00:00' FROM n;
            WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 40)
            INSERT INTO Item(Id, Name, Price, Status, CategoryId, CreatedOn, ModifiedOn)
            SELECT printf('%08X-0000-4000-8000-0000000000AA', i), 'Товар ' || i, i, 'новий', printf('%08X-0000-4000-8000-000000000000', 1 + (i * 37) % 300),
                   '2026-01-02 00:00:00', '2026-01-02 00:00:00' FROM n;
            """);

        lab.C1.Agent.Options.SnapshotMode = SnapshotMode.EmptyReplica;
        lab.C1.Agent.SetOpenTables("Item");
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();

        Assert.That(Inspect.Diff(lab.Owner, lab.C1), Is.Empty);
        foreach (SyncTable t in lab.Owner.Store.Model.Tables)
            Assert.That(lab.C1.Agent.CursorOf(t.Name).Ranges, Is.Empty, $"відрізки {t.Name} не злились");
    }

    [Test]
    public async Task Past_the_ranges_limit_items_go_without_parents_and_every_row_still_comes_once()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o =>
        {
            o.CatchupBatchRows = 5;
            o.ParentRangesLimit = 3;
        });
        OwnerSql.Exec(lab, """
            WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100)
            INSERT INTO Category(Id, Name, CreatedOn, ModifiedOn)
            SELECT printf('%08X-0000-4000-8000-000000000000', i), 'Категорія ' || i, '2026-01-01 00:00:00', '2026-01-01 00:00:00' FROM n;
            WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 30)
            INSERT INTO Item(Id, Name, Price, Status, CategoryId, CreatedOn, ModifiedOn)
            SELECT printf('%08X-0000-4000-8000-0000000000AA', i), 'Товар ' || i, i, 'новий', printf('%08X-0000-4000-8000-000000000000', 1 + (i * 37) % 100),
                   '2026-01-02 00:00:00', '2026-01-02 00:00:00' FROM n;
            """);

        List<SentBatch> batches = await CatchUpAsync(lab.Owner.Address, lab.Owner.Store, "Item");

        // Each pull adds up to a batch of points, so the limit stops it after a pull or two, long before the items end.
        int lastItem = batches.FindLastIndex(b => b.Tbl == "Item");
        int pulled = batches.Take(lastItem).Where(b => b.Tbl == "Category").Sum(b => b.Pks.Count);
        Assert.That(pulled, Is.InRange(1, 3 + 5), "категорії до останнього Item мали прийти лише до ліміту");
        AssertEachRowOnce(batches, lab.Owner.Store);
    }

    private sealed record SentBatch(string Tbl, List<Guid> Pks);

    /// <summary>A raw Subscribe from an empty replica with <paramref name="open"/> open, read until catch-up is done.</summary>
    private static async Task<List<SentBatch>> CatchUpAsync(string address, OwnerStore store, params string[] open)
    {
        CancellationToken ct = TestContext.CurrentContext.CancellationToken;
        using GrpcChannel channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions { MaxReceiveMessageSize = null });
        var client = new Sync.SyncClient(channel);
        using AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> call = client.Subscribe(deadline: DateTime.UtcNow.AddMinutes(5), cancellationToken: ct);
        var start = new Start { ClientId = "parents-by-key", SchemaVersion = store.Options.SchemaVersion, InstanceId = store.InstanceId };
        foreach (SyncTable t in store.Model.Tables)
            start.Cursors.Add(new TableCursor { Tbl = t.Name, Cursor = 0 });
        start.OpenTables.AddRange(open);
        await call.RequestStream.WriteAsync(new SubscribeMessage { Start = start }, ct);

        var batches = new List<SentBatch>();
        while (await call.ResponseStream.MoveNext(ct))
        {
            ChangeMessage m = call.ResponseStream.Current;
            if (m.BodyCase == ChangeMessage.BodyOneofCase.Batch)
                batches.Add(new SentBatch(m.Batch.Tbl, [.. m.Batch.Rows.Select(r => Wire.PkGuid(r.Pk)), .. m.Batch.Tombstones.Select(x => Wire.PkGuid(x.Pk))]));
            if (m.Progress is { Done: true })
                return batches;
        }
        throw new InvalidOperationException("потік закрився до кінця досинхронізації");
    }

    /// <summary>The parents that the rows <paramref name="ids"/> of <paramref name="table"/> reference by <paramref name="fk"/>.</summary>
    private static HashSet<Guid> Referenced(OwnerStore store, string table, string fk, IEnumerable<Guid> ids)
    {
        using SqliteConnection c = store.Open();
        var parents = new HashSet<Guid>();
        foreach (Guid id in ids)
            parents.Add(Guid.Parse((string)TestDb.Scalar(c, $"SELECT {fk} FROM {table} WHERE Id = @id", ("@id", Wire.PkText(id)))!));
        return parents;
    }

    /// <summary>Every row of every table arrived, and none twice.</summary>
    private static void AssertEachRowOnce(List<SentBatch> batches, OwnerStore store)
    {
        using SqliteConnection c = store.Open();
        Assert.Multiple(() =>
        {
            foreach (SyncTable t in store.Model.Tables)
            {
                var seen = new HashSet<Guid>();
                int twice = 0;
                foreach (Guid pk in batches.Where(b => b.Tbl == t.Name).SelectMany(b => b.Pks))
                {
                    if (!seen.Add(pk))
                        twice++;
                }
                Assert.That(twice, Is.Zero, $"{t.Name}: рядки приїхали повторно");
                Assert.That(seen.Count, Is.EqualTo((long)TestDb.Scalar(c, $"SELECT COUNT(*) FROM {Wire.Q(t.Name)}")!), $"{t.Name}: приїхали не всі рядки");
            }
        });
    }
}
