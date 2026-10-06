using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Owner;
using Sample.Domain;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Enabling sync on a database that already has rows (design 15.2).</summary>
public sealed class InstallTests
{
    private static List<(long Version, long Base, long Mask)> Stamps(OwnerStore store, SyncTable t)
    {
        using SqliteConnection c = store.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = $"SELECT SyncVersion, SyncBase, SyncMask FROM {Wire.Q(t.Name)}";
        using SqliteDataReader r = cmd.ExecuteReader();
        var rows = new List<(long, long, long)>();
        while (r.Read())
            rows.Add((r.GetInt64(0), r.GetInt64(1), r.GetInt64(2)));
        return rows;
    }

    private static long Head(OwnerStore store)
    {
        using SqliteConnection c = store.Open();
        return store.Head(c);
    }

    [Fact]
    public async Task First_install_gives_every_existing_row_its_own_version()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dir = Lab.NewDir();
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "owner.db");
        SyncModel model;
        // Rows written before sync is enabled: no triggers yet, so SyncVersion stays 0.
        await using (SampleDbContext db = SampleDbContext.Open(path))
        {
            await db.Database.EnsureCreatedAsync(ct);
            model = SyncModel.From(db);
            var office = new Category { Name = "Офіс" };
            db.Categories.Add(office);
            for (int i = 0; i < 50; i++)
                db.Items.Add(new Item { Name = $"Товар {i}", Price = i, Category = office });
            for (int i = 0; i < 20; i++)
                db.Log.Add(new LogEntry { Text = $"Запис {i}" });
            await db.SaveChangesAsync(ct);
        }

        var store = new OwnerStore(path, model);
        store.Install();

        // One contiguous run per table, parents first, ending at the head.
        long next = 1;
        foreach (SyncTable t in model.Tables)
        {
            List<(long Version, long Base, long Mask)> rows = Stamps(store, t);
            Assert.Equal(Enumerable.Range(0, rows.Count).Select(i => next + i), rows.Select(x => x.Version).Order());
            Assert.All(rows, x => Assert.Equal((0L, t.FullMask), (x.Base, x.Mask)));
            next += rows.Count;
        }
        long head = Head(store);
        Assert.Equal(1 + 50 + 20, head);
        Assert.Equal(head + 1, next);

        // A second start has nothing to stamp, and the triggers number the next row.
        store.Install();
        Assert.Equal(head, Head(store));
        await using (SampleDbContext db = SampleDbContext.Open(path))
        {
            db.Log.Add(new LogEntry { Text = "Після ввімкнення" });
            await db.SaveChangesAsync(ct);
        }
        Assert.Equal(head + 1, Head(store));
        Assert.Contains(head + 1, Stamps(store, model["Log"]).Select(x => x.Version));
        SqliteConnection.ClearAllPools();
    }
}
