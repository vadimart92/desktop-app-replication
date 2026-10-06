using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Owner;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>The per-table floor that holds SyncBase back: MIN(cursor) of the clients seen within the activity window (design 5.2).</summary>
public sealed class FloorTests
{
    /// <summary>Floors and client cursors read in one transaction, so an Ack cannot land between them.</summary>
    private static (Dictionary<string, long> Floors, List<(string Tbl, long Cursor)> Cursors) Read(Lab lab)
    {
        using SqliteConnection c = lab.Owner.Store.Open();
        using SqliteTransaction tx = c.BeginTransaction(deferred: true);
        List<(string, long)> Rows(string sql)
        {
            using SqliteCommand cmd = c.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tx;
            using SqliteDataReader r = cmd.ExecuteReader();
            var rows = new List<(string, long)>();
            while (r.Read())
                rows.Add((r.GetString(0), r.GetInt64(1)));
            return rows;
        }

        Dictionary<string, long> floors = Rows("SELECT tbl, floor FROM _sync_floor").ToDictionary(x => x.Item1, x => x.Item2);
        List<(string Tbl, long Cursor)> cursors = Rows("SELECT tbl, cursor FROM _sync_client_cursors");
        tx.Commit();
        return (floors, cursors);
    }

    [Fact]
    public async Task Floor_is_the_lowest_cursor_of_the_active_clients_for_each_model_table()
    {
        await using Lab lab = await Lab.StartAsync();
        OwnerStore store = lab.Owner.Store;
        await lab.SyncNowAsync(lab.C1);
        lab.C1.Link = false;
        // Клієнт 2 ends up ahead, so the floor is Клієнт 1's cursor.
        await lab.Owner.NewItemAsync();
        await lab.SyncNowAsync(lab.C2);
        lab.C2.Link = false;
        await Lab.WaitAsync(() => lab.Clients.All(n => !store.IsSubscribed(n.Replication.ClientId)), TimeSpan.FromSeconds(10), "сесії клієнтів не закрились");
        // A table that is no longer replicated keeps its row as it was.
        OwnerSql.Exec(lab, "INSERT INTO _sync_floor(tbl, floor) VALUES ('Retired', 5)");

        store.RecomputeFloor();

        (Dictionary<string, long> floors, List<(string Tbl, long Cursor)> cursors) = Read(lab);
        Assert.NotEmpty(cursors);
        foreach (SyncTable t in store.Model.Tables)
            Assert.Equal(cursors.Where(x => x.Tbl == t.Name).Select(x => x.Cursor).DefaultIfEmpty(OwnerStore.NoFloor).Min(), floors[t.Name]);
        Assert.Equal(5, floors["Retired"]);

        // Both clients leave the activity window: nothing holds the base back any more.
        lab.Owner.AdvanceClock(store.Options.ActivityWindow + TimeSpan.FromHours(1));

        floors = Read(lab).Floors;
        foreach (SyncTable t in store.Model.Tables)
            Assert.Equal(OwnerStore.NoFloor, floors[t.Name]);
        Assert.Equal(5, floors["Retired"]);
    }
}
