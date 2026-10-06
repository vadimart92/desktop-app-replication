using System.Data;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Replication.Client;
using Replication.Model;
using Sample.Domain;
using Xunit;

namespace Replication.Tests;

/// <summary>Design 8.1: the WriteRouter's own transaction ends with the save, whichever way the save ends.</summary>
public class WriteRouterTests
{
    public enum FailingChange
    {
        EditItem,
        DeleteItem,
        DeleteCategory
    }

    [Theory]
    [InlineData(FailingChange.EditItem, false)]
    [InlineData(FailingChange.EditItem, true)]
    [InlineData(FailingChange.DeleteItem, false)]
    [InlineData(FailingChange.DeleteItem, true)]
    [InlineData(FailingChange.DeleteCategory, false)]
    [InlineData(FailingChange.DeleteCategory, true)]
    public async Task Concurrency_failure_releases_the_write_lock(FailingChange change, bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (Guid categoryId, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.Db();
        Category category = await db.Categories.SingleAsync(x => x.Id == categoryId, TestContext.Current.CancellationToken);
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.Current.CancellationToken);
        // The agent removed the rows meanwhile (a tombstone, an Ignored reply): EF's statement finds 0 rows.
        client.Sql("DELETE FROM Category WHERE Id = @id", ("@id", Wire.PkText(categoryId)));
        switch (change)
        {
            case FailingChange.EditItem:
                item.Price = 1;
                break;
            case FailingChange.DeleteItem:
                db.Items.Remove(item);
                break;
            case FailingChange.DeleteCategory:
                db.Categories.Remove(category);
                break;
        }

        if (async)
        {
            await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync(TestContext.Current.CancellationToken));
        }
        else
        {
            Assert.Throws<DbUpdateConcurrencyException>(() => db.SaveChanges());
        }

        AssertReleased(client, db);
        await AssertNextSaveQueuesNothingAsync(client, db);
    }

    [Fact]
    public async Task Cancelled_save_releases_the_write_lock()
    {
        await using ClientDb client = ClientDb.Create();
        (_, Guid itemId) = client.SeedRemote();
        using var cts = new CancellationTokenSource();
        await using SampleDbContext db = client.Db(new CancelOnSaving(cts));
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.Current.CancellationToken);
        item.Price = 1;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.SaveChangesAsync(cts.Token));

        AssertReleased(client, db);
        await AssertNextSaveQueuesNothingAsync(client, db);
    }

    private static void AssertReleased(ClientDb client, SampleDbContext db)
    {
        Assert.Null(db.Database.CurrentTransaction);
        Assert.Equal(ConnectionState.Closed, db.Database.GetDbConnection().State);
        // A second writer, as the agent: while the lock is held this fails with "database is locked" after 2 s.
        using SqliteConnection c = client.Replication.Store.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = "BEGIN IMMEDIATE; COMMIT;";
        cmd.CommandTimeout = 2;
        cmd.ExecuteNonQuery();
    }

    /// <summary>The user drops the failed edit and saves something unrelated: nothing of the failed save reaches the outbox.</summary>
    private static async Task AssertNextSaveQueuesNothingAsync(ClientDb client, SampleDbContext db)
    {
        db.ChangeTracker.Clear();
        db.Log.Add(new LogEntry { Text = "нотатка" }).SetInstance(SyncColumns.LocalInstance);
        await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        Assert.Empty(client.Replication.Store.Entries());
    }

    private sealed class CancelOnSaving(CancellationTokenSource cts) : SaveChangesInterceptor
    {
        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
        {
            cts.Cancel();
            return ValueTask.FromResult(result);
        }
    }
}

/// <summary>A client database with a replica of one instance and no agent: the WriteRouter on its own.</summary>
internal sealed class ClientDb : IAsyncDisposable
{
    public const string Instance = "inst-x";

    private ClientDb(string dbPath, ClientReplication replication)
    {
        DbPath = dbPath;
        Replication = replication;
    }

    public string DbPath { get; }
    public ClientReplication Replication { get; }

    public static ClientDb Create()
    {
        string dir = Path.Combine(Path.GetTempPath(), "replication-tests", Guid.NewGuid().ToString("N")[..12]);
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "client.db");
        SyncModel model;
        using (SampleDbContext db = SampleDbContext.Open(path))
        {
            db.Database.EnsureCreated();
            model = SyncModel.From(db);
        }
        var replication = new ClientReplication(path, model);
        replication.Install();
        return new ClientDb(path, replication);
    }

    public SampleDbContext Db(params IInterceptor[] after) => SampleDbContext.Open(DbPath, [Replication.Router, .. after]);

    /// <summary>A remote category with one item, written past the router as the agent writes the replica.</summary>
    public (Guid Category, Guid Item) SeedRemote()
    {
        using SampleDbContext db = SampleDbContext.Open(DbPath);
        var category = new Category { Name = "Офіс" };
        var item = new Item { Name = "Степлер", Price = 100, CategoryId = category.Id };
        db.Categories.Add(category).SetInstance(Instance);
        db.Items.Add(item).SetInstance(Instance);
        db.SaveChanges();
        return (category.Id, item.Id);
    }

    public int Sql(string sql, params (string Name, object Value)[] args)
    {
        using SqliteConnection c = Replication.Store.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach ((string name, object value) in args)
            cmd.Parameters.AddWithValue(name, value);
        return cmd.ExecuteNonQuery();
    }

    public ValueTask DisposeAsync() => Replication.DisposeAsync();
}
