using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Replication.Client;
using Replication.Model;
using Sample.Domain;
using Xunit;

namespace Replication.Tests;

/// <summary>
/// Design 8.1: the outbox is written in the save's transaction, and the router's own transaction ends with the save;
/// 11.6: the archive check before a parent delete runs in that transaction.
/// </summary>
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

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Parent_delete_inside_a_caller_transaction_goes_through_the_archive_check(bool archiveBelow, bool async)
    {
        await using ClientDb client = ClientDb.Create();
        Guid office = client.Seed(new Category { Name = "Офіс" });
        Guid stapler = client.Seed(new Item { Name = "Степлер", CategoryId = office });
        Guid catalog = client.Seed(new Item { Name = "Каталог 2019", CategoryId = office }, archiveBelow ? ClientDb.Archive : ClientDb.Instance);
        await using SampleDbContext db = client.Db();
        Category category = await db.Categories.SingleAsync(x => x.Id == office, TestContext.Current.CancellationToken);

        if (async)
        {
            await using IDbContextTransaction tx = await db.Database.BeginTransactionAsync(TestContext.Current.CancellationToken);
            db.Categories.Remove(category);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
            await tx.CommitAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            using IDbContextTransaction tx = db.Database.BeginTransaction();
            db.Categories.Remove(category);
            db.SaveChanges();
            tx.Commit();
        }

        // Only the parent is queued; the owner cascades by its own schema (8.7).
        OutboxEntry entry = Assert.Single(client.Replication.Store.Entries());
        Assert.Equal(("Category", Wire.PkText(office), OutboxKind.Delete), (entry.Table, entry.Pk, entry.Kind));
        Assert.Null(client.InstanceOf("Item", stapler));
        Assert.Equal(archiveBelow ? ClientDb.Archive : null, client.InstanceOf("Category", office));
        Assert.Equal(archiveBelow ? ClientDb.Archive : null, client.InstanceOf("Item", catalog));
    }

    [Fact]
    public async Task Archive_move_committed_before_the_parent_delete_gets_the_lock_survives()
    {
        await using ClientDb client = ClientDb.Create();
        Guid office = client.Seed(new Category { Name = "Офіс" });
        Guid stapler = client.Seed(new Item { Name = "Степлер", CategoryId = office });
        Guid catalog = client.Seed(new Item { Name = "Каталог 2019", CategoryId = office });
        var starting = new TransactionStartingSignal();
        await using SampleDbContext db = client.Db(starting);
        db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == office, TestContext.Current.CancellationToken));
        // The agent moves the item to the archive (11.2, step 4) and holds the write lock while the user saves.
        using SqliteConnection agent = client.Replication.Store.Open(foreignKeys: false);
        using SqliteTransaction archiving = agent.BeginTransaction();
        using (SqliteCommand cmd = agent.CreateCommand())
        {
            cmd.Transaction = archiving;
            cmd.CommandText = "UPDATE Item SET InstanceId = @a WHERE Id = @id";
            cmd.Parameters.AddWithValue("@a", ClientDb.Archive);
            cmd.Parameters.AddWithValue("@id", Wire.PkText(catalog));
            cmd.ExecuteNonQuery();
        }

        Task<int> save = Task.Run(() => db.SaveChanges(), TestContext.Current.CancellationToken);
        await starting.Started.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        archiving.Commit();
        await save;

        Assert.Equal(ClientDb.Archive, client.InstanceOf("Item", catalog));
        Assert.Equal(ClientDb.Archive, client.InstanceOf("Category", office));
        Assert.Null(client.InstanceOf("Item", stapler));
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

    /// <summary>Completes when the router asks for its write transaction; the BEGIN itself then waits for the lock.</summary>
    private sealed class TransactionStartingSignal : DbTransactionInterceptor
    {
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Started => _started.Task;

        public override InterceptionResult<DbTransaction> TransactionStarting(DbConnection connection, TransactionStartingEventData eventData, InterceptionResult<DbTransaction> result)
        {
            _started.TrySetResult();
            return result;
        }
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
    public const string Archive = Instance + ":archive";

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

    /// <summary>A remote category with one item.</summary>
    public (Guid Category, Guid Item) SeedRemote()
    {
        Guid category = Seed(new Category { Name = "Офіс" });
        return (category, Seed(new Item { Name = "Степлер", Price = 100, CategoryId = category }));
    }

    /// <summary>Writes a row past the router, as the agent writes the replica.</summary>
    public Guid Seed(BaseEntity row, string instance = Instance)
    {
        using SampleDbContext db = SampleDbContext.Open(DbPath);
        db.Add(row).SetInstance(instance);
        db.SaveChanges();
        return row.Id;
    }

    /// <summary>The row's InstanceId, or null when the row is gone.</summary>
    public string? InstanceOf(string table, Guid id)
    {
        using SqliteConnection c = Replication.Store.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = $"SELECT InstanceId FROM {Wire.Q(table)} WHERE Id = @id";
        cmd.Parameters.AddWithValue("@id", Wire.PkText(id));
        return cmd.ExecuteScalar() as string;
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
