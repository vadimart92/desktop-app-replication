using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>
/// Design 8.1: the outbox is written in the save's transaction, and the router's own transaction ends with the save;
/// 11.6: the archive check before a parent delete runs in that transaction.
/// </summary>
public sealed class WriteRouterTests
{
    public enum FailingChange
    {
        EditItem,
        DeleteItem,
        DeleteCategory
    }

    [TestCase(FailingChange.EditItem, false)]
    [TestCase(FailingChange.EditItem, true)]
    [TestCase(FailingChange.DeleteItem, false)]
    [TestCase(FailingChange.DeleteItem, true)]
    [TestCase(FailingChange.DeleteCategory, false)]
    [TestCase(FailingChange.DeleteCategory, true)]
    public async Task Concurrency_failure_releases_the_write_lock(FailingChange change, bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (Guid categoryId, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.Db();
        Category category = await db.Categories.SingleAsync(x => x.Id == categoryId, TestContext.CurrentContext.CancellationToken);
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken);
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
            Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken));
        }
        else
        {
            Assert.Throws<DbUpdateConcurrencyException>(() => db.SaveChanges());
        }

        AssertReleased(client, db);
        await AssertNextSaveQueuesNothingAsync(client, db);
    }

    [Test]
    public async Task Cancelled_save_releases_the_write_lock()
    {
        await using ClientDb client = ClientDb.Create();
        (_, Guid itemId) = client.SeedRemote();
        using var cts = new CancellationTokenSource();
        await using SampleDbContext db = client.DbRouterFirst(new CancelOnSaving(cts));
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken);
        item.Price = 1;

        Assert.CatchAsync<OperationCanceledException>(() => db.SaveChangesAsync(cts.Token));

        AssertReleased(client, db);
        await AssertNextSaveQueuesNothingAsync(client, db);
    }

    [Test]
    public async Task Save_cancelled_before_the_router_begins_its_transaction_queues_nothing()
    {
        await using ClientDb client = ClientDb.Create();
        (_, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.Db();
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken);
        item.Price = 1;
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // The router's BEGIN throws inside SavingChanges; EF reports that to no interceptor.
        Assert.CatchAsync<OperationCanceledException>(() => db.SaveChangesAsync(cts.Token));

        AssertReleased(client, db);
        await AssertNextSaveQueuesNothingAsync(client, db);
    }

    [TestCase(FailingChange.EditItem, false)]
    [TestCase(FailingChange.EditItem, true)]
    [TestCase(FailingChange.DeleteCategory, false)]
    [TestCase(FailingChange.DeleteCategory, true)]
    public async Task Save_rejected_by_the_applications_interceptor_holds_no_lock(FailingChange change, bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (Guid categoryId, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.Db(new RejectFirstSave());
        if (change == FailingChange.DeleteCategory)
            db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == categoryId, TestContext.CurrentContext.CancellationToken));
        else
            (await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken)).Price = 1;

        Assert.ThrowsAsync<InvalidOperationException>(() => SaveAsync(db, async));

        // The context stays open, as a form that shows the error, and the agent can still write.
        AssertReleased(client, db);
        await SaveAsync(db, async);
        List<OutboxEntry> entries = client.Replication.Store.Entries();
        Assert.That(entries, Has.Count.EqualTo(1));
        OutboxEntry entry = entries[0];
        Assert.That((entry.Table, entry.Pk, entry.Kind), Is.EqualTo(change == FailingChange.DeleteCategory ? ("Category", Wire.PkText(categoryId), OutboxKind.Delete) : ("Item", Wire.PkText(itemId), OutboxKind.Patch)));
        AssertReleased(client, db);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Save_rejected_by_a_later_interceptor_leaves_nothing_to_the_next_save(bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (_, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.DbRouterFirst(new RejectFirstSave());
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken);
        item.Price = 1;

        // An interceptor wrongly registered after the router: EF reports its failure to no interceptor, and the router's transaction outlives the save.
        Assert.ThrowsAsync<InvalidOperationException>(() => SaveAsync(db, async));

        await AssertNextSaveQueuesNothingAsync(client, db);
        AssertReleased(client, db);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Next_save_succeeds_after_the_app_rolled_back_what_a_rejected_save_left(bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (_, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.DbRouterFirst(new RejectFirstSave());
        (await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken)).Price = 1;
        Assert.ThrowsAsync<InvalidOperationException>(() => SaveAsync(db, async));

        // The app releases the lock the router's transaction still holds.
        db.Database.RollbackTransaction();

        await AssertNextSaveQueuesNothingAsync(client, db);
        AssertReleased(client, db);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Save_retried_after_a_later_interceptor_rejected_it_commits_the_edit(bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (_, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.DbRouterFirst(new RejectFirstSave());
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken);
        item.Price = 1;

        Assert.ThrowsAsync<InvalidOperationException>(() => SaveAsync(db, async));
        await SaveAsync(db, async);

        await using (SampleDbContext other = client.Db())
            Assert.That((await other.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken)).Price, Is.EqualTo(1L));
        List<OutboxEntry> entries = client.Replication.Store.Entries();
        Assert.That(entries, Has.Count.EqualTo(1));
        OutboxEntry entry = entries[0];
        Assert.That((entry.Table, entry.Pk, entry.Kind), Is.EqualTo(("Item", Wire.PkText(itemId), OutboxKind.Patch)));
        AssertReleased(client, db);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Parent_delete_retried_after_a_later_interceptor_rejected_it_moves_the_parent_to_the_archive(bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (Guid office, Guid catalog) = client.SeedArchivedCatalog();
        await using SampleDbContext db = client.DbRouterFirst(new RejectFirstSave());
        db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == office, TestContext.CurrentContext.CancellationToken));

        Assert.ThrowsAsync<InvalidOperationException>(() => SaveAsync(db, async));
        await SaveAsync(db, async);

        AssertParentArchived(client, office, catalog);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Parent_delete_retried_after_a_concurrency_failure_moves_the_parent_to_the_archive(bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (Guid office, Guid catalog) = client.SeedArchivedCatalog();
        (_, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.Db();
        db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == office, TestContext.CurrentContext.CancellationToken));
        Item item = await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken);
        // An edit in the same save finds its row removed by the agent meanwhile.
        client.Sql("DELETE FROM Item WHERE Id = @id", ("@id", Wire.PkText(itemId)));
        item.Price = 1;

        Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => SaveAsync(db, async));
        // The user drops the edit of the vanished row and saves again.
        db.Entry(item).State = EntityState.Detached;
        await SaveAsync(db, async);

        AssertParentArchived(client, office, catalog);
    }

    [Test]
    public async Task Parent_delete_retried_after_a_cancelled_save_moves_the_parent_to_the_archive()
    {
        await using ClientDb client = ClientDb.Create();
        (Guid office, Guid catalog) = client.SeedArchivedCatalog();
        (_, Guid itemId) = client.SeedRemote();
        using var cts = new CancellationTokenSource();
        await using SampleDbContext db = client.DbRouterFirst(new CancelOnSaving(cts));
        db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == office, TestContext.CurrentContext.CancellationToken));
        // EF looks at the token only when it has a statement to run.
        (await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken)).Price = 1;

        Assert.CatchAsync<OperationCanceledException>(() => db.SaveChangesAsync(cts.Token));
        await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);

        AssertParentArchived(client, office, catalog);
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Parent_delete_inside_a_caller_transaction_goes_through_the_archive_check(bool archiveBelow, bool async)
    {
        await using ClientDb client = ClientDb.Create();
        Guid office = client.Seed(new Category { Name = "Офіс" });
        Guid stapler = client.Seed(new Item { Name = "Степлер", CategoryId = office });
        Guid catalog = client.Seed(new Item { Name = "Каталог 2019", CategoryId = office }, archiveBelow ? ClientDb.Archive : ClientDb.Instance);
        await using SampleDbContext db = client.Db();
        Category category = await db.Categories.SingleAsync(x => x.Id == office, TestContext.CurrentContext.CancellationToken);

        if (async)
        {
            await using IDbContextTransaction tx = await db.Database.BeginTransactionAsync(TestContext.CurrentContext.CancellationToken);
            db.Categories.Remove(category);
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
            await tx.CommitAsync(TestContext.CurrentContext.CancellationToken);
        }
        else
        {
            using IDbContextTransaction tx = db.Database.BeginTransaction();
            db.Categories.Remove(category);
            db.SaveChanges();
            tx.Commit();
        }

        // Only the parent is queued; the owner cascades by its own schema (8.7).
        List<OutboxEntry> entries = client.Replication.Store.Entries();
        Assert.That(entries, Has.Count.EqualTo(1));
        OutboxEntry entry = entries[0];
        Assert.That((entry.Table, entry.Pk, entry.Kind), Is.EqualTo(("Category", Wire.PkText(office), OutboxKind.Delete)));
        Assert.That(client.InstanceOf("Item", stapler), Is.Null);
        Assert.That(client.InstanceOf("Category", office), Is.EqualTo(archiveBelow ? ClientDb.Archive : null));
        Assert.That(client.InstanceOf("Item", catalog), Is.EqualTo(archiveBelow ? ClientDb.Archive : null));
    }

    [Test]
    public async Task Archive_move_committed_before_the_parent_delete_gets_the_lock_survives()
    {
        await using ClientDb client = ClientDb.Create();
        Guid office = client.Seed(new Category { Name = "Офіс" });
        Guid stapler = client.Seed(new Item { Name = "Степлер", CategoryId = office });
        Guid catalog = client.Seed(new Item { Name = "Каталог 2019", CategoryId = office });
        var starting = new TransactionStartingSignal();
        await using SampleDbContext db = client.Db(starting);
        db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == office, TestContext.CurrentContext.CancellationToken));
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

        Task<int> save = Task.Run(() => db.SaveChanges(), TestContext.CurrentContext.CancellationToken);
        await starting.Started.WaitAsync(TimeSpan.FromSeconds(30), TestContext.CurrentContext.CancellationToken);
        archiving.Commit();
        await save;

        Assert.That(client.InstanceOf("Item", catalog), Is.EqualTo(ClientDb.Archive));
        Assert.That(client.InstanceOf("Category", office), Is.EqualTo(ClientDb.Archive));
        Assert.That(client.InstanceOf("Item", stapler), Is.Null);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Outbox_write_that_fails_takes_the_edit_with_it_and_a_retry_goes_through(bool async)
    {
        await using ClientDb client = ClientDb.Create();
        (_, Guid itemId) = client.SeedRemote();
        await using SampleDbContext db = client.Db();
        (await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken)).Price = 1;
        client.Sql("CREATE TRIGGER _test_fault BEFORE INSERT ON _sync_outbox BEGIN SELECT RAISE(ABORT, 'injected'); END");

        Exception? thrown = Assert.CatchAsync(() => SaveAsync(db, async));

        Assert.That(thrown?.ToString(), Does.Contain("injected"));
        Assert.That(client.PriceOf(itemId), Is.EqualTo(100L), "the replica write and the outbox write are one transaction");
        Assert.That(client.Replication.Store.Entries(), Is.Empty);
        AssertReleased(client, db);

        client.Sql("DROP TRIGGER _test_fault");
        db.ChangeTracker.Clear();
        (await db.Items.SingleAsync(x => x.Id == itemId, TestContext.CurrentContext.CancellationToken)).Price = 2;
        await SaveAsync(db, async);
        Assert.That(client.PriceOf(itemId), Is.EqualTo(2L));
        Assert.That(client.Replication.Store.Entries().Select(e => (e.Table, e.Kind)), Is.EqualTo(new[] { ("Item", OutboxKind.Patch) }));
    }

    [Test]
    public async Task Edit_of_one_member_of_a_complex_type_queues_only_that_column()
    {
        await using ClientDb client = ClientDb.Create();
        Guid id = client.Seed(new StockObject { Name = "Стелаж", Settings = new ObjectSettings("верхній", 3) });
        await using SampleDbContext db = client.Db();
        StockObject o = await db.StockObjects.SingleAsync(x => x.Id == id, TestContext.CurrentContext.CancellationToken);

        o.Settings = o.Settings with { StockLevel = 9 };
        await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);

        OutboxEntry entry = client.Replication.Store.Entries().Single();
        Assert.That((entry.Table, entry.Kind), Is.EqualTo(("StockObject", OutboxKind.Patch)));
        Assert.That(entry.Columns, Does.Contain("Settings_StockLevel"));
        Assert.That(entry.Columns, Has.None.EqualTo("Settings_Description"));
        Assert.That(entry.Columns, Has.None.EqualTo("Name"));
    }

    [Test]
    public async Task Edit_that_leaves_every_member_of_a_complex_type_as_it_was_queues_nothing()
    {
        await using ClientDb client = ClientDb.Create();
        Guid id = client.Seed(new StockObject { Name = "Стелаж", Settings = new ObjectSettings("верхній", 3) });
        await using SampleDbContext db = client.Db();
        StockObject o = await db.StockObjects.SingleAsync(x => x.Id == id, TestContext.CurrentContext.CancellationToken);

        o.Settings = new ObjectSettings("верхній", 3);
        await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);

        Assert.That(client.Replication.Store.Entries(), Is.Empty);
    }

    private static async Task SaveAsync(SampleDbContext db, bool async)
    {
        if (async)
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        else
            db.SaveChanges();
    }

    /// <summary>The retried save carried out the parent delete: the parent went to the archive and its delete to the outbox (11.6).</summary>
    private static void AssertParentArchived(ClientDb client, Guid category, Guid archived)
    {
        Assert.That(client.InstanceOf("Category", category), Is.EqualTo(ClientDb.Archive));
        Assert.That(client.InstanceOf("Item", archived), Is.EqualTo(ClientDb.Archive));
        Assert.That(client.Replication.Store.Entries(), Has.Some.Matches<OutboxEntry>(e => (e.Table, e.Pk, e.Kind) == ("Category", Wire.PkText(category), OutboxKind.Delete)));
    }

    private static void AssertReleased(ClientDb client, SampleDbContext db)
    {
        Assert.That(db.Database.CurrentTransaction, Is.Null);
        Assert.That(db.Database.GetDbConnection().State, Is.EqualTo(ConnectionState.Closed));
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
        db.Log.Add(new LogEntry { Text = "нотатка" });
        await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        Assert.That(client.Replication.Store.Entries(), Is.Empty);
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

    /// <summary>Cancels the save once the router has begun; it throws nothing itself, so it may follow the router.</summary>
    private sealed class CancelOnSaving(CancellationTokenSource cts) : SaveChangesInterceptor
    {
        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
        {
            cts.Cancel();
            return ValueTask.FromResult(result);
        }
    }

    /// <summary>An application's validation: it turns down the first save.</summary>
    private sealed class RejectFirstSave : SaveChangesInterceptor
    {
        private bool _rejected;

        public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
        {
            if (_rejected)
                return result;
            _rejected = true;
            throw new InvalidOperationException("перевірка не пройшла");
        }

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default) =>
            ValueTask.FromResult(SavingChanges(eventData, result));
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
        string path = Path.Combine(TestDb.NewDir(), "client.db");
        SyncModel model;
        using (SampleDbContext db = SampleDbContext.Open(path))
        {
            db.Database.EnsureCreated();
            model = SyncModel.From(db);
        }
        var replication = new ClientReplication(ClientReplication.BuildServices(path, model).InstallClientStore());
        return new ClientDb(path, replication);
    }

    /// <summary>A context with the application's interceptors before the router, as the router asks (8.1).</summary>
    public SampleDbContext Db(params IInterceptor[] app) => SampleDbContext.Open(DbPath, [.. app, Replication.Router]);

    /// <summary>A context with interceptors after the router: they run once the router has begun its write transaction.</summary>
    public SampleDbContext DbRouterFirst(params IInterceptor[] after) => SampleDbContext.Open(DbPath, [Replication.Router, .. after]);

    /// <summary>A remote category with one item.</summary>
    public (Guid Category, Guid Item) SeedRemote()
    {
        Guid category = Seed(new Category { Name = "Офіс" });
        return (category, Seed(new Item { Name = "Степлер", Price = 100, CategoryId = category }));
    }

    /// <summary>A remote category that an archived item points to.</summary>
    public (Guid Category, Guid Item) SeedArchivedCatalog()
    {
        Guid category = Seed(new Category { Name = "Офіс" });
        return (category, Seed(new Item { Name = "Каталог 2019", CategoryId = category }, Archive));
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
        return TestDb.Scalar(c, $"SELECT InstanceId FROM {Wire.Q(table)} WHERE Id = @id", ("@id", Wire.PkText(id))) as string;
    }

    public long PriceOf(Guid item)
    {
        using SqliteConnection c = Replication.Store.Open();
        return (long)TestDb.Scalar(c, "SELECT Price FROM Item WHERE Id = @id", ("@id", Wire.PkText(item)))!;
    }

    public int Sql(string sql, params (string Name, object Value)[] args)
    {
        using SqliteConnection c = Replication.Store.Open();
        return TestDb.Exec(c, sql, args);
    }

    public ValueTask DisposeAsync() => Replication.DisposeAsync();
}
