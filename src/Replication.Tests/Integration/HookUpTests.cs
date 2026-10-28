using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Hooking the core into an application: one <c>UseReplication(...)</c> line in <c>OnModelCreating</c> (5.2, 5.4),
/// <see cref="SyncModel"/> built from the DbContext, and <c>Install</c> after migrations on the owner and on the client.
/// </summary>
[TestFixture]
public sealed class HookUpTests
{
    private static readonly string[] s_service = [SyncColumns.Version, SyncColumns.Base, SyncColumns.Mask, SyncColumns.Origin];

    [Test]
    public void UseReplication_adds_the_service_columns_as_shadow_properties_and_declares_the_triggers()
    {
        using SampleDbContext db = SampleDbContext.Open(":memory:");
        foreach (string table in new[] { "Category", "Item", "Log" })
        {
            IEntityType et = db.Model.GetEntityTypes().Single(e => e.GetTableName() == table);
            foreach (string column in s_service)
            {
                IProperty p = et.FindProperty(column)!;
                Assert.That(p, Is.Not.Null, $"{table}.{column}");
                Assert.That(p.IsShadowProperty(), Is.True, $"{table}.{column}: the entity class stays as it is");
                // EF never writes them: the owner's triggers and the sync agent do (5.2).
                Assert.That(p.ValueGenerated, Is.EqualTo(ValueGenerated.OnAddOrUpdate), $"{table}.{column}");
                Assert.That(p.GetBeforeSaveBehavior(), Is.EqualTo(PropertySaveBehavior.Ignore), $"{table}.{column}");
                Assert.That(p.GetAfterSaveBehavior(), Is.EqualTo(PropertySaveBehavior.Ignore), $"{table}.{column}");
            }
            // InstanceId is written by the application on the client: which replica a new row belongs to (5.4).
            Assert.That(et.FindProperty(SyncColumns.InstanceId)!.IsShadowProperty(), Is.True);
            Assert.That(et.FindProperty(SyncColumns.InstanceId)!.GetBeforeSaveBehavior(), Is.EqualTo(PropertySaveBehavior.Save));
            // Declared triggers keep EF from using RETURNING, which SQLite does not allow with triggers (5.2).
            Assert.That(et.GetDeclaredTriggers().Select(t => t.ModelName), Is.EquivalentTo(new[] { $"_sync_{table}_ins", $"_sync_{table}_upd", $"_sync_{table}_del" }));
            Assert.That(et.GetIndexes().Select(i => i.Properties.Single().Name), Is.SupersetOf(new[] { SyncColumns.Version, SyncColumns.InstanceId }));
        }
    }

    [Test]
    public void SyncModel_lists_the_replicated_tables_parents_first_with_data_columns_in_mask_order()
    {
        using SampleDbContext db = SampleDbContext.Open(":memory:");
        SyncModel model = SyncModel.From(db);

        Assert.That(model.Tables.Select(t => t.Name), Is.EqualTo(new[] { "Category", "Item", "Log", "StockObject" }));
        SyncTable item = model["Item"];
        // Bit i of SyncMask is Columns[i]: the key and the service columns are not data columns.
        Assert.That(item.Columns, Is.EqualTo(new[] { "CategoryId", "CreatedOn", "ModifiedOn", "Name", "Price", "Status" }));
        Assert.That(item.FullMask, Is.EqualTo(0b111111));
        Assert.That(item.MaskOf(["Price", "Status"]), Is.EqualTo(0b110000));
        Assert.That(item.ForeignKeys, Is.EqualTo(new[] { new SyncForeignKey("CategoryId", "Category", Cascade: true) }));
        Assert.That(model.ChildrenOf("Category").Select(x => x.Child.Name), Is.EqualTo(new[] { "Item" }));
    }

    [Test]
    public void SyncModel_refuses_a_replicated_table_without_a_single_Guid_key_named_Id()
    {
        using var db = new IntKeyDbContext();
        Assert.That(() => SyncModel.From(db), Throws.TypeOf<NotSupportedException>().With.Message.Contains("Guid key"));
    }

    [Test]
    public void Owner_install_creates_the_service_tables_and_triggers_and_a_second_install_changes_nothing()
    {
        string path = Path.Combine(TestDb.NewDir(), "owner.db");
        SyncModel model = CreateDatabase(path);
        var store = new OwnerStore(path, model);
        try
        {
            AssertOwnerInstall(path, model, store);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
        }
    }

    private static void AssertOwnerInstall(string path, SyncModel model, OwnerStore store)
    {
        store.Install();

        string instance = store.InstanceId;
        Assert.That(instance, Does.StartWith("inst-"));
        Assert.That(Tables(path), Is.SupersetOf(new[] { "_sync_meta", "_sync_tombstones", "_sync_clients", "_sync_client_cursors", "_sync_floor" }));
        foreach (SyncTable t in model.Tables)
            Assert.That(Rows.Triggers(path, t.Name), Is.EqualTo(new[] { $"_sync_{t.Name}_del", $"_sync_{t.Name}_ins", $"_sync_{t.Name}_upd" }));
        Assert.That(Rows.Scalar(path, "PRAGMA journal_mode"), Is.EqualTo("wal"));
        // No client yet: no floor holds the base back (5.2).
        Assert.That(Rows.Scalar(path, $"SELECT COUNT(*) FROM _sync_floor WHERE floor = {OwnerStore.NoFloor}"), Is.EqualTo(4L));
        // The version file outside the database guards against a restored backup (5.3).
        Assert.That(File.ReadAllText(path + ".syncstate"), Is.EqualTo($"{instance} 0"));

        store.Install();
        Assert.That(store.InstanceId, Is.EqualTo(instance));
        foreach (SyncTable t in model.Tables)
            Assert.That(Rows.Triggers(path, t.Name), Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Client_install_creates_the_client_tables_and_keeps_its_client_id_across_restarts()
    {
        string path = Path.Combine(TestDb.NewDir(), "client.db");
        SyncModel model = CreateDatabase(path);
        ClientReplication? first = null;
        ClientReplication? second = null;
        try
        {
            first = new ClientReplication(ClientReplication.BuildServices(path, model).InstallClientStore());

            Assert.That(first.ClientId, Does.StartWith("cl-"));
            Assert.That(Tables(path), Is.SupersetOf(new[] { "_sync_local", "_sync_instances", "_sync_cursors", "_sync_ranges", "_sync_outbox" }));
            // Replica versions mirror the owner's, so the client has no sync triggers (5.4).
            foreach (SyncTable t in model.Tables)
                Assert.That(Rows.Triggers(path, t.Name), Is.Empty);

            second = new ClientReplication(ClientReplication.BuildServices(path, model).InstallClientStore());
            Assert.That(second.ClientId, Is.EqualTo(first.ClientId));
        }
        finally
        {
            if (second is not null)
                await second.DisposeAsync();
            if (first is not null)
                await first.DisposeAsync();
            SqliteConnection.ClearAllPools();
        }
    }

    private static SyncModel CreateDatabase(string path)
    {
        using SampleDbContext db = SampleDbContext.Open(path);
        db.Database.EnsureCreated();
        return SyncModel.From(db);
    }

    private static List<string> Tables(string path)
    {
        using var c = new SqliteConnection($"Data Source={path};Pooling=False");
        c.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table'";
        using SqliteDataReader r = cmd.ExecuteReader();
        var names = new List<string>();
        while (r.Read())
            names.Add(r.GetString(0));
        return names;
    }

    private sealed class Counter
    {
        public int Id { get; set; }
        public long Value { get; set; }
    }

    private sealed class IntKeyDbContext : DbContext
    {
        public DbSet<Counter> Counters => Set<Counter>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) => options.UseSqlite("Data Source=:memory:");

        protected override void OnModelCreating(ModelBuilder b) => b.UseReplication();
    }
}
