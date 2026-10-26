using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Replication.Tests.Integration.Migrations;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Design 15.3: an application that already replicates ships a new version with an EF migration. The owner and the
/// client each run the migration on start, then call <c>Install</c> again, which brings back the triggers the table
/// rebuild dropped; replication goes on from the same cursors, and the new column and table replicate both ways.
/// </summary>
[TestFixture]
public sealed class SchemaMigrationTests
{
    private TestLog _log = null!;
    private string _dir = "";
    private string _ownerPath = "";
    private string _clientPath = "";
    private OwnerStore _owner = null!;
    private OwnerHost? _host;
    private ClientReplication? _client;
    private int _port;

    private SyncAgent Agent => _client!.Agents[0];
    private string Instance => Agent.InstanceId!;

    [SetUp]
    public void SetUp()
    {
        _log = new TestLog();
        _dir = TestDb.NewDir();
        _ownerPath = Path.Combine(_dir, "owner.db");
        _clientPath = Path.Combine(_dir, "client.db");
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_client is not null)
            await _client.DisposeAsync();
        if (_host is not null)
            await _host.DisposeAsync();
        SqliteConnection.ClearAllPools();
        _log.WriteTo(TestContext.Out);
        _log.Dispose();
    }

    private sealed record V1(Product Lamp, Product Cable, Product Paint, Dictionary<Guid, long> Versions, long Head);

    /// <summary>
    /// Version 1 of the application, replicating. The owner holds a partial change (Name only) the client has not received,
    /// the client is offline with an edit in its outbox.
    /// </summary>
    private async Task<V1> ArrangeVersion1Async()
    {
        Migrate(_ownerPath, 1);
        var shelf = new Shelf { Name = "Склад" };
        var lamp = new Product { Name = "Лампа", Price = 100, ShelfId = shelf.Id };
        var cable = new Product { Name = "Кабель", Price = 200, ShelfId = shelf.Id };
        var paint = new Product { Name = "Фарба", Price = 300, ShelfId = shelf.Id };
        await using (ShopDbContext db = ShopDbContext.Open(_ownerPath, 1))
        {
            db.Shelves.Add(shelf);
            db.Products.AddRange(lamp, cable, paint);
            await db.SaveChangesAsync();
        }
        SyncModel v1 = ModelOf(1);
        await StartOwnerAsync(v1, schema: 1);
        Migrate(_clientPath, 1);
        StartClient(v1, schema: 1);
        await SettleAsync();
        Assert.That(Rows.Diff(v1, _ownerPath, _clientPath, Instance), Is.Empty);

        // The mask of the partial change names Name in the version-1 column order (Name, Price, ShelfId).
        await OwnerEditAsync(1, lamp.Id, p => p.Price = 110);
        await SettleAsync();
        long received = Rows.One(_ownerPath, "Product", lamp.Id).Version;
        await Lab.WaitAsync(() => (long)Rows.Scalar(_ownerPath, "SELECT floor FROM _sync_floor WHERE tbl = 'Product'")! >= received,
            TimeSpan.FromSeconds(30), "the owner did not see the client's cursor pass the row");
        await GoOfflineAsync();
        await OwnerEditAsync(1, lamp.Id, p => p.Name = "Лампа LED");
        RowData lampOnOwner = Rows.One(_ownerPath, "Product", lamp.Id);
        Assert.That(lampOnOwner.Mask, Is.EqualTo(v1["Product"].MaskOf(["Name"])), "a partial change of Name only");
        await ClientEditAsync(1, cable.Id, p => p.Price = 999);
        Assert.That(_client!.Store.Entries(Instance).Select(e => (e.Kind, string.Join(",", e.Columns))), Is.EqualTo(new[] { (OutboxKind.Patch, "Price") }));

        return new V1(lamp, cable, paint, Rows.All(_ownerPath, "Product").ToDictionary(x => x.Key, x => x.Value.Version),
            (long)Rows.Scalar(_ownerPath, "SELECT version FROM _sync_meta")!);
    }

    /// <summary>The owner upgrades: the migration rebuilds Product, which drops its triggers and keeps every SyncVersion; then Install on start.</summary>
    private async Task<SyncModel> UpgradeOwnerAsync(V1 before, bool checkRebuild)
    {
        await _host!.DisposeAsync();
        _host = null;
        Migrate(_ownerPath, 2);
        if (checkRebuild)
        {
            Assert.That(Rows.Triggers(_ownerPath, "Product"), Is.Empty, "the rebuild drops the triggers");
            Assert.That(Rows.All(_ownerPath, "Product").ToDictionary(x => x.Key, x => x.Value.Version), Is.EqualTo(before.Versions));
        }
        SyncModel v2 = ModelOf(2);
        await StartOwnerAsync(v2, schema: 2);
        return v2;
    }

    [Test]
    public async Task Owner_install_after_a_table_rebuild_restores_triggers_and_remaps_the_partial_mask_without_new_versions()
    {
        V1 v1 = await ArrangeVersion1Async();

        SyncModel v2 = await UpgradeOwnerAsync(v1, checkRebuild: true);

        // Triggers for every table, the new one too, and no new versions.
        foreach (string table in new[] { "Shelf", "Product", "Tag" })
            Assert.That(Rows.Triggers(_ownerPath, table), Is.EqualTo(new[] { $"_sync_{table}_del", $"_sync_{table}_ins", $"_sync_{table}_upd" }), table);
        Assert.That(Rows.Scalar(_ownerPath, "SELECT version FROM _sync_meta"), Is.EqualTo(v1.Head));
        // The new column sorts first, so the partial change keeps naming Name in the new column order (5.2).
        Assert.That(Rows.One(_ownerPath, "Product", v1.Lamp.Id).Mask, Is.EqualTo(v2["Product"].MaskOf(["Name"])));
        Assert.That(Rows.One(_ownerPath, "Product", v1.Paint.Id).Mask, Is.EqualTo(v2["Product"].FullMask), "an untouched insert stays a full row");
    }

    [Test]
    public async Task Client_on_the_old_schema_gets_SchemaMismatch_turns_its_link_off_and_keeps_its_outbox()
    {
        V1 v1 = await ArrangeVersion1Async();
        await UpgradeOwnerAsync(v1, checkRebuild: false);

        Agent.LinkEnabled = true;
        await Lab.WaitAsync(() => Agent.GetStatus().State == AgentState.SchemaMismatch, TimeSpan.FromSeconds(30), "no SchemaMismatch");

        Assert.That(Agent.LinkEnabled, Is.False, "the agent turns its link off on SchemaMismatch");
        Assert.That(Rows.One(_ownerPath, "Product", v1.Cable.Id)["Price"], Is.EqualTo(200L), "nothing was sent");
        Assert.That(_client!.Store.Entries(Instance), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Sync_goes_on_after_both_sides_migrate_and_the_new_column_and_table_replicate_both_ways()
    {
        V1 v1 = await ArrangeVersion1Async();
        SyncModel v2 = await UpgradeOwnerAsync(v1, checkRebuild: false);
        Agent.LinkEnabled = true;
        await Lab.WaitAsync(() => Agent.GetStatus().State == AgentState.SchemaMismatch, TimeSpan.FromSeconds(30), "no SchemaMismatch");

        // The client upgrades and restarts: same migration, Install, and it carries on from its cursors.
        await _client!.DisposeAsync();
        _client = null;
        Migrate(_clientPath, 2);
        StartClient(v2, schema: 2);
        await SettleAsync();
        Assert.That(Rows.One(_ownerPath, "Product", v1.Cable.Id)["Price"], Is.EqualTo(999L), "the queued edit reached the owner");
        Assert.That(Rows.One(_clientPath, "Product", v1.Lamp.Id, Instance)["Name"], Is.EqualTo("Лампа LED"), "the partial change arrived as Name");
        Assert.That(Rows.Diff(v2, _ownerPath, _clientPath, Instance), Is.Empty);

        // The new column and the new table replicate both ways.
        await OwnerEditAsync(2, v1.Paint.Id, p => p.Discount = 15);
        Assert.That(Rows.One(_ownerPath, "Product", v1.Paint.Id).Mask, Is.EqualTo(v2["Product"].MaskOf(["Discount"])));
        var fresh = new Tag { Label = "новинка", ProductId = v1.Lamp.Id };
        await using (ShopDbContext db = ShopDbContext.Open(_ownerPath, 2))
        {
            db.Tags().Add(fresh);
            await db.SaveChangesAsync();
        }
        await ClientEditAsync(2, v1.Lamp.Id, p => p.Discount = 5);
        var hit = new Tag { Label = "хіт", ProductId = v1.Cable.Id };
        await using (ShopDbContext db = ShopDbContext.Open(_clientPath, 2, _client!.Router))
        {
            db.Tags().Add(hit).SetInstance(Instance);
            await db.SaveChangesAsync();
        }
        await SettleAsync();

        Assert.That(Rows.One(_clientPath, "Product", v1.Paint.Id, Instance)["Discount"], Is.EqualTo(15L));
        Assert.That(Rows.One(_clientPath, "Tag", fresh.Id, Instance)["Label"], Is.EqualTo("новинка"));
        Assert.That(Rows.One(_ownerPath, "Product", v1.Lamp.Id)["Discount"], Is.EqualTo(5L));
        RowData hitOnOwner = Rows.One(_ownerPath, "Tag", hit.Id);
        Assert.That(hitOnOwner["Label"], Is.EqualTo("хіт"));
        Assert.That(hitOnOwner.Origin, Is.EqualTo(_client.ClientId));
        Assert.That(Rows.Diff(v2, _ownerPath, _clientPath, Instance), Is.Empty);
    }

    /// <summary>Runs the application's migrations up to <paramref name="schema"/>, as the app does on start.</summary>
    private static void Migrate(string path, int schema)
    {
        using ShopDbContext db = ShopDbContext.Open(path, schema);
        db.GetService<IMigrator>().Migrate(ShopDbContext.MigrationOf(schema));
    }

    private static SyncModel ModelOf(int schema)
    {
        using ShopDbContext db = ShopDbContext.Open(":memory:", schema);
        return SyncModel.From(db);
    }

    private async Task StartOwnerAsync(SyncModel model, int schema)
    {
        // The same port after a restart: the client keeps the owner's address.
        _host = await OwnerHost.StartAsync(_ownerPath, model,
            new OwnerOptions { SchemaVersion = schema, OnlineInterval = TimeSpan.FromMilliseconds(300) }, _port, logging: _log.AddTo);
        _owner = _host.Store;
        _port = _host.Port;
    }

    private void StartClient(SyncModel model, int schema)
    {
        _client = new ClientReplication(ClientReplication.BuildServices(_clientPath, model, loggers: _log.Loggers).InstallClientStore());
        var options = new AgentOptions
        {
            SchemaVersion = schema,
            AckInterval = TimeSpan.FromMilliseconds(300),
            ReconnectMin = TimeSpan.FromMilliseconds(200),
            ReconnectMax = TimeSpan.FromSeconds(1),
        };
        _client.Connect($"http://127.0.0.1:{_port}", "клієнт", options).LinkEnabled = true;
    }

    private async Task GoOfflineAsync()
    {
        Agent.LinkEnabled = false;
        await Lab.WaitAsync(() => Agent.GetStatus().State == AgentState.Offline, TimeSpan.FromSeconds(30), "the client did not go offline");
    }

    /// <summary>Online, outbox empty, every cursor at the owner's head.</summary>
    private async Task SettleAsync()
    {
        await Lab.WaitAsync(() =>
        {
            using SqliteConnection c = _owner.Open();
            return Agent.GetStatus() is { State: AgentState.Online, Pending: 0 } && Agent.MinCursor >= _owner.Head(c);
        }, TimeSpan.FromSeconds(30), "the client did not catch up with the owner");
    }

    private async Task OwnerEditAsync(int schema, Guid id, Action<Product> change)
    {
        await using ShopDbContext db = ShopDbContext.Open(_ownerPath, schema);
        change(await db.Products.SingleAsync(x => x.Id == id));
        await db.SaveChangesAsync();
    }

    private async Task ClientEditAsync(int schema, Guid id, Action<Product> change)
    {
        await using ShopDbContext db = ShopDbContext.Open(_clientPath, schema, _client!.Router);
        change(await db.Products.SingleAsync(x => x.Id == id && EF.Property<string>(x, SyncColumns.InstanceId) == Instance));
        await db.SaveChangesAsync();
    }
}
