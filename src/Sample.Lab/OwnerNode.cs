using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Replication;
using Replication.Model;
using Replication.Owner;
using Sample.Domain;

namespace Sample.Lab;

/// <summary>
/// The owner: the same application, here headless. Only its automation writes, through ordinary EF Core calls
/// (SaveChanges, ExecuteUpdate, ExecuteDelete); triggers catch every path (design 5.2).
/// </summary>
public sealed class OwnerNode : IAsyncDisposable
{
    private static readonly string[] s_autoNames = ["Ліхтарик", "Батарейки", "Клей", "Блокнот", "Лампа", "Кабель", "Фарба", "Рукавиці"];
    private static readonly string[] s_statuses = ["новий", "активний", "архів"];
    private readonly Random _rnd = new(42);
    private int _logN;

    private OwnerNode(string dbPath, OwnerStore store, OwnerHost host, SyncLog log)
    {
        DbPath = dbPath;
        Store = store;
        Host = host;
        Log = log;
    }

    public string DbPath { get; }
    public OwnerStore Store { get; }
    public OwnerHost Host { get; }
    public SyncLog Log { get; }
    public string Address => Host.Address;

    public static async Task<OwnerNode> StartAsync(string dbPath, SyncLog log, int port = 0, bool listenAnywhere = false, Action<OwnerOptions>? configure = null)
    {
        SyncModel model;
        bool empty;
        await using (SampleDbContext db = SampleDbContext.Open(dbPath))
        {
            await db.Database.EnsureCreatedAsync();
            model = SyncModel.From(db);
            empty = !await db.Categories.AnyAsync();
        }
        var options = new OwnerOptions { Log = log };
        configure?.Invoke(options);
        var store = new OwnerStore(dbPath, model, options);
        store.Install();
        OwnerHost host = await OwnerHost.StartAsync(store, port, listenAnywhere);
        var node = new OwnerNode(dbPath, store, host, log);
        if (empty)
            await node.SeedAsync();
        return node;
    }

    public SampleDbContext Db() => SampleDbContext.Open(DbPath);

    private void Say(string text, SyncLogLevel level = SyncLogLevel.Info) => Log.Write("owner", "автоматика: " + text, level);

    private async Task SeedAsync()
    {
        await using SampleDbContext db = Db();
        Dictionary<string, Category> cats = new[] { "Офіс", "Склад", "Архів" }.ToDictionary(n => n, n => new Category { Name = n });
        db.Categories.AddRange(cats.Values);
        foreach ((string n, long p, string s, string c) in new[]
                 {
                     ("Степлер", 120L, "активний", "Офіс"), ("Папір A4", 240L, "активний", "Офіс"), ("Маркери", 85L, "новий", "Офіс"),
                     ("Палета", 450L, "активний", "Склад"), ("Стрейч-плівка", 310L, "активний", "Склад"), ("Скотч", 40L, "новий", "Склад"),
                     ("Старий принтер", 900L, "архів", "Архів"), ("Факс", 300L, "архів", "Архів"), ("Каталог 2019", 15L, "архів", "Офіс"),
                 })
        {
            db.Items.Add(new Item { Name = n, Price = p, Status = s, Category = cats[c] });
        }

        for (int i = 0; i < 3; i++)
            db.Log.Add(new LogEntry { Text = $"Автоматика: перерахунок залишків #{++_logN}" });
        await db.SaveChangesAsync();
        Say($"початкові дані: 3 категорії, 9 товарів, 3 записи журналу; голова {Head()}");
    }

    public long Head()
    {
        using SqliteConnection c = Store.Open();
        return Store.Head(c);
    }

    private static async Task<Item> ItemAsync(SampleDbContext db, string name) =>
        await db.Items.FirstOrDefaultAsync(x => x.Name == name) ?? throw new InvalidOperationException($"на власнику нема «{name}»");

    // Automation.

    /// <summary>SaveChanges path.</summary>
    public async Task SetPriceAsync(string name, long? price = null)
    {
        await using SampleDbContext db = Db();
        Item it = await ItemAsync(db, name);
        it.Price = price ?? 10 + _rnd.Next(99) * 10;
        await db.SaveChangesAsync();
        Say($"«{name}».Price = {it.Price}");
    }

    /// <summary>ExecuteUpdate path: no change tracker, the trigger still versions the row.</summary>
    public async Task SetStatusAsync(string name, string? status = null)
    {
        await using SampleDbContext db = Db();
        Item it = await ItemAsync(db, name);
        string s = status ?? s_statuses[(Array.IndexOf(s_statuses, it.Status) + 1) % s_statuses.Length];
        await db.Items.Where(x => x.Id == it.Id).ExecuteUpdateAsync(u => u.SetProperty(x => x.Status, s).SetProperty(x => x.ModifiedOn, DateTime.UtcNow));
        Say($"«{name}».Status = {s} (ExecuteUpdate)");
    }

    public async Task RenameAsync(string name, string newName)
    {
        await using SampleDbContext db = Db();
        Item it = await ItemAsync(db, name);
        it.Name = newName;
        await db.SaveChangesAsync();
        Say($"«{name}».Name = «{newName}»");
    }

    public async Task<Guid> NewItemAsync(string? name = null, string status = "новий", string? category = null)
    {
        await using SampleDbContext db = Db();
        List<Category> cats = await db.Categories.ToListAsync();
        Category cat = category is null ? cats[_rnd.Next(cats.Count)] : cats.First(c => c.Name == category);
        var it = new Item { Name = name ?? s_autoNames[_rnd.Next(s_autoNames.Length)], Price = 10 + _rnd.Next(60) * 10, Status = status, CategoryId = cat.Id };
        db.Items.Add(it);
        await db.SaveChangesAsync();
        Say($"створено «{it.Name}» ({status}, {cat.Name})");
        return it.Id;
    }

    /// <summary>ExecuteDelete path: the delete trigger writes the tombstone.</summary>
    public async Task DeleteItemAsync(string name)
    {
        await using SampleDbContext db = Db();
        Item it = await ItemAsync(db, name);
        await db.Items.Where(x => x.Id == it.Id).ExecuteDeleteAsync();
        Say($"видалено «{name}» (ExecuteDelete), tombstone", SyncLogLevel.Warn);
    }

    /// <summary>The FK cascade in SQLite deletes the items; each gets its tombstone from the trigger.</summary>
    public async Task DeleteCategoryAsync(string name)
    {
        await using SampleDbContext db = Db();
        Category cat = await db.Categories.FirstAsync(x => x.Name == name);
        int n = await db.Items.CountAsync(x => x.CategoryId == cat.Id);
        db.Categories.Remove(cat);
        await db.SaveChangesAsync();
        Say($"видалено категорію «{name}» з каскадом ({n} товарів), tombstones: {n + 1}", SyncLogLevel.Warn);
    }

    public async Task AddLogAsync(int n)
    {
        await using SampleDbContext db = Db();
        for (int i = 0; i < n; i++)
            db.Log.Add(new LogEntry { Text = $"Автоматика: перерахунок залишків #{++_logN}" });
        await db.SaveChangesAsync();
        Say($"+{n} у журнал");
    }

    /// <summary>The demo's "packet of 17 changes": 5 edits, 2 new items, 10 log entries.</summary>
    public async Task BurstAsync()
    {
        await using SampleDbContext db = Db();
        List<Item> items = await db.Items.ToListAsync();
        List<Category> cats = await db.Categories.ToListAsync();
        for (int i = 0; i < 5; i++)
        {
            Item it = items[_rnd.Next(items.Count)];
            if (_rnd.Next(2) == 0)
                it.Price = 10 + _rnd.Next(99) * 10;
            else
                it.Status = s_statuses[_rnd.Next(3)];
            await db.SaveChangesAsync();
        }
        for (int i = 0; i < 2; i++)
            db.Items.Add(new Item { Name = s_autoNames[_rnd.Next(s_autoNames.Length)], Price = 100, Status = "новий", CategoryId = cats[_rnd.Next(cats.Count)].Id });
        await db.SaveChangesAsync();
        for (int i = 0; i < 10; i++)
        {
            db.Log.Add(new LogEntry { Text = $"Автоматика: перерахунок залишків #{++_logN}" });
            await db.SaveChangesAsync();
        }
        Say($"пакет із 17 змін (5 правок, 2 нові товари, 10 записів журналу), голова {Head()}");
    }

    /// <summary>A stream of changes in Items: new rows and price edits, <paramref name="perRound"/> per round.</summary>
    public async Task FlowRoundAsync(int perRound)
    {
        await using SampleDbContext db = Db();
        List<Item> items = await db.Items.ToListAsync();
        List<Category> cats = await db.Categories.ToListAsync();
        for (int i = 0; i < perRound; i++)
        {
            if (i % 2 == 0)
                db.Items.Add(new Item { Name = $"{s_autoNames[_rnd.Next(s_autoNames.Length)]} {++_logN}", Price = 100, Status = "новий", CategoryId = cats[_rnd.Next(cats.Count)].Id });
            else
                items[_rnd.Next(items.Count)].Price = 10 + _rnd.Next(99) * 10;
            await db.SaveChangesAsync();
        }
    }

    public Task<OwnerStore.PurgeResult> PurgeAsync()
    {
        OwnerStore.PurgeResult r = Store.Purge();
        Log.Write("owner", r.Tombstones > 0
            ? $"очищення: видалено {r.Tombstones} tombstones (floor = MIN(acked_version) = {(r.Floor == OwnerStore.NoFloor ? "∞" : r.Floor)}), purged_version = {r.PurgedVersion}"
            : $"очищення: нічого видаляти, floor = {(r.Floor == OwnerStore.NoFloor ? "∞" : r.Floor)}");
        return Task.FromResult(r);
    }

    public void AdvanceClock(TimeSpan by)
    {
        Store.AdvanceClock(by);
        Log.Write("owner", $"годинник власника +{by.TotalDays:0.#} дн.");
    }

    public ValueTask DisposeAsync() => Host.DisposeAsync();
}
