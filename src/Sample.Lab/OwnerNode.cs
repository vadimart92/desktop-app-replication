using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
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
    private readonly Random _rnd;
    private readonly OwnerSeed _seed;
    private readonly ILogger<OwnerNode> _log;
    private int _logN;

    private OwnerNode(string dbPath, OwnerStore store, OwnerHost host, OwnerSeed seed)
    {
        DbPath = dbPath;
        Store = store;
        Host = host;
        _log = host.Loggers.CreateLogger<OwnerNode>();
        _seed = seed;
        _rnd = new Random(seed.RandomSeed);
    }

    public string DbPath { get; }
    public OwnerStore Store { get; }
    public OwnerHost Host { get; }
    public string Address => Host.Address;

    /// <param name="logging">Logging providers of the owner's host; none when null.</param>
    /// <param name="seed">Initial data of an empty database; <see cref="OwnerSeed.Default"/> when null.</param>
    public static async Task<OwnerNode> StartAsync(string dbPath, int port = 0, bool listenAnywhere = false, Action<OwnerOptions>? configure = null, OwnerSeed? seed = null, Action<ILoggingBuilder>? logging = null)
    {
        SyncModel model;
        using (SampleDbContext db = SampleDbContext.Open(dbPath))
            model = SyncModel.From(db);
        var options = new OwnerOptions();
        configure?.Invoke(options);
        OwnerHost host = await OwnerHost.StartAsync(dbPath, model, options, port, listenAnywhere, logging);
        bool empty;
        await using (SampleDbContext db = SampleDbContext.Open(dbPath))
            empty = !await db.Categories.AnyAsync();
        var node = new OwnerNode(dbPath, host.Store, host, seed ?? OwnerSeed.Default);
        if (empty)
            await node.SeedAsync();
        return node;
    }

    public SampleDbContext Db() => SampleDbContext.Open(DbPath);

    private void Say(string text, LogLevel level = LogLevel.Information) => _log.Log(level, "автоматика: {Text}", text);

    private T Pick<T>(IReadOnlyList<T> list) => list[_rnd.Next(list.Count)];

    private long RandomPrice(int steps = 99) => 10 + _rnd.Next(steps) * 10;

    private LogEntry NextLogEntry() => new() { Text = $"Автоматика: перерахунок залишків #{++_logN}" };

    private async Task SeedAsync()
    {
        await using SampleDbContext db = Db();
        Dictionary<string, Category> cats = _seed.Categories.ToDictionary(n => n, n => new Category { Name = n });
        db.Categories.AddRange(cats.Values);
        foreach ((string n, long p, string s, string c) in _seed.Items)
            db.Items.Add(new Item { Name = n, Price = p, Status = s, Category = cats[c] });
        for (int i = 0; i < _seed.LogEntries; i++)
            db.Log.Add(NextLogEntry());
        await db.SaveChangesAsync();
        Say($"початкові дані: {_seed.Categories.Count} категорії, {_seed.Items.Count} товарів, {_seed.LogEntries} записи журналу; голова {Head()}");
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
        it.Price = price ?? RandomPrice();
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
        Category cat = category is null ? Pick(cats) : cats.First(c => c.Name == category);
        var it = new Item { Name = name ?? Pick(s_autoNames), Price = RandomPrice(60), Status = status, CategoryId = cat.Id };
        db.Items.Add(it);
        await db.SaveChangesAsync();
        Say($"створено «{it.Name}» ({status}, {cat.Name})");
        return it.Id;
    }

    public async Task<Guid> NewStockObjectAsync(string name, string? description, int stockLevel)
    {
        await using SampleDbContext db = Db();
        var o = new StockObject { Name = name, Settings = new ObjectSettings(description, stockLevel) };
        db.StockObjects.Add(o);
        await db.SaveChangesAsync();
        Say($"створено об'єкт «{name}» (Settings: {description}, {stockLevel})");
        return o.Id;
    }

    /// <summary>ExecuteDelete path: the delete trigger writes the tombstone.</summary>
    public async Task DeleteItemAsync(string name)
    {
        await using SampleDbContext db = Db();
        Item it = await ItemAsync(db, name);
        await db.Items.Where(x => x.Id == it.Id).ExecuteDeleteAsync();
        Say($"видалено «{name}» (ExecuteDelete), tombstone", LogLevel.Warning);
    }

    /// <summary>The FK cascade in SQLite deletes the items; each gets its tombstone from the trigger.</summary>
    public async Task DeleteCategoryAsync(string name)
    {
        await using SampleDbContext db = Db();
        Category cat = await db.Categories.FirstAsync(x => x.Name == name);
        int n = await db.Items.CountAsync(x => x.CategoryId == cat.Id);
        db.Categories.Remove(cat);
        await db.SaveChangesAsync();
        Say($"видалено категорію «{name}» з каскадом ({n} товарів), tombstones: {n + 1}", LogLevel.Warning);
    }

    public async Task AddLogAsync(int n)
    {
        await using SampleDbContext db = Db();
        for (int i = 0; i < n; i++)
            db.Log.Add(NextLogEntry());
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
            Item it = Pick(items);
            if (_rnd.Next(2) == 0)
                it.Price = RandomPrice();
            else
                it.Status = Pick(s_statuses);
            await db.SaveChangesAsync();
        }
        for (int i = 0; i < 2; i++)
            db.Items.Add(new Item { Name = Pick(s_autoNames), Price = 100, Status = "новий", CategoryId = Pick(cats).Id });
        await db.SaveChangesAsync();
        for (int i = 0; i < 10; i++)
        {
            db.Log.Add(NextLogEntry());
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
                db.Items.Add(new Item { Name = $"{Pick(s_autoNames)} {++_logN}", Price = 100, Status = "новий", CategoryId = Pick(cats).Id });
            else
                Pick(items).Price = RandomPrice();
            await db.SaveChangesAsync();
        }
    }

    public Task<OwnerStore.PurgeResult> PurgeAsync()
    {
        OwnerStore.PurgeResult r = Store.Purge();
        object floor = r.Floor == OwnerStore.NoFloor ? "∞" : r.Floor;
        if (r.Tombstones > 0)
            _log.LogInformation("очищення: видалено {Tombstones} tombstones (floor = MIN(acked_version) = {Floor}), purged_version = {PurgedVersion}", r.Tombstones, floor, r.PurgedVersion);
        else
            _log.LogInformation("очищення: нічого видаляти, floor = {Floor}", floor);
        return Task.FromResult(r);
    }

    public void AdvanceClock(TimeSpan by)
    {
        Store.AdvanceClock(by);
        _log.LogInformation("годинник власника +{Days} дн.", by.TotalDays.ToString("0.#"));
    }

    public ValueTask DisposeAsync() => Host.DisposeAsync();
}

/// <summary>What an empty owner database starts with, and the seed of its automation's random choices.</summary>
public sealed record OwnerSeed(int RandomSeed, IReadOnlyList<string> Categories, IReadOnlyList<(string Name, long Price, string Status, string Category)> Items, int LogEntries)
{
    /// <summary>3 categories, 9 items and 3 log entries, as on the demo page.</summary>
    public static readonly OwnerSeed Default = new(42, ["Офіс", "Склад", "Архів"],
    [
        ("Степлер", 120L, "активний", "Офіс"), ("Папір A4", 240L, "активний", "Офіс"), ("Маркери", 85L, "новий", "Офіс"),
        ("Палета", 450L, "активний", "Склад"), ("Стрейч-плівка", 310L, "активний", "Склад"), ("Скотч", 40L, "новий", "Склад"),
        ("Старий принтер", 900L, "архів", "Архів"), ("Факс", 300L, "архів", "Архів"), ("Каталог 2019", 15L, "архів", "Офіс"),
    ], 3);

    /// <summary>A second owner with its own data: 2 categories, 5 items and 6 log entries, none of the names shared with <see cref="Default"/>.</summary>
    public static readonly OwnerSeed Second = new(7, ["Кухня", "Гараж"],
    [
        ("Чайник", 650L, "активний", "Кухня"), ("Тарілки", 180L, "новий", "Кухня"), ("Ключі", 320L, "активний", "Гараж"),
        ("Мастило", 95L, "новий", "Гараж"), ("Шини", 2400L, "архів", "Гараж"),
    ], 6);
}
