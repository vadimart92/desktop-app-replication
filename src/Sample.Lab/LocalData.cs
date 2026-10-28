using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Replication;
using Replication.Model;
using Sample.Domain;

namespace Sample.Lab;

/// <summary>
/// The client's own data: rows with <c>InstanceId</c> NULL in the same tables as the replicas (5.4). They come from no owner,
/// never reach the outbox, and may point only to rows of their own (the WriteRouter refuses a save that breaks it).
/// </summary>
public sealed class LocalData(ClientReplication replication, string label = "Клієнт")
{
    private readonly ILogger<LocalData> _log = replication.Loggers.CreateLogger<LocalData>();

    /// <summary>What the lab client has before it connects to any owner: 1 category, 3 items, 2 log entries.</summary>
    public static readonly OwnerSeed Seed = new(0, ["Мої"],
    [
        ("Чернетка", 10L, "новий", "Мої"), ("Ідея", 20L, "активний", "Мої"), ("Список покупок", 5L, "архів", "Мої"),
    ], 2);

    public ClientReplication Replication { get; } = replication;

    public SampleDbContext Db() => Replication.Services.GetRequiredService<IDbContextFactory<SampleDbContext>>().CreateDbContext();

    private void Say(string text) => _log.LogInformation("[{Agent}] власні дані: {Text}", label, text);

    private static IQueryable<T> Mine<T>(SampleDbContext db) where T : BaseEntity =>
        db.Set<T>().Where(x => EF.Property<string?>(x, SyncColumns.InstanceId) == null);

    /// <summary>Writes <paramref name="seed"/> when the client has no own rows yet; nothing is set as InstanceId.</summary>
    public async Task SeedAsync(OwnerSeed seed)
    {
        await using SampleDbContext db = Db();
        if (await Mine<Category>(db).AnyAsync())
            return;
        Dictionary<string, Category> cats = seed.Categories.ToDictionary(n => n, n => new Category { Name = n });
        db.Categories.AddRange(cats.Values);
        foreach ((string n, long p, string s, string c) in seed.Items)
            db.Items.Add(new Item { Name = n, Price = p, Status = s, Category = cats[c] });
        for (int i = 1; i <= seed.LogEntries; i++)
            db.Log.Add(new LogEntry { Text = $"Моя нотатка #{i}" });
        await db.SaveChangesAsync();
        Say($"початкові: {seed.Categories.Count} категорія, {seed.Items.Count} товари, {seed.LogEntries} записи журналу (InstanceId NULL)");
    }

    public async Task UpdateItemAsync(Guid id, string name, long price, string status)
    {
        await using SampleDbContext db = Db();
        Item it = await Mine<Item>(db).FirstAsync(x => x.Id == id);
        (it.Name, it.Price, it.Status) = (name, price, status);
        await db.SaveChangesAsync();
        Say($"«{name}»: Price = {price}, Status = {status}, у чергу нічого");
    }

    public async Task DeleteAsync(string table, Guid id)
    {
        await using SampleDbContext db = Db();
        BaseEntity e = table switch
        {
            "Category" => await Mine<Category>(db).FirstAsync(x => x.Id == id),
            "Log" => await Mine<LogEntry>(db).FirstAsync(x => x.Id == id),
            _ => await Mine<Item>(db).FirstAsync(x => x.Id == id),
        };
        db.Remove(e);
        await db.SaveChangesAsync();
        Say($"видалено запис {table} {Wire.Short(Wire.PkText(id))}, у чергу нічого");
    }

    public async Task<Guid> AddItemAsync()
    {
        await using SampleDbContext db = Db();
        Category cat = await Mine<Category>(db).OrderBy(x => x.Name).FirstOrDefaultAsync()
                       ?? throw new InvalidOperationException("нема власної категорії: спершу додайте категорію");
        int n = await Mine<Item>(db).CountAsync() + 1;
        var it = new Item { Name = $"Мій товар {n}", Price = 100, Status = "новий", CategoryId = cat.Id };
        db.Items.Add(it);
        await db.SaveChangesAsync();
        Say($"створено «{it.Name}» у «{cat.Name}»");
        return it.Id;
    }

    public async Task<Guid> CreateCategoryAsync(string name)
    {
        await using SampleDbContext db = Db();
        var c = new Category { Name = name };
        db.Categories.Add(c);
        await db.SaveChangesAsync();
        Say($"створено категорію «{name}»");
        return c.Id;
    }

    public async Task<int> DeleteWhereStatusAsync(string status)
    {
        await using SampleDbContext db = Db();
        List<Item> items = await Mine<Item>(db).Where(x => x.Status == status).ToListAsync();
        db.Items.RemoveRange(items);
        await db.SaveChangesAsync();
        Say($"видалено товари зі статусом «{status}»: {items.Count}");
        return items.Count;
    }
}
