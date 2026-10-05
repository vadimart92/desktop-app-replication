using Microsoft.EntityFrameworkCore;
using Replication;
using Replication.Client;
using Replication.Model;
using Sample.Domain;

namespace Sample.Lab;

/// <summary>
/// A client: one database for every instance, the user's edits go through an ordinary DbContext with the
/// WriteRouter interceptor; the agent syncs in the background (design 4, 8.1).
/// </summary>
public sealed class ClientNode : IAsyncDisposable
{
    private ClientNode(string id, string label, string dbPath, ClientReplication replication, SyncAgent agent, SyncLog log)
    {
        Id = id;
        Label = label;
        DbPath = dbPath;
        Replication = replication;
        Agent = agent;
        Log = log;
    }

    public string Id { get; }
    public string Label { get; }
    public string DbPath { get; }
    public ClientReplication Replication { get; }
    public SyncAgent Agent { get; }
    public SyncLog Log { get; }

    public static ClientNode Create(string id, string label, string dbPath, string ownerAddress, SyncLog log, AgentOptions? options = null)
    {
        SyncModel model;
        using (SampleDbContext db = SampleDbContext.Open(dbPath))
        {
            db.Database.EnsureCreated();
            model = SyncModel.From(db);
        }
        var replication = new ClientReplication(dbPath, model, log);
        replication.Install();
        SyncAgent agent = replication.Connect(ownerAddress, "Власник", options, label);
        agent.SetOpenTables("Item");
        return new ClientNode(id, label, dbPath, replication, agent, log);
    }

    public bool Link
    {
        get => Agent.LinkEnabled;
        set => Agent.LinkEnabled = value;
    }

    public string Instance => Agent.InstanceId ?? throw new InvalidOperationException($"{Label}: репліки ще нема");

    public SampleDbContext Db() => SampleDbContext.Open(DbPath, Replication.Router);

    private void Say(string text) => Log.Write(Label, "користувач: " + text);

    private IQueryable<T> Mine<T>(SampleDbContext db) where T : BaseEntity =>
        db.Set<T>().Where(x => EF.Property<string>(x, SyncColumns.InstanceId) == Instance);

    private async Task<Item> ItemAsync(SampleDbContext db, string name) =>
        await Mine<Item>(db).FirstOrDefaultAsync(x => x.Name == name) ?? throw new InvalidOperationException($"{Label}: нема «{name}»");

    public async Task EditItemAsync(string name, Action<Item> change, string what)
    {
        await using SampleDbContext db = Db();
        Item it = await ItemAsync(db, name);
        change(it);
        await db.SaveChangesAsync();
        Say($"«{name}» {what}");
    }

    public Task SetPriceAsync(string name, long price) => EditItemAsync(name, x => x.Price = price, $"Price = {price}");
    public Task SetStatusAsync(string name, string status) => EditItemAsync(name, x => x.Status = status, $"Status = {status}");
    public Task RenameAsync(string name, string newName) => EditItemAsync(name, x => x.Name = newName, $"Name = «{newName}»");

    public async Task<Guid> CreateCategoryAsync(string name)
    {
        await using SampleDbContext db = Db();
        var c = new Category { Name = name };
        db.Categories.Add(c).SetInstance(Instance);
        await db.SaveChangesAsync();
        Say($"створив категорію «{name}»");
        return c.Id;
    }

    public async Task<Guid> CreateItemAsync(string name, long price, string status, string category)
    {
        await using SampleDbContext db = Db();
        Category cat = await Mine<Category>(db).FirstAsync(x => x.Name == category);
        var it = new Item { Name = name, Price = price, Status = status, CategoryId = cat.Id };
        db.Items.Add(it).SetInstance(Instance);
        await db.SaveChangesAsync();
        Say($"створив «{name}» у «{category}», SyncVersion 0");
        return it.Id;
    }

    public async Task DeleteItemAsync(string name)
    {
        await using SampleDbContext db = Db();
        db.Items.Remove(await ItemAsync(db, name));
        await db.SaveChangesAsync();
        Say($"видалив «{name}»");
    }

    public async Task DeleteCategoryAsync(string name)
    {
        await using SampleDbContext db = Db();
        db.Categories.Remove(await Mine<Category>(db).FirstAsync(x => x.Name == name));
        await db.SaveChangesAsync();
        Say($"видалив категорію «{name}» (локальний каскад прибрав товари, у чергу йде тільки категорія)");
    }

    // ---------- by id, for the UI ----------

    public async Task UpdateItemAsync(Guid id, string name, long price, string status)
    {
        await using SampleDbContext db = Db();
        Item it = await Mine<Item>(db).FirstAsync(x => x.Id == id);
        (it.Name, it.Price, it.Status) = (name, price, status);
        await db.SaveChangesAsync();
        Say($"«{name}»: Price = {price}, Status = {status}");
    }

    public async Task RenameCategoryAsync(Guid id, string name)
    {
        await using SampleDbContext db = Db();
        Category c = await Mine<Category>(db).FirstAsync(x => x.Id == id);
        c.Name = name;
        await db.SaveChangesAsync();
        Say($"категорія «{name}»");
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
        Say($"видалив запис {table} {Wire.Short(Wire.PkText(id))}");
    }

    public async Task<Guid> AddItemAsync()
    {
        await using SampleDbContext db = Db();
        Category cat = await Mine<Category>(db).OrderBy(x => x.Name).FirstAsync();
        int n = await Mine<Item>(db).CountAsync() + 1;
        var it = new Item { Name = $"Товар {n}", Price = 100, Status = "новий", CategoryId = cat.Id };
        db.Items.Add(it).SetInstance(Instance);
        await db.SaveChangesAsync();
        Say($"створив «{it.Name}» у «{cat.Name}»");
        return it.Id;
    }

    public Task<string> ArchiveByIdAsync(string table, Guid id)
    {
        Say("переносить запис в архів");
        return Agent.ArchiveAsync([(table, id)]);
    }

    public async Task<int> DeleteWhereStatusAsync(string status)
    {
        Say($"видаляє всі товари зі статусом «{status}»");
        return await Agent.DeleteWhereAsync("Item", new Predicate(nameof(Item.Status), status));
    }

    public async Task<string> ArchiveAsync(params (string Table, string Name)[] rows)
    {
        await using SampleDbContext db = Db();
        var ids = new List<(string, Guid)>();
        foreach ((string? t, string? n) in rows)
            ids.Add((t, t == "Category" ? (await Mine<Category>(db).FirstAsync(x => x.Name == n)).Id : (await ItemAsync(db, n)).Id));
        Say($"переносить в архів: {string.Join(", ", rows.Select(r => $"«{r.Name}»"))}");
        return await Agent.ArchiveAsync(ids);
    }

    public ValueTask DisposeAsync() => Replication.DisposeAsync();
}
