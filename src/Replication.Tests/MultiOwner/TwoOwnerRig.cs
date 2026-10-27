using System.Globalization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Sample.Domain;

namespace Replication.Tests.MultiOwner;

/// <summary>Initial data of an owner database: categories, items in them, log entries.</summary>
internal sealed record OwnerData(IReadOnlyList<string> Categories, IReadOnlyList<(string Name, long Price, string Status, string Category)> Items, int LogEntries);

/// <summary>An owner built from the core alone: Kestrel with the sync gRPC service over the sample database (h2c on loopback).</summary>
internal sealed class TestOwner : IAsyncDisposable
{
    private readonly WebApplication _app;
    private int _logN;
    private int _disposed;

    private TestOwner(string name, string dbPath, WebApplication app, OwnerStore store, int port)
    {
        Name = name;
        DbPath = dbPath;
        _app = app;
        Store = store;
        Address = $"http://127.0.0.1:{port}";
    }

    public string Name { get; }
    public string DbPath { get; }
    public OwnerStore Store { get; }
    public string Address { get; }

    /// <summary>Starts the server and seeds the empty database with <paramref name="data"/>.</summary>
    public static async Task<TestOwner> StartAsync(string name, string dbPath, OwnerData data, Action<OwnerOptions>? configure = null)
    {
        SyncModel model;
        using (SampleDbContext db = SampleDbContext.Open(dbPath))
            model = SyncModel.From(db);
        var options = new OwnerOptions { OnlineInterval = TimeSpan.FromMilliseconds(300) };
        configure?.Invoke(options);

        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ConfigureSyncKestrelServerOptions();
            k.Listen(System.Net.IPAddress.Loopback, 0, o => o.Protocols = HttpProtocols.Http2);
        });
        builder.Services
            .AddSampleDb(dbPath)
            .AddOwnerServices(dbPath, model, options)
            .AddGrpc(o =>
            {
                o.EnableDetailedErrors = true;
            })
            .ConfigureSyncGrpcServiceOptions();
        WebApplication app = builder.Build();
        app.MapSyncGrpcService();
        app.Services.MigrateSampleDb().InstallOwnerStore();
        await app.StartAsync();
        int port = BindingAddress.Parse(app.Urls.First()).Port;
        var owner = new TestOwner(name, dbPath, app, app.Services.GetRequiredService<OwnerStore>(), port);
        await owner.SeedAsync(data);
        return owner;
    }

    private async Task SeedAsync(OwnerData data)
    {
        await using SampleDbContext db = Db();
        Dictionary<string, Category> cats = data.Categories.ToDictionary(n => n, n => new Category { Name = n });
        db.Categories.AddRange(cats.Values);
        foreach ((string n, long p, string s, string c) in data.Items)
            db.Items.Add(new Item { Name = n, Price = p, Status = s, Category = cats[c] });
        for (int i = 0; i < data.LogEntries; i++)
            db.Log.Add(NextLogEntry());
        await db.SaveChangesAsync();
    }

    private LogEntry NextLogEntry() => new() { Text = $"{Name}: запис #{Interlocked.Increment(ref _logN)}" };

    public SampleDbContext Db() => SampleDbContext.Open(DbPath);

    /// <summary><c>_sync_meta.version</c>: the last version the owner gave out.</summary>
    public long Head() => Sql.Scalar<long>(DbPath, "SELECT version FROM _sync_meta");

    public Guid ItemId(string name) => Sql.Scalar<string>(DbPath, "SELECT Id FROM Item WHERE Name = @n", ("@n", name)) is { } id
        ? Guid.Parse(id)
        : throw new InvalidOperationException($"{Name}: no item «{name}»");

    public Guid CategoryId(string name) => Guid.Parse(Sql.Scalar<string>(DbPath, "SELECT Id FROM Category WHERE Name = @n", ("@n", name))!);

    public async Task SetPriceAsync(string name, long price)
    {
        await using SampleDbContext db = Db();
        (await db.Items.SingleAsync(x => x.Name == name)).Price = price;
        await db.SaveChangesAsync();
    }

    public async Task<Guid> NewItemAsync(string name, string category)
    {
        await using SampleDbContext db = Db();
        Category cat = await db.Categories.SingleAsync(x => x.Name == category);
        var it = new Item { Name = name, Price = 100, Status = "новий", CategoryId = cat.Id };
        db.Items.Add(it);
        await db.SaveChangesAsync();
        return it.Id;
    }

    /// <summary>ExecuteDelete: the delete trigger writes the tombstone.</summary>
    public async Task DeleteItemAsync(string name)
    {
        await using SampleDbContext db = Db();
        await db.Items.Where(x => x.Name == name).ExecuteDeleteAsync();
    }

    public async Task AddLogAsync(int n)
    {
        await using SampleDbContext db = Db();
        for (int i = 0; i < n; i++)
            db.Log.Add(NextLogEntry());
        await db.SaveChangesAsync();
    }

    /// <summary>Stops the server; a second call does nothing, so a test may stop an owner before the rig is disposed.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        using (var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2)))
            await _app.StopAsync(stop.Token);
        await _app.DisposeAsync();
    }
}

/// <summary>What a <see cref="TestClient"/> saw from one agent: a state change (no table) or rows of a table applied in that state.</summary>
internal sealed record Step(string Agent, AgentState State, string? Table, DateTimeOffset At);

/// <summary>
/// A client built from the core alone: one database, one <see cref="ClientStore"/> and <see cref="WriteRouter"/>,
/// one <see cref="SyncAgent"/> per owner (4). Records what each agent applies, in which state.
/// </summary>
internal sealed class TestClient : IAsyncDisposable
{
    private readonly ServiceProvider _services;
    private readonly ILoggerFactory? _loggers;
    private readonly List<SyncAgent> _agents = [];
    private readonly List<Step> _steps = [];

    private TestClient(string dbPath, ServiceProvider services, ILoggerFactory? loggers)
    {
        DbPath = dbPath;
        _services = services;
        _loggers = loggers;
        Store = services.GetRequiredService<ClientStore>();
    }

    public string DbPath { get; }
    public ClientStore Store { get; }
    public string ClientId => Store.ClientId;

    public IReadOnlyList<SyncAgent> Agents
    {
        get
        {
            lock (_agents)
                return [.. _agents];
        }
    }

    public IReadOnlyList<Step> Steps
    {
        get
        {
            lock (_steps)
                return [.. _steps];
        }
    }

    /// <summary>The database migrated and the client service tables installed; no owners yet.</summary>
    public static TestClient Create(string dbPath, ILoggerFactory? loggers)
    {
        SyncModel model;
        using (SampleDbContext db = SampleDbContext.Open(dbPath))
            model = SyncModel.From(db);
        var collection = new ServiceCollection();
        if (loggers is not null)
            collection.AddSingleton(loggers);
        ServiceProvider services = collection
            .AddClientServices(dbPath, model)
            .AddSampleDb(dbPath, sp => [sp.GetRequiredService<WriteRouter>()])
            .BuildServiceProvider();
        services.MigrateSampleDb().InstallClientStore();
        return new TestClient(dbPath, services, loggers);
    }

    /// <summary>A new agent for <paramref name="owner"/> with the link off; catch-up instead of a snapshot, Item open.</summary>
    public SyncAgent Connect(TestOwner owner)
    {
        var options = new AgentOptions
        {
            AckInterval = TimeSpan.FromMilliseconds(300),
            ApplyTimeout = TimeSpan.FromSeconds(15),
            LagWarningWindow = TimeSpan.FromSeconds(6),
            SkipSnapshotOnEmptyReplica = true,
        };
        var agent = new SyncAgent(Store, _loggers?.CreateLogger<SyncAgent>() ?? NullLogger<SyncAgent>.Instance, owner.Address, AgentName(owner), options);
        agent.SetOpenTables(["Item"]);
        AgentState last = AgentState.Offline;
        agent.StatusChanged += () =>
        {
            AgentState s = agent.GetStatus().State;
            lock (_steps)
            {
                if (s == last)
                    return;
                last = s;
                _steps.Add(new Step(agent.Name, s, null, DateTimeOffset.Now));
            }
        };
        agent.DataChanged += tables =>
        {
            AgentState s = agent.GetStatus().State;
            lock (_steps)
                _steps.AddRange(tables.Select(t => new Step(agent.Name, s, t, DateTimeOffset.Now)));
        };
        lock (_agents)
            _agents.Add(agent);
        agent.Start();
        return agent;
    }

    public void ClearSteps()
    {
        lock (_steps)
            _steps.Clear();
    }

    /// <summary>Tables applied by the agent of <paramref name="owner"/> in <paramref name="state"/>, in order.</summary>
    public static string AgentName(TestOwner owner) => $"клієнт → {owner.Name}";

    public List<string> Tables(TestOwner owner, AgentState state) =>
        [.. Steps.Where(s => s.Agent == AgentName(owner) && s.State == state && s.Table is not null).Select(s => s.Table!)];

    /// <summary>The application's DbContext with the WriteRouter, as the UI would use it.</summary>
    public SampleDbContext Db() => _services.GetRequiredService<IDbContextFactory<SampleDbContext>>().CreateDbContext();

    public async ValueTask DisposeAsync()
    {
        foreach (SyncAgent a in Agents)
            await a.DisposeAsync();
        await _services.DisposeAsync();
    }
}

/// <summary>Two owners with different data and one client with an agent for each; the links start off.</summary>
internal sealed class TwoOwnerRig : IAsyncDisposable
{
    public const string OwnerA = "Власник 1";
    public const string OwnerB = "Власник 2";

    public static readonly OwnerData DataA = new(["Офіс", "Склад", "Архів"],
    [
        ("Степлер", 120L, "активний", "Офіс"), ("Папір A4", 240L, "активний", "Офіс"), ("Маркери", 85L, "новий", "Офіс"),
        ("Палета", 450L, "активний", "Склад"), ("Стрейч-плівка", 310L, "активний", "Склад"), ("Скотч", 40L, "новий", "Склад"),
        ("Старий принтер", 900L, "архів", "Архів"), ("Факс", 300L, "архів", "Архів"), ("Каталог 2019", 15L, "архів", "Офіс"),
    ], 3);

    public static readonly OwnerData DataB = new(["Кухня", "Гараж"],
    [
        ("Чайник", 650L, "активний", "Кухня"), ("Тарілки", 180L, "новий", "Кухня"), ("Ключі", 320L, "активний", "Гараж"),
        ("Мастило", 95L, "новий", "Гараж"), ("Шини", 2400L, "архів", "Гараж"),
    ], 6);

    private TwoOwnerRig(string dir, TestLog? log, TestOwner a, TestOwner b)
    {
        Dir = dir;
        Log = log;
        A = a;
        B = b;
    }

    public string Dir { get; }
    /// <summary>Where the agents log; nowhere when null.</summary>
    public TestLog? Log { get; }
    public TestOwner A { get; }
    public TestOwner B { get; }
    public TestClient Client { get; private set; } = null!;
    public SyncAgent ToA { get; private set; } = null!;
    public SyncAgent ToB { get; private set; } = null!;
    public string ClientPath => Path.Combine(Dir, "client.db");

    public IReadOnlyList<(TestOwner Owner, SyncAgent Agent)> Pairs => [(A, ToA), (B, ToB)];

    /// <param name="configureOwner">Applied to both owners.</param>
    /// <param name="seedClient">Writes the client's own rows through its DbContext before any agent exists.</param>
    public static async Task<TwoOwnerRig> StartAsync(TestLog? log = null, Action<OwnerOptions>? configureOwner = null, Func<SampleDbContext, Task>? seedClient = null)
    {
        string dir = Path.Combine(Path.GetTempPath(), "replication-tests", $"{DateTime.Now:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..6]}");
        Directory.CreateDirectory(dir);
        TestOwner a = await TestOwner.StartAsync(OwnerA, Path.Combine(dir, "owner1.db"), DataA, configureOwner);
        TestOwner b = await TestOwner.StartAsync(OwnerB, Path.Combine(dir, "owner2.db"), DataB, configureOwner);
        var rig = new TwoOwnerRig(dir, log, a, b);
        await rig.OpenClientAsync(seedClient);
        return rig;
    }

    private async Task OpenClientAsync(Func<SampleDbContext, Task>? seed = null)
    {
        Client = TestClient.Create(ClientPath, Log?.Loggers);
        if (seed is not null)
        {
            await using SampleDbContext db = Client.Db();
            await seed(db);
        }
        ToA = Client.Connect(A);
        ToB = Client.Connect(B);
    }

    /// <summary>Closes the client (both agents) and opens it again on the same database, links off.</summary>
    public async Task RestartClientAsync()
    {
        await Client.DisposeAsync();
        SqliteConnection.ClearAllPools();
        await OpenClientAsync();
    }

    public TestOwner OwnerOf(SyncAgent agent) => Pairs.First(p => p.Agent == agent).Owner;

    /// <summary>Turns the link on and waits until the agent is online (catch-up done).</summary>
    public static Task SyncNowAsync(SyncAgent agent, TimeSpan? timeout = null)
    {
        agent.LinkEnabled = true;
        return Wait.UntilAsync(() => agent.GetStatus().State == AgentState.Online, timeout ?? TimeSpan.FromSeconds(30), $"{agent.Name}: not online");
    }

    public static Task GoOfflineAsync(SyncAgent agent)
    {
        agent.LinkEnabled = false;
        return Wait.UntilAsync(() => agent.GetStatus().State == AgentState.Offline, TimeSpan.FromSeconds(30), $"{agent.Name}: not offline");
    }

    /// <summary>
    /// Waits until every agent with the link on is online with an empty outbox and cursors at its owner's head,
    /// and its owner has the cursors acknowledged (<c>_sync_clients.acked_version</c> = head).
    /// </summary>
    public async Task SettleAsync(TimeSpan? timeout = null)
    {
        DateTimeOffset until = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        using var tick = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        int quiet = 0;
        while (quiet < 3)
        {
            if (DateTimeOffset.UtcNow > until)
                throw new TimeoutException("not settled: " + string.Join("; ", Pairs.Select(p => $"{p.Agent.Name}: {p.Agent.GetStatus().State}, cursor {p.Agent.MinCursor}, head {p.Owner.Head()}")));
            quiet = Pairs.All(p => IsQuiet(p.Owner, p.Agent)) ? quiet + 1 : 0;
            await tick.WaitForNextTickAsync();
        }
    }

    private bool IsQuiet(TestOwner owner, SyncAgent agent)
    {
        if (!agent.LinkEnabled)
            return true;
        AgentStatus s = agent.GetStatus();
        long head = owner.Head();
        return s.State == AgentState.Online && s.Pending == 0 && !agent.HasPendingArchiveWork && agent.MinCursor >= head
               && Sql.Scalar<long?>(owner.DbPath, "SELECT acked_version FROM _sync_clients WHERE client_id = @c", ("@c", Client.ClientId)) >= head;
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await A.DisposeAsync();
        await B.DisposeAsync();
        SqliteConnection.ClearAllPools();
    }
}

internal static class Wait
{
    public static async Task UntilAsync(Func<bool> condition, TimeSpan timeout, string what)
    {
        DateTimeOffset until = DateTimeOffset.UtcNow + timeout;
        using var tick = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        while (!condition())
        {
            if (DateTimeOffset.UtcNow > until)
                throw new TimeoutException(what);
            await tick.WaitForNextTickAsync();
        }
    }
}

/// <summary>Raw SQL on a database file, read-only and unpooled: what is really stored, not what the sync code reports.</summary>
internal static class Sql
{
    private static SqliteConnection Open(string path)
    {
        var c = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ToString());
        c.Open();
        return c;
    }

    private static SqliteCommand Cmd(SqliteConnection c, string sql, (string, object?)[] args)
    {
        SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach ((string n, object? v) in args)
            cmd.Parameters.AddWithValue(n, v ?? DBNull.Value);
        return cmd;
    }

    public static T? Scalar<T>(string dbPath, string sql, params (string, object?)[] args)
    {
        using SqliteConnection c = Open(dbPath);
        using SqliteCommand cmd = Cmd(c, sql, args);
        object? r = cmd.ExecuteScalar();
        return r is null or DBNull ? default : r is T t ? t : (T)Convert.ChangeType(r, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T), CultureInfo.InvariantCulture);
    }

    /// <summary>Each row as its columns joined by "|", NULL as ∅.</summary>
    public static List<string> Lines(string dbPath, string sql, params (string, object?)[] args)
    {
        using SqliteConnection c = Open(dbPath);
        using SqliteCommand cmd = Cmd(c, sql, args);
        using SqliteDataReader r = cmd.ExecuteReader();
        var lines = new List<string>();
        while (r.Read())
            lines.Add(string.Join("|", Enumerable.Range(0, r.FieldCount).Select(i => r.IsDBNull(i) ? "∅" : Convert.ToString(r.GetValue(i), CultureInfo.InvariantCulture))));
        return lines;
    }
}
