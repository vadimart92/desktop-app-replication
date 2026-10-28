using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Sample.Lab;

namespace Replication.Tests;

public sealed class Node
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = "";
    public Guid? ParentId { get; set; }
    public Node? Parent { get; set; }
}

public sealed class Dept
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = "";
    public Guid? ManagerId { get; set; }
    public Emp? Manager { get; set; }
}

public sealed class Emp
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = "";
    public Guid? DeptId { get; set; }
    public Dept? Dept { get; set; }
}

/// <summary>A model with FK cycles: a tree and two tables that point to each other.</summary>
public sealed class GraphDbContext(DbContextOptions<GraphDbContext> options) : DbContext(options)
{
    public DbSet<Node> Nodes => Set<Node>();
    public DbSet<Dept> Depts => Set<Dept>();
    public DbSet<Emp> Emps => Set<Emp>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Node>(e =>
        {
            e.ToTable("Node");
            e.HasOne(x => x.Parent).WithMany().HasForeignKey(x => x.ParentId);
        });
        b.Entity<Dept>(e =>
        {
            e.ToTable("Dept");
            e.HasOne(x => x.Manager).WithMany().HasForeignKey(x => x.ManagerId);
        });
        b.Entity<Emp>(e =>
        {
            e.ToTable("Emp");
            e.HasOne(x => x.Dept).WithMany().HasForeignKey(x => x.DeptId);
        });
        b.UseReplication();
    }

    public static GraphDbContext Open(string path, params IInterceptor[] interceptors) =>
        new GraphDbContext(new DbContextOptionsBuilder<GraphDbContext>()
            .UseSqlite($"Data Source={path};Pooling=True;Default Timeout=30")
            .AddInterceptors(interceptors)
            .Options);
}

/// <summary>An owner and one client of <see cref="GraphDbContext"/> over gRPC on localhost.</summary>
internal sealed class GraphPair : IAsyncDisposable
{
    private const string Label = "клієнт";

    private readonly TestLog _log;

    private GraphPair(OwnerHost owner, ClientReplication client, TestLog log)
    {
        Owner = owner;
        Client = client;
        _log = log;
    }

    public OwnerHost Owner { get; }
    public ClientReplication Client { get; }
    public SyncAgent Agent => Client.Agents[0];
    public string Instance => Agent.InstanceId ?? throw new InvalidOperationException("репліки ще нема");

    /// <summary>The log lines of the client's agent.</summary>
    public IReadOnlyList<string> Lines => _log.OfAgent(Label);

    /// <summary>The "→ Apply:" log lines of the client: one per sent batch.</summary>
    public IReadOnlyList<string> Applies => [.. Lines.Where(l => l.StartsWith("→ Apply:", StringComparison.Ordinal))];

    public static async Task<GraphPair> StartAsync(Action<GraphDbContext>? seedOwner = null)
    {
        string dir = TestDb.NewDir();
        var log = new TestLog();
        string ownerPath = Path.Combine(dir, "owner.db");
        SyncModel model;
        using (GraphDbContext db = GraphDbContext.Open(ownerPath))
        {
            db.Database.EnsureCreated();
            model = SyncModel.From(db);
        }
        OwnerHost owner = await OwnerHost.StartAsync(ownerPath, model, new OwnerOptions { OnlineInterval = TimeSpan.FromMilliseconds(300) });
        if (seedOwner is not null)
        {
            using GraphDbContext db = GraphDbContext.Open(ownerPath);
            seedOwner(db);
            db.SaveChanges();
        }

        string clientPath = Path.Combine(dir, "client.db");
        using (GraphDbContext db = GraphDbContext.Open(clientPath))
            db.Database.EnsureCreated();
        var client = new ClientReplication(ClientReplication.BuildServices(clientPath, model, loggers: log.Loggers).InstallClientStore());
        var pair = new GraphPair(owner, client, log);
        client.Connect(owner.Address, Label, new AgentOptions { AckInterval = TimeSpan.FromMilliseconds(300) });
        await pair.SyncAsync();
        return pair;
    }

    /// <summary>Owner rows: a root whose ParentId is its own Id, and the nodes below it.</summary>
    public static void SeedTree(GraphDbContext owner, Node root, params Node[] below)
    {
        owner.Nodes.Add(root);
        owner.Nodes.AddRange(below);
        owner.SaveChanges();
        root.ParentId = root.Id;
        owner.SaveChanges();
    }

    public GraphDbContext ClientDb() => GraphDbContext.Open(Client.Store.DbPath, Client.Router);

    public GraphDbContext OwnerDb() => GraphDbContext.Open(Owner.Store.DbPath);

    public async Task GoOfflineAsync()
    {
        Agent.LinkEnabled = false;
        await Lab.WaitAsync(() => Agent.GetStatus().State == AgentState.Offline, TimeSpan.FromSeconds(30), "клієнт не вийшов з мережі");
    }

    /// <summary>Turns the link on and waits until the client is online with an empty outbox.</summary>
    public async Task SyncAsync()
    {
        Agent.LinkEnabled = true;
        await Lab.WaitAsync(() => Agent.GetStatus() is { State: AgentState.Online, Pending: 0 }, TimeSpan.FromSeconds(30), "клієнт не відправив чергу");
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await Owner.DisposeAsync();
        _log.Dispose();
        SqliteConnection.ClearAllPools();
    }
}
