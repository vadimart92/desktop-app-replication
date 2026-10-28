using Microsoft.Extensions.Logging;
using Replication;
using Replication.Client;
using Replication.Owner;

namespace Sample.Lab;

/// <summary>
/// Two owners with different data and one client connected to both: one database, one agent per owner (4).
/// Each owner logs through its own host; the logging callback gets the owner's name (<see cref="OwnerA"/>, <see cref="OwnerB"/>).
/// The links start off; both agents ask for catch-up instead of a snapshot (<see cref="AgentOptions.SkipSnapshotOnEmptyReplica"/>).
/// </summary>
public sealed class TwoOwnerLab : IAsyncDisposable
{
    public const string OwnerA = "Власник 1";
    public const string OwnerB = "Власник 2";

    private TwoOwnerLab(string dir, ILoggerFactory loggers, OwnerNode a, OwnerNode b, ClientNode toA, ClientNode toB, LocalData local)
    {
        Dir = dir;
        Loggers = loggers;
        A = a;
        B = b;
        ToA = toA;
        ToB = toB;
        Local = local;
    }

    public string Dir { get; }
    /// <summary>Loggers of the client; each owner's host has its own.</summary>
    public ILoggerFactory Loggers { get; }

    /// <summary>The first owner, seeded with <see cref="OwnerSeed.Default"/>.</summary>
    public OwnerNode A { get; }

    /// <summary>The second owner, seeded with <see cref="OwnerSeed.Second"/>.</summary>
    public OwnerNode B { get; }

    /// <summary>The client's replica of <see cref="A"/>.</summary>
    public ClientNode ToA { get; }

    /// <summary>The client's replica of <see cref="B"/>: the same database as <see cref="ToA"/>, its own agent.</summary>
    public ClientNode ToB { get; }

    /// <summary>The client's own rows (<c>InstanceId</c> NULL) in the same database.</summary>
    public LocalData Local { get; }

    public IReadOnlyList<(OwnerNode Owner, ClientNode Client)> Pairs => [(A, ToA), (B, ToB)];

    /// <param name="configureOwner">Applied to both owners.</param>
    /// <param name="localSeed">The client's own data, written before it connects to any owner; none when null.</param>
    /// <param name="logging">Logging providers of the client; none when null.</param>
    /// <param name="ownerLogging">Logging providers of each owner's host, by owner name; <paramref name="logging"/> when null.</param>
    public static async Task<TwoOwnerLab> StartAsync(string? dir = null, Action<OwnerOptions>? configureOwner = null, OwnerSeed? localSeed = null,
        Action<ILoggingBuilder>? logging = null, Action<string, ILoggingBuilder>? ownerLogging = null)
    {
        dir ??= Lab.NewDir();
        Directory.CreateDirectory(dir);
        ILoggerFactory loggers = LoggerFactory.Create(b => logging?.Invoke(b));
        ownerLogging ??= (_, b) => logging?.Invoke(b);

        async Task<OwnerNode> Owner(string name, string file, OwnerSeed seed) =>
            await OwnerNode.StartAsync(Path.Combine(dir, file), configure: o =>
            {
                o.OnlineInterval = TimeSpan.FromMilliseconds(300);
                configureOwner?.Invoke(o);
            }, seed: seed, logging: b => ownerLogging(name, b));

        OwnerNode a = await Owner(OwnerA, "owner1.db", OwnerSeed.Default);
        OwnerNode b = await Owner(OwnerB, "owner2.db", OwnerSeed.Second);
        AgentOptions Opt() => new AgentOptions { AckInterval = TimeSpan.FromMilliseconds(300), ApplyTimeout = TimeSpan.FromSeconds(15), LagWarningWindow = TimeSpan.FromSeconds(6), SkipSnapshotOnEmptyReplica = true };
        ClientReplication client = ClientNode.Create(Path.Combine(dir, "client.db"), loggers);
        var local = new LocalData(client);
        if (localSeed is not null)
            await local.SeedAsync(localSeed);
        ClientNode toA = ClientNode.Connect(client, $"Клієнт → {OwnerA}", a.Address, Opt(), OwnerA);
        ClientNode toB = ClientNode.Connect(client, $"Клієнт → {OwnerB}", b.Address, Opt(), OwnerB);
        return new TwoOwnerLab(dir, loggers, a, b, toA, toB, local);
    }

    public OwnerNode OwnerOf(ClientNode c) => Pairs.First(p => p.Client == c).Owner;

    /// <summary>Turns the link on and waits until the agent is online.</summary>
    public static async Task SyncNowAsync(ClientNode c, TimeSpan? timeout = null)
    {
        c.Link = true;
        await Lab.WaitAsync(() => c.Agent.GetStatus().State == AgentState.Online, timeout ?? TimeSpan.FromSeconds(30), $"{c.Label} не вийшов онлайн");
    }

    public static async Task GoOfflineAsync(ClientNode c, TimeSpan? timeout = null)
    {
        c.Link = false;
        await Lab.WaitAsync(() => c.Agent.GetStatus().State == AgentState.Offline, timeout ?? TimeSpan.FromSeconds(30), $"{c.Label} не вийшов з мережі");
    }

    /// <summary>Waits until every agent with the link on is online, with an empty outbox and cursors at its owner's head.</summary>
    public async Task SettleAsync(TimeSpan? timeout = null)
    {
        DateTimeOffset until = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        int quiet = 0;
        while (quiet < 3)
        {
            if (DateTimeOffset.UtcNow > until)
                throw new TimeoutException("система не заспокоїлась: " + string.Join("; ", Pairs.Select(p => $"{p.Client.Label}: {p.Client.Agent.GetStatus().State}, курсор {p.Client.Agent.MinCursor}, голова {p.Owner.Head()}")));
            quiet = Pairs.All(p => IsQuiet(p.Owner, p.Client)) ? quiet + 1 : 0;
            await Task.Delay(100);
        }
    }

    private static bool IsQuiet(OwnerNode owner, ClientNode c)
    {
        // Unlike Lab.IsQuiet, SchemaMismatch / Duplicate clients are not skipped here: they never become quiet.
        if (!c.Link)
            return true;
        AgentStatus s = c.Agent.GetStatus();
        return s.State == AgentState.Online && s.Pending == 0 && !c.Agent.HasPendingArchiveWork && c.Agent.MinCursor >= owner.Head();
    }

    public async ValueTask DisposeAsync()
    {
        // One client: disposing it disposes both agents.
        await ToA.Replication.DisposeAsync();
        await A.DisposeAsync();
        await B.DisposeAsync();
        Loggers.Dispose();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
    }
}
