using Microsoft.Extensions.Logging;
using Replication;
using Replication.Client;
using Replication.Owner;

namespace Sample.Lab;

/// <summary>
/// An owner and two clients in one process, each with its own SQLite file, talking real gRPC over localhost.
/// Scenarios drive them like the demo page drives its model.
/// </summary>
public sealed class Lab : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger<Lab> _log;
    private int _scheduled;

    private Lab(string dir, ILoggerFactory loggers, LogCapture notes, OwnerNode owner, ClientNode c1, ClientNode c2)
    {
        Dir = dir;
        Loggers = loggers;
        Notes = notes;
        _log = loggers.CreateLogger<Lab>();
        Owner = owner;
        C1 = c1;
        C2 = c2;
    }

    public string Dir { get; }
    /// <summary>Loggers of the clients and the scenarios; the owner's host has its own.</summary>
    public ILoggerFactory Loggers { get; }
    /// <summary>What the clients' agents logged, for the scenario checks.</summary>
    public LogCapture Notes { get; }
    public OwnerNode Owner { get; }
    public ClientNode C1 { get; }
    public ClientNode C2 { get; }
    public IReadOnlyList<ClientNode> Clients => [C1, C2];

    /// <summary>Background actions scheduled by a step (<see cref="When"/>, <see cref="Later"/>) that have not run yet.</summary>
    public int Scheduled => Volatile.Read(ref _scheduled);

    /// <summary>A new path under %TEMP%/replication-lab for one run's databases.</summary>
    public static string NewDir() => Path.Combine(Path.GetTempPath(), "replication-lab", $"{DateTime.Now:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..6]}");

    /// <param name="logging">Logging providers of the owner's host, the clients and the scenarios; none when null.</param>
    public static async Task<Lab> StartAsync(string? dir = null, Action<OwnerOptions>? configureOwner = null, Action<ILoggingBuilder>? logging = null)
    {
        dir ??= NewDir();
        Directory.CreateDirectory(dir);
        var notes = new LogCapture();
        ILoggerFactory loggers = LoggerFactory.Create(b =>
        {
            b.AddProvider(notes);
            logging?.Invoke(b);
        });
        OwnerNode owner = await OwnerNode.StartAsync(Path.Combine(dir, "owner.db"), configure: o =>
        {
            o.OnlineInterval = TimeSpan.FromMilliseconds(300);
            configureOwner?.Invoke(o);
        }, logging: logging);
        AgentOptions Opt() => new AgentOptions { AckInterval = TimeSpan.FromMilliseconds(300), ApplyTimeout = TimeSpan.FromSeconds(15), LagWarningWindow = TimeSpan.FromSeconds(6) };
        ClientNode c1 = ClientNode.Connect(ClientNode.Create(Path.Combine(dir, "client1.db"), loggers), "Клієнт 1", owner.Address, Opt());
        ClientNode c2 = ClientNode.Connect(ClientNode.Create(Path.Combine(dir, "client2.db"), loggers), "Клієнт 2", owner.Address, Opt());
        return new Lab(dir, loggers, notes, owner, c1, c2);
    }

    public void Say(string text, LogLevel level = LogLevel.Information) => _log.Log(level, "{Text}", text);

    /// <summary>Turns the link on and waits until the client is online (the demo's syncNow).</summary>
    public async Task SyncNowAsync(ClientNode c, TimeSpan? timeout = null)
    {
        c.Link = true;
        await WaitAsync(() => c.Agent.GetStatus().State == AgentState.Online, timeout ?? TimeSpan.FromSeconds(30), $"{c.Label} не вийшов онлайн");
    }

    /// <summary>Turns the link off and waits until the client is offline.</summary>
    public async Task GoOfflineAsync(ClientNode c, TimeSpan? timeout = null)
    {
        c.Link = false;
        await WaitAsync(() => c.Agent.GetStatus().State == AgentState.Offline, timeout ?? TimeSpan.FromSeconds(30), $"{c.Label} не вийшов з мережі");
    }

    public static async Task WaitAsync(Func<bool> condition, TimeSpan timeout, string what)
    {
        DateTimeOffset until = DateTimeOffset.UtcNow + timeout;
        while (!condition())
        {
            if (DateTimeOffset.UtcNow > until)
                throw new TimeoutException(what);
            await Task.Delay(50);
        }
    }

    /// <summary>Runs <paramref name="action"/> once <paramref name="condition"/> holds (the demo's when()).</summary>
    public void When(Func<bool> condition, Func<Task> action, TimeSpan? timeout = null)
    {
        Interlocked.Increment(ref _scheduled);
        _ = Task.Run(async () =>
        {
            try
            {
                DateTimeOffset until = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(60));
                while (!_cts.IsCancellationRequested && DateTimeOffset.UtcNow < until)
                {
                    bool ok;
                    try
                    {
                        ok = condition();
                    }
                    catch
                    {
                        ok = false;
                    }
                    if (ok)
                    {
                        await action();
                        return;
                    }
                    await Task.Delay(20, _cts.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                Say("відкладена дія: " + e.Message, LogLevel.Error);
            }
            finally
            {
                Interlocked.Decrement(ref _scheduled);
            }
        });
    }

    /// <summary>Runs <paramref name="action"/> after a delay (the demo's later()).</summary>
    public void Later(TimeSpan delay, Func<Task> action)
    {
        DateTimeOffset at = DateTimeOffset.UtcNow + delay;
        When(() => DateTimeOffset.UtcNow >= at, action);
    }

    /// <summary>
    /// Waits until everything settles: no scheduled actions, every connected client online with an empty outbox,
    /// cursors at the owner's head. A client stopped on a schema mismatch or as a duplicate is skipped.
    /// </summary>
    public async Task SettleAsync(TimeSpan? timeout = null)
    {
        DateTimeOffset until = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        int quiet = 0;
        while (quiet < 3)
        {
            if (DateTimeOffset.UtcNow > until)
                throw new TimeoutException("система не заспокоїлась: " + string.Join("; ", Clients.Select(c => $"{c.Label}: {Describe(c)}")));
            quiet = IsQuiet() ? quiet + 1 : 0;
            await Task.Delay(100);
        }
    }

    private string Describe(ClientNode c)
    {
        AgentStatus s = c.Agent.GetStatus();
        return $"{s.State}, черга {s.Pending}, курсор {c.Agent.MinCursor}, голова {Owner.Head()}";
    }

    private bool IsQuiet()
    {
        if (Scheduled > 0)
            return false;
        long head = Owner.Head();
        foreach (ClientNode c in Clients)
        {
            if (!c.Link)
                continue;
            AgentStatus s = c.Agent.GetStatus();
            if (s.State is AgentState.SchemaMismatch or AgentState.Duplicate)
                continue;
            if (s.State != AgentState.Online || s.Pending > 0 || c.Agent.HasPendingArchiveWork || c.Agent.MinCursor < head)
                return false;
        }
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await C1.Replication.DisposeAsync();
        await C2.Replication.DisposeAsync();
        await Owner.DisposeAsync();
        Loggers.Dispose();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
    }
}
