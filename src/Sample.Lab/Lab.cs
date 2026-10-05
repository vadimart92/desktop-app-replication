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
    private int _scheduled;

    private Lab(string dir, SyncLog log, OwnerNode owner, ClientNode c1, ClientNode c2)
    {
        Dir = dir;
        Log = log;
        Owner = owner;
        C1 = c1;
        C2 = c2;
    }

    public string Dir { get; }
    public SyncLog Log { get; }
    public OwnerNode Owner { get; }
    public ClientNode C1 { get; }
    public ClientNode C2 { get; }
    public IReadOnlyList<ClientNode> Clients => [C1, C2];

    /// <summary>Background actions scheduled by a step (<see cref="When"/>, <see cref="Later"/>) that have not run yet.</summary>
    public int Scheduled => Volatile.Read(ref _scheduled);

    public static async Task<Lab> StartAsync(string? dir = null, SyncLog? log = null, Action<OwnerOptions>? configureOwner = null)
    {
        dir ??= Path.Combine(Path.GetTempPath(), "replication-lab", $"{DateTime.Now:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..6]}");
        Directory.CreateDirectory(dir);
        log ??= new SyncLog();
        var owner = await OwnerNode.StartAsync(Path.Combine(dir, "owner.db"), log, configure: o =>
        {
            o.OnlineInterval = TimeSpan.FromMilliseconds(300);
            configureOwner?.Invoke(o);
        });
        AgentOptions Opt() => new() { AckInterval = TimeSpan.FromMilliseconds(300), ApplyTimeout = TimeSpan.FromSeconds(15), LagWarningWindow = TimeSpan.FromSeconds(6) };
        var c1 = ClientNode.Create("c1", "Клієнт 1", Path.Combine(dir, "client1.db"), owner.Address, log, Opt());
        var c2 = ClientNode.Create("c2", "Клієнт 2", Path.Combine(dir, "client2.db"), owner.Address, log, Opt());
        return new Lab(dir, log, owner, c1, c2);
    }

    public ClientNode C(string id) => id == "c1" ? C1 : C2;

    public void Say(string text, SyncLogLevel level = SyncLogLevel.Info) => Log.Write("сценарій", text, level);

    /// <summary>Turns the link on and waits until the client is online (the demo's syncNow).</summary>
    public async Task SyncNowAsync(ClientNode c, TimeSpan? timeout = null)
    {
        c.Link = true;
        await WaitAsync(() => c.Agent.GetStatus().State == AgentState.Online, timeout ?? TimeSpan.FromSeconds(30), $"{c.Label} не вийшов онлайн");
    }

    public static async Task WaitAsync(Func<bool> condition, TimeSpan timeout, string what)
    {
        var until = DateTimeOffset.UtcNow + timeout;
        while (!condition())
        {
            if (DateTimeOffset.UtcNow > until) throw new TimeoutException(what);
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
                var until = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(60));
                while (!_cts.IsCancellationRequested && DateTimeOffset.UtcNow < until)
                {
                    bool ok;
                    try { ok = condition(); } catch { ok = false; }
                    if (ok)
                    {
                        await action();
                        return;
                    }
                    await Task.Delay(20, _cts.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e) { Say("відкладена дія: " + e.Message, SyncLogLevel.Bad); }
            finally { Interlocked.Decrement(ref _scheduled); }
        });
    }

    /// <summary>Runs <paramref name="action"/> after a delay (the demo's later()).</summary>
    public void Later(TimeSpan delay, Func<Task> action)
    {
        var at = DateTimeOffset.UtcNow + delay;
        When(() => DateTimeOffset.UtcNow >= at, action);
    }

    /// <summary>
    /// Waits until everything settles: no scheduled actions, every connected client online with an empty outbox,
    /// cursors at the owner's head. A client stopped on a schema mismatch is skipped.
    /// </summary>
    public async Task SettleAsync(TimeSpan? timeout = null)
    {
        var until = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        var quiet = 0;
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
        var s = c.Agent.GetStatus();
        return $"{s.State}, черга {s.Pending}, курсор {c.Agent.MinCursor}, голова {Owner.Head()}";
    }

    public bool IsQuiet()
    {
        if (Scheduled > 0) return false;
        var head = Owner.Head();
        foreach (var c in Clients)
        {
            if (!c.Link) continue;
            var s = c.Agent.GetStatus();
            if (s.State == AgentState.SchemaMismatch) continue;
            if (s.State != AgentState.Online || s.Pending > 0 || c.Agent.HasPendingArchiveWork || c.Agent.MinCursor < head) return false;
        }
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await C1.DisposeAsync();
        await C2.DisposeAsync();
        await Owner.DisposeAsync();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
    }
}
