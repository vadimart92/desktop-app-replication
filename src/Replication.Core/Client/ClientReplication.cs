using Replication.Model;

namespace Replication.Client;

/// <summary>
/// Entry point on the client: one database for all instances, a WriteRouter for the app's DbContext,
/// and one <see cref="SyncAgent"/> per owner the client connects to (4).
/// </summary>
/// <example>
/// <code>
/// var model = SyncModel.From(db);                      // once, from the shared DbContext
/// var replication = new ClientReplication("client.db", model);
/// replication.Install();                               // after EnsureCreated / migrations
/// options.AddInterceptors(replication.Router);         // edits of remote rows go to the outbox
/// var agent = replication.Connect("http://10.0.0.5:5005", "Склад");
/// agent.LinkEnabled = true;
/// </code>
/// </example>
public sealed class ClientReplication : IAsyncDisposable
{
    private readonly List<SyncAgent> _agents = [];
    private readonly SyncLog _log;

    public ClientReplication(string dbPath, SyncModel model, SyncLog? log = null)
    {
        _log = log ?? SyncLog.Null;
        Store = new ClientStore(dbPath, model);
        Router = new WriteRouter(model);
        Router.OutboxChanged += instance =>
        {
            foreach (SyncAgent a in Agents.Where(a => a.InstanceId == instance))
                a.NotifyOutbox();
        };
        Router.Routed += (instance, text) =>
        {
            SyncAgent? agent = Agents.FirstOrDefault(a => a.InstanceId == instance);
            _log.Write(agent?.Label ?? instance, text);
        };
    }

    public ClientStore Store { get; }

    /// <summary>Add to the client DbContext options: <c>options.AddInterceptors(replication.Router)</c>.</summary>
    public WriteRouter Router { get; }

    public SyncModel Model => Store.Model;

    public string ClientId => Store.ClientId;

    public IReadOnlyList<SyncAgent> Agents
    {
        get
        {
            lock (_agents)
                return [.. _agents];
        }
    }

    public void Install() => Store.Install();

    public SyncAgent Connect(string address, string name, AgentOptions? options = null, string? label = null)
    {
        var agent = new SyncAgent(Store, address, name, options, _log, label);
        lock (_agents)
            _agents.Add(agent);
        agent.Start();
        return agent;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (SyncAgent a in Agents)
            await a.DisposeAsync();
    }
}
