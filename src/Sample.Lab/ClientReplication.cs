using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Replication.Client;
using Replication.Model;

namespace Sample.Lab;

/// <summary>
/// The client side of the application: one database for all instances, one <see cref="WriteRouter"/>, and one
/// <see cref="SyncAgent"/> per owner the client connects to (4). The services are built and set up once.
/// </summary>
/// <example>
/// <code>
/// ServiceProvider services = ClientReplication.BuildServices("client.db", model, s => s.AddSampleDb("client.db", sp => [sp.GetRequiredService&lt;WriteRouter&gt;()]), loggerFactory);
/// services.MigrateSampleDb().InstallClientStore();
/// var replication = new ClientReplication(services);
/// SyncAgent agent = replication.Connect("http://10.0.0.5:5005", "Склад");
/// agent.LinkEnabled = true;
/// </code>
/// </example>
public sealed class ClientReplication : IAsyncDisposable
{
    private readonly List<SyncAgent> _agents = [];

    /// <summary>The agents log through the services' <see cref="ILoggerFactory"/>; nowhere without one.</summary>
    public ClientReplication(IServiceProvider services)
    {
        Services = services;
        Loggers = services.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        Store = services.GetRequiredService<ClientStore>();
        Router = services.GetRequiredService<WriteRouter>();
    }

    public IServiceProvider Services { get; }

    public ClientStore Store { get; }

    public WriteRouter Router { get; }

    public SyncModel Model => Store.Model;

    public ILoggerFactory Loggers { get; }

    public string ClientId => Store.ClientId;

    public IReadOnlyList<SyncAgent> Agents
    {
        get
        {
            lock (_agents)
                return [.. _agents];
        }
    }

    /// <param name="configure">Registers the application's DbContext factory.</param>
    /// <param name="loggers">Loggers of the router and the agents; none when null. The caller disposes them.</param>
    public static ServiceProvider BuildServices(string dbPath, SyncModel model, Action<IServiceCollection>? configure = null, ILoggerFactory? loggers = null)
    {
        var services = new ServiceCollection();
        if (loggers is not null)
            services.AddSingleton(loggers);
        services.AddClientServices(dbPath, model);
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Starts an agent for the owner at <paramref name="address"/> (link off). The agent learns the owner's instance id on
    /// the first link, and <see cref="SyncAgent.InstanceId"/> stays null until then.
    /// </summary>
    /// <param name="wrapStream">Decorates the agent's TCP stream (see <see cref="SyncAgent"/>); none when null.</param>
    public SyncAgent Connect(string address, string name, AgentOptions? options = null, Func<Stream, Stream>? wrapStream = null)
    {
        var agent = new SyncAgent(Store, Loggers.CreateLogger<SyncAgent>(), address, name, options, wrapStream);
        lock (_agents)
            _agents.Add(agent);
        agent.Start();
        return agent;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (SyncAgent a in Agents)
            await a.DisposeAsync();
        if (Services is IAsyncDisposable d)
            await d.DisposeAsync();
    }
}
