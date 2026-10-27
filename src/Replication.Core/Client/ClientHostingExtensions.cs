using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Replication.Model;

namespace Replication.Client;

/// <summary>Client registration: one store and one router per client database, shared by every <see cref="SyncAgent"/>.</summary>
public static class ClientHostingExtensions
{
    /// <summary>
    /// Registers <see cref="ClientStore"/> and <see cref="WriteRouter"/>; pass the router to the client DbContext explicitly.
    /// The router logs through the registered <see cref="ILoggerFactory"/>, if any.
    /// </summary>
    public static IServiceCollection AddClientServices(this IServiceCollection services, string dbPath, SyncModel model) =>
        services
            .AddSingleton(new ClientStore(dbPath, model))
            .AddSingleton(sp => new WriteRouter(model));

    /// <summary>Call after the application's own migrations.</summary>
    public static IServiceProvider InstallClientStore(this IServiceProvider services)
    {
        services.GetRequiredService<ClientStore>().Install();
        return services;
    }
}
