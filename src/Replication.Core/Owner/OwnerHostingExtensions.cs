using System.IO.Compression;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Replication.Model;

namespace Replication.Owner;

/// <summary>Owner registration for any ASP.NET Core host. The host keeps URLs, ports, protocols and TLS.</summary>
public static class OwnerHostingExtensions
{
    /// <summary>Registers the owner store, the snapshot store and the housekeeping jobs.</summary>
    public static IServiceCollection AddOwnerServices(this IServiceCollection services, string dbPath, SyncModel model, OwnerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return services
            .AddSingleton(new OwnerStore(dbPath, model, options))
            .AddSingleton<SnapshotStore>()
            .AddHostedService<SyncHousekeepingJobsService>();
    }

    /// <summary>gRPC sync defaults</summary>
    public static IGrpcServerBuilder ConfigureSyncGrpcServiceOptions(this IGrpcServerBuilder builder)
    {
        return builder.AddServiceOptions<SyncGrpcService>(o =>
        {
            // Gzip: 3-5x on table data (6.1).
            o.ResponseCompressionAlgorithm = "gzip";
            o.ResponseCompressionLevel = CompressionLevel.Optimal;
            o.MaxReceiveMessageSize = 32 * 1024 * 1024;
            o.MaxSendMessageSize = 32 * 1024 * 1024;
        });
    }

    /// <summary>HTTP/2 windows and keep-alive pings</summary>
    public static void ConfigureSyncKestrelServerOptions(this KestrelServerOptions k)
    {
        // HTTP/2 windows: at least bandwidth × RTT, ~1 MB for 2 Mbit/s and 3 s (6.1).
        k.Limits.Http2.InitialStreamWindowSize = 1024 * 1024;
        k.Limits.Http2.InitialConnectionWindowSize = 2 * 1024 * 1024;
        k.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(20);
        k.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(20);
    }

    public static GrpcServiceEndpointConventionBuilder MapSyncGrpcService(this IEndpointRouteBuilder endpoints)
    {
        return endpoints.MapGrpcService<SyncGrpcService>();
    }

    /// <summary>Call after the application's own migrations and before the host starts.</summary>
    public static void InstallOwnerStore(this IServiceProvider services)
    {
        services.GetRequiredService<OwnerStore>().Install();
    }
}
