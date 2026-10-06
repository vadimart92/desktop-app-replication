using System.IO.Compression;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Replication.Owner;

/// <summary>
/// Hosts the gRPC server inside the owner process (it may be headless) and runs the background jobs:
/// tombstone cleanup (10.2), floor refresh (5.2) and the version file (5.3).
/// </summary>
public sealed class OwnerHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _jobs;

    private OwnerHost(WebApplication app, OwnerStore store, int port)
    {
        _app = app;
        Store = store;
        Port = port;
        _jobs = Task.Run(() => JobsAsync(_cts.Token));
    }

    public OwnerStore Store { get; }
    public int Port { get; }
    public string Address => $"http://127.0.0.1:{Port}";

    /// <param name="port">0 picks a free port.</param>
    /// <param name="listenAnywhere">Listen on all interfaces instead of loopback only.</param>
    public static async Task<OwnerHost> StartAsync(OwnerStore store, int port = 0, bool listenAnywhere = false)
    {
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.Limits.Http2.InitialStreamWindowSize = store.Options.Http2StreamWindowBytes;
            k.Limits.Http2.InitialConnectionWindowSize = store.Options.Http2ConnectionWindowBytes;
            k.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(20);
            k.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(20);
            // Plain HTTP/2 (h2c), no TLS in v1 (2).
            void Listen(ListenOptions o) => o.Protocols = HttpProtocols.Http2;
            if (listenAnywhere)
                k.ListenAnyIP(port, Listen);
            else
                k.Listen(System.Net.IPAddress.Loopback, port, Listen);
        });
        builder.Services.AddSingleton(store);
        builder.Services.AddSingleton<SnapshotStore>();
        builder.Services.AddGrpc(o =>
        {
            // Gzip: 3-5x on table data (6.1).
            o.ResponseCompressionAlgorithm = "gzip";
            o.ResponseCompressionLevel = CompressionLevel.Optimal;
            o.MaxReceiveMessageSize = 32 * 1024 * 1024;
            o.MaxSendMessageSize = 32 * 1024 * 1024;
            o.EnableDetailedErrors = true;
        });
        WebApplication app = builder.Build();
        app.MapGrpcService<SyncGrpcService>();
        await app.StartAsync();
        string address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        int actualPort = new Uri(address.Replace("[::]", "localhost").Replace("+", "localhost").Replace("*", "localhost")).Port;
        store.Options.Log.Write("owner", $"gRPC-сервер слухає порт {actualPort}, instance_id {store.InstanceId}", SyncLogLevel.Ok);
        return new OwnerHost(app, store, actualPort);
    }

    private async Task JobsAsync(CancellationToken ct)
    {
        DateTimeOffset lastPurge = DateTimeOffset.UtcNow;
        DateTimeOffset lastFloor = DateTimeOffset.UtcNow;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
                Store.WriteVersionFile();
                if (DateTimeOffset.UtcNow - lastFloor > TimeSpan.FromMinutes(1))
                {
                    // A client that left the activity window stops holding the base back.
                    Store.RecomputeFloor();
                    lastFloor = DateTimeOffset.UtcNow;
                }
                if (DateTimeOffset.UtcNow - lastPurge > Store.Options.PurgeInterval)
                {
                    OwnerStore.PurgeResult r = Store.Purge();
                    if (r.Tombstones > 0)
                        Store.Options.Log.Write("owner", $"очищення: видалено {r.Tombstones} tombstones, purged_version = {r.PurgedVersion}");
                    Store.IncrementalVacuum(256);
                    lastPurge = DateTimeOffset.UtcNow;
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                Store.Options.Log.Write("owner", "фонова задача: " + e.Message, SyncLogLevel.Bad);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try
        {
            await _jobs;
        }
        catch { }
        using (var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2)))
            await _app.StopAsync(stop.Token);
        await _app.DisposeAsync();
        try
        {
            Store.WriteVersionFile();
        }
        catch { }
    }
}
