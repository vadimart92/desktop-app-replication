using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Replication;
using Replication.Model;
using Replication.Owner;
using Sample.Domain;

namespace Sample.Lab;

/// <summary>Runs the owner's gRPC server on Kestrel (it may be headless). Address and protocol are decided here.</summary>
public sealed class OwnerHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private int _disposed;

    private OwnerHost(WebApplication app, OwnerStore store, int port)
    {
        _app = app;
        Store = store;
        Port = port;
    }

    public OwnerStore Store { get; }
    public int Port { get; }
    public string Address => $"http://127.0.0.1:{Port}";
    public ILoggerFactory Loggers => _app.Services.GetRequiredService<ILoggerFactory>();

    /// <param name="port">0 picks a free port.</param>
    /// <param name="listenAnywhere">Listen on all interfaces instead of loopback only.</param>
    /// <param name="logging">Logging providers of the host; none when null.</param>
    public static async Task<OwnerHost> StartAsync(string dbPath, SyncModel model, OwnerOptions options, int port = 0, bool listenAnywhere = false, Action<ILoggingBuilder>? logging = null)
    {
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        logging?.Invoke(builder.Logging);
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ConfigureSyncKestrelServerOptions();

            // Plain HTTP/2 (h2c), no TLS in v1 (2).
            void Listen(ListenOptions o) => o.Protocols = HttpProtocols.Http2;
            if (listenAnywhere)
                k.ListenAnyIP(port, Listen);
            else
                k.Listen(System.Net.IPAddress.Loopback, port, Listen);
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
        OwnerStore store = app.Services.GetRequiredService<OwnerStore>();
        int actualPort = BindingAddress.Parse(app.Urls.First()).Port;
        app.Services.GetRequiredService<ILogger<OwnerHost>>().LogInformation("gRPC-сервер слухає порт {Port}, instance_id {InstanceId}", actualPort, store.InstanceId);
        return new OwnerHost(app, store, actualPort);
    }

    /// <summary>Stops the server; a second call does nothing, so a test may stop an owner before the lab is disposed.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        using (var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2)))
            await _app.StopAsync(stop.Token);
        await _app.DisposeAsync();
    }
}
