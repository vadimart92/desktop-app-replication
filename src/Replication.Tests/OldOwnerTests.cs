using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Replication.Client;
using Replication.Protocol;
using Replication.Tests.Integration;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>An owner older than the Identity call answers Unimplemented: the agent stops as on SchemaMismatch instead of retrying.</summary>
public sealed class OldOwnerTests
{
    [Test]
    public async Task Owner_without_Identity_stops_the_agent_as_SchemaMismatch_with_the_link_off()
    {
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k => k.Listen(System.Net.IPAddress.Loopback, 0, o => o.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc();
        await using WebApplication app = builder.Build();
        app.MapGrpcService<OldSync>();
        await app.StartAsync();
        string address = $"http://127.0.0.1:{BindingAddress.Parse(app.Urls.First()).Port}";

        await using ClientReplication replication = ClientNode.Create(Path.Combine(TestDb.NewDir(), "client.db"));
        await using var agent = new SyncAgent(replication.Store, NullLogger<SyncAgent>.Instance, address, "клієнт");
        agent.Start();
        agent.LinkEnabled = true;

        await Lab.WaitAsync(() => agent.GetStatus().State == AgentState.SchemaMismatch, TimeSpan.FromSeconds(30), $"no SchemaMismatch; last: {agent.GetStatus().LastError}");

        Assert.That(agent.LinkEnabled, Is.False, "no retry: the owner answers the same until it is updated");
        Assert.That(agent.GetStatus().LastError, Does.Contain("Identity"));
        await app.StopAsync();
    }

    /// <summary>The service as an owner without Identity serves it: the base class answers Unimplemented.</summary>
    private sealed class OldSync : Sync.SyncBase;
}
