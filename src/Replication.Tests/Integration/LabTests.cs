using NUnit.Framework;
using Replication.Owner;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Base for tests on the lab: an owner seeded with 3 categories, 9 items and 3 log entries, and two clients, each with
/// its own SQLite file, over real gRPC on localhost. The log of the test is written to the test output.
/// </summary>
public abstract class LabTests
{
    private TestLog _log = new();
    private Lab? _lab;

    protected Lab Lab => _lab ?? throw new InvalidOperationException("call StartAsync first");

    protected async Task<Lab> StartAsync(Action<OwnerOptions>? configureOwner = null)
    {
        _lab = await Lab.StartAsync(configureOwner: configureOwner, logging: _log.AddTo);
        return _lab;
    }

    /// <summary>A lab with both clients online and caught up.</summary>
    protected async Task<Lab> StartSyncedAsync(Action<OwnerOptions>? configureOwner = null)
    {
        Lab lab = await StartAsync(configureOwner);
        await lab.SyncNowAsync(lab.C1);
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();
        return lab;
    }

    /// <summary>Lines the owner wrote about the session of <paramref name="client"/>.</summary>
    protected List<string> OwnerLogAbout(ClientNode client) => _log.About("Replication.Owner.", client.Replication.ClientId[..8]);

    /// <summary>Lines the agent of <paramref name="client"/> wrote.</summary>
    protected List<string> ClientLog(ClientNode client) => _log.OfAgent(client.Agent.Name);

    [TearDown]
    public async Task StopLabAsync()
    {
        if (_lab is not null)
            await _lab.DisposeAsync();
        _lab = null;
        _log.WriteTo(TestContext.Out);
        _log.Dispose();
        _log = new TestLog();
    }
}
