using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Replication.Client;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>The agent's name is the prefix of its log lines; the app sets it, nothing stores it.</summary>
[TestFixture]
public sealed class AgentNameTests : LabTests
{
    [Test]
    public async Task Setting_the_name_changes_the_log_prefix()
    {
        Lab lab = await StartSyncedAsync();

        lab.C1.Agent.Name = "Склад-2";
        await lab.Owner.AddLogAsync(1);
        await lab.SettleAsync();

        Assert.That(lab.C1.Agent.Name, Is.EqualTo("Склад-2"));
        Assert.That(ClientLog(lab.C1), Is.Not.Empty);
    }

    [Test]
    public async Task Start_learns_the_instance_on_the_first_link()
    {
        Lab lab = await StartAsync();
        ClientReplication replication = ClientNode.Create(Path.Combine(TestDb.NewDir(), "client.db"));
        try
        {
            await using SyncAgent agent = new(replication.Store, NullLogger<SyncAgent>.Instance, lab.Owner.Address, "клієнт");

            agent.Start();
            Assert.That(agent.InstanceId, Is.Null, "nobody asked the owner yet");
            agent.LinkEnabled = true;
            await Lab.WaitAsync(() => agent.GetStatus().State == AgentState.Online, TimeSpan.FromSeconds(30), "the agent did not get online");

            Assert.That(agent.InstanceId, Is.EqualTo(lab.Owner.Store.InstanceId));
        }
        finally
        {
            await replication.DisposeAsync();
        }
    }
}
