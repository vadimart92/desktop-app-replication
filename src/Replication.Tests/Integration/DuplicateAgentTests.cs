using NUnit.Framework;
using Replication.Client;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>One agent per owner instance (4): a second agent of the same client that reaches the same owner does not subscribe.</summary>
[TestFixture]
public sealed class DuplicateAgentTests : LabTests
{
    [Test]
    public async Task Second_agent_for_the_same_owner_turns_its_link_off_and_takes_over_once_the_first_is_gone()
    {
        Lab lab = await StartSyncedAsync();
        SyncAgent second = lab.C1.Replication.Connect(lab.Owner.Address, "дубль");
        second.LinkEnabled = true;

        await Lab.WaitAsync(() => second.GetStatus().State == AgentState.Duplicate, TimeSpan.FromSeconds(30), "no Duplicate");
        await lab.Owner.SetPriceAsync("Палета", 470);
        await lab.SettleAsync();
        Assert.Multiple(() =>
        {
            Assert.That(second.InstanceId, Is.Null);
            Assert.That(second.LinkEnabled, Is.False, "the agent turned its link off");
            Assert.That(second.GetStatus().LastError, Does.Contain(lab.C1.Agent.Name));
            Assert.That(lab.C1.Agent.GetStatus().State, Is.EqualTo(AgentState.Online), "the first agent keeps the replica");
            Assert.That(lab.Diff(lab.C1), Is.Empty);
            Assert.That(OwnerLogAbout(lab.C1).Count(l => l.Contains("Start accepted", StringComparison.Ordinal)), Is.EqualTo(1));
        });

        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.That(second.GetStatus().State, Is.EqualTo(AgentState.Duplicate), "no retry on its own");

        await lab.C1.Agent.DisposeAsync();
        second.LinkEnabled = true;
        await Lab.WaitAsync(() => second.GetStatus().State == AgentState.Online, TimeSpan.FromSeconds(30), "the second agent did not take over");

        Assert.Multiple(() =>
        {
            Assert.That(second.InstanceId, Is.EqualTo(lab.Owner.Store.InstanceId));
            Assert.That(second.GetStatus().Cursors, Is.EqualTo(lab.C1.Agent.GetStatus().Cursors), "the replica and its cursors are taken over, no snapshot");
        });
    }
}
