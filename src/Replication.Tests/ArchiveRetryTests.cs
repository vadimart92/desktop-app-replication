using NUnit.Framework;
using Replication.Client;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Design 11.2, step 6: rows the owner kept wait for an archive retry; the user may archive them meanwhile.</summary>
public sealed class ArchiveRetryTests
{
    /// <summary>The race with the retry pass cannot be forced, so each width runs in a lab of its own.</summary>
    [TestCase(2)]
    [TestCase(4)]
    [TestCase(8)]
    [TestCase(16)]
    public async Task User_archive_during_an_archive_retry_converges(int clicks)
    {
        using var log = new TestLog();
        await using Lab lab = await Lab.StartAsync(logging: log.AddTo);
        var run = new ScenarioRunner(lab, Scenarios.Find("archive"));
        await run.SetupAsync();
        await lab.SettleAsync();
        await run.NextAsync();
        await Lab.WaitAsync(() => lab.C1.Agent.HasPendingArchiveWork, TimeSpan.FromSeconds(30), "Клієнт 1 не чекає повтору перенесення");

        // The leftover parent is back in the replica, so the user can select it again, from several clicks at once.
        Guid parent = Guid.Parse(Inspect.ClientRows(lab.C1, "Category").Single(r => r.Label == "Архів").Id);
        await Task.WhenAll(Enumerable.Range(0, clicks).Select(_ => Task.Run(() => lab.C1.Agent.ArchiveAsync([("Category", parent)]), TestContext.CurrentContext.CancellationToken)));
        await lab.SettleAsync(TimeSpan.FromSeconds(60));

        Assert.That(lab.C1.Agent.GetStatus().State, Is.EqualTo(AgentState.Online));
        Assert.That(lab.C1.Agent.HasPendingArchiveWork, Is.False);
        foreach (string n in new[] { "Старий принтер", "Факс", "Каталог 2019" })
        {
            Assert.That(Inspect.OwnerHas(lab.Owner, n), Is.False, n);
            Assert.That(Inspect.ClientHas(lab.C1, n, archive: true), Is.True, n);
        }

        foreach (ClientNode c in lab.Clients.Where(c => c.Link))
            Assert.That(Inspect.Diff(lab.Owner, c), Is.Empty);
        Assert.That(log.OfAgent(lab.C1.Agent.Name), Has.None.Matches<string>(l => l.StartsWith("link lost", StringComparison.Ordinal)));
    }
}
