using Replication.Client;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Design 11.2, step 6: rows the owner kept wait for an archive retry; the user may archive them meanwhile.</summary>
public sealed class ArchiveRetryTests
{
    /// <summary>The race with the retry pass cannot be forced, so each width runs in a lab of its own.</summary>
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public async Task User_archive_during_an_archive_retry_converges(int clicks)
    {
        var log = new SyncLog();
        var c1 = new List<string>();
        log.Written += e =>
        {
            if (e.Source == "Клієнт 1")
            {
                lock (c1)
                    c1.Add(e.Text);
            }
        };
        await using Lab lab = await Lab.StartAsync(log: log);
        var run = new ScenarioRunner(lab, Scenarios.Find("archive"));
        await run.SetupAsync();
        await lab.SettleAsync();
        await run.NextAsync();
        await Lab.WaitAsync(() => lab.C1.Agent.HasPendingArchiveWork, TimeSpan.FromSeconds(30), "Клієнт 1 не чекає повтору перенесення");

        // The leftover parent is back in the replica, so the user can select it again, from several clicks at once.
        Guid parent = Guid.Parse(Inspect.ClientRows(lab.C1, "Category").Single(r => r.Label == "Архів").Id);
        await Task.WhenAll(Enumerable.Range(0, clicks).Select(_ => Task.Run(() => lab.C1.Agent.ArchiveAsync([("Category", parent)]), TestContext.Current.CancellationToken)));
        await lab.SettleAsync(TimeSpan.FromSeconds(60));

        Assert.Equal(AgentState.Online, lab.C1.Agent.GetStatus().State);
        Assert.False(lab.C1.Agent.HasPendingArchiveWork);
        foreach (string n in new[] { "Старий принтер", "Факс", "Каталог 2019" })
        {
            Assert.False(Inspect.OwnerHas(lab.Owner, n), n);
            Assert.True(Inspect.ClientHas(lab.C1, n, archive: true), n);
        }

        foreach (ClientNode c in lab.Clients.Where(c => c.Link))
            Assert.Empty(Inspect.Diff(lab.Owner, c));
        lock (c1)
            Assert.DoesNotContain(c1, l => l.StartsWith("зв'язок обірвався", StringComparison.Ordinal));
    }
}
