using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Every scenario of the demo page, run against the real core over gRPC on localhost.</summary>
public class ScenarioTests(ITestOutputHelper output)
{
    public static TheoryData<string> Ids => [.. Scenarios.All.Select(s => s.Id)];

    [Theory]
    [MemberData(nameof(Ids))]
    public async Task Scenario_converges(string id)
    {
        var log = new SyncLog();
        var lines = new List<string>();
        log.Written += e =>
        {
            lock (lines)
                lines.Add($"{e.At:HH:mm:ss.fff} [{e.Source}] {e.Text}");
        };
        await using Lab lab = await Lab.StartAsync(log: log);
        var run = new ScenarioRunner(lab, Scenarios.Find(id));
        List<string> problems;
        try
        {
            await run.SetupAsync();
            await lab.SettleAsync();
            while (!run.Done)
            {
                await run.NextAsync();
                await lab.SettleAsync(TimeSpan.FromSeconds(60));
            }
            problems = await run.VerifyAsync(TimeSpan.FromSeconds(60));
        }
        finally
        {
            lock (lines)
            {
                foreach (string l in lines)
                    output.WriteLine(l);
            }
        }
        Assert.Empty(problems);
    }
}
