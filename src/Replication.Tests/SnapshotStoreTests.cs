using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Snapshot files on the owner: kept a few hours, then removed (design 6.3).</summary>
public class SnapshotStoreTests
{
    [Fact]
    public async Task New_snapshot_removes_old_files_left_by_an_earlier_run()
    {
        string dir = Path.Combine(Path.GetTempPath(), "replication-lab", $"{DateTime.Now:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..6]}");
        string snaps = Path.Combine(dir, "snapshots");
        Directory.CreateDirectory(snaps);
        string orphan = Path.Combine(snaps, "snap-orphan.db");
        File.WriteAllBytes(orphan, [1, 2, 3]);
        File.SetLastWriteTimeUtc(orphan, DateTime.UtcNow.AddDays(-1));
        // Young enough to belong to another owner that shares the folder.
        File.WriteAllBytes(Path.Combine(snaps, "snap-fresh.db"), [1, 2, 3]);

        await using Lab lab = await Lab.StartAsync(dir, configureOwner: o => o.SnapshotKeep = TimeSpan.FromHours(1));
        await lab.SyncNowAsync(lab.C2);

        List<string> left = [.. Directory.GetFiles(snaps, "snap-*.db").Select(f => Path.GetFileName(f))];
        Assert.DoesNotContain("snap-orphan.db", left);
        Assert.Contains("snap-fresh.db", left);
        Assert.Equal(2, left.Count);
    }
}
