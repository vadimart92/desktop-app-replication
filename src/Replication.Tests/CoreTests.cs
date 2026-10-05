using Replication.Model;
using Xunit;

namespace Replication.Tests;

public class CursorStateTests
{
    [Fact]
    public void Ranges_merge_and_lift_the_cursor()
    {
        var k = new CursorState(10);
        k.AddRange(40, 50);
        k.AddRange(20, 30);
        Assert.Equal([(20L, 30L), (40L, 50L)], k.Ranges);
        Assert.Equal([(50L, 60L), (30L, 40L), (10L, 20L)], k.Gaps(60));
        k.AddRange(30, 40);
        Assert.Equal([(20L, 50L)], k.Ranges);
        k.AddRange(10, 20);
        Assert.Equal(50, k.Cursor);
        Assert.Empty(k.Ranges);
    }

    [Fact]
    public void Wire_encoding_round_trips()
    {
        var k = new CursorState(1000, [(1500L, 1600L), (2000L, 2100L)]);
        var back = CursorState.FromWire(k.ToWire("T"));
        Assert.Equal(k.Cursor, back.Cursor);
        Assert.Equal(k.Ranges, back.Ranges);
    }

    [Fact]
    public void Head_lifts_only_tables_without_ranges()
    {
        var a = new CursorState(5);
        var b = new CursorState(5, [(7L, 9L)]);
        a.LiftTo(20);
        b.LiftTo(20);
        Assert.Equal(20, a.Cursor);
        Assert.Equal(5, b.Cursor);
    }
}

public class EmptyReplicaTests
{
    [Fact]
    public async Task Empty_replica_fills_from_the_stream_after_tombstones_were_purged()
    {
        await using var lab = await Sample.Lab.Lab.StartAsync(configureOwner: o => o.CatchupBatchRows = 4);
        await lab.Owner.DeleteItemAsync("Скотч");
        await lab.Owner.DeleteItemAsync("Маркери");
        var purged = await lab.Owner.PurgeAsync(); // no clients yet: every tombstone goes, purged_version > 0
        Assert.True(purged.PurgedVersion > 0);

        lab.C2.Agent.Options.SnapshotMode = Replication.Client.SnapshotMode.EmptyReplica;
        await lab.SyncNowAsync(lab.C2);
        await lab.SettleAsync();
        Assert.Empty(Sample.Lab.Inspect.Diff(lab.Owner, lab.C2));
    }
}

/// <summary>Design 11.6: a parent that archived rows point to is moved to the archive instead of deleted.</summary>
public class ArchiveParentTests
{
    private static async Task<Sample.Lab.Lab> ArchivedCatalogAsync()
    {
        var lab = await Sample.Lab.Lab.StartAsync();
        await lab.SyncNowAsync(lab.C1);
        await lab.SyncNowAsync(lab.C2);
        await lab.C1.ArchiveAsync(("Item", "Каталог 2019"));
        await lab.SettleAsync();
        Assert.True(Sample.Lab.Inspect.ClientHas(lab.C1, "Каталог 2019", archive: true));
        return lab;
    }

    private static void AssertArchiveIntact(Sample.Lab.Lab lab)
    {
        Assert.True(Sample.Lab.Inspect.ClientHas(lab.C1, "Каталог 2019", archive: true));
        Assert.Contains(Sample.Lab.Inspect.ClientRows(lab.C1, "Category"), r => r.Label == "Офіс" && r.Mark == "архів");
        Assert.False(Sample.Lab.Inspect.OwnerHas(lab.Owner, "Степлер")); // the owner cascaded the live children
        foreach (var c in lab.Clients) Assert.Empty(Sample.Lab.Inspect.Diff(lab.Owner, c));
    }

    [Fact]
    public async Task Local_delete_moves_the_parent_to_the_archive()
    {
        await using var lab = await ArchivedCatalogAsync();
        await lab.C1.DeleteCategoryAsync("Офіс");
        await lab.SettleAsync();
        AssertArchiveIntact(lab);
    }

    [Fact]
    public async Task Tombstone_from_another_client_moves_the_parent_to_the_archive()
    {
        await using var lab = await ArchivedCatalogAsync();
        await lab.C2.DeleteCategoryAsync("Офіс");
        await lab.SettleAsync();
        AssertArchiveIntact(lab);
    }
}
