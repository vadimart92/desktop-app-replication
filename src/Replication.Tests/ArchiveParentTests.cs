using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Design 11.6: a parent that archived rows point to is moved to the archive instead of deleted.</summary>
public sealed class ArchiveParentTests
{
    private static async Task ArchiveCatalogAsync(Lab lab)
    {
        await lab.SyncNowAsync(lab.C1);
        await lab.SyncNowAsync(lab.C2);
        await lab.C1.ArchiveAsync(("Item", "Каталог 2019"));
        await lab.SettleAsync();
        Assert.True(Inspect.ClientHas(lab.C1, "Каталог 2019", archive: true));
    }

    private static void AssertArchiveIntact(Lab lab, string category = "Офіс")
    {
        Assert.True(Inspect.ClientHas(lab.C1, "Каталог 2019", archive: true));
        Assert.Contains(Inspect.ClientRows(lab.C1, "Category"), r => r.Label == category && r.IsArchived);
        // The owner cascaded the live children.
        Assert.False(Inspect.OwnerHas(lab.Owner, "Степлер"));
        foreach (ClientNode c in lab.Clients)
            Assert.Empty(Inspect.Diff(lab.Owner, c));
    }

    [Fact]
    public async Task Local_delete_moves_the_parent_to_the_archive()
    {
        await using Lab lab = await Lab.StartAsync();
        await ArchiveCatalogAsync(lab);
        await lab.C1.DeleteCategoryAsync("Офіс");
        await lab.SettleAsync();
        AssertArchiveIntact(lab);
    }

    [Fact]
    public async Task Tombstone_from_another_client_moves_the_parent_to_the_archive()
    {
        await using Lab lab = await Lab.StartAsync();
        await ArchiveCatalogAsync(lab);
        await lab.C2.DeleteCategoryAsync("Офіс");
        await lab.SettleAsync();
        AssertArchiveIntact(lab);
    }

    [Fact]
    public async Task Bulk_delete_moves_the_parent_to_the_archive()
    {
        await using Lab lab = await Lab.StartAsync();
        await ArchiveCatalogAsync(lab);
        await lab.C1.Agent.DeleteWhereAsync("Category", new Predicate("Name", "Офіс"));
        await lab.SettleAsync();
        AssertArchiveIntact(lab);
    }

    [Fact]
    public async Task Ignored_reply_moves_the_parent_to_the_archive()
    {
        await using Lab lab = await Lab.StartAsync();
        await ArchiveCatalogAsync(lab);
        string office = Inspect.ClientRows(lab.C1, "Category").Single(r => r.Label == "Офіс" && r.InstanceId == lab.C1.Instance).Id;
        // C1's stream is held, so the reply to its patch overtakes the tombstone: Ignored, and C1 removes the row itself (6.5, 9.1).
        OwnerFaults faults = lab.Owner.Store.Options.Faults;
        faults.DelayStream(lab.C1.Replication.ClientId, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
        await lab.Owner.DeleteCategoryAsync("Офіс");
        await lab.C1.RenameCategoryAsync(Guid.Parse(office), "Офіс 2");
        await Lab.WaitAsync(() => lab.C1.Agent.GetStatus().Pending == 0, TimeSpan.FromSeconds(30), "Клієнт 1 не отримав ApplyReply");
        Assert.Contains(Inspect.ClientRows(lab.C1, "Category"), r => r.Id == office && r.IsArchived);

        // A reconnect drops the held send; catch-up brings the tombstones, which find nothing left to delete.
        faults.DelayStream(lab.C1.Replication.ClientId, TimeSpan.Zero, TimeSpan.Zero);
        await lab.GoOfflineAsync(lab.C1);
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();
        AssertArchiveIntact(lab, "Офіс 2");
    }
}
