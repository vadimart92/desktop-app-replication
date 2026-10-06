using Replication.Owner;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Tombstone cleanup and the activity window on the owner (design 5.2, 10.2).</summary>
public sealed class PurgeTests
{
    [Fact]
    public async Task Purge_takes_every_tombstone_in_batches_and_reports_the_count()
    {
        await using Lab lab = await Lab.StartAsync();
        long head = lab.Owner.Head();
        long before = OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM _sync_tombstones");
        // More than two batches of 1000, with versions above every real tombstone.
        long added = OwnerSql.Scalar(lab, $"""
            WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2500)
            INSERT INTO _sync_tombstones(tbl, pk, version, deleted_at) SELECT 'Item', 'gone-' || i, {head} + i, 0 FROM n;
            SELECT changes();
            """);
        Assert.Equal(2500, added);

        // No client is registered: the floor is open and every tombstone goes.
        OwnerStore.PurgeResult r = await lab.Owner.PurgeAsync();

        Assert.Equal(before + added, r.Tombstones);
        Assert.Equal(head + added, r.PurgedVersion);
        Assert.Equal(OwnerStore.NoFloor, r.Floor);
        Assert.Equal(0, OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM _sync_tombstones"));
    }

    [Fact]
    public async Task Clock_jump_forgets_a_client_whose_stream_has_ended()
    {
        await using Lab lab = await Lab.StartAsync();
        await lab.SyncNowAsync(lab.C1);
        await lab.SyncNowAsync(lab.C2);
        string c2 = lab.C2.Replication.ClientId;
        lab.C2.Link = false;
        await Lab.WaitAsync(() => !lab.Owner.Store.IsSubscribed(c2), TimeSpan.FromSeconds(10), "сесія Клієнта 2 не закрилась");

        lab.Owner.AdvanceClock(TimeSpan.FromDays(31));
        OwnerStore.PurgeResult r = await lab.Owner.PurgeAsync();

        List<ClientView> clients = Inspect.Clients(lab.Owner);
        Assert.DoesNotContain(clients, x => x.ClientId == c2);
        Assert.Equal(1, r.Clients);
        Assert.Equal(clients.Single().Acked, r.Floor);
    }

    [Fact]
    public async Task Age_rule_keeps_tombstones_a_connected_client_still_catches_up_on()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o => o.CatchupBatchRows = 2);
        await lab.SyncNowAsync(lab.C1);
        await lab.SyncNowAsync(lab.C2);
        string c2 = lab.C2.Replication.ClientId;
        lab.C2.Link = false;
        await Lab.WaitAsync(() => !lab.Owner.Store.IsSubscribed(c2), TimeSpan.FromSeconds(10), "сесія Клієнта 2 не закрилась");

        // The deletion lands in the bottom gap of Клієнт 2, which newest-first catch-up sends last.
        await lab.Owner.DeleteItemAsync("Скотч");
        for (int i = 0; i < 12; i++)
            await lab.Owner.NewItemAsync();
        lab.Owner.Store.Options.Faults.DelayStream(c2, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60));
        lab.C2.Link = true;
        await Lab.WaitAsync(() => lab.Owner.Store.IsSubscribed(c2), TimeSpan.FromSeconds(10), "Клієнт 2 не підключився");

        // The tombstone ages out while Клієнт 2 is still catching up.
        lab.Owner.AdvanceClock(TimeSpan.FromDays(31));
        await lab.Owner.PurgeAsync();

        await lab.SettleAsync(TimeSpan.FromSeconds(60));
        Assert.Empty(Inspect.Diff(lab.Owner, lab.C2));
    }
}
