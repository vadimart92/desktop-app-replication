using Replication.Owner;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Tombstone cleanup and the activity window on the owner (design 5.2, 10.2).</summary>
public class PurgeTests
{
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
}
