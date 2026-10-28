using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;

namespace Replication.Tests;

/// <summary>Design 11.6 over an FK cycle: the archive check and the move to the archive visit each row once.</summary>
public sealed class ArchiveGuardTests
{
    [Test]
    public async Task Bulk_delete_of_a_row_pointing_to_itself_deletes_it()
    {
        var root = new Node { Name = "Корінь" };
        await using GraphPair pair = await GraphPair.StartAsync(owner => GraphPair.SeedTree(owner, root));

        Assert.That(await pair.Agent.DeleteWhereAsync("Node", new Predicate("Name", "Корінь")), Is.EqualTo(1));
        await pair.SyncAsync();

        await using GraphDbContext owner = pair.OwnerDb();
        Assert.That(await owner.Nodes.ToListAsync(TestContext.CurrentContext.CancellationToken), Is.Empty);
        await using GraphDbContext client = pair.ClientDb();
        Assert.That(await client.Nodes.ToListAsync(TestContext.CurrentContext.CancellationToken), Is.Empty);
    }

    [Test]
    public async Task Bulk_delete_of_a_row_pointing_to_itself_moves_it_to_the_archive_below_which_rows_were_archived()
    {
        var root = new Node { Name = "Корінь" };
        var leaf = new Node { Name = "Лист", ParentId = root.Id };
        await using GraphPair pair = await GraphPair.StartAsync(owner => GraphPair.SeedTree(owner, root, leaf));
        Assert.That(await pair.Agent.ArchiveAsync([("Node", leaf.Id)]), Does.StartWith("archive:"));
        await pair.SyncAsync();

        Assert.That(await pair.Agent.DeleteWhereAsync("Node", new Predicate("Name", "Корінь")), Is.EqualTo(1));
        await pair.SyncAsync();

        await using GraphDbContext owner = pair.OwnerDb();
        Assert.That(await owner.Nodes.ToListAsync(TestContext.CurrentContext.CancellationToken), Is.Empty);
        await using GraphDbContext client = pair.ClientDb();
        string archive = SyncColumns.ArchiveOf(pair.Instance);
        Assert.That(await client.Nodes.Select(x => EF.Property<string>(x, SyncColumns.InstanceId)).ToListAsync(TestContext.CurrentContext.CancellationToken), Is.EqualTo(new[] { archive, archive }));
    }
}
