using Microsoft.EntityFrameworkCore;
using Replication.Client;
using Replication.Model;
using Xunit;

namespace Replication.Tests;

/// <summary>Design 11.6 over an FK cycle: the archive check and the move to the archive visit each row once.</summary>
public class ArchiveGuardTests
{
    [Fact]
    public async Task Bulk_delete_of_a_row_pointing_to_itself_deletes_it()
    {
        var root = new Node { Name = "Корінь" };
        await using GraphPair pair = await GraphPair.StartAsync(owner => SeedTree(owner, root));

        Assert.Equal(1, await pair.Agent.DeleteWhereAsync("Node", new Predicate("Name", "Корінь")));
        await pair.SyncAsync();

        await using GraphDbContext owner = pair.OwnerDb();
        Assert.Empty(await owner.Nodes.ToListAsync(TestContext.Current.CancellationToken));
        await using GraphDbContext client = pair.ClientDb();
        Assert.Empty(await client.Nodes.ToListAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Bulk_delete_of_a_row_pointing_to_itself_moves_it_to_the_archive_below_which_rows_were_archived()
    {
        var root = new Node { Name = "Корінь" };
        var leaf = new Node { Name = "Лист", ParentId = root.Id };
        await using GraphPair pair = await GraphPair.StartAsync(owner => SeedTree(owner, root, leaf));
        Assert.StartsWith("перенесення в архів", await pair.Agent.ArchiveAsync([("Node", leaf.Id)]), StringComparison.Ordinal);
        await pair.SyncAsync();

        Assert.Equal(1, await pair.Agent.DeleteWhereAsync("Node", new Predicate("Name", "Корінь")));
        await pair.SyncAsync();

        await using GraphDbContext owner = pair.OwnerDb();
        Assert.Empty(await owner.Nodes.ToListAsync(TestContext.Current.CancellationToken));
        await using GraphDbContext client = pair.ClientDb();
        string archive = SyncColumns.ArchiveOf(pair.Instance);
        Assert.Equal([archive, archive], await client.Nodes.Select(x => EF.Property<string>(x, SyncColumns.InstanceId)).ToListAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>Owner rows: a root whose ParentId is its own Id, and the nodes below it.</summary>
    private static void SeedTree(GraphDbContext owner, Node root, params Node[] below)
    {
        owner.Nodes.Add(root);
        owner.Nodes.AddRange(below);
        owner.SaveChanges();
        root.ParentId = root.Id;
        owner.SaveChanges();
    }
}
