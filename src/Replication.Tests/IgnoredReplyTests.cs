using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Replication.Client;
using Replication.Model;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Design 6.5, 9.1: an Ignored reply removes the row with its children at once, over an FK cycle too.</summary>
public sealed class IgnoredReplyTests
{
    [Fact]
    public async Task Ignored_patch_of_a_row_pointing_to_itself_removes_it()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var root = new Node { Name = "Корінь" };
        await using GraphPair pair = await GraphPair.StartAsync(owner => GraphPair.SeedTree(owner, root));

        await PatchRowDeletedOnOwnerAsync(pair, "DELETE FROM \"Node\" WHERE Id = @id", [("@id", Wire.PkText(root.Id))],
            async db => (await db.Nodes.SingleAsync(x => x.Id == root.Id, ct)).Name = "Корінь 2");

        await using GraphDbContext client = pair.ClientDb();
        Assert.Empty(await client.Nodes.ToListAsync(ct));
        Assert.Equal(AgentState.Online, pair.Agent.GetStatus().State);
    }

    [Fact]
    public async Task Ignored_patch_of_rows_pointing_to_each_other_removes_both()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var dept = new Dept { Name = "Відділ" };
        var emp = new Emp { Name = "Керівник", DeptId = dept.Id };
        await using GraphPair pair = await GraphPair.StartAsync(owner =>
        {
            owner.Depts.Add(dept);
            owner.Emps.Add(emp);
            owner.SaveChanges();
            dept.ManagerId = emp.Id;
        });

        await PatchRowDeletedOnOwnerAsync(pair,
            "UPDATE \"Dept\" SET ManagerId = NULL WHERE Id = @d; DELETE FROM \"Emp\" WHERE Id = @e; DELETE FROM \"Dept\" WHERE Id = @d;",
            [("@d", Wire.PkText(dept.Id)), ("@e", Wire.PkText(emp.Id))],
            async db => (await db.Depts.SingleAsync(x => x.Id == dept.Id, ct)).Name = "Відділ 2");

        await using GraphDbContext client = pair.ClientDb();
        Assert.Empty(await client.Depts.ToListAsync(ct));
        Assert.Empty(await client.Emps.ToListAsync(ct));
        Assert.Equal(AgentState.Online, pair.Agent.GetStatus().State);
    }

    /// <summary>
    /// The owner deletes rows while the client's stream is held, then the client patches one of them: the reply
    /// overtakes the tombstones, so the client removes the rows on <c>Ignored</c> itself.
    /// </summary>
    private static async Task PatchRowDeletedOnOwnerAsync(GraphPair pair, string deleteSql, (string Name, object Value)[] args, Func<GraphDbContext, Task> patch)
    {
        pair.Owner.Store.Options.Faults.DelayStream(pair.Client.ClientId, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
        using (SqliteConnection c = pair.Owner.Store.Open())
            TestDb.Exec(c, deleteSql, args);
        await using (GraphDbContext db = pair.ClientDb())
        {
            await patch(db);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
        await Lab.WaitAsync(() => pair.Agent.GetStatus().Pending == 0, TimeSpan.FromSeconds(30), "клієнт не отримав ApplyReply");
    }
}
