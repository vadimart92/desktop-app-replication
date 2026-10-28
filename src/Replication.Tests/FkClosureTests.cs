using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Design 8.4: a create goes in the same or an earlier batch than the rows that point to it, FK cycles included.</summary>
public sealed class FkClosureTests
{
    [Test]
    public async Task Row_created_pointing_to_itself_is_sent()
    {
        await using GraphPair pair = await GraphPair.StartAsync();
        await pair.GoOfflineAsync();
        var node = new Node { Name = "Корінь" };
        await using (GraphDbContext db = pair.ClientDb())
        {
            db.Nodes.Add(node).SetInstance(pair.Instance);
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
            node.ParentId = node.Id;
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        await pair.SyncAsync();

        await using GraphDbContext owner = pair.OwnerDb();
        Node sent = await owner.Nodes.SingleAsync(x => x.Id == node.Id, TestContext.CurrentContext.CancellationToken);
        Assert.That(sent.ParentId, Is.EqualTo(node.Id));
        Assert.That(pair.Client.Store.Entries(), Is.Empty);
    }

    [Test]
    public async Task Rows_created_pointing_to_each_other_go_in_one_batch()
    {
        await using GraphPair pair = await GraphPair.StartAsync();
        await pair.GoOfflineAsync();
        var dept = new Dept { Name = "Відділ" };
        var emp = new Emp { Name = "Керівник" };
        await using (GraphDbContext db = pair.ClientDb())
        {
            db.Depts.Add(dept).SetInstance(pair.Instance);
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
            emp.DeptId = dept.Id;
            db.Emps.Add(emp).SetInstance(pair.Instance);
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
            dept.ManagerId = emp.Id;
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        await pair.SyncAsync();

        await using GraphDbContext owner = pair.OwnerDb();
        Assert.That((await owner.Depts.SingleAsync(x => x.Id == dept.Id, TestContext.CurrentContext.CancellationToken)).ManagerId, Is.EqualTo(emp.Id));
        Assert.That((await owner.Emps.SingleAsync(x => x.Id == emp.Id, TestContext.CurrentContext.CancellationToken)).DeptId, Is.EqualTo(dept.Id));
        Assert.That(pair.Client.Store.Entries(), Is.Empty);
        IReadOnlyList<string> applies = pair.Applies;
        Assert.That(applies, Has.Count.EqualTo(1));
        string apply = applies[0];
        Assert.That(apply, Does.Contain(Wire.Short(Wire.PkText(dept.Id))));
        Assert.That(apply, Does.Contain(Wire.Short(Wire.PkText(emp.Id))));
    }

    [Test]
    public async Task Acyclic_chain_goes_parents_first_and_bulk_actions_last_in_one_batch()
    {
        await using GraphPair pair = await GraphPair.StartAsync(owner => owner.Nodes.Add(new Node { Name = "Старий" }));
        await pair.GoOfflineAsync();
        // The bulk action is queued first, yet interactive actions go before it.
        Assert.That(await pair.Agent.DeleteWhereAsync("Node", new Predicate("Name", "Старий")), Is.EqualTo(1));
        var a = new Node { Name = "A" };
        var b = new Node { Name = "B" };
        var c = new Node { Name = "C" };
        await using (GraphDbContext db = pair.ClientDb())
        {
            foreach (Node n in new[] { a, b, c })
                db.Nodes.Add(n).SetInstance(pair.Instance);
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
            // A → B → C: the outbox holds A, B, C in this order, the parents must reach the owner first.
            a.ParentId = b.Id;
            b.ParentId = c.Id;
            await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        await pair.SyncAsync();

        IReadOnlyList<string> applies = pair.Applies;
        Assert.That(applies, Has.Count.EqualTo(1));
        string apply = applies[0];
        int[] at = [.. new[] { c, b, a }.Select(n => apply.IndexOf(Wire.Short(Wire.PkText(n.Id)), StringComparison.Ordinal))];
        Assert.That(at, Is.All.GreaterThanOrEqualTo(0), apply);
        Assert.That(at[0], Is.LessThan(at[1]), apply);
        Assert.That(at[1], Is.LessThan(at[2]), apply);
        Assert.That(at[2], Is.LessThan(apply.IndexOf("[bulk]", StringComparison.Ordinal)), apply);
        await using GraphDbContext owner = pair.OwnerDb();
        Assert.That(await owner.Nodes.OrderBy(x => x.Name).Select(x => x.Name).ToListAsync(TestContext.CurrentContext.CancellationToken), Is.EqualTo(new[] { "A", "B", "C" }));
        Assert.That((await owner.Nodes.SingleAsync(x => x.Id == a.Id, TestContext.CurrentContext.CancellationToken)).ParentId, Is.EqualTo(b.Id));
        Assert.That((await owner.Nodes.SingleAsync(x => x.Id == b.Id, TestContext.CurrentContext.CancellationToken)).ParentId, Is.EqualTo(c.Id));
    }
}
