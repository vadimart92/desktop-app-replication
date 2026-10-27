using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Design 6.5: Apply on the owner, through the real gRPC path: here the unique-constraint rejections. Other branches are in ApplyHandlerBranchTests.</summary>
public sealed class ApplyHandlerTests
{
    [Test]
    public async Task Children_of_a_create_rejected_as_unique_are_rejected_in_the_same_batch()
    {
        using var log = new TestLog();
        try
        {
            await using Lab lab = await StartWithUniqueNamesAsync(log);

            // Offline, so the new category and its item go in one batch (8.4).
            await lab.GoOfflineAsync(lab.C1);
            await using (SampleDbContext db = lab.C1.Db())
            {
                var category = new Category { Name = "Офіс" };
                db.Categories.Add(category).SetInstance(lab.C1.Instance);
                db.Items.Add(new Item { Name = "Діркопробивач", Price = 150, Category = category }).SetInstance(lab.C1.Instance);
                await db.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
            }
            await lab.SyncNowAsync(lab.C1);
            await lab.SettleAsync();

            AgentStatus status = lab.C1.Agent.GetStatus();
            Assert.That(status.Pending, Is.EqualTo(0));
            Assert.That(status.LastError, Is.Null);
            Assert.That(Inspect.OwnerRows(lab.Owner, "Category"), Has.Exactly(1).Matches<RowView>(r => r.Label == "Офіс"));
            Assert.That(Inspect.OwnerHas(lab.Owner, "Діркопробивач"), Is.False);
            List<string> notes = lab.Notes.Of(lab.C1.Label);
            Assert.That(notes, Has.Some.Matches<string>(n => n.Contains("«Офіс»", StringComparison.Ordinal) && n.Contains("already has a row", StringComparison.Ordinal)));
            Assert.That(notes, Has.Some.Matches<string>(n => n.Contains("«Діркопробивач»", StringComparison.Ordinal) && n.Contains("the parent row is not saved or was deleted", StringComparison.Ordinal)));
            Assert.That(log.OfAgent(lab.C1.Agent.Name), Has.None.Matches<string>(l => l.StartsWith("Apply: Unknown", StringComparison.Ordinal)));

            // The outbox is not blocked: a later edit reaches the owner.
            await lab.C1.SetPriceAsync("Степлер", 777);
            await lab.SettleAsync();
            Assert.That(Inspect.OwnerItem(lab.Owner, "Степлер")?.Price, Is.EqualTo(777));
        }
        finally
        {
            log.WriteTo(TestContext.Out);
        }
    }

    [Test]
    public async Task Patch_pointing_to_a_create_rejected_as_unique_goes_back_to_the_owners_row()
    {
        CancellationToken ct = TestContext.CurrentContext.CancellationToken;
        await using Lab lab = await StartWithUniqueNamesAsync();

        // Offline, the user creates a duplicate category and moves an existing item into it: both go in one batch (8.4).
        await lab.GoOfflineAsync(lab.C1);
        var duplicate = new Category { Name = "Склад" };
        await using (SampleDbContext db = lab.C1.Db())
        {
            db.Categories.Add(duplicate).SetInstance(lab.C1.Instance);
            (await db.Items.SingleAsync(x => x.Name == "Степлер", ct)).CategoryId = duplicate.Id;
            await db.SaveChangesAsync(ct);
        }
        await lab.SyncNowAsync(lab.C1);

        // The rejected create stays in the replica (9); the item gets the owner's row back, in its old category.
        await WaitForDiffAsync(lab, $"Category {Wire.Short(Wire.PkText(duplicate.Id))}: зайвий у репліці");
        Assert.That(lab.Notes.Of(lab.C1.Label), Has.Some.Matches<string>(n => n.StartsWith("the edit of «Степлер» is not saved", StringComparison.Ordinal)
            && n.Contains("the parent row is not saved or was deleted", StringComparison.Ordinal)));
    }

    [Test]
    public async Task Patch_rejected_as_unique_goes_back_to_the_owners_row()
    {
        await using Lab lab = await StartWithUniqueNamesAsync();
        Guid store = Guid.Parse(Inspect.ClientRows(lab.C1, "Category").Single(r => r.Label == "Склад").Id);

        await lab.C1.RenameCategoryAsync(store, "Офіс");

        await WaitForDiffAsync(lab);
        Assert.That(lab.Notes.Of(lab.C1.Label), Has.Some.Matches<string>(n => n.StartsWith("the edit of «Офіс» is not saved", StringComparison.Ordinal)
            && n.Contains("already has a row", StringComparison.Ordinal)));
    }

    /// <summary>A lab with C1 synced, and a unique business column on the owner only (9): the client's replica has no such index.</summary>
    private static async Task<Lab> StartWithUniqueNamesAsync(TestLog? log = null)
    {
        Lab lab = await Lab.StartAsync(logging: log is null ? null : log.AddTo);
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();
        OwnerSql.Exec(lab, "CREATE UNIQUE INDEX UX_Category_Name ON \"Category\"(\"Name\")");
        return lab;
    }

    /// <summary>Waits until C1's replica differs from the owner by exactly these lines; the full row a reply asks for comes after it.</summary>
    private static Task WaitForDiffAsync(Lab lab, params string[] expected) =>
        Lab.WaitAsync(() => Inspect.Diff(lab.Owner, lab.C1).SequenceEqual(expected), TimeSpan.FromSeconds(30), "репліка Клієнта 1 не зрівнялась з власником");
}
