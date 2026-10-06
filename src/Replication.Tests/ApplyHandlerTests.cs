using Microsoft.EntityFrameworkCore;
using Replication.Client;
using Replication.Model;
using Sample.Domain;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>Design 6.5: Apply on the owner, through the real gRPC path.</summary>
public sealed class ApplyHandlerTests(ITestOutputHelper output)
{
    [Fact]
    public async Task Children_of_a_create_rejected_as_unique_are_rejected_in_the_same_batch()
    {
        var log = new SyncLog();
        var entries = new List<SyncLogEntry>();
        log.Written += e =>
        {
            lock (entries)
                entries.Add(e);
        };
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
                await db.SaveChangesAsync(TestContext.Current.CancellationToken);
            }
            await lab.SyncNowAsync(lab.C1);
            await lab.SettleAsync();

            AgentStatus status = lab.C1.Agent.GetStatus();
            Assert.Equal(0, status.Pending);
            Assert.Null(status.LastError);
            Assert.Single(Inspect.OwnerRows(lab.Owner, "Category"), r => r.Label == "Офіс");
            Assert.False(Inspect.OwnerHas(lab.Owner, "Діркопробивач"));
            List<ClientNote> notes = lab.C1.Replication.Store.Notes();
            Assert.Contains(notes, n => n.Text.Contains("«Офіс»", StringComparison.Ordinal) && n.Text.Contains("вже є запис", StringComparison.Ordinal));
            Assert.Contains(notes, n => n.Text.Contains("«Діркопробивач»", StringComparison.Ordinal) && n.Text.Contains("батьківський запис не збережено або видалено", StringComparison.Ordinal));
            lock (entries)
            {
                Assert.Contains(entries, e => e.Source == "owner" && e.Text.Contains("Rejected (unique)", StringComparison.Ordinal)
                    && e.Text.Contains("Rejected (parent deleted)", StringComparison.Ordinal));
                Assert.DoesNotContain(entries, e => e.Text.StartsWith("Apply: Unknown", StringComparison.Ordinal));
            }

            // The outbox is not blocked: a later edit reaches the owner.
            await lab.C1.SetPriceAsync("Степлер", 777);
            await lab.SettleAsync();
            Assert.Equal(777, Inspect.OwnerItem(lab.Owner, "Степлер")?.Price);
        }
        finally
        {
            lock (entries)
            {
                foreach (SyncLogEntry e in entries)
                    output.WriteLine($"{e.At:HH:mm:ss.fff} [{e.Source}] {e.Text}");
            }
        }
    }

    [Fact]
    public async Task Patch_pointing_to_a_create_rejected_as_unique_goes_back_to_the_owners_row()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
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
        Assert.Contains(lab.C1.Replication.Store.Notes(), n => n.Text.StartsWith("правку запису «Степлер» не збережено", StringComparison.Ordinal)
            && n.Text.Contains("батьківський запис не збережено або видалено", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Patch_rejected_as_unique_goes_back_to_the_owners_row()
    {
        await using Lab lab = await StartWithUniqueNamesAsync();
        Guid store = Guid.Parse(Inspect.ClientRows(lab.C1, "Category").Single(r => r.Label == "Склад").Id);

        await lab.C1.RenameCategoryAsync(store, "Офіс");

        await WaitForDiffAsync(lab);
        Assert.Contains(lab.C1.Replication.Store.Notes(), n => n.Text.StartsWith("правку запису «Офіс» не збережено", StringComparison.Ordinal)
            && n.Text.Contains("вже є запис", StringComparison.Ordinal));
    }

    /// <summary>A lab with C1 synced, and a unique business column on the owner only (9): the client's replica has no such index.</summary>
    private static async Task<Lab> StartWithUniqueNamesAsync(SyncLog? log = null)
    {
        Lab lab = await Lab.StartAsync(log: log);
        await lab.SyncNowAsync(lab.C1);
        await lab.SettleAsync();
        OwnerSql.Exec(lab, "CREATE UNIQUE INDEX UX_Category_Name ON \"Category\"(\"Name\")");
        return lab;
    }

    /// <summary>Waits until C1's replica differs from the owner by exactly these lines; the full row a reply asks for comes after it.</summary>
    private static Task WaitForDiffAsync(Lab lab, params string[] expected) =>
        Lab.WaitAsync(() => Inspect.Diff(lab.Owner, lab.C1).SequenceEqual(expected), TimeSpan.FromSeconds(30), "репліка Клієнта 1 не зрівнялась з власником");
}
