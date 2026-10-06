using Microsoft.Data.Sqlite;
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
            await using Lab lab = await Lab.StartAsync(log: log);
            await lab.SyncNowAsync(lab.C1);
            await lab.SettleAsync();
            // A unique business column on the owner only (9): the client's replica has no such index.
            using (SqliteConnection c = lab.Owner.Store.Open())
            using (SqliteCommand cmd = c.CreateCommand())
            {
                cmd.CommandText = "CREATE UNIQUE INDEX UX_Category_Name ON \"Category\"(\"Name\")";
                cmd.ExecuteNonQuery();
            }

            // Offline, so the new category and its item go in one batch (8.4).
            lab.C1.Link = false;
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
            Assert.Contains(notes, n => n.Text.Contains("«Діркопробивач»", StringComparison.Ordinal) && n.Text.Contains("батьківський запис видалено", StringComparison.Ordinal));
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
}
