using Grpc.Net.Client;
using NUnit.Framework;
using Replication.Model;
using Replication.Protocol;
using Replication.Tests.Integration;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>
/// Design 6.5, 8.6, 9.1, 11: the branches of Apply on the owner, with hand-built requests over real gRPC:
/// dedup by seq, delete wins, unknown names, the echo reason, the archive and bulk-delete results.
/// </summary>
public sealed class ApplyHandlerBranchTests
{
    private static readonly string[] s_itemColumns = ["CategoryId", "CreatedOn", "ModifiedOn", "Name", "Price", "Status"];

    [Test]
    public async Task Action_on_an_unknown_table_is_rejected_and_the_rest_of_the_batch_goes_through()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid stapler = lab.ItemId("Степлер");

        ApplyReply reply = await client.ApplyAsync(
            client.Act(1, "NoSuchTable", Guid.NewGuid(), ActionKind.Create, ("Name", "x")),
            client.Act(2, "Item", stapler, ActionKind.Patch, ("Price", 777L)));

        Assert.That(reply.Results.Select(r => (r.Seq, r.Status, r.Reason)), Is.EqualTo(new[] { (1L, ResultStatus.Rejected, "unknown table"), (2L, ResultStatus.Applied, "echo") }));
        Assert.That(reply.AppliedUpToSeq, Is.EqualTo(2));
        Assert.That(lab.OwnerRow("Item", stapler)["Price"], Is.EqualTo(777L));
    }

    [Test]
    public async Task Predicate_delete_on_an_unknown_column_is_rejected_and_deletes_nothing()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        long items = OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item");

        ApplyReply reply = await client.ApplyAsync(client.Predicate(1, "Item", lab.Head(), ("NoSuchColumn", "x")));

        Assert.That((reply.Results[0].Status, reply.Results[0].Reason), Is.EqualTo((ResultStatus.Rejected, "unknown column")));
        Assert.That(OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item"), Is.EqualTo(items));
    }

    [Test]
    public async Task Retry_of_a_create_after_a_lost_reply_is_skipped_and_returns_the_same_version()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid id = Guid.NewGuid();
        Protocol.Action create = client.NewItem(1, id, "Ліхтарик", lab.CategoryId("Склад"));
        ApplyReply first = await client.ApplyAsync(create);
        long items = OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item");

        ApplyReply retry = await client.ApplyAsync(create);

        Assert.That(first.Results[0].Status, Is.EqualTo(ResultStatus.Applied));
        Assert.That(first.Results[0].HasVersion, Is.True);
        ActionResult again = retry.Results.Single();
        Assert.That((again.Status, again.Reason), Is.EqualTo((ResultStatus.Skipped, "already applied")));
        Assert.That((again.HasVersion, again.Version), Is.EqualTo((true, first.Results[0].Version)));
        Assert.That(retry.AppliedUpToSeq, Is.EqualTo(1));
        Assert.That(OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item"), Is.EqualTo(items), "the retry did not insert again");
    }

    [Test]
    public async Task Retry_of_a_create_the_owner_changed_since_is_skipped_without_a_version()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid id = Guid.NewGuid();
        Protocol.Action create = client.NewItem(1, id, "Ліхтарик", lab.CategoryId("Склад"));
        await client.ApplyAsync(create);
        OwnerSql.Exec(lab, $"UPDATE Item SET Price = 5 WHERE Id = '{Wire.PkText(id)}'");

        ApplyReply retry = await client.ApplyAsync(create);

        Assert.That(retry.Results.Single().Status, Is.EqualTo(ResultStatus.Skipped));
        Assert.That(retry.Results.Single().HasVersion, Is.False, "the row is no longer this client's change");
    }

    [Test]
    public async Task Retry_of_a_delete_is_skipped_and_returns_the_version_of_the_tombstone()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid tape = lab.ItemId("Скотч");
        Protocol.Action delete = client.Act(1, "Item", tape, ActionKind.Delete);
        await client.ApplyAsync(delete);
        long tombstone = OwnerSql.Scalar(lab, $"SELECT version FROM _sync_tombstones WHERE tbl = 'Item' AND pk = '{Wire.PkText(tape)}'");

        ApplyReply retry = await client.ApplyAsync(delete);

        ActionResult r = retry.Results.Single();
        Assert.That((r.Status, r.HasVersion, r.Version), Is.EqualTo((ResultStatus.Skipped, true, tombstone)));
        Assert.That(lab.Tombstone("Item", tape)?.Origin, Is.EqualTo(client.ClientId), "the delete is attributed to the client");
    }

    [Test]
    public async Task Create_of_an_id_the_owner_has_a_tombstone_for_is_rejected_as_deleted()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid tape = lab.ItemId("Скотч");
        await lab.Owner.DeleteItemAsync("Скотч");

        ApplyReply reply = await client.ApplyAsync(client.NewItem(1, tape, "Скотч", lab.CategoryId("Склад")));

        Assert.That((reply.Results[0].Status, reply.Results[0].Reason), Is.EqualTo((ResultStatus.Rejected, "deleted")));
        Assert.That(lab.OwnerHas("Item", tape), Is.False, "delete wins over the create");
    }

    [Test]
    public async Task Patch_of_a_row_the_owner_does_not_have_is_ignored_as_deleted()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);

        ApplyReply reply = await client.ApplyAsync(client.Act(1, "Item", Guid.NewGuid(), ActionKind.Patch, ("Price", 1L)));

        Assert.That((reply.Results[0].Status, reply.Results[0].Reason, reply.Results[0].HasVersion), Is.EqualTo((ResultStatus.Ignored, "deleted", false)));
        Assert.That(reply.AppliedUpToSeq, Is.EqualTo(1), "an ignored action still counts as applied");
    }

    [Test]
    public async Task Patch_of_a_row_created_earlier_in_the_same_batch_is_applied_not_ignored()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid id = Guid.NewGuid();

        ApplyReply reply = await client.ApplyAsync(
            client.NewItem(1, id, "Ліхтарик", lab.CategoryId("Склад")),
            client.Act(2, "Item", id, ActionKind.Patch, ("Price", 99L)));

        Assert.That(reply.Results.Select(r => r.Status), Is.EqualTo(new[] { ResultStatus.Applied, ResultStatus.Applied }));
        Assert.That(lab.OwnerRow("Item", id)["Price"], Is.EqualTo(99L));
    }

    [Test]
    public async Task Patch_pointing_to_a_parent_the_owner_does_not_have_is_rejected_as_parent_deleted()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid stapler = lab.ItemId("Степлер");
        object before = lab.OwnerRow("Item", stapler)["CategoryId"]!;

        ApplyReply reply = await client.ApplyAsync(client.Act(1, "Item", stapler, ActionKind.Patch, ("CategoryId", Wire.PkText(Guid.NewGuid()))));

        Assert.That((reply.Results[0].Status, reply.Results[0].Reason), Is.EqualTo((ResultStatus.Rejected, "parent deleted")));
        Assert.That(lab.OwnerRow("Item", stapler)["CategoryId"], Is.EqualTo(before));
    }

    [Test]
    public async Task Child_created_before_its_parent_in_the_same_batch_is_applied_because_the_order_does_not_matter()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid category = Guid.NewGuid();
        Guid item = Guid.NewGuid();

        ApplyReply reply = await client.ApplyAsync(
            client.NewItem(1, item, "Дрон", category),
            client.Act(2, "Category", category, ActionKind.Create, ("CreatedOn", "2026-01-01 00:00:00"), ("ModifiedOn", "2026-01-01 00:00:00"), ("Name", "Новинки")));

        Assert.That(reply.Results.Select(r => r.Status), Is.EqualTo(new[] { ResultStatus.Applied, ResultStatus.Applied }));
        Assert.That(lab.OwnerHas("Item", item) && lab.OwnerHas("Category", category), Is.True);
    }

    [Test]
    public async Task Patch_by_a_client_behind_the_row_is_applied_with_the_echo_reason_and_no_version()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid stapler = lab.ItemId("Степлер");

        ApplyReply reply = await client.ApplyAsync(client.Act(1, "Item", stapler, ActionKind.Patch, ("Price", 5L)));

        ActionResult r = reply.Results.Single();
        Assert.That((r.Status, r.Reason, r.HasVersion), Is.EqualTo((ResultStatus.Applied, "echo", false)));
        Assert.That(lab.OwnerRow("Item", stapler)["Price"], Is.EqualTo(5L), "the value is applied");
        Assert.That(lab.OwnerRow("Item", stapler).Origin, Is.Null, "the change comes back to the client as an ordinary one");
    }

    [Test]
    public async Task Patch_by_a_client_that_has_the_row_is_applied_with_its_version_and_origin()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid stapler = lab.ItemId("Степлер");
        client.Register(lab, itemCursor: lab.Head());

        ApplyReply reply = await client.ApplyAsync(client.Act(1, "Item", stapler, ActionKind.Patch, ("Price", 5L)));

        ActionResult r = reply.Results.Single();
        Assert.That((r.Status, r.Reason, r.HasVersion), Is.EqualTo((ResultStatus.Applied, "", true)));
        Assert.That(r.Version, Is.EqualTo(lab.OwnerRow("Item", stapler).Version));
        Assert.That(lab.OwnerRow("Item", stapler).Origin, Is.EqualTo(client.ClientId), "no echo for the author");
    }

    [Test]
    public async Task Second_consecutive_patch_by_the_same_client_keeps_its_origin_so_it_is_not_echoed_back()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid stapler = lab.ItemId("Степлер");
        client.Register(lab, itemCursor: lab.Head());
        await client.ApplyAsync(client.Act(1, "Item", stapler, ActionKind.Patch, ("Price", 5L)));
        client.Register(lab, itemCursor: lab.Head());

        ApplyReply reply = await client.ApplyAsync(client.Act(2, "Item", stapler, ActionKind.Patch, ("Price", 6L)));

        ActionResult r = reply.Results.Single();
        Assert.That((r.Status, r.Reason, r.HasVersion), Is.EqualTo((ResultStatus.Applied, "", true)));
        Assert.That(r.Version, Is.EqualTo(lab.OwnerRow("Item", stapler).Version));
        Assert.That(lab.OwnerRow("Item", stapler)["Price"], Is.EqualTo(6L));
        Assert.That(lab.OwnerRow("Item", stapler).Origin, Is.EqualTo(client.ClientId), "the author does not get its own second change back");
    }

    [Test]
    public async Task Archive_keeps_a_row_changed_after_the_expected_version_and_deletes_the_rest()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid fax = lab.ItemId("Факс");
        Guid printer = lab.ItemId("Старий принтер");
        long seen = lab.Head();
        OwnerSql.Exec(lab, $"UPDATE Item SET Price = 1 WHERE Id = '{Wire.PkText(printer)}'");

        ApplyReply reply = await client.ApplyAsync(client.Archive(1, seen, ("Item", fax), ("Item", printer)));

        ActionResult r = reply.Results.Single();
        Assert.That(r.Status, Is.EqualTo(ResultStatus.Applied));
        Assert.That(r.Deleted, Is.EqualTo(1));
        Assert.That(r.Changed.Select(x => Wire.PkText(x.Pk)), Is.EqualTo(new[] { Wire.PkText(printer) }));
        Assert.That(r.Children, Is.Empty);
        Assert.That(lab.OwnerHas("Item", fax), Is.False);
        Assert.That(lab.OwnerHas("Item", printer), Is.True);
        Assert.That(lab.Tombstone("Item", fax)?.Origin, Is.Null, "the client does not remove archived rows itself, so it hears the tombstone");
    }

    [Test]
    public async Task Archive_of_a_row_the_owner_no_longer_has_deletes_nothing_and_reports_nothing()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        long items = OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item");

        ApplyReply reply = await client.ApplyAsync(client.Archive(1, lab.Head(), ("Item", Guid.NewGuid()), ("NoSuchTable", Guid.NewGuid())));

        ActionResult r = reply.Results.Single();
        Assert.That((r.Status, r.Deleted, r.Changed.Count, r.Children.Count), Is.EqualTo((ResultStatus.Applied, 0, 0, 0)));
        Assert.That(OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item"), Is.EqualTo(items));
    }

    [Test]
    public async Task Archive_keeps_a_parent_whose_children_are_not_all_in_the_set()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid storage = lab.CategoryId("Склад");
        Guid pallet = lab.ItemId("Палета");

        ApplyReply reply = await client.ApplyAsync(client.Archive(1, lab.Head(), ("Category", storage), ("Item", pallet)));

        ActionResult r = reply.Results.Single();
        Assert.That(r.Children.Select(x => (x.Tbl, Wire.PkText(x.Pk))), Is.EqualTo(new[] { ("Category", Wire.PkText(storage)) }));
        Assert.That(r.Deleted, Is.EqualTo(1), "only the item went");
        Assert.That(lab.OwnerHas("Category", storage), Is.True);
        Assert.That(lab.OwnerHas("Item", pallet), Is.False);
    }

    [Test]
    public async Task Archive_deletes_a_parent_together_with_all_of_its_children()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid storage = lab.CategoryId("Склад");
        var set = Rows.All(lab.Owner.DbPath, "Item").Where(x => (string?)x.Value["CategoryId"] == Wire.PkText(storage)).Select(x => ("Item", x.Key)).ToList();
        Assert.That(set, Is.Not.Empty);

        ApplyReply reply = await client.ApplyAsync(client.Archive(1, lab.Head(), [("Category", storage), .. set]));

        ActionResult r = reply.Results.Single();
        Assert.That(r.Deleted, Is.EqualTo(set.Count + 1));
        Assert.That(r.Children, Is.Empty);
        Assert.That(lab.OwnerHas("Category", storage), Is.False);
    }

    [Test]
    public async Task Archive_keeps_a_parent_when_one_of_its_children_in_the_set_was_changed()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid storage = lab.CategoryId("Склад");
        long seen = lab.Head();
        List<(string, Guid)> set = [.. Rows.All(lab.Owner.DbPath, "Item").Where(x => (string?)x.Value["CategoryId"] == Wire.PkText(storage)).Select(x => ("Item", x.Key))];
        OwnerSql.Exec(lab, $"UPDATE Item SET Price = 1 WHERE Id = '{Wire.PkText(set[0].Item2)}'");

        ApplyReply reply = await client.ApplyAsync(client.Archive(1, seen, [("Category", storage), .. set]));

        ActionResult r = reply.Results.Single();
        Assert.That(r.Changed.Select(x => Wire.PkText(x.Pk)), Is.EqualTo(new[] { Wire.PkText(set[0].Item2) }));
        Assert.That(r.Children.Select(x => Wire.PkText(x.Pk)), Is.EqualTo(new[] { Wire.PkText(storage) }), "the changed child stays, so its parent stays");
        Assert.That(r.Deleted, Is.EqualTo(set.Count - 1));
    }

    [Test]
    public async Task Predicate_delete_removes_the_rows_seen_and_keeps_those_changed_since_with_their_ids()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        long seen = lab.Head();
        Guid fax = lab.ItemId("Факс");
        OwnerSql.Exec(lab, $"UPDATE Item SET Price = 1 WHERE Id = '{Wire.PkText(fax)}'");
        long archived = OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item WHERE Status = 'архів'");

        ApplyReply reply = await client.ApplyAsync(client.Predicate(1, "Item", seen, ("Status", "архів")));

        ActionResult r = reply.Results.Single();
        Assert.That((r.Status, r.Deleted), Is.EqualTo((ResultStatus.Applied, (int)archived - 1)));
        Assert.That(r.Changed.Select(x => Wire.PkText(x.Pk)), Is.EqualTo(new[] { Wire.PkText(fax) }), "left on the owner, they reach the client in the stream");
        Assert.That(OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM Item WHERE Status = 'архів'"), Is.EqualTo(1));
    }

    [Test]
    public async Task Predicate_delete_marks_the_tombstones_it_made_as_the_clients_own()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid fax = lab.ItemId("Факс");
        long other = OwnerSql.Scalar(lab, "SELECT COUNT(*) FROM _sync_tombstones");

        await client.ApplyAsync(client.Predicate(1, "Item", lab.Head(), ("Status", "архів")));

        Assert.That(lab.Tombstone("Item", fax)?.Origin, Is.EqualTo(client.ClientId));
        Assert.That(OwnerSql.Scalar(lab, $"SELECT COUNT(*) FROM _sync_tombstones WHERE origin = '{client.ClientId}'"), Is.EqualTo(3), "three archived items went");
        Assert.That(other, Is.Zero);
    }

    [Test]
    public async Task Create_overwrites_an_existing_row_of_the_same_id_and_takes_the_clients_origin()
    {
        await using Lab lab = await Lab.StartAsync();
        using var client = new ApplyClient(lab);
        Guid stapler = lab.ItemId("Степлер");

        ApplyReply reply = await client.ApplyAsync(client.NewItem(1, stapler, "Степлер 2", lab.CategoryId("Офіс")));

        Assert.That(reply.Results.Single().Status, Is.EqualTo(ResultStatus.Applied));
        RowData row = lab.OwnerRow("Item", stapler);
        Assert.That(row["Name"], Is.EqualTo("Степлер 2"));
        Assert.That(row.Origin, Is.EqualTo(client.ClientId));
    }

    /// <summary>A client that talks to the owner's Apply with hand-made actions.</summary>
    private sealed class ApplyClient : IDisposable
    {
        private readonly GrpcChannel _channel;
        private readonly Sync.SyncClient _client;

        public ApplyClient(Lab lab)
        {
            _channel = GrpcChannel.ForAddress(lab.Owner.Address);
            _client = new Sync.SyncClient(_channel);
        }

        public string ClientId { get; } = "cl-apply-" + Guid.NewGuid().ToString("N")[..8];

        public async Task<ApplyReply> ApplyAsync(params Protocol.Action[] actions)
        {
            var req = new ApplyRequest { ClientId = ClientId };
            req.Actions.AddRange(actions);
            return await _client.ApplyAsync(req);
        }

        public Protocol.Action Act(long seq, string table, Guid pk, ActionKind kind, params (string Column, object? Value)[] values)
        {
            var a = new Protocol.Action { Seq = seq, Tbl = table, Pk = Wire.PkBytes(pk), Kind = kind };
            foreach ((string column, object? value) in values)
            {
                a.Columns.Add(column);
                a.Values.Add(Wire.ToValue(value));
            }
            return a;
        }

        public Protocol.Action NewItem(long seq, Guid id, string name, Guid category) =>
            Act(seq, "Item", id, ActionKind.Create,
                ("CategoryId", Wire.PkText(category)), ("CreatedOn", "2026-01-01 00:00:00"), ("ModifiedOn", "2026-01-01 00:00:00"),
                ("Name", name), ("Price", 10L), ("Status", "новий"));

        public Protocol.Action Archive(long seq, long expectedVersion, params (string Table, Guid Pk)[] rows)
        {
            var a = new Protocol.Action { Seq = seq, Tbl = "Item", Kind = ActionKind.Archive, ExpectedVersion = expectedVersion };
            a.Rows.AddRange(rows.Select(r => Wire.Ref(r.Table, Wire.PkText(r.Pk))));
            return a;
        }

        public Protocol.Action Predicate(long seq, string table, long expectedVersion, params (string Column, object? Value)[] equalities)
        {
            var a = new Protocol.Action { Seq = seq, Tbl = table, Kind = ActionKind.PredicateDelete, ExpectedVersion = expectedVersion };
            foreach ((string column, object? value) in equalities)
                a.Predicate.Add(new Condition { Column = column, Value = Wire.ToValue(value) });
            return a;
        }

        /// <summary>Registers the client on the owner as one that has the Item rows up to <paramref name="itemCursor"/>.</summary>
        public void Register(Lab lab, long itemCursor)
        {
            OwnerSql.Exec(lab, $"INSERT OR IGNORE INTO _sync_clients(client_id, acked_version, applied_seq, last_seen) VALUES ('{ClientId}', 0, 0, 0);"
                + $"INSERT OR REPLACE INTO _sync_client_cursors(client_id, tbl, cursor) VALUES ('{ClientId}', 'Item', {itemCursor})");
        }

        public void Dispose() => _channel.Dispose();
    }
}
