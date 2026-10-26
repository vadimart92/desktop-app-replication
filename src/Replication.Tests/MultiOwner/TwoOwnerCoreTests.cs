using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Replication.Tests.Integration;
using Sample.Domain;

namespace Replication.Tests.MultiOwner;

/// <summary>
/// One client, two owners with different data (4), built from Replication.Core and Sample.Domain only (no Sample.Lab).
/// Every check reads the database files with raw SQL: the client's rows per <c>InstanceId</c>, <c>_sync_instances</c>,
/// <c>_sync_cursors</c>, <c>_sync_outbox</c>, <c>_sync_notes</c>; on the owners the rows, <c>_sync_meta</c>,
/// <c>_sync_tombstones</c>, <c>_sync_clients</c> and <c>_sync_client_cursors</c>. Clients join by catch-up from zero cursors
/// (<c>skip_snapshot_on_empty_replica</c>), no snapshots. The order of steps comes from the agents' <c>DataChanged</c> and state changes.
/// </summary>
[TestFixture]
public sealed class TwoOwnerCoreTests
{
    private static readonly string[] s_tables = ["Category", "Item", "Log", "StockObject"];

    /// <summary>The tables the seed fills: an empty table sends nothing in a catch-up.</summary>
    private static readonly string[] s_seeded = ["Category", "Item", "Log"];
    private TestLog _log = new();
    private TwoOwnerRig? _rig;

    private async Task<TwoOwnerRig> StartAsync(int catchupBatchRows = 3, Func<SampleDbContext, Task>? seedClient = null)
    {
        _rig = await TwoOwnerRig.StartAsync(_log, o => o.CatchupBatchRows = catchupBatchRows, seedClient);
        return _rig;
    }

    private async Task<TwoOwnerRig> StartSyncedAsync(Func<SampleDbContext, Task>? seedClient = null)
    {
        TwoOwnerRig rig = await StartAsync(seedClient: seedClient);
        await TwoOwnerRig.SyncNowAsync(rig.ToA);
        await TwoOwnerRig.SyncNowAsync(rig.ToB);
        await rig.SettleAsync();
        return rig;
    }

    [TearDown]
    public async Task StopAsync()
    {
        if (_rig is not null)
            await _rig.DisposeAsync();
        _rig = null;
        _log.WriteTo(TestContext.Out);
        _log.Dispose();
        _log = new TestLog();
    }

    // Raw reads.

    private static string Instance(SyncAgent agent) => agent.InstanceId ?? throw new InvalidOperationException($"{agent.Name}: no replica yet");

    private static string OwnerInstance(TestOwner o) => Sql.Scalar<string>(o.DbPath, "SELECT instance_id FROM _sync_meta")!;

    private long Count(string table, string instance) =>
        Sql.Scalar<long>(_rig!.ClientPath, $"SELECT COUNT(*) FROM \"{table}\" WHERE InstanceId = @i", ("@i", instance));

    private object? ClientValue(string table, Guid id, string column) =>
        Sql.Scalar<object>(_rig!.ClientPath, $"SELECT \"{column}\" FROM \"{table}\" WHERE Id = @id", ("@id", Wire.PkText(id)));

    private static object? OwnerValue(TestOwner o, string table, Guid id, string column) =>
        Sql.Scalar<object>(o.DbPath, $"SELECT \"{column}\" FROM \"{table}\" WHERE Id = @id", ("@id", Wire.PkText(id)));

    private static bool OwnerHas(TestOwner o, string table, Guid id) =>
        Sql.Scalar<long>(o.DbPath, $"SELECT COUNT(*) FROM \"{table}\" WHERE Id = @id", ("@id", Wire.PkText(id))) > 0;

    private bool ClientHas(string table, Guid id) =>
        Sql.Scalar<long>(_rig!.ClientPath, $"SELECT COUNT(*) FROM \"{table}\" WHERE Id = @id", ("@id", Wire.PkText(id))) > 0;

    /// <summary>Every row of one replica with its service columns, by table and Id.</summary>
    private List<string> Replica(string instance) =>
        [.. s_tables.SelectMany(t => Sql.Lines(_rig!.ClientPath, $"SELECT '{t}', * FROM \"{t}\" WHERE InstanceId = @i ORDER BY Id", ("@i", instance)))];

    /// <summary>The client's own rows (<c>InstanceId</c> NULL) with every column, by table and Id.</summary>
    private List<string> Local() =>
        [.. s_tables.SelectMany(t => Sql.Lines(_rig!.ClientPath, $"SELECT '{t}', * FROM \"{t}\" WHERE InstanceId IS NULL ORDER BY Id"))];

    /// <summary>Everything in the client database: every row of every source and the outbox.</summary>
    private List<string> ClientState() =>
    [
        .. s_tables.SelectMany(t => Sql.Lines(_rig!.ClientPath, $"SELECT '{t}', * FROM \"{t}\" ORDER BY Id")),
        .. Sql.Lines(_rig!.ClientPath, "SELECT 'outbox', * FROM _sync_outbox ORDER BY id"),
    ];

    private long AllOutbox() => Sql.Scalar<long>(_rig!.ClientPath, "SELECT COUNT(*) FROM _sync_outbox");

    /// <summary>Whether a row with this Id is anywhere in either owner's database (as a row or a tombstone).</summary>
    private bool OnAnyOwner(Guid id) =>
        _rig!.Pairs.Any(p => s_tables.Any(t => OwnerHas(p.Owner, t, id))
                             || Sql.Scalar<long>(p.Owner.DbPath, "SELECT COUNT(*) FROM _sync_tombstones WHERE pk = @p", ("@p", Wire.PkText(id))) > 0);

    /// <summary>Everything an owner stores: rows, tombstones, head, and what it knows of its clients.</summary>
    private static List<string> OwnerState(TestOwner o) =>
    [
        .. s_tables.SelectMany(t => Sql.Lines(o.DbPath, $"SELECT '{t}', * FROM \"{t}\" ORDER BY Id")),
        .. Sql.Lines(o.DbPath, "SELECT 'tombstone', tbl, pk, version, origin FROM _sync_tombstones ORDER BY tbl, pk"),
        .. Sql.Lines(o.DbPath, "SELECT 'meta', instance_id, version FROM _sync_meta"),
        .. Sql.Lines(o.DbPath, "SELECT 'client', client_id, applied_seq FROM _sync_clients ORDER BY client_id"),
    ];

    /// <summary><c>_sync_cursors</c> of one replica: "table|cursor".</summary>
    private List<string> ClientCursors(string instance) =>
        Sql.Lines(_rig!.ClientPath, "SELECT tbl, cursor FROM _sync_cursors WHERE instance = @i ORDER BY tbl", ("@i", instance));

    /// <summary><c>_sync_client_cursors</c> the owner keeps for the client: "table|cursor".</summary>
    private List<string> OwnerCursors(TestOwner o) =>
        Sql.Lines(o.DbPath, "SELECT tbl, cursor FROM _sync_client_cursors WHERE client_id = @c ORDER BY tbl", ("@c", _rig!.Client.ClientId));

    private static List<string> AtHead(TestOwner o) => [.. s_tables.Order(StringComparer.Ordinal).Select(t => $"{t}|{o.Head()}")];

    /// <summary><c>_sync_outbox</c> of one replica: "table|pk|kind|sent|columns".</summary>
    private List<string> Outbox(string instance) =>
        Sql.Lines(_rig!.ClientPath, "SELECT tbl, pk, kind, sent, columns FROM _sync_outbox WHERE instance = @i ORDER BY id", ("@i", instance));

    private long? AppliedSeq(TestOwner o) =>
        Sql.Scalar<long?>(o.DbPath, "SELECT applied_seq FROM _sync_clients WHERE client_id = @c", ("@c", _rig!.Client.ClientId));

    /// <summary>The last outbox seq the client gave out for the instance.</summary>
    private long LastSeq(string instance) =>
        Sql.Scalar<long>(_rig!.ClientPath, "SELECT next_seq - 1 FROM _sync_instances WHERE instance = @i", ("@i", instance));

    private List<AgentState> States(TestOwner o) =>
        [.. _rig!.Client.Steps.Where(s => s.Agent == TestClient.AgentName(o) && s.Table is null).Select(s => s.State)];

    /// <summary>The replica equals the owner, the cursors on both sides are at the owner's head, nothing is queued.</summary>
    private void AssertConverged(TestOwner o, SyncAgent agent)
    {
        string inst = Instance(agent);
        Assert.That(Rows.Diff(o.Store.Model, o.DbPath, _rig!.ClientPath, inst), Is.Empty, $"{o.Name}: replica = owner");
        Assert.That(ClientCursors(inst), Is.EqualTo(AtHead(o)), $"{o.Name}: client cursors at the head");
        Assert.That(OwnerCursors(o), Is.EqualTo(AtHead(o)), $"{o.Name}: the owner has the cursors acknowledged");
        Assert.That(Outbox(inst), Is.Empty, $"{o.Name}: outbox");
    }

    private async Task EditItemAsync(Guid id, Action<Item> change)
    {
        await using SampleDbContext db = _rig!.Client.Db();
        change(await db.Items.SingleAsync(x => x.Id == id));
        await db.SaveChangesAsync();
    }

    // 1. An empty client joins both owners.

    [Test]
    public async Task Empty_client_catches_up_from_both_owners_then_gets_their_online_changes()
    {
        TwoOwnerRig rig = await StartAsync();
        Assert.Multiple(() =>
        {
            Assert.That(Sql.Scalar<long>(rig.ClientPath, "SELECT COUNT(*) FROM _sync_instances"), Is.Zero, "the agents learn the owners on the first link");
            Assert.That(s_tables.Sum(t => Sql.Scalar<long>(rig.ClientPath, $"SELECT COUNT(*) FROM \"{t}\"")), Is.Zero, "the client starts empty");
        });

        rig.ToA.LinkEnabled = true;
        rig.ToB.LinkEnabled = true;
        await rig.SettleAsync();

        string a = Instance(rig.ToA);
        string b = Instance(rig.ToB);
        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(OwnerInstance(rig.A)));
            Assert.That(b, Is.EqualTo(OwnerInstance(rig.B)));
            Assert.That(a, Is.Not.EqualTo(b));
            Assert.That(Sql.Lines(rig.ClientPath, "SELECT instance, snapshot_version FROM _sync_instances"),
                Is.EquivalentTo(new[] { $"{a}|∅", $"{b}|∅" }), "no snapshot was installed");
            Assert.That((Count("Category", a), Count("Item", a), Count("Log", a)), Is.EqualTo((3L, 9L, 3L)));
            Assert.That((Count("Category", b), Count("Item", b), Count("Log", b)), Is.EqualTo((2L, 5L, 6L)));
            foreach (string t in s_tables)
                Assert.That(Sql.Scalar<long>(rig.ClientPath, $"SELECT COUNT(*) FROM \"{t}\" WHERE InstanceId IS NULL OR InstanceId NOT IN (@a, @b)", ("@a", a), ("@b", b)), Is.Zero, $"{t}: nothing but the two replicas");
            Assert.That(Sql.Lines(rig.A.DbPath, "SELECT client_id FROM _sync_clients"), Is.EqualTo(new[] { rig.Client.ClientId }));
            Assert.That(Sql.Lines(rig.B.DbPath, "SELECT client_id FROM _sync_clients"), Is.EqualTo(new[] { rig.Client.ClientId }), "one client id for both owners");
            foreach ((TestOwner o, SyncAgent agent) in rig.Pairs)
            {
                AssertConverged(o, agent);
                Assert.That(AppliedSeq(o), Is.Zero, $"{o.Name}: the client sent nothing");
                Assert.That(States(o), Is.EqualTo(new[] { AgentState.Connecting, AgentState.Catchup, AgentState.Online }), o.Name);
                // 3 rows a batch: more than one step per owner.
                Assert.That(rig.Client.Tables(o, AgentState.Catchup), Has.Count.GreaterThan(3), o.Name);
                Assert.That(rig.Client.Tables(o, AgentState.Online), Is.Empty, $"{o.Name}: nothing changed after catch-up");
            }
        });

        Guid stapler = rig.A.ItemId("Степлер");
        Guid kettle = rig.B.ItemId("Чайник");
        await rig.A.SetPriceAsync("Степлер", 175);
        await rig.B.SetPriceAsync("Чайник", 700);
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ClientValue("Item", stapler, "Price"), Is.EqualTo(175L));
            Assert.That(ClientValue("Item", stapler, SyncColumns.InstanceId), Is.EqualTo(a));
            Assert.That(ClientValue("Item", kettle, "Price"), Is.EqualTo(700L));
            Assert.That(ClientValue("Item", kettle, SyncColumns.InstanceId), Is.EqualTo(b));
            foreach ((TestOwner o, SyncAgent agent) in rig.Pairs)
            {
                Assert.That(rig.Client.Tables(o, AgentState.Online), Is.EqualTo(new[] { "Item" }), $"{o.Name}: the change came as an online step");
                AssertConverged(o, agent);
            }
        });
    }

    // 2. A local edit goes to the owner of the row only.

    [Test]
    public async Task Local_edits_go_only_to_the_owner_the_row_came_from()
    {
        TwoOwnerRig rig = await StartSyncedAsync();
        string a = Instance(rig.ToA);
        string b = Instance(rig.ToB);
        List<string> bBefore = OwnerState(rig.B);
        Guid stapler = rig.A.ItemId("Степлер");
        Guid tape = rig.A.ItemId("Скотч");
        Guid kettle = rig.B.ItemId("Чайник");

        await EditItemAsync(stapler, x => x.Price = 999);
        await using (SampleDbContext db = rig.Client.Db())
        {
            db.Items.Remove(await db.Items.SingleAsync(x => x.Id == tape));
            await db.SaveChangesAsync();
        }
        var drone = new Item { Name = "Дрон", Price = 4200, Status = "новий", CategoryId = rig.A.CategoryId("Офіс") };
        await using (SampleDbContext db = rig.Client.Db())
        {
            db.Items.Add(drone).SetInstance(a);
            await db.SaveChangesAsync();
        }
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(OwnerValue(rig.A, "Item", stapler, "Price"), Is.EqualTo(999L));
            Assert.That(OwnerValue(rig.A, "Item", stapler, SyncColumns.Origin), Is.EqualTo(rig.Client.ClientId), "the owner marks the row as the client's");
            Assert.That(OwnerHas(rig.A, "Item", tape), Is.False);
            Assert.That(Sql.Scalar<string>(rig.A.DbPath, "SELECT origin FROM _sync_tombstones WHERE tbl = 'Item' AND pk = @p", ("@p", Wire.PkText(tape))),
                Is.EqualTo(rig.Client.ClientId), "the delete left a tombstone from the client");
            Assert.That(OwnerValue(rig.A, "Item", drone.Id, "Name"), Is.EqualTo("Дрон"));
            Assert.That(OwnerValue(rig.A, "Item", drone.Id, SyncColumns.Version), Is.GreaterThan(0L));
            Assert.That(ClientValue("Item", drone.Id, SyncColumns.Version), Is.EqualTo(OwnerValue(rig.A, "Item", drone.Id, SyncColumns.Version)), "the version came back");
            Assert.That(AppliedSeq(rig.A), Is.EqualTo(LastSeq(a)), "owner 1 applied every action of its replica");
            Assert.That(AppliedSeq(rig.A), Is.EqualTo(3L));
            Assert.That(OwnerState(rig.B), Is.EqualTo(bBefore), "owner 2 got nothing");
            Assert.That(AppliedSeq(rig.B), Is.Zero);
            Assert.That(LastSeq(b), Is.Zero, "nothing was queued for owner 2");
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });

        // One SaveChanges with rows of both replicas, both links off: each change is queued for its own owner.
        await TwoOwnerRig.GoOfflineAsync(rig.ToA);
        await TwoOwnerRig.GoOfflineAsync(rig.ToB);
        await using (SampleDbContext db = rig.Client.Db())
        {
            (await db.Items.SingleAsync(x => x.Id == stapler)).Status = "архів";
            (await db.Items.SingleAsync(x => x.Id == kettle)).Price = 777;
            await db.SaveChangesAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(Outbox(a), Is.EqualTo(new[] { $"Item|{Wire.PkText(stapler)}|{(int)OutboxKind.Patch}|0|ModifiedOn,Status" }));
            Assert.That(Outbox(b), Is.EqualTo(new[] { $"Item|{Wire.PkText(kettle)}|{(int)OutboxKind.Patch}|0|ModifiedOn,Price" }));
            Assert.That(ClientValue("Item", stapler, SyncColumns.InstanceId), Is.EqualTo(a), "the edit did not move the row");
            Assert.That(ClientValue("Item", kettle, SyncColumns.InstanceId), Is.EqualTo(b));
        });

        rig.ToA.LinkEnabled = true;
        rig.ToB.LinkEnabled = true;
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(OwnerValue(rig.A, "Item", stapler, "Status"), Is.EqualTo("архів"));
            Assert.That(OwnerValue(rig.A, "Item", stapler, "Price"), Is.EqualTo(999L));
            Assert.That(OwnerValue(rig.B, "Item", kettle, "Price"), Is.EqualTo(777L));
            Assert.That(OwnerValue(rig.B, "Item", kettle, "Status"), Is.EqualTo("активний"), "only the changed column went");
            Assert.That(OwnerHas(rig.A, "Item", kettle), Is.False);
            Assert.That(OwnerHas(rig.B, "Item", stapler), Is.False);
            Assert.That(AppliedSeq(rig.A), Is.EqualTo(4L));
            Assert.That(AppliedSeq(rig.B), Is.EqualTo(1L));
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    // 3. One owner stays online while the other one is caught up again.

    [Test]
    public async Task Reconnect_to_one_owner_catches_up_while_the_other_keeps_streaming_online()
    {
        TwoOwnerRig rig = await StartSyncedAsync();
        string b = Instance(rig.ToB);
        List<string> cursorsBefore = ClientCursors(b);
        await TwoOwnerRig.GoOfflineAsync(rig.ToB);
        foreach ((string name, long price) in new[] { ("Чайник", 660L), ("Тарілки", 190L), ("Ключі", 330L), ("Мастило", 99L), ("Чайник", 670L) })
            await rig.B.SetPriceAsync(name, price);
        Guid colander = await rig.B.NewItemAsync("Сито", "Кухня");
        Guid jack = await rig.B.NewItemAsync("Домкрат", "Гараж");
        Guid tyres = rig.B.ItemId("Шини");
        await rig.B.DeleteItemAsync("Шини");
        await rig.B.AddLogAsync(10);

        Assert.Multiple(() =>
        {
            Assert.That(ClientCursors(b), Is.EqualTo(cursorsBefore), "offline: the replica of owner 2 stays where it was");
            Assert.That(ClientHas("Item", colander), Is.False);
            Assert.That(ClientHas("Item", tyres), Is.True);
        });

        // Every message to the client waits, so the catch-up of owner 2 spans several online steps of owner 1.
        rig.B.Store.Options.Faults.DelayStream(rig.Client.ClientId, TimeSpan.FromMilliseconds(150), TimeSpan.FromSeconds(60));
        rig.Client.ClearSteps();
        var notOnline = new List<AgentState>();
        Guid pallet = rig.A.ItemId("Палета");
        using var stop = new CancellationTokenSource();
        // Owner 1 changes a price and waits until the client database has it, over and over, until owner 2 is caught up.
        Task automation = Task.Run(async () =>
        {
            long price = 500;
            while (!stop.IsCancellationRequested)
            {
                long p = price += 10;
                await rig.A.SetPriceAsync("Палета", p);
                await Wait.UntilAsync(() => ClientValue("Item", pallet, "Price") is long v && v == p, TimeSpan.FromSeconds(10), "owner 1's change did not arrive");
                AgentState s = rig.ToA.GetStatus().State;
                if (s != AgentState.Online)
                    notOnline.Add(s);
            }
        });
        await TwoOwnerRig.SyncNowAsync(rig.ToB, TimeSpan.FromSeconds(60));
        await stop.CancelAsync();
        await automation;
        Guid kettle = rig.B.ItemId("Чайник");
        await rig.B.SetPriceAsync("Чайник", 710);
        await rig.SettleAsync(TimeSpan.FromSeconds(60));

        List<Step> steps = [.. rig.Client.Steps];
        List<Step> bSteps = [.. steps.Where(s => s.Agent == TestClient.AgentName(rig.B))];
        DateTimeOffset firstCatchup = bSteps.First(s => s is { State: AgentState.Catchup, Table: not null }).At;
        DateTimeOffset online = bSteps.First(s => s is { State: AgentState.Online, Table: null }).At;
        int aDuring = steps.Count(s => s is { State: AgentState.Online, Table: "Item" } && s.Agent == TestClient.AgentName(rig.A) && s.At > firstCatchup && s.At < online);
        Assert.Multiple(() =>
        {
            Assert.That(notOnline, Is.Empty, "owner 1 stayed online the whole time");
            Assert.That(aDuring, Is.GreaterThanOrEqualTo(2), "owner 1's online steps arrived during owner 2's catch-up");
            Assert.That(States(rig.B), Is.EqualTo(new[] { AgentState.Connecting, AgentState.Catchup, AgentState.Online }), "no snapshot");
            Assert.That(rig.Client.Tables(rig.B, AgentState.Catchup), Has.Count.GreaterThan(3));
            Assert.That(rig.Client.Tables(rig.B, AgentState.Online), Is.EqualTo(new[] { "Item" }), "the change after catch-up is an online step");
            Assert.That(Sql.Scalar<long>(rig.ClientPath, "SELECT COUNT(*) FROM _sync_instances WHERE snapshot_version IS NOT NULL"), Is.Zero);
            Assert.That(Instance(rig.ToB), Is.EqualTo(b), "the same replica");
            Assert.That(ClientValue("Item", kettle, "Price"), Is.EqualTo(710L));
            Assert.That(ClientValue("Item", colander, SyncColumns.InstanceId), Is.EqualTo(b));
            Assert.That(ClientValue("Item", jack, SyncColumns.InstanceId), Is.EqualTo(b));
            Assert.That(ClientHas("Item", tyres), Is.False, "the tombstone reached the replica");
            Assert.That(Count("Log", b), Is.EqualTo(16L));
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    // 4. A client with one replica joins the second owner.

    [Test]
    public async Task Client_with_a_replica_of_one_owner_joins_the_second_and_keeps_the_first()
    {
        TwoOwnerRig rig = await StartAsync();
        await TwoOwnerRig.SyncNowAsync(rig.ToA);
        await rig.SettleAsync();
        string a = Instance(rig.ToA);
        // A pending edit for owner 1 waits in the outbox while the client joins owner 2.
        await TwoOwnerRig.GoOfflineAsync(rig.ToA);
        Guid stapler = rig.A.ItemId("Степлер");
        await EditItemAsync(stapler, x => x.Price = 135);
        List<string> aReplica = Replica(a);
        List<string> aCursors = ClientCursors(a);
        List<string> aOutbox = Outbox(a);
        Assert.That(aOutbox, Has.Count.EqualTo(1));

        await TwoOwnerRig.SyncNowAsync(rig.ToB);
        await rig.SettleAsync();
        string b = Instance(rig.ToB);

        Assert.Multiple(() =>
        {
            Assert.That(Replica(a), Is.EqualTo(aReplica), "the replica of owner 1 is untouched");
            Assert.That(ClientCursors(a), Is.EqualTo(aCursors));
            Assert.That(Outbox(a), Is.EqualTo(aOutbox));
            Assert.That(OwnerValue(rig.A, "Item", stapler, "Price"), Is.EqualTo(120L), "owner 1 has not got the edit yet");
            Assert.That(States(rig.B), Is.EqualTo(new[] { AgentState.Connecting, AgentState.Catchup, AgentState.Online }), "no snapshot");
            Assert.That(AppliedSeq(rig.B), Is.Zero);
            Assert.That((Count("Category", b), Count("Item", b), Count("Log", b)), Is.EqualTo((2L, 5L, 6L)));
            AssertConverged(rig.B, rig.ToB);
        });

        Guid colander = await rig.B.NewItemAsync("Сито", "Кухня");
        await TwoOwnerRig.SyncNowAsync(rig.ToA);
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ClientValue("Item", colander, SyncColumns.InstanceId), Is.EqualTo(b));
            Assert.That(rig.Client.Tables(rig.B, AgentState.Online), Is.EqualTo(new[] { "Item" }));
            Assert.That(OwnerValue(rig.A, "Item", stapler, "Price"), Is.EqualTo(135L));
            Assert.That(AppliedSeq(rig.A), Is.EqualTo(1L));
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    // 5. Each owner orders its catch-up by the open tables of its own agent.

    [Test]
    public async Task Each_owner_orders_catch_up_by_the_open_tables_of_its_agent()
    {
        TwoOwnerRig rig = await StartAsync();
        await rig.A.AddLogAsync(6);
        rig.ToA.SetOpenTables(["Log"]);
        rig.ToB.SetOpenTables(["Item"]);

        rig.ToA.LinkEnabled = true;
        rig.ToB.LinkEnabled = true;
        await rig.SettleAsync();

        List<string> a = rig.Client.Tables(rig.A, AgentState.Catchup);
        List<string> b = rig.Client.Tables(rig.B, AgentState.Catchup);
        Assert.Multiple(() =>
        {
            Assert.That(a, Is.SupersetOf(s_seeded), "owner 1 sent every table");
            Assert.That(b, Is.SupersetOf(s_seeded), "owner 2 sent every table");
            // Owner 1: Log is open, so it goes first; Category and Item follow.
            Assert.That(a.FindLastIndex(t => t == "Log"), Is.LessThan(a.FindIndex(t => t is "Category" or "Item")), string.Join(", ", a));
            // Owner 2: Item is open and Category is its FK parent, so both go before Log.
            Assert.That(b.FindLastIndex(t => t is "Category" or "Item"), Is.LessThan(b.FindIndex(t => t == "Log")), string.Join(", ", b));
            Assert.That(b.FindLastIndex(t => t == "Category"), Is.LessThan(b.FindIndex(t => t == "Item")), "the parent goes before the open table");
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    [Test]
    public async Task Changing_open_tables_during_catch_up_reorders_only_that_owner()
    {
        TwoOwnerRig rig = await StartAsync(catchupBatchRows: 2);
        await rig.A.AddLogAsync(6);
        await rig.B.AddLogAsync(6);
        // Owner 2's Item catch-up takes long enough to change the open tables in the middle of it: 15 rows, 8 batches.
        for (int i = 0; i < 10; i++)
            await rig.B.NewItemAsync($"Товар {i}", "Кухня");
        foreach ((TestOwner o, _) in rig.Pairs)
            o.Store.Options.Faults.DelayStream(rig.Client.ClientId, TimeSpan.FromMilliseconds(150), TimeSpan.FromSeconds(60));

        rig.ToA.LinkEnabled = true;
        rig.ToB.LinkEnabled = true;
        await Wait.UntilAsync(() => rig.Client.Tables(rig.B, AgentState.Catchup).Contains("Item"), TimeSpan.FromSeconds(30), "owner 2 did not start on Item");
        rig.ToB.SetOpenTables(["Log"]);
        await rig.SettleAsync(TimeSpan.FromSeconds(60));

        List<string> a = rig.Client.Tables(rig.A, AgentState.Catchup);
        List<string> b = rig.Client.Tables(rig.B, AgentState.Catchup);
        Assert.Multiple(() =>
        {
            Assert.That(a, Is.SupersetOf(s_seeded), "owner 1 sent every table");
            Assert.That(b, Is.SupersetOf(s_seeded), "owner 2 sent every table");
            // Owner 2 switched to Log before Item was done.
            Assert.That(b.IndexOf("Log"), Is.LessThan(b.LastIndexOf("Item")), string.Join(", ", b));
            // Owner 1 kept Item first.
            Assert.That(a.FindLastIndex(t => t is "Category" or "Item"), Is.LessThan(a.IndexOf("Log")), string.Join(", ", a));
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    // Edge cases.

    [Test]
    public async Task One_owner_offline_does_not_hold_back_the_other()
    {
        TwoOwnerRig rig = await StartSyncedAsync();
        string b = Instance(rig.ToB);
        List<string> bReplica = Replica(b);
        await rig.B.DisposeAsync();

        Guid stapler = rig.A.ItemId("Степлер");
        Guid pallet = rig.A.ItemId("Палета");
        Guid kettle = rig.B.ItemId("Чайник");
        await EditItemAsync(stapler, x => x.Price = 321);
        await rig.A.SetPriceAsync("Палета", 480);
        await Wait.UntilAsync(() => rig.ToA.GetStatus().Pending == 0 && ClientValue("Item", pallet, "Price") is 480L, TimeSpan.FromSeconds(30), "owner 1 stalled");
        await EditItemAsync(kettle, x => x.Price = 1);

        Assert.Multiple(() =>
        {
            Assert.That(OwnerValue(rig.A, "Item", stapler, "Price"), Is.EqualTo(321L));
            Assert.That(Outbox(Instance(rig.ToA)), Is.Empty);
            Assert.That(rig.ToA.GetStatus().State, Is.EqualTo(AgentState.Online));
            Assert.That(rig.ToB.GetStatus().State, Is.Not.EqualTo(AgentState.Online));
            Assert.That(Outbox(b), Is.EqualTo(new[] { $"Item|{Wire.PkText(kettle)}|{(int)OutboxKind.Patch}|0|ModifiedOn,Price" }), "the edit for owner 2 waits in its outbox");
            Assert.That(OwnerValue(rig.B, "Item", kettle, "Price"), Is.EqualTo(650L));
            Assert.That(Replica(b).Count, Is.EqualTo(bReplica.Count), "the replica of owner 2 keeps its rows");
        });
    }

    [Test]
    public async Task Item_put_in_the_replica_of_owner_2_under_a_category_of_owner_1_is_refused_on_the_client()
    {
        TwoOwnerRig rig = await StartSyncedAsync();
        string b = Instance(rig.ToB);
        List<string> client = ClientState();
        List<string> aBefore = OwnerState(rig.A);
        List<string> bBefore = OwnerState(rig.B);
        var item = new Item { Name = "Чужий", Price = 1, Status = "новий", CategoryId = rig.A.CategoryId("Офіс") };
        await using (SampleDbContext db = rig.Client.Db())
        {
            db.Items.Add(item).SetInstance(b);
            Assert.That(() => db.SaveChangesAsync(), Throws.InvalidOperationException.With.Message.Contains("own source"));
        }
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ClientState(), Is.EqualTo(client), "nothing was written, nothing was queued");
            Assert.That(OwnerState(rig.A), Is.EqualTo(aBefore));
            Assert.That(OwnerState(rig.B), Is.EqualTo(bBefore), "owner 2 got nothing");
            Assert.That(AppliedSeq(rig.B), Is.Zero);
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    // The client's own data: InstanceId NULL, one more source next to the owners.

    private static readonly Guid s_mine = Guid.Parse("0000000a-0000-0000-0000-000000000001");
    private static readonly Guid s_draft = Guid.Parse("0000000a-0000-0000-0000-000000000002");
    private static readonly Guid s_idea = Guid.Parse("0000000a-0000-0000-0000-000000000003");
    private static readonly Guid s_note = Guid.Parse("0000000a-0000-0000-0000-000000000004");

    /// <summary>The client's data from before replication: a category with 2 items and a log entry, saved with no InstanceId.</summary>
    private static async Task SeedLocalAsync(SampleDbContext db)
    {
        var mine = new Category { Id = s_mine, Name = "Мої" };
        db.Categories.Add(mine);
        db.Items.Add(new Item { Id = s_draft, Name = "Чернетка", Price = 10, Category = mine });
        db.Items.Add(new Item { Id = s_idea, Name = "Ідея", Price = 20, Category = mine });
        db.Log.Add(new LogEntry { Id = s_note, Text = "моя нотатка" });
        await db.SaveChangesAsync();
    }

    [Test]
    public async Task Client_with_its_own_data_joins_both_owners_and_keeps_it_apart()
    {
        TwoOwnerRig rig = await StartAsync(seedClient: SeedLocalAsync);
        List<string> local = Local();
        long aHead = rig.A.Head();
        long bHead = rig.B.Head();
        Assert.Multiple(() =>
        {
            Assert.That(local, Has.Count.EqualTo(4), "the own rows are there before any owner");
            Assert.That(s_tables.Sum(t => Sql.Scalar<long>(rig.ClientPath, $"SELECT COUNT(*) FROM \"{t}\" WHERE InstanceId IS NOT NULL")), Is.Zero, "own rows keep InstanceId NULL");
            Assert.That(AllOutbox(), Is.Zero);
        });

        rig.ToA.LinkEnabled = true;
        rig.ToB.LinkEnabled = true;
        await rig.SettleAsync();

        string a = Instance(rig.ToA);
        string b = Instance(rig.ToB);
        Assert.Multiple(() =>
        {
            Assert.That(Local(), Is.EqualTo(local), "the catch-up of both owners left the own rows alone");
            Assert.That(Sql.Lines(rig.ClientPath, "SELECT COALESCE(InstanceId, 'NULL'), COUNT(*) FROM Item GROUP BY InstanceId"),
                Is.EquivalentTo(new[] { "NULL|2", $"{a}|9", $"{b}|5" }));
            Assert.That(new[] { s_mine, s_draft, s_idea, s_note }.Where(OnAnyOwner), Is.Empty, "no own row reached an owner");
            Assert.That((rig.A.Head(), rig.B.Head()), Is.EqualTo((aHead, bHead)), "the owners got nothing");
            Assert.That(AppliedSeq(rig.A), Is.Zero);
            Assert.That(AppliedSeq(rig.B), Is.Zero);
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });

        // After the sync the user goes on with the own data: edit, delete, add, a new category with an item in one save.
        List<string> aBefore = OwnerState(rig.A);
        List<string> bBefore = OwnerState(rig.B);
        await EditItemAsync(s_draft, x => x.Price = 15);
        await using (SampleDbContext db = rig.Client.Db())
        {
            db.Items.Remove(await db.Items.SingleAsync(x => x.Id == s_idea));
            db.Items.Add(new Item { Name = "Нова", Price = 30, CategoryId = s_mine });
            var more = new Category { Name = "Ще" };
            db.Categories.Add(more);
            db.Items.Add(new Item { Name = "Ще одна", Price = 40, Category = more });
            await db.SaveChangesAsync();
        }
        await rig.SettleAsync();

        local = Local();
        Assert.Multiple(() =>
        {
            Assert.That(Sql.Lines(rig.ClientPath, "SELECT Name, Price FROM Item WHERE InstanceId IS NULL ORDER BY Price"),
                Is.EqualTo(new[] { "Чернетка|15", "Нова|30", "Ще одна|40" }));
            Assert.That(Sql.Lines(rig.ClientPath, "SELECT Name FROM Category WHERE InstanceId IS NULL ORDER BY Name"), Is.EquivalentTo(new[] { "Мої", "Ще" }));
            Assert.That(AllOutbox(), Is.Zero, "own edits are never queued");
            Assert.That(OwnerState(rig.A), Is.EqualTo(aBefore));
            Assert.That(OwnerState(rig.B), Is.EqualTo(bBefore));
        });

        // Owner traffic meanwhile, a cascade delete on owner 2 among it, does not reach the own rows.
        await rig.A.SetPriceAsync("Степлер", 130);
        await rig.A.AddLogAsync(2);
        await using (SampleDbContext db = rig.B.Db())
        {
            db.Categories.Remove(await db.Categories.SingleAsync(x => x.Name == "Гараж"));
            await db.SaveChangesAsync();
        }
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Local(), Is.EqualTo(local));
            Assert.That((Count("Category", b), Count("Item", b)), Is.EqualTo((1L, 2L)));
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });

        // Deleting an own category cascades over its own items only, still with nothing queued.
        await using (SampleDbContext db = rig.Client.Db())
        {
            db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == s_mine));
            await db.SaveChangesAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(Sql.Lines(rig.ClientPath, "SELECT Name FROM Item WHERE InstanceId IS NULL"), Is.EqualTo(new[] { "Ще одна" }));
            Assert.That(AllOutbox(), Is.Zero);
            Assert.That((Count("Item", a), Count("Item", b)), Is.EqualTo((9L, 2L)), "the replicas keep their rows");
        });
    }

    [Test]
    public async Task Restarted_client_keeps_its_own_data_and_catch_up_does_not_touch_it()
    {
        TwoOwnerRig rig = await StartSyncedAsync(SeedLocalAsync);
        List<string> local = Local();
        await rig.RestartClientAsync();
        await rig.B.SetPriceAsync("Чайник", 730);
        rig.ToA.LinkEnabled = true;
        rig.ToB.LinkEnabled = true;
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Local(), Is.EqualTo(local));
            Assert.That(rig.Client.Tables(rig.A, AgentState.Catchup), Is.Empty);
            Assert.That(rig.Client.Tables(rig.B, AgentState.Catchup), Is.EqualTo(new[] { "Item" }));
            Assert.That(AllOutbox(), Is.Zero);
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });

        // The own data stays editable after the restart.
        await EditItemAsync(s_draft, x => x.Name = "Чернетка 2");
        Assert.Multiple(() =>
        {
            Assert.That(ClientValue("Item", s_draft, "Name"), Is.EqualTo("Чернетка 2"));
            Assert.That(ClientValue("Item", s_draft, SyncColumns.InstanceId), Is.Null);
            Assert.That(AllOutbox(), Is.Zero);
        });
    }

    [Test]
    public async Task Snapshot_of_one_owner_replaces_its_replica_and_drops_its_outbox_and_leaves_the_rest_alone()
    {
        TwoOwnerRig rig = await StartSyncedAsync(SeedLocalAsync);
        string a = Instance(rig.ToA);
        string b = Instance(rig.ToB);
        List<string> local = Local();
        List<string> aReplica = Replica(a);
        List<string> aState = OwnerState(rig.A);
        Guid keys = rig.B.ItemId("Ключі");
        Guid kettle = rig.B.ItemId("Чайник");
        long keysPrice = (long)OwnerValue(rig.B, "Item", keys, "Price")!;

        // Owner 2 moves on while the client is away from it; a deleted row leaves a tombstone for the purge to remove.
        await TwoOwnerRig.GoOfflineAsync(rig.ToB);
        await rig.B.SetPriceAsync("Чайник", 660);
        Guid colander = await rig.B.NewItemAsync("Сито", "Кухня");
        await rig.B.DeleteItemAsync("Шини");
        await rig.B.AddLogAsync(5);

        // The client edits a row of owner 2 and creates one for it: two actions wait in the outbox.
        await EditItemAsync(keys, x => x.Price = 777);
        var drone = new Item { Name = "Дрон", Price = 4200, Status = "новий", CategoryId = rig.B.CategoryId("Кухня") };
        await using (SampleDbContext db = rig.Client.Db())
        {
            db.Items.Add(drone).SetInstance(b);
            await db.SaveChangesAsync();
        }
        Assert.Multiple(() =>
        {
            Assert.That(Outbox(b), Has.Count.EqualTo(2));
            Assert.That(ClientValue("Item", keys, "Price"), Is.EqualTo(777L));
            Assert.That(ClientHas("Item", drone.Id), Is.True);
        });

        // A month later owner 2 forgets the silent client and purges the tombstone it never got: a snapshot is the only way back.
        rig.B.Store.AdvanceClock(TimeSpan.FromDays(31));
        Owner.OwnerStore.PurgeResult purged = rig.B.Store.Purge();
        Assert.That(purged.PurgedVersion, Is.GreaterThan(rig.ToB.CursorOf("Item").Cursor));

        rig.Client.ClearSteps();
        rig.ToB.Options.SkipSnapshotOnEmptyReplica = false;
        await TwoOwnerRig.SyncNowAsync(rig.ToB);
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(States(rig.B), Does.Contain(AgentState.Snapshot), "owner 2 sent the replica as a snapshot");
            Assert.That(Sql.Scalar<long?>(rig.ClientPath, "SELECT snapshot_version FROM _sync_instances WHERE instance = @i", ("@i", b)), Is.Not.Null);
            Assert.That(Instance(rig.ToB), Is.EqualTo(b), "the same owner instance");

            // The client's own rows and the replica of owner 1 were not touched.
            Assert.That(Local(), Is.EqualTo(local));
            Assert.That(Replica(a), Is.EqualTo(aReplica));
            Assert.That(OwnerState(rig.A), Is.EqualTo(aState));
            Assert.That(States(rig.A), Is.Empty, "owner 1 saw no state change");
            AssertConverged(rig.A, rig.ToA);

            // The outbox of owner 2 was dropped, not sent.
            Assert.That(Outbox(b), Is.Empty);
            Assert.That(AppliedSeq(rig.B), Is.Zero, "owner 2 got no action");
            Assert.That(OwnerValue(rig.B, "Item", keys, "Price"), Is.EqualTo(keysPrice), "the edit never reached the owner");
            Assert.That(OwnerHas(rig.B, "Item", drone.Id), Is.False, "the created row never reached the owner");
            Assert.That(ClientValue("Item", keys, "Price"), Is.EqualTo(keysPrice), "the replica shows the owner's value");
            Assert.That(ClientHas("Item", drone.Id), Is.False);

            // Everything owner 2 has is in the replica, including what changed while the client was away.
            Assert.That(ClientValue("Item", kettle, "Price"), Is.EqualTo(660L));
            Assert.That(ClientValue("Item", colander, SyncColumns.InstanceId), Is.EqualTo(b));
            Assert.That((Count("Category", b), Count("Item", b), Count("Log", b)), Is.EqualTo((2L, 5L, 11L)));
            AssertConverged(rig.B, rig.ToB);
        });
    }

    /// <summary>A row may point only to rows of its own source: a save that breaks it throws before any transaction opens, and writes nothing.</summary>
    [TestCase("own item under an owner category")]
    [TestCase("owner item under an own category")]
    [TestCase("own item moved to an owner category")]
    [TestCase("owner item moved to an own category")]
    public async Task Row_pointing_to_another_source_is_refused(string what)
    {
        TwoOwnerRig rig = await StartSyncedAsync(SeedLocalAsync);
        string a = Instance(rig.ToA);
        Guid office = rig.A.CategoryId("Офіс");
        Guid stapler = rig.A.ItemId("Степлер");
        List<string> client = ClientState();

        await using (SampleDbContext db = rig.Client.Db())
        {
            switch (what)
            {
                case "own item under an owner category":
                    db.Items.Add(new Item { Name = "Моя", CategoryId = office });
                    break;
                case "owner item under an own category":
                    db.Items.Add(new Item { Name = "Чужа", CategoryId = s_mine }).SetInstance(a);
                    break;
                case "own item moved to an owner category":
                    (await db.Items.SingleAsync(x => x.Id == s_draft)).CategoryId = office;
                    break;
                case "owner item moved to an own category":
                    (await db.Items.SingleAsync(x => x.Id == stapler)).CategoryId = s_mine;
                    break;
            }
            Assert.That(() => db.SaveChangesAsync(), Throws.InvalidOperationException.With.Message.Contains("own source"));
        }

        Assert.That(ClientState(), Is.EqualTo(client), "nothing was written, nothing was queued");

        // The failed save did not poison the connection: the next saves go through, the owner's one reaches the owner.
        await EditItemAsync(s_draft, x => x.Price = 11);
        await EditItemAsync(stapler, x => x.Price = 121);
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ClientValue("Item", s_draft, "CategoryId"), Is.EqualTo(Wire.PkText(s_mine)));
            Assert.That(ClientValue("Item", s_draft, "Price"), Is.EqualTo(11L));
            Assert.That(ClientValue("Item", stapler, "CategoryId"), Is.EqualTo(Wire.PkText(office)));
            Assert.That(OwnerValue(rig.A, "Item", stapler, "Price"), Is.EqualTo(121L));
            Assert.That(OwnerValue(rig.A, "Item", stapler, "CategoryId"), Is.EqualTo(Wire.PkText(office)));
            Assert.That(OwnerHas(rig.A, "Item", s_draft), Is.False);
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    [Test]
    public async Task Category_deleted_with_cascade_on_owner_2_leaves_the_replica_of_owner_1_alone()
    {
        TwoOwnerRig rig = await StartSyncedAsync();
        string a = Instance(rig.ToA);
        string b = Instance(rig.ToB);
        List<string> aReplica = Replica(a);
        Guid garage = rig.B.CategoryId("Гараж");
        await using (SampleDbContext db = rig.B.Db())
        {
            db.Categories.Remove(await db.Categories.SingleAsync(x => x.Id == garage));
            await db.SaveChangesAsync();
        }
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Sql.Scalar<long>(rig.B.DbPath, "SELECT COUNT(*) FROM _sync_tombstones"), Is.EqualTo(4L), "the category and its 3 items");
            Assert.That(ClientHas("Category", garage), Is.False);
            Assert.That((Count("Category", b), Count("Item", b)), Is.EqualTo((1L, 2L)));
            Assert.That(Replica(a), Is.EqualTo(aReplica), "the replica of owner 1 is untouched");
            Assert.That(rig.Client.Tables(rig.A, AgentState.Online), Is.Empty);
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }

    [Test]
    public async Task Restarted_client_resumes_each_replica_from_its_own_cursors()
    {
        TwoOwnerRig rig = await StartSyncedAsync();
        string a = Instance(rig.ToA);
        string b = Instance(rig.ToB);
        string clientId = rig.Client.ClientId;
        List<string> aReplica = Replica(a);
        List<string> aCursors = ClientCursors(a);
        await rig.RestartClientAsync();
        Guid kettle = rig.B.ItemId("Чайник");
        await rig.B.SetPriceAsync("Чайник", 720);
        Assert.That(rig.Client.ClientId, Is.EqualTo(clientId), "the client id lives in the database");

        rig.ToA.LinkEnabled = true;
        rig.ToB.LinkEnabled = true;
        await rig.SettleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rig.ToA.InstanceId, Is.EqualTo(a), "the owner's instance id found the replica");
            Assert.That(rig.ToB.InstanceId, Is.EqualTo(b));
            Assert.That(rig.Client.Tables(rig.A, AgentState.Catchup), Is.Empty, "owner 1 had nothing new");
            Assert.That(rig.Client.Tables(rig.B, AgentState.Catchup), Is.EqualTo(new[] { "Item" }), "owner 2 sent only the changed row");
            Assert.That(Replica(a), Is.EqualTo(aReplica));
            Assert.That(ClientCursors(a), Is.EqualTo(aCursors));
            Assert.That(ClientValue("Item", kettle, "Price"), Is.EqualTo(720L));
            Assert.That(Sql.Lines(rig.ClientPath, "SELECT instance, snapshot_version FROM _sync_instances"), Is.EquivalentTo(new[] { $"{a}|∅", $"{b}|∅" }));
            Assert.That(Sql.Lines(rig.B.DbPath, "SELECT client_id FROM _sync_clients"), Is.EqualTo(new[] { clientId }), "the same client, not a new one");
            AssertConverged(rig.A, rig.ToA);
            AssertConverged(rig.B, rig.ToB);
        });
    }
}
