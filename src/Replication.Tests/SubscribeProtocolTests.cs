using Grpc.Core;
using Grpc.Net.Client;
using NUnit.Framework;
using Replication.Model;
using Replication.Protocol;
using Replication.Tests.Integration;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>
/// Design 6.2, 9.1: the owner's side of Subscribe, driven with a hand-built stream: why a Start is refused, and what
/// NeedFull in an Ack gets back.
/// </summary>
public sealed class SubscribeProtocolTests
{
    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Start_with_another_owners_instance_id_gets_SnapshotRequired_naming_the_owner_that_answers_and_the_stream_closes()
    {
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);

        await session.SendStartAsync(session.NewStart(instanceId: "inst-somebody-else"));
        ChangeMessage first = await session.ReadAsync();

        Assert.That(first.BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.SnapshotRequired));
        Assert.That(first.SnapshotRequired.Reason, Does.Contain("instance_id"));
        Assert.That(first.SnapshotRequired.InstanceId, Is.EqualTo(lab.Owner.Store.InstanceId));
        Assert.That(first.SnapshotRequired.SizeBytes, Is.GreaterThan(0));
        Assert.That(await session.AtEndAsync(), Is.True);
        Assert.That(lab.Owner.Store.IsSubscribed(session.ClientId), Is.False, "a refused client is not registered as subscribed");
    }

    [Test]
    public async Task Start_with_cursors_ahead_of_the_owners_head_gets_SnapshotRequired()
    {
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);
        long head = lab.Owner.Head();
        Start start = session.NewStart();
        foreach (SyncTable t in lab.Owner.Store.Model.Tables)
            start.Cursors.Add(new TableCursor { Tbl = t.Name, Cursor = head + 100 });

        await session.SendStartAsync(start);
        ChangeMessage first = await session.ReadAsync();

        Assert.That(first.BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.SnapshotRequired));
        Assert.That(first.SnapshotRequired.Reason, Does.Contain($"{head + 100} > version {head}"));
        Assert.That(await session.AtEndAsync(), Is.True);
    }

    [Test]
    public async Task Start_with_one_table_ahead_of_the_head_and_the_rest_empty_is_accepted_because_the_lowest_cursor_decides()
    {
        // The check compares the lowest cursor with the head, so a single table ahead does not refuse the Start.
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);
        Start start = session.NewStart(skipSnapshot: true);
        start.Cursors.Add(new TableCursor { Tbl = "Log", Cursor = lab.Owner.Head() + 100 });

        await session.SendStartAsync(start);
        ChangeMessage first = await session.ReadAsync();

        Assert.That(first.BodyCase, Is.Not.EqualTo(ChangeMessage.BodyOneofCase.SnapshotRequired));
    }

    [Test]
    public async Task Start_with_another_schema_version_gets_SchemaMismatch_with_the_owners_version_and_is_not_subscribed()
    {
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);
        Start start = session.NewStart();
        start.SchemaVersion = lab.Owner.Store.Options.SchemaVersion + 1;

        await session.SendStartAsync(start);
        ChangeMessage first = await session.ReadAsync();

        Assert.That(first.BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.SchemaMismatch));
        Assert.That(first.SchemaMismatch.OwnerSchemaVersion, Is.EqualTo(lab.Owner.Store.Options.SchemaVersion));
        Assert.That(await session.AtEndAsync(), Is.True);
        Assert.That(lab.Owner.Store.IsSubscribed(session.ClientId), Is.False);
    }

    [Test]
    public async Task Schema_check_comes_before_the_instance_check()
    {
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);
        Start start = session.NewStart(instanceId: "inst-somebody-else");
        start.SchemaVersion = lab.Owner.Store.Options.SchemaVersion + 1;

        await session.SendStartAsync(start);

        Assert.That((await session.ReadAsync()).BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.SchemaMismatch));
    }

    [Test]
    public async Task First_message_that_is_not_Start_fails_the_call_with_InvalidArgument()
    {
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);

        await session.SendAsync(new SubscribeMessage { OpenTables = new OpenTables { Tables = { "Item" } } });

        RpcException e = Assert.ThrowsAsync<RpcException>(() => session.ReadAsync())!;
        Assert.That(e.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public async Task Accepted_Start_is_registered_with_its_cursors_and_a_new_replica_streams_from_zero()
    {
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);

        await session.SendStartAsync(session.NewStart(skipSnapshot: true));
        ChangeMessage first = await session.ReadAsync();

        Assert.That(first.BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.Batch));
        Assert.That(lab.Owner.Store.IsSubscribed(session.ClientId), Is.True);
        Assert.That(OwnerSql.Scalar(lab, $"SELECT COUNT(*) FROM _sync_clients WHERE client_id = '{session.ClientId}'"), Is.EqualTo(1));
    }

    [Test]
    public async Task NeedFull_gets_the_full_row_or_the_tombstone_and_nothing_for_a_row_the_owner_forgot()
    {
        await using Lab lab = await Lab.StartAsync();
        Guid alive = lab.ItemId("Степлер");
        Guid deleted = lab.ItemId("Скотч");
        Guid forgotten = lab.ItemId("Маркери");
        await lab.Owner.DeleteItemAsync("Скотч");
        await lab.Owner.DeleteItemAsync("Маркери");
        // The tombstone of "Маркери" was purged: the owner has neither the row nor its tombstone.
        OwnerSql.Exec(lab, $"DELETE FROM _sync_tombstones WHERE tbl = 'Item' AND pk = '{Wire.PkText(forgotten)}'");
        long tombstoneVersion = OwnerSql.Scalar(lab, $"SELECT version FROM _sync_tombstones WHERE tbl = 'Item' AND pk = '{Wire.PkText(deleted)}'");
        using RawSession session = RawSession.Open(lab);
        await session.SendStartAsync(session.NewStart(skipSnapshot: true));
        await session.ReadUntilAsync(m => m.BodyCase == ChangeMessage.BodyOneofCase.Progress && m.Progress.Done);

        await session.SendAsync(new SubscribeMessage
        {
            Ack = new Ack { NeedFull = { Wire.Ref("Item", Wire.PkText(forgotten)), Wire.Ref("Item", Wire.PkText(deleted)), Wire.Ref("Item", Wire.PkText(alive)) } },
        });
        ChangeMessage answer = await session.ReadUntilAsync(m => m.BodyCase == ChangeMessage.BodyOneofCase.Batch);

        Batch batch = answer.Batch;
        Assert.That(batch.Tbl, Is.EqualTo("Item"));
        Assert.That(batch.Rows.Select(r => Wire.PkText(r.Pk)), Is.EqualTo(new[] { Wire.PkText(alive) }));
        Assert.That(batch.Rows.Single().Full, Is.True, "a NeedFull row is always sent in full");
        Assert.That(batch.Tombstones.Select(t => (Wire.PkText(t.Pk), t.Version)), Is.EqualTo(new[] { (Wire.PkText(deleted), tombstoneVersion) }));
    }

    [Test]
    public async Task NeedFull_for_a_table_the_owner_does_not_know_is_ignored_and_the_stream_goes_on()
    {
        await using Lab lab = await Lab.StartAsync();
        using RawSession session = RawSession.Open(lab);
        await session.SendStartAsync(session.NewStart(skipSnapshot: true));
        await session.ReadUntilAsync(m => m.BodyCase == ChangeMessage.BodyOneofCase.Progress && m.Progress.Done);

        await session.SendAsync(new SubscribeMessage { Ack = new Ack { NeedFull = { Wire.Ref("NoSuchTable", Wire.PkText(Guid.NewGuid())), Wire.Ref("Item", Wire.PkText(lab.ItemId("Степлер"))) } } });
        ChangeMessage answer = await session.ReadUntilAsync(m => m.BodyCase == ChangeMessage.BodyOneofCase.Batch);

        Assert.That(answer.Batch.Tbl, Is.EqualTo("Item"));
        Assert.That(answer.Batch.Rows, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Online_stream_that_falls_behind_goes_back_to_catchup_and_online_again_without_losing_rows()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o =>
        {
            o.OnlineBatchRows = 10;
            o.OnlineLagVersions = 20;
        });
        using RawSession session = RawSession.Open(lab);
        await session.SendStartAsync(session.NewStart(skipSnapshot: true));
        await session.ReadUntilAsync(m => m.BodyCase == ChangeMessage.BodyOneofCase.Progress && m.Progress.Done);

        // One commit of 100 rows: the online step takes 10, the other 90 are more than the lag limit.
        InsertLogRows(lab, 100);
        var received = new HashSet<string>();
        ChangeMessage m;
        while ((m = await session.ReadAsync()).BodyCase != ChangeMessage.BodyOneofCase.Progress)
        {
            if (m.BodyCase == ChangeMessage.BodyOneofCase.Batch)
            {
                Assert.That(m.Batch.Online, Is.True, "before the fallback the stream is online");
                received.UnionWith(m.Batch.Rows.Select(r => Wire.PkText(r.Pk)));
            }
        }
        Assert.That(m.Progress.Done, Is.False);
        Assert.That(m.Progress.Remaining, Is.EqualTo(90));
        Assert.That(received, Has.Count.EqualTo(10));

        int catchupBatches = 0;
        while ((m = await session.ReadAsync()).BodyCase != ChangeMessage.BodyOneofCase.Progress)
        {
            if (m.BodyCase == ChangeMessage.BodyOneofCase.Batch)
            {
                Assert.That(m.Batch.Online, Is.False, "after the fallback the owner sends catch-up batches");
                catchupBatches++;
                received.UnionWith(m.Batch.Rows.Select(r => Wire.PkText(r.Pk)));
            }
        }
        Assert.That(m.Progress.Done, Is.True);
        Assert.That(catchupBatches, Is.GreaterThan(0));
        Assert.That(received, Has.Count.EqualTo(100), "every row arrives once the stream is online again");
    }

    [Test]
    public async Task Tail_of_a_commit_cut_at_the_online_batch_arrives_without_another_commit()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o =>
        {
            o.OnlineBatchRows = 10;
            o.OnlineLagVersions = 20;
        });
        // Nothing else may wake the owner: an Ack from a lab client is a commit too, it changes data_version.
        foreach (ClientNode c in lab.Clients)
            await lab.GoOfflineAsync(c);
        using RawSession session = RawSession.Open(lab);
        await session.SendStartAsync(session.NewStart(skipSnapshot: true));
        await session.ReadUntilAsync(m => m.BodyCase == ChangeMessage.BodyOneofCase.Progress && m.Progress.Done);

        // One commit of 15 rows: the online step takes 10, the other 5 are under the lag limit, so the stream stays online.
        InsertLogRows(lab, 15);
        long head = lab.Owner.Head();
        var received = new HashSet<string>();
        ChangeMessage m;
        while ((m = await session.ReadAsync()).BodyCase != ChangeMessage.BodyOneofCase.Head || m.Head.Version < head)
        {
            Assert.That(m.BodyCase, Is.Not.EqualTo(ChangeMessage.BodyOneofCase.Progress), "the tail stays online");
            if (m.BodyCase == ChangeMessage.BodyOneofCase.Batch)
                received.UnionWith(m.Batch.Rows.Select(r => Wire.PkText(r.Pk)));
        }
        Assert.That(received, Has.Count.EqualTo(15));
    }

    internal static void InsertLogRows(Lab lab, int count)
    {
        IEnumerable<string> values = Enumerable.Range(0, count)
            .Select(i => $"('{Wire.PkText(Guid.NewGuid())}', 'lag {i}', '2026-01-01 00:00:00', '2026-01-01 00:00:00')");
        OwnerSql.Exec(lab, $"INSERT INTO Log(Id, Text, CreatedOn, ModifiedOn) VALUES {string.Join(", ", values)}");
    }

    /// <summary>A Subscribe call made by hand to the lab's owner.</summary>
    private sealed class RawSession : IDisposable
    {
        private readonly GrpcChannel _channel;
        private readonly AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> _call;
        private readonly Lab _lab;

        private RawSession(Lab lab)
        {
            _lab = lab;
            _channel = GrpcChannel.ForAddress(lab.Owner.Address);
            _call = new Sync.SyncClient(_channel).Subscribe();
        }

        public string ClientId { get; } = "cl-raw-" + Guid.NewGuid().ToString("N")[..8];

        public static RawSession Open(Lab lab) => new(lab);

        public Start NewStart(string? instanceId = null, bool skipSnapshot = false) => new()
        {
            ClientId = ClientId,
            SchemaVersion = _lab.Owner.Store.Options.SchemaVersion,
            InstanceId = instanceId ?? _lab.Owner.Store.InstanceId,
            SkipSnapshotOnEmptyReplica = skipSnapshot,
        };

        public Task SendStartAsync(Start start) => SendAsync(new SubscribeMessage { Start = start });

        public Task SendAsync(SubscribeMessage m) => _call.RequestStream.WriteAsync(m);

        public async Task<ChangeMessage> ReadAsync()
        {
            using var cts = new CancellationTokenSource(s_wait);
            if (!await _call.ResponseStream.MoveNext(cts.Token))
                throw new InvalidOperationException("the owner closed the stream");
            return _call.ResponseStream.Current;
        }

        public async Task<ChangeMessage> ReadUntilAsync(Func<ChangeMessage, bool> match)
        {
            while (true)
            {
                ChangeMessage m = await ReadAsync();
                if (match(m))
                    return m;
            }
        }

        /// <summary>True when the owner closed its side of the stream.</summary>
        public async Task<bool> AtEndAsync()
        {
            using var cts = new CancellationTokenSource(s_wait);
            return !await _call.ResponseStream.MoveNext(cts.Token);
        }

        public void Dispose()
        {
            _call.Dispose();
            _channel.Dispose();
        }
    }
}
