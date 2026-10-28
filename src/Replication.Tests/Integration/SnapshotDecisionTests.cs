using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Replication.Owner;
using Replication.Protocol;
using Sample.Domain;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// The owner decides about the snapshot in the handshake (6.2): a new replica (all cursors empty) unless it asked for
/// catch-up, a cursor behind purged_version.
/// </summary>
[TestFixture]
public sealed class SnapshotDecisionTests : LabTests
{
    [Test]
    public async Task Owner_answers_a_new_replica_with_SnapshotRequired_unless_it_skips_the_snapshot()
    {
        Lab lab = await StartAsync();

        Assert.That((await FirstAnswerAsync(lab, skipSnapshotOnEmptyReplica: false)).BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.SnapshotRequired));
        Assert.That((await FirstAnswerAsync(lab, skipSnapshotOnEmptyReplica: true)).BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.Batch), "catch-up from zero starts with data");
    }

    [Test]
    public async Task Owner_refuses_a_Start_with_a_cursor_behind_purged_version_even_with_skip_snapshot_on_empty_replica()
    {
        Lab lab = await StartAsync();
        await using (SampleDbContext db = lab.Owner.Db())
            await db.Log.ExecuteDeleteAsync(TestContext.CurrentContext.CancellationToken);
        // No clients yet: every tombstone goes, purged_version > 0.
        OwnerStore.PurgeResult purged = await lab.Owner.PurgeAsync();
        Assert.That(purged.PurgedVersion, Is.GreaterThan(1));

        ChangeMessage first = await FirstAnswerAsync(lab, skipSnapshotOnEmptyReplica: true, logCursor: 1);

        Assert.That(first.BodyCase, Is.EqualTo(ChangeMessage.BodyOneofCase.SnapshotRequired));
        Assert.That(first.SnapshotRequired.Reason, Does.Contain("purged_version"));
    }

    /// <summary>The owner's first answer to a hand-built Start with zero cursors, or a Log cursor of <paramref name="logCursor"/>.</summary>
    private static async Task<ChangeMessage> FirstAnswerAsync(Lab lab, bool skipSnapshotOnEmptyReplica, long logCursor = 0)
    {
        using GrpcChannel channel = GrpcChannel.ForAddress(lab.Owner.Address);
        using AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> call = new Sync.SyncClient(channel).Subscribe();
        var start = new Start
        {
            ClientId = Guid.NewGuid().ToString(),
            SchemaVersion = lab.Owner.Store.Options.SchemaVersion,
            InstanceId = lab.Owner.Store.InstanceId,
            SkipSnapshotOnEmptyReplica = skipSnapshotOnEmptyReplica,
        };
        if (logCursor > 0)
            start.Cursors.Add(new TableCursor { Tbl = "Log", Cursor = logCursor });
        await call.RequestStream.WriteAsync(new SubscribeMessage { Start = start });
        Assert.That(await call.ResponseStream.MoveNext(), Is.True);
        return call.ResponseStream.Current;
    }
}
