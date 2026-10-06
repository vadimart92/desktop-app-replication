using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Owner;
using Replication.Protocol;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

/// <summary>The owner's Subscribe stream: a fault in either loop ends the call with an error, so the client reconnects (14).</summary>
public class SubscribeSessionTests
{
    private static void OwnerSql(Lab lab, string sql)
    {
        using SqliteConnection c = lab.Owner.Store.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Main_loop_fault_ends_the_stream_and_the_client_reconnects()
    {
        await using Lab lab = await Lab.StartAsync();
        await lab.SyncNowAsync(lab.C1);

        // Raw SQL can write an Id that is not a GUID (5.1); the online step cannot encode it and throws.
        OwnerSql(lab, "INSERT INTO Log(Id, Text, CreatedOn, ModifiedOn) VALUES ('not-a-guid', 'raw SQL', '2026-01-01 00:00:00', '2026-01-01 00:00:00')");
        await Lab.WaitAsync(() => lab.C1.Agent.GetStatus().LastError?.Contains(nameof(FormatException)) == true, TimeSpan.FromSeconds(20),
            "Клієнт 1 не отримав помилки потоку");

        // Once the row is repaired, the reconnecting client catches up.
        OwnerSql(lab, $"UPDATE Log SET Id = '{Wire.PkText(Guid.NewGuid())}' WHERE Id = 'not-a-guid'");
        await lab.SettleAsync();
        Assert.Empty(Inspect.Diff(lab.Owner, lab.C1));
    }

    [Fact]
    public async Task Reader_fault_ends_the_stream_with_an_error()
    {
        await using Lab lab = await Lab.StartAsync();
        OwnerStore store = lab.Owner.Store;
        long head = lab.Owner.Head();
        CancellationToken ct = TestContext.Current.CancellationToken;
        using GrpcChannel channel = GrpcChannel.ForAddress(lab.Owner.Address);
        var client = new Sync.SyncClient(channel);
        using AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> call = client.Subscribe(deadline: DateTime.UtcNow.AddSeconds(20), cancellationToken: ct);

        var start = new Start { ClientId = "raw-reader-fault", SchemaVersion = store.Options.SchemaVersion, InstanceId = store.InstanceId };
        foreach (SyncTable t in store.Model.Tables)
            start.Cursors.Add(new TableCursor { Tbl = t.Name, Cursor = head });
        await call.RequestStream.WriteAsync(new SubscribeMessage { Start = start }, ct);
        // An Ack that names a table twice cannot be saved: the session's reader throws. The request stream stays open.
        var ack = new Ack();
        ack.Cursors.Add(new TableCursor { Tbl = "Item", Cursor = head });
        ack.Cursors.Add(new TableCursor { Tbl = "Item", Cursor = head });
        await call.RequestStream.WriteAsync(new SubscribeMessage { Ack = ack }, ct);

        RpcException e = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            while (await call.ResponseStream.MoveNext(ct))
            {
                // Catch-up and online messages, until the owner ends the stream.
            }
        });
        Assert.Equal(StatusCode.Unknown, e.StatusCode);
        Assert.Contains(nameof(ArgumentException), e.Status.Detail);
    }
}
