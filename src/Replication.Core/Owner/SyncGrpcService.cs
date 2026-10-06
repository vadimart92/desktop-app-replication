using Google.Protobuf;
using Grpc.Core;
using Replication.Protocol;

namespace Replication.Owner;

/// <summary>The only gRPC server, hosted by the owner (2, 6.1).</summary>
internal sealed class SyncGrpcService(OwnerStore store, SnapshotStore snapshots) : Sync.SyncBase
{
    private readonly ApplyHandler _apply = new(store);

    public override Task Subscribe(IAsyncStreamReader<SubscribeMessage> requestStream, IServerStreamWriter<ChangeMessage> responseStream, ServerCallContext context) =>
        new SubscribeSession(store, requestStream, responseStream, context.CancellationToken).RunAsync();

    public override Task<ApplyReply> Apply(ApplyRequest request, ServerCallContext context)
    {
        ApplyReply reply = _apply.Apply(request);
        store.Options.Log.Write("owner", $"Apply від {Short(request.ClientId)}: {string.Join("; ", reply.Results.Select(r => $"#{r.Seq} {r.Status}{(r.HasVersion ? $" → v{r.Version}" : "")}{(r.Reason.Length > 0 ? $" ({r.Reason})" : "")}"))}; applied_seq = {reply.AppliedUpToSeq}",
            reply.Results.Any(r => r.Status is ResultStatus.Rejected or ResultStatus.Ignored || r.Changed.Count > 0) ? SyncLogLevel.Warn : SyncLogLevel.Info);
        return Task.FromResult(reply);
    }

    public override async Task Snapshot(SnapshotRequest request, IServerStreamWriter<SnapshotChunk> responseStream, ServerCallContext context)
    {
        SnapshotStore.Snap snap = (request.SnapshotId.Length > 0 ? snapshots.Find(request.SnapshotId) : null) ?? await snapshots.CreateAsync(request.ClientId, context.CancellationToken);
        long offset = snap.Id == request.SnapshotId ? request.Offset : 0;
        if (offset > 0)
            store.Options.Log.Write("owner", $"знімок {snap.Id}: продовження з {offset / 1024} КБ");
        byte[] buffer = new byte[store.Options.SnapshotChunkBytes];
        await using FileStream f = File.OpenRead(snap.Path);
        f.Position = offset;
        ByteString hash = ByteString.CopyFrom(snap.Sha256);
        while (true)
        {
            int n = await f.ReadAsync(buffer, context.CancellationToken);
            if (n == 0)
                break;
            await responseStream.WriteAsync(new SnapshotChunk
            {
                SnapshotId = snap.Id,
                Offset = offset,
                TotalSize = snap.Size,
                Data = ByteString.CopyFrom(buffer, 0, n),
                Version = snap.Version,
                InstanceId = snap.InstanceId,
                Sha256 = hash,
            }, context.CancellationToken);
            offset += n;
        }
    }

    private static string Short(string id) => id.Length > 8 ? id[..8] : id;
}
