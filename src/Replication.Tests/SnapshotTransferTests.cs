using System.Security.Cryptography;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Replication.Client;
using Replication.Protocol;
using Replication.Tests.Integration;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>
/// Design 6.3, 10.3: the snapshot file is served in chunks that resume by id and offset, swept after SnapshotKeep, and
/// the client installs it only when it is complete and its hash matches.
/// </summary>
public sealed class SnapshotTransferTests
{
    private const int ChunkBytes = 4096;

    [Test]
    public async Task Snapshot_is_served_in_contiguous_chunks_each_carrying_the_total_size_and_the_hash_of_the_file()
    {
        await using Lab lab = await StartLabAsync();

        List<SnapshotChunk> chunks = await DownloadAsync(lab, "", 0);

        Assert.That(chunks, Has.Count.GreaterThan(1), "the file is bigger than one chunk");
        Assert.That(chunks.Select(c => c.Offset), Is.EqualTo(Offsets(chunks)));
        Assert.That(chunks.Select(c => c.Data.Length), Has.All.LessThanOrEqualTo(ChunkBytes));
        Assert.That(chunks.Select(c => c.TotalSize).Distinct(), Is.EqualTo(new[] { (long)chunks.Sum(c => c.Data.Length) }));
        Assert.That(chunks.Select(c => c.SnapshotId).Distinct().Count(), Is.EqualTo(1));
        Assert.That(chunks.Select(c => c.Sha256.ToBase64()).Distinct().Count(), Is.EqualTo(1), "the hash of the whole file is in every chunk");
        Assert.That(SHA256.HashData(Join(chunks)), Is.EqualTo(chunks[0].Sha256.ToByteArray()));
        Assert.That(chunks[0].Version, Is.EqualTo(lab.Owner.Head()));
        Assert.That(chunks[0].InstanceId, Is.EqualTo(lab.Owner.Store.InstanceId));
    }

    [Test]
    public async Task Snapshot_request_with_an_id_and_an_offset_resumes_the_same_file_from_that_offset()
    {
        await using Lab lab = await StartLabAsync();
        List<SnapshotChunk> whole = await DownloadAsync(lab, "", 0);
        long from = whole[2].Offset;

        List<SnapshotChunk> rest = await DownloadAsync(lab, whole[0].SnapshotId, from);

        Assert.That(rest[0].Offset, Is.EqualTo(from));
        Assert.That(rest.Select(c => c.SnapshotId).Distinct(), Is.EqualTo(new[] { whole[0].SnapshotId }), "no new snapshot was made");
        Assert.That(Join(rest), Is.EqualTo(Join(whole.Skip(2))));
        Assert.That(Directory.GetFiles(SnapshotDir(lab), "snap-*.db"), Has.Length.EqualTo(1));
    }

    [Test]
    public async Task Snapshot_request_with_an_unknown_id_makes_a_new_snapshot_and_starts_at_zero_whatever_the_offset()
    {
        await using Lab lab = await StartLabAsync();

        List<SnapshotChunk> chunks = await DownloadAsync(lab, "snap-nope", 3 * ChunkBytes);

        Assert.That(chunks[0].Offset, Is.Zero);
        Assert.That(chunks[0].SnapshotId, Is.Not.EqualTo("snap-nope"));
        Assert.That(SHA256.HashData(Join(chunks)), Is.EqualTo(chunks[0].Sha256.ToByteArray()));
    }

    [Test]
    public async Task Snapshot_file_swept_from_the_folder_is_replaced_by_a_new_one_on_resume()
    {
        await using Lab lab = await StartLabAsync();
        List<SnapshotChunk> first = await DownloadAsync(lab, "", 0);
        File.Delete(Path.Combine(SnapshotDir(lab), first[0].SnapshotId + ".db"));

        List<SnapshotChunk> again = await DownloadAsync(lab, first[0].SnapshotId, first[1].Offset);

        Assert.That(again[0].SnapshotId, Is.Not.EqualTo(first[0].SnapshotId));
        Assert.That(again[0].Offset, Is.Zero, "the old offset means nothing in a new file");
    }

    [Test]
    public async Task Next_snapshot_deletes_the_files_older_than_SnapshotKeep()
    {
        await using Lab lab = await StartLabAsync(keep: TimeSpan.Zero);
        List<SnapshotChunk> first = await DownloadAsync(lab, "", 0);
        string firstFile = Path.Combine(SnapshotDir(lab), first[0].SnapshotId + ".db");
        Assert.That(File.Exists(firstFile), Is.True);

        List<SnapshotChunk> second = await DownloadAsync(lab, "", 0);

        Assert.That(File.Exists(firstFile), Is.False, "swept when the next snapshot was made");
        Assert.That(File.Exists(Path.Combine(SnapshotDir(lab), second[0].SnapshotId + ".db")), Is.True);
    }

    [Test]
    public async Task Snapshot_registers_the_client_at_the_snapshot_version_in_every_table()
    {
        await using Lab lab = await StartLabAsync();
        string clientId = lab.C1.Replication.ClientId;

        List<SnapshotChunk> chunks = await DownloadAsync(lab, "", 0, clientId);

        Assert.That(OwnerSql.Scalar(lab, $"SELECT COUNT(*) FROM _sync_client_cursors WHERE client_id = '{clientId}' AND cursor = {chunks[0].Version}"),
            Is.EqualTo(lab.Owner.Store.Model.Tables.Count), "tombstones above V are kept for this client");
    }

    [Test]
    public async Task Download_whose_hash_does_not_match_is_not_installed_and_the_part_file_is_dropped()
    {
        byte[] data = new byte[300];
        await using FakeOwner owner = await FakeOwner.StartAsync(async w =>
        {
            await w.WriteAsync(Chunk("snap-bad", 0, data.Length, data, new byte[32]));
        });
        await using Probe probe = Probe.Start(owner);

        await probe.WaitForErrorAsync("the snapshot hash does not match");

        probe.AssertNothingInstalled();
        Assert.That(Directory.GetFiles(probe.Dir, "*.part"), Is.Empty);
    }

    [Test]
    public async Task Download_that_ends_short_of_the_total_size_is_not_installed_and_the_part_file_stays_for_resume()
    {
        byte[] data = new byte[300];
        await using FakeOwner owner = await FakeOwner.StartAsync(async w =>
        {
            await w.WriteAsync(Chunk("snap-short", 0, data.Length + 100, data, SHA256.HashData(data)));
        });
        await using Probe probe = Probe.Start(owner);

        await probe.WaitForErrorAsync("the snapshot was cut off");

        probe.AssertNothingInstalled();
        Assert.That(Directory.GetFiles(probe.Dir, "*.part"), Has.Length.EqualTo(1));
    }

    [Test]
    public async Task Download_with_no_chunks_at_all_is_an_error_and_nothing_is_installed()
    {
        await using FakeOwner owner = await FakeOwner.StartAsync(_ => Task.CompletedTask);
        await using Probe probe = Probe.Start(owner);

        await probe.WaitForErrorAsync("the snapshot is empty");

        probe.AssertNothingInstalled();
    }

    private static async Task<Lab> StartLabAsync(TimeSpan? keep = null)
    {
        string snapshots = Path.Combine(TestDb.NewDir(), "snapshots");
        return await Lab.StartAsync(configureOwner: o =>
        {
            o.SnapshotChunkBytes = ChunkBytes;
            o.SnapshotDirectory = snapshots;
            if (keep is { } k)
                o.SnapshotKeep = k;
        });
    }

    private static string SnapshotDir(Lab lab) => lab.Owner.Store.Options.SnapshotDirectory!;

    private static async Task<List<SnapshotChunk>> DownloadAsync(Lab lab, string id, long offset, string? clientId = null)
    {
        using GrpcChannel channel = GrpcChannel.ForAddress(lab.Owner.Address);
        using AsyncServerStreamingCall<SnapshotChunk> call = new Sync.SyncClient(channel).Snapshot(
            new SnapshotRequest { ClientId = clientId ?? "cl-snap-" + Guid.NewGuid().ToString("N")[..8], SnapshotId = id, Offset = offset });
        var chunks = new List<SnapshotChunk>();
        while (await call.ResponseStream.MoveNext(CancellationToken.None))
            chunks.Add(call.ResponseStream.Current);
        return chunks;
    }

    private static long[] Offsets(List<SnapshotChunk> chunks)
    {
        long[] offsets = new long[chunks.Count];
        long at = chunks[0].Offset;
        for (int i = 0; i < chunks.Count; i++)
        {
            offsets[i] = at;
            at += chunks[i].Data.Length;
        }
        return offsets;
    }

    private static byte[] Join(IEnumerable<SnapshotChunk> chunks) => [.. chunks.SelectMany(c => c.Data.ToByteArray())];

    private static SnapshotChunk Chunk(string id, long offset, long total, byte[] data, byte[] sha) => new()
    {
        SnapshotId = id,
        Offset = offset,
        TotalSize = total,
        Data = ByteString.CopyFrom(data),
        Version = 1,
        InstanceId = FakeOwner.Instance,
        Sha256 = ByteString.CopyFrom(sha),
    };

    /// <summary>An owner that only says SnapshotRequired and then serves whatever the test writes as the snapshot.</summary>
    private sealed class FakeOwner : IAsyncDisposable
    {
        public const string Instance = "inst-fake";

        private readonly WebApplication _app;

        private FakeOwner(WebApplication app, string address)
        {
            _app = app;
            Address = address;
        }

        public string Address { get; }

        public static async Task<FakeOwner> StartAsync(Func<IServerStreamWriter<SnapshotChunk>, Task> snapshot)
        {
            WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(k => k.Listen(System.Net.IPAddress.Loopback, 0, o => o.Protocols = HttpProtocols.Http2));
            builder.Services.AddGrpc();
            builder.Services.AddSingleton(new Behavior(snapshot));
            WebApplication app = builder.Build();
            app.MapGrpcService<FakeSync>();
            await app.StartAsync();
            return new FakeOwner(app, $"http://127.0.0.1:{BindingAddress.Parse(app.Urls.First()).Port}");
        }

        public async ValueTask DisposeAsync()
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await _app.StopAsync(stop.Token);
            await _app.DisposeAsync();
        }

        public sealed record Behavior(Func<IServerStreamWriter<SnapshotChunk>, Task> Snapshot);

        public sealed class FakeSync(Behavior behavior) : Sync.SyncBase
        {
            public override Task<IdentityReply> Identity(IdentityRequest request, ServerCallContext context) =>
                Task.FromResult(new IdentityReply { InstanceId = Instance });

            public override async Task Subscribe(IAsyncStreamReader<SubscribeMessage> requestStream, IServerStreamWriter<ChangeMessage> responseStream, ServerCallContext context)
            {
                await requestStream.MoveNext(context.CancellationToken);
                await responseStream.WriteAsync(new ChangeMessage { SnapshotRequired = new SnapshotRequired { Reason = "тест", InstanceId = Instance } });
            }

            public override Task Snapshot(SnapshotRequest request, IServerStreamWriter<SnapshotChunk> responseStream, ServerCallContext context) =>
                behavior.Snapshot(responseStream);
        }
    }

    /// <summary>A client with an agent pointed at a <see cref="FakeOwner"/>.</summary>
    private sealed class Probe : IAsyncDisposable
    {
        private readonly ClientReplication _replication;
        private readonly SyncAgent _agent;

        private Probe(string dir, ClientReplication replication, SyncAgent agent)
        {
            Dir = dir;
            _replication = replication;
            _agent = agent;
        }

        public string Dir { get; }

        public static Probe Start(FakeOwner owner)
        {
            string dir = TestDb.NewDir();
            ClientReplication replication = ClientNode.Create(Path.Combine(dir, "client.db"));
            var agent = new SyncAgent(replication.Store, NullLogger<SyncAgent>.Instance, owner.Address, "клієнт");
            agent.Start();
            agent.LinkEnabled = true;
            return new Probe(dir, replication, agent);
        }

        public Task WaitForErrorAsync(string text) =>
            Lab.WaitAsync(() => _agent.GetStatus().LastError?.Contains(text, StringComparison.Ordinal) == true, TimeSpan.FromSeconds(30), $"no error «{text}»; last: {_agent.GetStatus().LastError}");

        public void AssertNothingInstalled()
        {
            string db = Path.Combine(Dir, "client.db");
            Assert.That(Rows.Scalar(db, "SELECT COUNT(*) FROM _sync_instances WHERE snapshot_version IS NOT NULL"), Is.EqualTo(0L), "no snapshot version was recorded");
            Assert.That(Rows.Scalar(db, "SELECT COUNT(*) FROM Item"), Is.EqualTo(0L));
        }

        public async ValueTask DisposeAsync()
        {
            await _agent.DisposeAsync();
            await _replication.DisposeAsync();
        }
    }
}
