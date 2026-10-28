using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Replication.Client;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>The agent's TCP stream goes through the caller's decorator: the lab's meter and emulated link, or a test's own.</summary>
[TestFixture]
public sealed class StreamHookTests : LabTests
{
    [Test]
    public async Task The_decorator_wraps_every_connection_and_sees_both_directions()
    {
        Lab lab = await StartAsync();
        ClientReplication replication = ClientNode.Create(Path.Combine(TestDb.NewDir(), "client.db"));
        try
        {
            var streams = new List<CountingStream>();
            await using SyncAgent agent = new(replication.Store, NullLogger<SyncAgent>.Instance, lab.Owner.Address, "клієнт", wrapStream: s =>
            {
                var c = new CountingStream(s);
                lock (streams)
                    streams.Add(c);
                return c;
            });
            agent.Start();
            agent.LinkEnabled = true;
            await Lab.WaitAsync(() => agent.GetStatus().State == AgentState.Online, TimeSpan.FromSeconds(30), "the agent did not get online");

            CountingStream first;
            lock (streams)
                first = streams.Single();
            Assert.That(first.BytesRead, Is.GreaterThan(0), "owner → client");
            Assert.That(first.BytesWritten, Is.GreaterThan(0), "client → owner");

            agent.LinkEnabled = false;
            await Lab.WaitAsync(() => agent.GetStatus().State == AgentState.Offline, TimeSpan.FromSeconds(30), "the agent did not go offline");
            agent.LinkEnabled = true;
            await Lab.WaitAsync(() => agent.GetStatus().State == AgentState.Online, TimeSpan.FromSeconds(30), "the agent did not reconnect");

            lock (streams)
                Assert.That(streams, Has.Count.EqualTo(2), "a reconnect is a new TCP connection");
        }
        finally
        {
            await replication.DisposeAsync();
        }
    }

    [Test]
    public async Task Lab_clients_count_their_bytes()
    {
        Lab lab = await StartSyncedAsync();

        await lab.Owner.AddLogAsync(1);
        await lab.SettleAsync();

        Assert.That(lab.C1.Meter.BytesDown, Is.GreaterThan(0));
        Assert.That(lab.C1.Meter.BytesUp, Is.GreaterThan(0));
    }

    /// <summary>Passes everything through and counts the bytes per direction.</summary>
    private sealed class CountingStream(Stream inner) : Stream
    {
        private long _read;
        private long _written;

        public long BytesRead => Interlocked.Read(ref _read);
        public long BytesWritten => Interlocked.Read(ref _written);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            int n = await inner.ReadAsync(buffer, ct);
            Interlocked.Add(ref _read, n);
            return n;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
            ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

        public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer, offset, count).GetAwaiter().GetResult();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            await inner.WriteAsync(buffer, ct);
            Interlocked.Add(ref _written, buffer.Length);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
            WriteAsync(buffer.AsMemory(offset, count), ct).AsTask();

        public override void Write(byte[] buffer, int offset, int count) => WriteAsync(buffer, offset, count).GetAwaiter().GetResult();

        public override Task FlushAsync(CancellationToken ct) => inner.FlushAsync(ct);

        public override void Flush() => inner.Flush();

        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
