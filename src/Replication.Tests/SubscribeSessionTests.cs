using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Replication.Owner;
using Replication.Protocol;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>The owner's Subscribe stream: a fault in either loop ends the call with an error, so the client reconnects (14).</summary>
public sealed class SubscribeSessionTests
{
    [Test]
    public async Task Main_loop_fault_ends_the_stream_and_the_client_reconnects()
    {
        await using Lab lab = await Lab.StartAsync();
        await lab.SyncNowAsync(lab.C1);

        // Raw SQL can write an Id that is not a GUID (5.1); the online step cannot encode it and throws.
        OwnerSql.Exec(lab, "INSERT INTO Log(Id, Text, CreatedOn, ModifiedOn) VALUES ('not-a-guid', 'raw SQL', '2026-01-01 00:00:00', '2026-01-01 00:00:00')");
        await Lab.WaitAsync(() => lab.C1.Agent.GetStatus().LastError?.Contains(nameof(FormatException)) == true, TimeSpan.FromSeconds(20),
            "Клієнт 1 не отримав помилки потоку");

        // Once the row is repaired, the reconnecting client catches up.
        OwnerSql.Exec(lab, $"UPDATE Log SET Id = '{Wire.PkText(Guid.NewGuid())}' WHERE Id = 'not-a-guid'");
        await lab.SettleAsync();
        Assert.That(Inspect.Diff(lab.Owner, lab.C1), Is.Empty);
    }

    [Test]
    public async Task Reader_fault_ends_the_stream_with_an_error()
    {
        await using Lab lab = await Lab.StartAsync();
        OwnerStore store = lab.Owner.Store;
        long head = lab.Owner.Head();
        CancellationToken ct = TestContext.CurrentContext.CancellationToken;
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

        RpcException e = Assert.ThrowsAsync<RpcException>(async () =>
        {
            while (await call.ResponseStream.MoveNext(ct))
            {
                // Catch-up and online messages, until the owner ends the stream.
            }
        })!;
        Assert.That(e.StatusCode, Is.EqualTo(StatusCode.Unknown));
        Assert.That(e.Status.Detail, Does.Contain(nameof(ArgumentException)));
    }

    [Test]
    public async Task Lagging_client_shows_catchup_then_online_and_ends_equal_to_the_owner()
    {
        await using Lab lab = await Lab.StartAsync(configureOwner: o =>
        {
            o.OnlineBatchRows = 10;
            o.OnlineLagVersions = 20;
            o.CatchupBatchRows = 10;
        });
        await lab.SyncNowAsync(lab.C1);
        var states = new List<AgentState> { lab.C1.Agent.GetStatus().State };
        lab.C1.Agent.StatusChanged += () =>
        {
            AgentState s = lab.C1.Agent.GetStatus().State;
            lock (states)
            {
                if (states[^1] != s)
                    states.Add(s);
            }
        };
        // Slow catch-up batches, so more rows land while the client catches up.
        lab.Owner.Store.Options.Faults.DelayStream(lab.C1.Replication.ClientId, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5));

        SubscribeProtocolTests.InsertLogRows(lab, 100);
        await Lab.WaitAsync(() => lab.C1.Agent.GetStatus().State == AgentState.Catchup, TimeSpan.FromSeconds(20), "Клієнт 1 не повернувся до досинхронізації");
        SubscribeProtocolTests.InsertLogRows(lab, 50);
        await lab.SettleAsync();

        lock (states)
            Assert.That(states, Is.EqualTo(new[] { AgentState.Online, AgentState.Catchup, AgentState.Online }));
        Assert.That(Inspect.Diff(lab.Owner, lab.C1), Is.Empty);
    }

    [Test]
    public async Task Fault_right_after_the_handshake_still_unmarks_the_client()
    {
        // A logging provider is user code; the owner's first accepted Start makes it throw once.
        int thrown = 0;
        var provider = new ThrowingLoggerProvider(m => m.Contains("Start accepted, head", StringComparison.Ordinal) && Interlocked.Exchange(ref thrown, 1) == 0);
        await using Lab lab = await Lab.StartAsync(logging: b => b.AddProvider(provider));
        string c1 = lab.C1.Replication.ClientId;

        // The first session fails after registering the client; the reconnect goes online.
        await lab.SyncNowAsync(lab.C1);
        Assert.That(Volatile.Read(ref thrown), Is.EqualTo(1));

        lab.C1.Link = false;
        await Lab.WaitAsync(() => !lab.Owner.Store.IsSubscribed(c1), TimeSpan.FromSeconds(10), "Клієнт 1 лишився відміченим як підписаний");
    }

    /// <summary>Throws from <see cref="ILogger.Log"/> on a message that matches <paramref name="throwOn"/>.</summary>
    private sealed class ThrowingLoggerProvider(Func<string, bool> throwOn) : ILoggerProvider, ILogger
    {
        public ILogger CreateLogger(string categoryName) => this;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (throwOn(formatter(state, exception)))
                throw new InvalidOperationException("logging provider failed");
        }

        public void Dispose() { }
    }
}
