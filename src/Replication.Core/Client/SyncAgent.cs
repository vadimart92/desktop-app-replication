using System.Diagnostics;
using System.Net.Sockets;
using System.Security.Cryptography;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Net;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Client;

public enum AgentState
{
    Offline,
    Connecting,
    Flushing,
    Snapshot,
    Catchup,
    Online,
    SchemaMismatch
}

public enum SnapshotMode
{
    Auto,
    File,
    EmptyReplica
}

public sealed class AgentOptions
{
    public int SchemaVersion { get; set; } = 1;
    public TimeSpan AckInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan ApplyTimeout { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan ReconnectMin { get; set; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan ReconnectMax { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>An Apply batch is sized by send time, not by count: ~15 s at the measured rate, within 16 KB-1 MB (6.5).</summary>
    public TimeSpan ApplyTargetTime { get; set; } = TimeSpan.FromSeconds(15);
    public int ApplyMinBytes { get; set; } = 16 * 1024;
    public int ApplyMaxBytes { get; set; } = 1024 * 1024;

    public SnapshotMode SnapshotMode { get; set; } = SnapshotMode.Auto;

    /// <summary>File if size / measured speed is under this, otherwise an empty replica filled by the stream (6.3).</summary>
    public TimeSpan SnapshotFileThreshold { get; set; } = TimeSpan.FromMinutes(5);
    public double AssumedDownBytesPerSecond { get; set; } = 250_000;

    /// <summary>"The link does not keep up" when the catch-up backlog keeps growing this long (12).</summary>
    public TimeSpan LagWarningWindow { get; set; } = TimeSpan.FromMinutes(3);

    public int Http2StreamWindowBytes { get; set; } = 1024 * 1024;
}

public sealed record AgentStatus(
    AgentState State, bool LinkEnabled, string? InstanceId, long Remaining, bool LagWarning, DateTimeOffset? OfflineSince,
    int Pending, int PendingDeletes, int InFlight, double? SnapshotProgress, string? LastError, IReadOnlyDictionary<string, string> Cursors);

/// <summary>
/// One per owner instance (4): offline → handshake → snapshot if needed → change stream (catch-up, then online),
/// in parallel with sending the outbox. The client always initiates; it reconnects with exponential back-off.
/// </summary>
public sealed class SyncAgent : IAsyncDisposable
{
    private static readonly Metadata s_gzip = new() { { "grpc-internal-encoding-request", "gzip" } };

    private readonly ClientStore _store;
    private readonly SyncLog _log;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SqliteConnection _conn;
    private readonly AsyncSignal _linkSignal = new();
    private readonly AsyncSignal _outboxSignal = new();
    private readonly CancellationTokenSource _life = new();
    private readonly List<RowRef> _needFull = [];
    private readonly List<ArchiveRetry> _archiveRetry = [];
    private readonly List<(DateTimeOffset At, long Remaining)> _lag = [];
    private readonly object _cursorsLock = new();
    private Dictionary<string, CursorState> _cursors;
    private CancellationTokenSource? _sessionCts;
    private Func<SubscribeMessage, Task>? _write;
    private Task? _loop;
    private string? _instance;
    private string[] _open = [];
    private volatile bool _link;
    private volatile bool _ackDirty;
    private AgentState _state = AgentState.Offline;
    private DateTimeOffset? _offlineSince = DateTimeOffset.Now;
    private long _remaining;
    private bool _lagWarning;
    private string? _lastError;
    private Download? _download;
    private double? _snapshotProgress;
    private double _applyRate;
    private readonly Dictionary<string, int> _archiveTries = [];

    private sealed record Download(string Id, string Path, long Total);

    private sealed record ArchiveRetry(string Table, string Pk, string Why, long Version, int Tries);

    public SyncAgent(ClientStore store, string address, string name, AgentOptions? options = null, SyncLog? log = null, string? label = null)
    {
        _store = store;
        Address = address;
        Name = name;
        Label = label ?? name;
        Options = options ?? new AgentOptions();
        _log = log ?? SyncLog.Null;
        store.EnsureInstance(address, name);
        // Children may arrive before parents during catch-up (7).
        _conn = store.Open(foreignKeys: false);
        _instance = store.InstanceOf(_conn, address);
        _cursors = _instance is null ? NewCursors() : store.LoadCursors(_conn, _instance);
    }

    public string Address { get; }
    public string Name { get; }
    public string Label { get; }
    public AgentOptions Options { get; }
    public string? InstanceId => _instance;
    public WireMeter Meter { get; } = new WireMeter();
    public NetworkProfile Network { get; } = new NetworkProfile();

    /// <summary>Drop the next ApplyReply as if it was lost in the network (the demo's "lost reply").</summary>
    public bool LoseNextApplyReply { get; set; }

    /// <summary>Outbox rows waiting for an archive retry (11.2, step 6).</summary>
    public bool HasPendingArchiveWork => _archiveRetry.Count > 0;

    public CursorState CursorOf(string table)
    {
        lock (_cursorsLock)
            return _cursors[table].Clone();
    }

    public long MinCursor
    {
        get
        {
            lock (_cursorsLock)
                return _cursors.Values.Min(k => k.Cursor);
        }
    }

    public event System.Action? StatusChanged;

    /// <summary>Rows of these tables changed in the replica (a batch, a reply, a snapshot).</summary>
    public event System.Action<IReadOnlyCollection<string>>? DataChanged;

    private void Log(string text, SyncLogLevel level = SyncLogLevel.Info) => _log.Write(Label, text, level);

    private Dictionary<string, CursorState> NewCursors() => _store.Model.Tables.ToDictionary(t => t.Name, _ => new CursorState(0), StringComparer.Ordinal);

    // Control.

    public bool LinkEnabled
    {
        get => _link;
        set
        {
            if (_link == value)
                return;
            _link = value;
            Log(value ? "зв'язок є" : "зв'язок вимкнено", value ? SyncLogLevel.Info : SyncLogLevel.Warn);
            if (!value)
                _sessionCts?.Cancel();
            _linkSignal.Set();
            StatusChanged?.Invoke();
        }
    }

    public void Start() => _loop ??= Task.Run(() => RunAsync(_life.Token));

    /// <summary>Tables open in the UI go first in catch-up (6.4).</summary>
    public void SetOpenTables(params string[] tables)
    {
        _open = tables;
        if (_write is { } w && _state == AgentState.Catchup)
        {
            var m = new SubscribeMessage { OpenTables = new OpenTables() };
            m.OpenTables.Tables.AddRange(tables);
            _ = w(m);
        }
    }

    /// <summary>Wakes the sender after the WriteRouter committed an outbox row.</summary>
    public void NotifyOutbox()
    {
        _outboxSignal.Set();
        StatusChanged?.Invoke();
    }

    private void SetState(AgentState s)
    {
        if (_state == s)
            return;
        _state = s;
        if (s == AgentState.Offline)
            _offlineSince ??= DateTimeOffset.Now;
        else if (s is AgentState.Catchup or AgentState.Online)
            _offlineSince = null;
        if (s != AgentState.Catchup)
        {
            _lag.Clear();
            _lagWarning = false;
        }
        StatusChanged?.Invoke();
    }

    public AgentStatus GetStatus()
    {
        int pending = 0;
        int deletes = 0;
        int inflight = 0;
        if (_instance is not null)
        {
            using SqliteConnection c = _store.Open();
            pending = (int)c.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i AND sent < 2", null, ("@i", _instance));
            deletes = (int)c.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i AND sent < 2 AND kind IN (3, 5)", null, ("@i", _instance));
            inflight = (int)c.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i AND sent = 1", null, ("@i", _instance));
        }
        Dictionary<string, string> cursors;
        lock (_cursorsLock)
            cursors = _cursors.ToDictionary(x => x.Key, x => x.Value.ToString());
        return new AgentStatus(_state, _link, _instance, _remaining, _lagWarning, _link && _state is AgentState.Catchup or AgentState.Online ? null : _offlineSince,
            pending, deletes, inflight, _snapshotProgress, _lastError, cursors);
    }

    /// <summary>Applies a schema version change (the demo's "new schema") and reconnects.</summary>
    public void SetSchemaVersion(int v)
    {
        Options.SchemaVersion = v;
        _sessionCts?.Cancel();
        _linkSignal.Set();
    }

    // Main loop.

    private async Task RunAsync(CancellationToken life)
    {
        TimeSpan backoff = Options.ReconnectMin;
        while (!life.IsCancellationRequested)
        {
            if (!_link)
            {
                SetState(AgentState.Offline);
                await _linkSignal.WaitAsync(TimeSpan.FromSeconds(1), life);
                continue;
            }
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(life);
            _sessionCts = cts;
            try
            {
                int schemaAtStart = Options.SchemaVersion;
                await SessionAsync(cts.Token);
                backoff = Options.ReconnectMin;
                if (_state == AgentState.SchemaMismatch)
                {
                    while (_link && Options.SchemaVersion == schemaAtStart && !life.IsCancellationRequested)
                        await _linkSignal.WaitAsync(TimeSpan.FromSeconds(1), life);
                }
            }
            catch (Exception) when (life.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                if (cts.IsCancellationRequested && !_link)
                {
                    SetState(AgentState.Offline);
                    continue;
                }
                _lastError = e is RpcException re ? $"{re.StatusCode}: {re.Status.Detail}" : e.Message;
                if (_state != AgentState.SchemaMismatch)
                    SetState(AgentState.Offline);
                Log($"зв'язок обірвався ({_lastError}), повтор через {backoff.TotalSeconds:0.#} с", SyncLogLevel.Warn);
                await _linkSignal.WaitAsync(backoff, life);
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, Options.ReconnectMax.Ticks));
            }
            finally
            {
                _sessionCts = null;
                _write = null;
            }
        }
    }

    private GrpcChannel CreateChannel()
    {
        var handler = new SocketsHttpHandler
        {
            // Window ≥ bandwidth × RTT (6.1).
            InitialHttp2StreamWindowSize = Options.Http2StreamWindowBytes,
            KeepAlivePingDelay = TimeSpan.FromSeconds(20),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(20),
            KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
            ConnectTimeout = TimeSpan.FromSeconds(15),
            ConnectCallback = async (ctx, ct) =>
            {
                var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                try
                {
                    await s.ConnectAsync(ctx.DnsEndPoint, ct);
                }
                catch
                {
                    s.Dispose();
                    throw;
                }
                return new ShapedStream(new NetworkStream(s, ownsSocket: true), Meter, Network);
            },
        };
        return GrpcChannel.ForAddress(Address, new GrpcChannelOptions
        {
            HttpHandler = handler,
            DisposeHttpClient = true,
            MaxReceiveMessageSize = 64 * 1024 * 1024,
            MaxSendMessageSize = 64 * 1024 * 1024,
        });
    }

    private async Task SessionAsync(CancellationToken ct)
    {
        using GrpcChannel channel = CreateChannel();
        var client = new Sync.SyncClient(channel);
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            SetState(AgentState.Connecting);
            using AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> call = client.Subscribe(s_gzip, cancellationToken: ct);
            var writeGate = new SemaphoreSlim(1, 1);

            async Task Write(SubscribeMessage m)
            {
                await writeGate.WaitAsync(ct);
                try
                {
                    Meter.CountMessage("↑ " + m.BodyCase, m.CalculateSize());
                    await call.RequestStream.WriteAsync(m, ct);
                }
                finally
                {
                    writeGate.Release();
                }
            }

            Start start = BuildStart();
            string cursors;
            lock (_cursorsLock)
                cursors = string.Join(", ", _cursors.Select(x => $"{x.Key}={x.Value}"));
            Log($"→ Start: схема {start.SchemaVersion}, {(start.InstanceId.Length > 0 ? $"instance_id {start.InstanceId}, " + cursors : "репліки нема")}");
            await Write(new SubscribeMessage { Start = start });
            if (!await call.ResponseStream.MoveNext(ct))
                throw new IOException("власник закрив потік");
            ChangeMessage first = call.ResponseStream.Current;
            Meter.CountMessage("↓ " + first.BodyCase, first.CalculateSize());

            if (first.BodyCase == ChangeMessage.BodyOneofCase.SchemaMismatch)
            {
                SetState(AgentState.SchemaMismatch);
                Log($"← SchemaMismatch: схема власника {first.SchemaMismatch.OwnerSchemaVersion}, моя {Options.SchemaVersion}. Синк зупинено, черга зберігається", SyncLogLevel.Warn);
                return;
            }
            if (first.BodyCase == ChangeMessage.BodyOneofCase.SnapshotRequired)
            {
                Log($"← SnapshotRequired: {first.SnapshotRequired.Reason}, знімок ~{first.SnapshotRequired.SizeBytes / 1024} КБ", SyncLogLevel.Warn);
                await SnapshotFlowAsync(client, first.SnapshotRequired, ct);
                // A new Start with the fresh replica.
                continue;
            }

            SetState(AgentState.Catchup);
            Log("Start прийнято: потік пішов, Apply паралельно", SyncLogLevel.Ok);
            _write = Write;
            await HandleAsync(first);
            using CancellationTokenSource inner = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task[] tasks = new[]
            {
                ReadLoopAsync(call, inner.Token),
                ApplyLoopAsync(client, inner.Token),
                AckLoopAsync(Write, inner.Token),
            };
            Task done = await Task.WhenAny(tasks);
            inner.Cancel();
            try
            {
                await Task.WhenAll(tasks);
            }
            catch
            {
                // The first failure is rethrown below.
            }
            await done;
            throw new IOException("власник закрив потік");
        }
    }

    private Start BuildStart()
    {
        var s = new Start { ClientId = _store.ClientId, SchemaVersion = Options.SchemaVersion, InstanceId = _instance ?? "" };
        if (_instance is not null)
        {
            lock (_cursorsLock)
            {
                foreach ((string t, CursorState k) in _cursors)
                    s.Cursors.Add(k.ToWire(t));
            }
        }

        s.OpenTables.AddRange(_open);
        return s;
    }

    private async Task ReadLoopAsync(AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> call, CancellationToken ct)
    {
        while (await call.ResponseStream.MoveNext(ct))
        {
            ChangeMessage m = call.ResponseStream.Current;
            Meter.CountMessage("↓ " + m.BodyCase, m.CalculateSize());
            await HandleAsync(m);
        }
    }

    private async Task AckLoopAsync(Func<SubscribeMessage, Task> write, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(Options.AckInterval, ct);
            List<RowRef> nf;
            lock (_needFull)
            {
                nf = [.. _needFull];
                _needFull.Clear();
            }
            if (!_ackDirty && nf.Count == 0)
                continue;
            _ackDirty = false;
            var ack = new Ack();
            lock (_cursorsLock)
            {
                foreach ((string t, CursorState k) in _cursors)
                    ack.Cursors.Add(k.ToWire(t, withRanges: false));
            }

            ack.NeedFull.AddRange(nf);
            await write(new SubscribeMessage { Ack = ack });
            if (nf.Count > 0)
                Log($"→ Ack з NeedFull: {nf.Count} рядк.");
        }
    }

    // Receiving (7).

    private async Task HandleAsync(ChangeMessage m)
    {
        await _gate.WaitAsync();
        ReplicaWriter w;
        string? text = null;
        try
        {
            w = new ReplicaWriter(_store.Model, _instance!);
            using SqliteTransaction tx = _conn.BeginTransaction();
            lock (_cursorsLock)
            {
                foreach (RangeExtra x in m.Extra)
                    _cursors[x.Tbl].AddRange(x.Lo, x.Hi);
                switch (m.BodyCase)
                {
                    case ChangeMessage.BodyOneofCase.Batch:
                        Batch b = m.Batch;
                        int n = w.ApplyBatch(_conn, tx, b);
                        if (_cursors.TryGetValue(b.Tbl, out CursorState? k))
                        {
                            foreach (RangeExtra c in b.Covers)
                                k.AddRange(c.Lo, c.Hi);
                        }

                        if (!b.Online && b.Covers.Count > 0)
                            TrackRemaining(b.Remaining);
                        text = $"← Batch {b.Tbl}{(b.Online ? " онлайн" : "")}: застосовано {n} з {b.Rows.Count + b.Tombstones.Count}"
                               + (b.Covers.Count > 0 ? $", курсор {_cursors[b.Tbl]}" : "");
                        break;
                    case ChangeMessage.BodyOneofCase.TableSynced:
                        _cursors[m.TableSynced.Tbl].Reset(m.TableSynced.Version);
                        text = $"← TableSynced {m.TableSynced.Tbl}, курсор = {m.TableSynced.Version}";
                        break;
                    case ChangeMessage.BodyOneofCase.Head:
                        foreach (CursorState kc in _cursors.Values)
                            kc.LiftTo(m.Head.Version);
                        break;
                    case ChangeMessage.BodyOneofCase.Progress:
                        TrackRemaining(m.Progress.Remaining);
                        if (m.Progress.Done)
                            text = "усі таблиці досинхронізовано, репліка цілісна";
                        break;
                }
                foreach ((string t, CursorState k) in _cursors)
                    _store.SaveCursor(_conn, tx, _instance!, t, k);
                // A confirmed delete waits until the cursor passes its tombstone: a late batch cannot bring the row back (9.1).
                foreach ((string t, CursorState k) in _cursors)
                {
                    _conn.Exec("DELETE FROM _sync_outbox WHERE instance = @i AND tbl = @t AND sent = 2 AND expected_version <= @c", tx,
                        ("@i", _instance), ("@t", t), ("@c", k.Cursor));
                }
            }
            tx.Commit();
            lock (_needFull)
                _needFull.AddRange(w.NeedFull);
            _ackDirty = true;
        }
        finally
        {
            _gate.Release();
        }
        if (text is not null)
            Log(text, m.Progress?.Done == true ? SyncLogLevel.Ok : SyncLogLevel.Info);
        foreach (string n in w.Notes)
            Log(n, SyncLogLevel.Bad);
        if (w.NeedFull.Count > 0)
            Log($"часткові рядки без рядка в репліці: {w.NeedFull.Count}, прошу повні через NeedFull", SyncLogLevel.Warn);
        if (m.Progress?.Done == true)
            SetState(AgentState.Online);
        if (w.TouchedTables.Count > 0)
            DataChanged?.Invoke(w.TouchedTables);
        StatusChanged?.Invoke();
    }

    private void TrackRemaining(long remaining)
    {
        _remaining = remaining;
        DateTimeOffset now = DateTimeOffset.UtcNow;
        _lag.Add((now, remaining));
        _lag.RemoveAll(x => now - x.At > Options.LagWarningWindow + TimeSpan.FromSeconds(5));
        TimeSpan span = _lag.Count > 1 ? _lag[^1].At - _lag[0].At : TimeSpan.Zero;
        bool growing = span >= Options.LagWarningWindow && _lag[^1].Remaining > _lag[0].Remaining
                      && _lag.Zip(_lag.Skip(1)).All(p => p.Second.Remaining >= p.First.Remaining);
        if (growing && !_lagWarning)
            Log($"канал не встигає: відстаємо на {remaining} змін, росте", SyncLogLevel.Warn);
        if (growing)
            _lagWarning = true;
        else if (_lagWarning && _lag[^1].Remaining < _lag[0].Remaining)
            _lagWarning = false;
    }

    // Sending (6.5, 8).

    private async Task ApplyLoopAsync(Sync.SyncClient client, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await _outboxSignal.WaitAsync(TimeSpan.FromMilliseconds(500), ct);
            try
            {
                while (await SendOnceAsync(client, ct))
                {
                }
                if (_state == AgentState.Online)
                    await RetryArchiveAsync();
            }
            catch (RpcException e) when (!ct.IsCancellationRequested)
            {
                Log($"Apply: {e.StatusCode}, повторю пізніше", SyncLogLevel.Warn);
                await Task.Delay(1000, ct);
            }
        }
    }

    private int ApplyBudget() =>
        _applyRate <= 0 ? 64 * 1024 : (int)Math.Clamp(_applyRate * Options.ApplyTargetTime.TotalSeconds, Options.ApplyMinBytes, Options.ApplyMaxBytes);

    /// <summary>Sends one batch: retries first, then interactive actions, then bulk ones, closed over FK dependencies.</summary>
    private async Task<bool> SendOnceAsync(Sync.SyncClient client, CancellationToken ct)
    {
        (ApplyRequest? req, List<OutboxEntry> entries) = await BuildBatchAsync();
        if (req is null)
            return false;
        int size = req.CalculateSize();
        Meter.CountMessage("↑ Apply", size);
        Log($"→ Apply: {string.Join("; ", req.Actions.Select(a => Describe(a, entries)))}");
        Stopwatch sw = Stopwatch.StartNew();
        ApplyReply reply;
        try
        {
            reply = await client.ApplyAsync(req, s_gzip, DateTime.UtcNow + Options.ApplyTimeout, ct);
        }
        catch (RpcException e) when (e.StatusCode is StatusCode.DeadlineExceeded or StatusCode.Unavailable && !ct.IsCancellationRequested)
        {
            Log("відповіді на Apply нема, повторна відправка тих самих дій з тими самими seq", SyncLogLevel.Warn);
            return false;
        }
        double secs = Math.Max(sw.Elapsed.TotalSeconds, 0.05);
        _applyRate = _applyRate <= 0 ? size / secs : _applyRate * 0.5 + size / secs * 0.5;
        Meter.CountMessage("↓ ApplyReply", reply.CalculateSize());
        if (LoseNextApplyReply)
        {
            LoseNextApplyReply = false;
            Log("✕ відповідь ApplyReply загубилась у мережі; дії лишаються в черзі з sent = 1", SyncLogLevel.Bad);
            // The timeout before a retry.
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            return false;
        }
        await ProcessReplyAsync(reply, entries);
        return true;
    }

    private string Describe(Protocol.Action a, List<OutboxEntry> entries)
    {
        OutboxEntry? e = entries.FirstOrDefault(x => x.Seq == a.Seq);
        string cls = e?.Class == OutboxClass.Bulk ? "[масова] " : "";
        return a.Kind switch
        {
            ActionKind.Archive => $"#{a.Seq} {cls}архів {a.Rows.Count} записів, SyncVersion ≤ {a.ExpectedVersion}",
            ActionKind.PredicateDelete => $"#{a.Seq} {cls}видалення {a.Tbl} де {string.Join(" AND ", a.Predicate.Select(p => $"{p.Column} = {FromValue(p.Value)}"))}, SyncVersion ≤ {a.ExpectedVersion}",
            _ => $"#{a.Seq} {cls}{a.Kind} {a.Tbl} {Short(PkText(a.Pk))}{(a.Columns.Count > 0 && a.Kind == ActionKind.Patch ? " " + string.Join(", ", a.Columns.Select((c, i) => $"{c}={FromValue(a.Values[i])}")) : "")}",
        };
    }

    private async Task<(ApplyRequest?, List<OutboxEntry>)> BuildBatchAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_instance is null)
                return (null, []);
            using SqliteTransaction tx = _conn.BeginTransaction();
            List<OutboxEntry> all = ClientStore.Entries(_conn, tx, _instance, "sent IN (0, 1)")
                .OrderBy(e => e.Seq is null ? 1 : 0).ThenBy(e => e.Seq ?? 0).ThenBy(e => (int)e.Class).ThenBy(e => e.Id).ToList();
            if (all.Count == 0)
                return (null, []);

            var batch = new List<OutboxEntry>();
            var actions = new Dictionary<long, Protocol.Action>();
            var visiting = new HashSet<long>();
            int budget = ApplyBudget();
            int size = 0;

            void Add(OutboxEntry e)
            {
                if (actions.ContainsKey(e.Id) || !visiting.Add(e.Id))
                    return;
                if (_store.Model.TryGet(e.Table, out SyncTable? t) && e.Kind is OutboxKind.Create or OutboxKind.Patch && e.Pk is not null)
                {
                    foreach (SyncForeignKey fk in t.ForeignKeys.Where(f => e.Kind == OutboxKind.Create || e.Columns.Contains(f.Column)))
                    {
                        // A create this row points to must go in the same or an earlier batch (8.4).
                        string? pid = _conn.Scalar<string>($"SELECT {Q(fk.Column)} FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @i", tx, ("@id", e.Pk), ("@i", _instance));
                        if (all.FirstOrDefault(x => x.Table == fk.ParentTable && x.Pk == pid && x.Kind == OutboxKind.Create && x.Sent == 0) is { } pe)
                            Add(pe);
                    }
                }

                Protocol.Action? a = ToAction(e, tx);
                actions[e.Id] = a!;
                batch.Add(e);
                size += a?.CalculateSize() ?? 0;
            }

            foreach (OutboxEntry e in all)
            {
                if (size >= budget && batch.Count > 0)
                    break;
                Add(e);
            }

            long next = _conn.Scalar<long>("SELECT next_seq FROM _sync_instances WHERE address = @a", tx, ("@a", Address));
            var sent = new List<OutboxEntry>();
            foreach (OutboxEntry e in batch)
            {
                // Seq is given at the first send, so the owner always sees seq ascending (8.5).
                long seq = e.Seq ?? next++;
                _conn.Exec("UPDATE _sync_outbox SET seq = @s, sent = 1 WHERE id = @id", tx, ("@s", seq), ("@id", e.Id));
                if (actions[e.Id] is { } a)
                    a.Seq = seq;
                sent.Add(e with { Seq = seq, Sent = 1 });
            }
            _conn.Exec("UPDATE _sync_instances SET next_seq = @n WHERE address = @a", tx, ("@n", next), ("@a", Address));
            tx.Commit();

            var req = new ApplyRequest { ClientId = _store.ClientId };
            req.Actions.AddRange(batch.Select(e => actions[e.Id]).Where(a => a is not null).OrderBy(a => a.Seq));
            StatusChanged?.Invoke();
            return (req.Actions.Count > 0 ? req : null, sent);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Values are read from the replica at send time, so the latest state goes and edits collapse by themselves (5.4).</summary>
    private Protocol.Action? ToAction(OutboxEntry e, SqliteTransaction tx)
    {
        var a = new Protocol.Action { Tbl = e.Table, Kind = (ActionKind)e.Kind };
        switch (e.Kind)
        {
            case OutboxKind.Create or OutboxKind.Patch:
            {
                SyncTable t = _store.Model[e.Table];
                List<string> cols = e.Kind == OutboxKind.Create ? t.Columns.ToList() : e.Columns.Where(t.HasColumn).ToList();
                using SqliteCommand cmd = _conn.Cmd($"SELECT {string.Join(", ", cols.Select(Q).DefaultIfEmpty("1"))} FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @i", tx, ("@id", e.Pk), ("@i", _instance));
                using SqliteDataReader r = cmd.ExecuteReader();
                if (!r.Read())
                {
                    // The row is gone: nothing to send, the reply's applied_up_to_seq clears the entry.
                    return null;
                }

                a.Pk = PkBytes(e.Pk!);
                for (int i = 0; i < cols.Count; i++)
                {
                    a.Columns.Add(cols[i]);
                    a.Values.Add(ToValue(r.Raw(i)));
                }
                return a;
            }
            case OutboxKind.Delete:
                a.Pk = PkBytes(e.Pk!);
                return a;
            case OutboxKind.Archive:
                a.ExpectedVersion = e.ExpectedVersion ?? 0;
                a.Rows.AddRange(ArchiveSet.Parse(e.Predicate).Select(x => Ref(x.Table, x.Pk)));
                return a;
            case OutboxKind.PredicateDelete:
                a.ExpectedVersion = e.ExpectedVersion ?? 0;
                a.Predicate.AddRange(Predicate.Parse(e.Predicate)!.ToWire());
                return a;
        }
        return null;
    }

    private async Task ProcessReplyAsync(ApplyReply reply, List<OutboxEntry> sent)
    {
        await _gate.WaitAsync();
        ReplicaWriter w;
        var logs = new List<(string, SyncLogLevel)>();
        try
        {
            w = new ReplicaWriter(_store.Model, _instance!);
            using SqliteTransaction tx = _conn.BeginTransaction();
            Dictionary<long, OutboxEntry> bySeq = sent.ToDictionary(e => e.Seq!.Value);
            var versions = new List<string>();
            foreach (ActionResult r in reply.Results)
            {
                if (!bySeq.TryGetValue(r.Seq, out OutboxEntry? e))
                    continue;
                SyncTable? t = _store.Model.TryGet(e.Table, out SyncTable? tt) ? tt : null;
                string label = t is not null && e.Pk is not null ? ClientStore.Label(_conn, tx, t, e.Pk) : "";
                switch (r.Status)
                {
                    case ResultStatus.Rejected when r.Reason == "unique":
                        w.Note(_conn, tx, $"не збережено {label}: на інстансі вже є запис з таким значенням");
                        break;
                    case ResultStatus.Rejected when t is not null && e.Pk is not null:
                    {
                        var gone = new List<string>();
                        w.RemoveWithChildren(_conn, tx, t, e.Pk, gone);
                        w.Note(_conn, tx, $"не збережено: {string.Join(", ", gone.DefaultIfEmpty(label))} ({(r.Reason == "parent deleted" ? "батьківський запис видалено" : "запис видалено на інстансі")})");
                        break;
                    }
                    case ResultStatus.Ignored when t is not null && e.Pk is not null:
                    {
                        // Delete wins: the record goes here right away, without waiting for the tombstone (6.5, 9.1).
                        var gone = new List<string>();
                        w.RemoveWithChildren(_conn, tx, t, e.Pk, gone);
                        w.Note(_conn, tx, $"правку запису {label} втрачено: його видалено на інстансі");
                        break;
                    }
                    case ResultStatus.Applied or ResultStatus.Skipped:
                        switch (e.Kind)
                        {
                            case OutboxKind.Create or OutboxKind.Patch when r.HasVersion && t is not null:
                                w.SetVersion(_conn, tx, t, e.Pk!, r.Version);
                                versions.Add($"{label} v{r.Version}");
                                break;
                            case OutboxKind.Delete when r.HasVersion:
                                long cur;
                                lock (_cursorsLock)
                                    cur = _cursors[e.Table].Cursor;
                                if (cur < r.Version)
                                    _conn.Exec("UPDATE _sync_outbox SET sent = 2, expected_version = @v WHERE id = @id", tx, ("@v", r.Version), ("@id", e.Id));
                                break;
                            case OutboxKind.PredicateDelete when r.Changed.Count > 0:
                                w.Note(_conn, tx, $"не видалено {r.Changed.Count}: змінено або створено на інстансі після того, як ви бачили дані; вони лишаються і приїдуть потоком");
                                break;
                            case OutboxKind.Archive when r.Changed.Count + r.Children.Count > 0:
                                ArchiveLeftovers(tx, w, r, e, logs);
                                break;
                        }
                        break;
                }
            }
            _conn.Exec("DELETE FROM _sync_outbox WHERE instance = @i AND sent = 1 AND seq <= @s", tx, ("@i", _instance), ("@s", reply.AppliedUpToSeq));
            tx.Commit();
            lock (_needFull)
                _needFull.AddRange(w.NeedFull);
            if (versions.Count > 0)
                logs.Add(($"версії з ApplyReply: {string.Join(", ", versions)}; відлуння цих змін не прийде", SyncLogLevel.Info));
            logs.Add(($"← ApplyReply: applied_up_to_seq = {reply.AppliedUpToSeq}", SyncLogLevel.Ok));
        }
        finally
        {
            _gate.Release();
        }
        foreach ((string text, SyncLogLevel level) in logs)
            Log(text, level);
        foreach (string n in w.Notes)
            Log(n, SyncLogLevel.Bad);
        if (w.TouchedTables.Count > 0)
            DataChanged?.Invoke(w.TouchedTables);
        StatusChanged?.Invoke();
    }

    // Snapshot (6.3, 10.3).

    private async Task SnapshotFlowAsync(Sync.SyncClient client, SnapshotRequired sr, CancellationToken ct)
    {
        // 1. The outbox goes first: Apply does not depend on the cursor.
        if (_instance is not null && HasSendable())
        {
            SetState(AgentState.Flushing);
            Log("потрібен знімок: спершу відправляю чергу дій");
            while (HasSendable())
            {
                if (!await SendOnceAsync(client, ct))
                    await Task.Delay(300, ct);
            }
        }

        SnapshotMode mode = Options.SnapshotMode;
        if (mode == SnapshotMode.Auto)
        {
            double rate = Meter.SampleRate().Down is > 10_000 and double r ? r : Options.AssumedDownBytesPerSecond;
            TimeSpan eta = TimeSpan.FromSeconds(sr.SizeBytes / rate);
            mode = eta > Options.SnapshotFileThreshold && _instance is null ? SnapshotMode.EmptyReplica : SnapshotMode.File;
            Log($"знімок ~{sr.SizeBytes / 1024} КБ, за швидкістю ≈ {eta.TotalSeconds:0} с: {(mode == SnapshotMode.File ? "беру файл" : "порожня репліка, дані прийдуть потоком від нових до старих")}");
        }

        if (mode == SnapshotMode.EmptyReplica)
        {
            await _gate.WaitAsync(ct);
            try
            {
                using SqliteTransaction tx = _conn.BeginTransaction();
                foreach (SyncTable t in _store.Model.Tables)
                {
                    if (_instance is not null)
                        _conn.Exec($"DELETE FROM {Q(t.Name)} WHERE InstanceId = @i", tx, ("@i", _instance));
                    _store.SaveCursor(_conn, tx, sr.InstanceId, t.Name, new CursorState(0));
                }
                _conn.Exec("UPDATE _sync_instances SET instance = @i WHERE address = @a", tx, ("@i", sr.InstanceId), ("@a", Address));
                tx.Commit();
                _instance = sr.InstanceId;
                lock (_cursorsLock)
                    _cursors = NewCursors();
            }
            finally
            {
                _gate.Release();
            }
            return;
        }

        SetState(AgentState.Snapshot);
        string path = await DownloadAsync(client, ct);
        await _gate.WaitAsync(ct);
        try
        {
            (string inst, int carried, List<string> notes) = ReplicaWriter.InstallSnapshot(_store, Address, path);
            _instance = inst;
            long cursor;
            lock (_cursorsLock)
            {
                _cursors = _store.LoadCursors(_conn, inst);
                cursor = _cursors.Values.First().Cursor;
            }

            Log($"знімок перевірено і влито однією транзакцією: рядки InstanceId = {inst} замінено, курсори = {cursor}{(carried > 0 ? $", у нову репліку перенесено {carried} дій зі значеннями" : "")}", SyncLogLevel.Ok);
            foreach (string n in notes)
                Log(n, SyncLogLevel.Bad);
        }
        finally
        {
            _gate.Release();
        }
        File.Delete(path);
        _download = null;
        _snapshotProgress = null;
        DataChanged?.Invoke(_store.Model.Tables.Select(t => t.Name).ToList());
        StatusChanged?.Invoke();
    }

    private bool HasSendable()
    {
        using SqliteConnection c = _store.Open();
        return c.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i AND sent IN (0, 1)", null, ("@i", _instance)) > 0;
    }

    /// <summary>Downloads the snapshot file, resuming from the same offset by snapshot_id after a break (6.3).</summary>
    private async Task<string> DownloadAsync(Sync.SyncClient client, CancellationToken ct)
    {
        long offset = _download is { } d && File.Exists(d.Path) ? new FileInfo(d.Path).Length : 0;
        if (offset > 0)
            Log($"знімок {_download!.Id}: продовжую з {offset / 1024} КБ");
        using AsyncServerStreamingCall<SnapshotChunk> call = client.Snapshot(new SnapshotRequest { ClientId = _store.ClientId, SnapshotId = _download?.Id ?? "", Offset = offset }, cancellationToken: ct);
        byte[]? sha = null;
        while (await call.ResponseStream.MoveNext(ct))
        {
            SnapshotChunk ch = call.ResponseStream.Current;
            Meter.CountMessage("↓ SnapshotChunk", ch.CalculateSize());
            if (_download is null || _download.Id != ch.SnapshotId)
            {
                if (_download is not null)
                    File.Delete(_download.Path);
                _download = new Download(ch.SnapshotId, _store.DbPath + "." + ch.SnapshotId + ".part", ch.TotalSize);
                File.Delete(_download.Path);
                Log($"знімок {ch.SnapshotId}: V = {ch.Version}, {ch.TotalSize / 1024} КБ");
            }
            await using (var f = new FileStream(_download.Path, FileMode.OpenOrCreate, FileAccess.Write))
            {
                if (f.Length != ch.Offset)
                    f.SetLength(ch.Offset);
                f.Position = ch.Offset;
                await f.WriteAsync(ch.Data.Memory, ct);
            }
            sha = ch.Sha256.ToByteArray();
            _snapshotProgress = (double)(ch.Offset + ch.Data.Length) / Math.Max(1, ch.TotalSize);
            StatusChanged?.Invoke();
        }
        if (_download is null)
            throw new IOException("знімок порожній");
        await using (FileStream f = File.OpenRead(_download.Path))
        {
            if (f.Length != _download.Total)
                throw new IOException("знімок обірвався");
            if (sha is not null && !(await SHA256.HashDataAsync(f, ct)).SequenceEqual(sha))
            {
                f.Close();
                File.Delete(_download.Path);
                _download = null;
                throw new IOException("хеш знімка не збігся");
            }
        }
        return _download.Path;
    }

    // Bulk actions (8.6) and archive (11).

    /// <summary>
    /// Deletes the rows matching <paramref name="predicate"/> locally and queues one predicate action with the version V the
    /// replica is complete up to. If the replica has ranges above the cursor, the keys go instead.
    /// </summary>
    public async Task<int> DeleteWhereAsync(string table, Predicate predicate)
    {
        SyncTable t = _store.Model[table];
        await _gate.WaitAsync();
        int count;
        try
        {
            if (_instance is null)
                return 0;
            // FK on: the local cascade removes children the same way the owner will (8.7); a parent of archived rows goes to the archive (11.6).
            using SqliteConnection c = _store.Open();
            using SqliteTransaction tx = c.BeginTransaction();
            (string where, (string, object?)[] args) = predicate.ToSql(t);
            var ids = new List<string>();
            using (SqliteCommand cmd = c.Cmd($"SELECT Id FROM {Q(t.Name)} WHERE InstanceId = @inst AND {where}", tx, [("@inst", _instance), .. args]))
            using (SqliteDataReader r = cmd.ExecuteReader())
            {
                while (r.Read())
                    ids.Add(r.GetString(0));
            }

            count = ids.Count;
            if (count == 0)
                return 0;
            CursorState k;
            lock (_cursorsLock)
                k = _cursors[table].Clone();
            if (k.Ranges.Count > 0)
            {
                foreach (string id in ids)
                {
                    ArchiveGuard.DeleteOrArchive(c, tx, _store.Model, _instance, t, id);
                    ClientStore.Put(c, tx, _instance, table, id, OutboxKind.Delete, null, OutboxClass.Bulk);
                }
                Log($"масове видалення: репліка «{table}» неповна (є відрізки над курсором), у чергу пішли ключі: {count}");
            }
            else
            {
                // Rows with their own actions in the outbox go by key, the rest as one predicate.
                List<string> own = ids.Where(id => ClientStore.FindEntry(c, tx, _instance, table, id) is not null).ToList();
                foreach (string id in ids)
                    ArchiveGuard.DeleteOrArchive(c, tx, _store.Model, _instance, t, id);
                foreach (string id in own)
                    ClientStore.Put(c, tx, _instance, table, id, OutboxKind.Delete, null, OutboxClass.Bulk);
                ClientStore.Insert(c, tx, _instance, table, null, OutboxKind.PredicateDelete, OutboxClass.Bulk, predicate: predicate.Serialize(), expectedVersion: k.Cursor);
                Log($"масове видалення {count} записів: у черзі одна дія {predicate} і SyncVersion ≤ {k.Cursor}{(own.Count > 0 ? $", ключами ще {own.Count} (мають свої дії в черзі)" : "")}");
            }
            WriteRouter.DropOrphanEntries(c, tx, _store.Model, _instance);
            tx.Commit();
        }
        finally
        {
            _gate.Release();
        }
        DataChanged?.Invoke(_store.Model.Tables.Select(x => x.Name).ToList());
        NotifyOutbox();
        return count;
    }

    /// <summary>
    /// Moves rows with their FK children to the archive of this instance (<c>InstanceId = X:archive</c>) and queues one
    /// conditional delete for the owner (11.2). Allowed only online, fully synced, with an empty outbox.
    /// </summary>
    public Task<string> ArchiveAsync(IEnumerable<(string Table, Guid Id)> rows) =>
        ArchiveCoreAsync(rows.Select(r => (r.Table, PkText(r.Id))).ToList(), 0);

    private async Task<string> ArchiveCoreAsync(List<(string Table, string Pk)> rows, int tries)
    {
        await _gate.WaitAsync();
        string text;
        try
        {
            if (_instance is null)
                return "репліки нема";
            if (tries == 0)
            {
                long pending = _conn.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i", null, ("@i", _instance));
                if (_state != AgentState.Online || pending > 0)
                {
                    text = "перенесення доступне, коли інстанс на зв'язку, повністю досинхронізований і черга порожня";
                    ClientStore.Note(_conn, null, _instance, text);
                    return text;
                }
            }
            using SqliteTransaction tx = _conn.BeginTransaction();
            var set = new List<(string Table, string Pk)>();

            void Add(string table, string pk)
            {
                if (set.Contains((table, pk)))
                    return;
                if (_conn.Scalar<string>($"SELECT InstanceId FROM {Q(table)} WHERE Id = @id AND InstanceId = @i", tx, ("@id", pk), ("@i", _instance)) is null)
                    return;
                set.Add((table, pk));
                // FK closure: the owner's cascade would delete the children with the parent (11.2, step 3).
                foreach ((SyncTable child, SyncForeignKey fk) in _store.Model.ChildrenOf(table))
                {
                    var ids = new List<string>();
                    using (SqliteCommand cmd = _conn.Cmd($"SELECT Id FROM {Q(child.Name)} WHERE {Q(fk.Column)} = @p AND InstanceId = @i", tx, ("@p", pk), ("@i", _instance)))
                    using (SqliteDataReader r = cmd.ExecuteReader())
                    {
                        while (r.Read())
                            ids.Add(r.GetString(0));
                    }

                    foreach (string id in ids)
                        Add(child.Name, id);
                }
            }
            foreach ((string table, string pk) in rows)
                Add(table, pk);
            if (set.Count == 0)
                return "нічого переносити";
            string archive = SyncColumns.ArchiveOf(_instance);
            List<string> labels = set.Select(x => ClientStore.Label(_conn, tx, _store.Model[x.Table], x.Pk)).ToList();
            foreach ((string table, string pk) in set)
                _conn.Exec($"UPDATE {Q(table)} SET InstanceId = @a WHERE Id = @id AND InstanceId = @i", tx, ("@a", archive), ("@id", pk), ("@i", _instance));
            long v;
            lock (_cursorsLock)
                v = _cursors.Values.Min(k => k.Cursor);
            ClientStore.Insert(_conn, tx, _instance, set[0].Table, null, OutboxKind.Archive, OutboxClass.Bulk, predicate: ArchiveSet.Serialize(set), expectedVersion: v);
            tx.Commit();
            text = $"{(tries > 0 ? "повторне перенесення" : "перенесення в архів")}: {string.Join(", ", labels)} отримали InstanceId = {archive} в одній транзакції; у черзі одна дія: Id ∈ набір і SyncVersion ≤ {v}";
            _archiveRetry.RemoveAll(x => set.Contains((x.Table, x.Pk)));
            foreach ((string _, string pk) in set)
                _archiveTries[pk] = tries;
        }
        finally
        {
            _gate.Release();
        }
        Log(text);
        DataChanged?.Invoke(_store.Model.Tables.Select(x => x.Name).ToList());
        NotifyOutbox();
        return text;
    }

    /// <summary>
    /// Rows the owner kept: changed after V, or parents of kept children. They go back from the archive to the replica;
    /// changed ones also into NeedFull, since their updates bypassed the archive (11.2, step 6).
    /// </summary>
    private void ArchiveLeftovers(SqliteTransaction tx, ReplicaWriter w, ActionResult r, OutboxEntry e, List<(string, SyncLogLevel)> logs)
    {
        string archive = SyncColumns.ArchiveOf(_instance!);
        var names = new List<string>();
        foreach ((string why, Google.Protobuf.Collections.RepeatedField<RowRef> list) in new[] { ("changed", r.Changed), ("children", r.Children) })
        {
            foreach (RowRef x in list)
            {
                string pk = PkText(x.Pk);
                long v = _conn.Scalar<long>($"SELECT SyncVersion FROM {Q(x.Tbl)} WHERE Id = @id", tx, ("@id", pk));
                _conn.Exec($"UPDATE {Q(x.Tbl)} SET InstanceId = @i WHERE Id = @id AND InstanceId = @a", tx, ("@i", _instance), ("@id", pk), ("@a", archive));
                names.Add(ClientStore.Label(_conn, tx, _store.Model[x.Tbl], pk));
                if (why == "changed")
                    w.NeedFull.Add(x);
                _archiveRetry.Add(new ArchiveRetry(x.Tbl, pk, why, v, _archiveTries.GetValueOrDefault(pk) + 1));
                w.TouchedTables.Add(x.Tbl);
            }
        }

        logs.Add(($"лишились на власнику: {string.Join(", ", names)} (змінено після V або мають дочірні); повернуто з архіву в репліку, змінені в NeedFull, повторю перенесення", SyncLogLevel.Warn));
    }

    private async Task RetryArchiveAsync()
    {
        if (_archiveRetry.Count == 0)
            return;
        List<ArchiveRetry> ready = [];
        foreach (string pass in new[] { "changed", "children" })
        {
            foreach (ArchiveRetry rt in _archiveRetry.Where(x => x.Why == pass).ToList())
            {
                long? v;
                using (SqliteConnection c = _store.Open())
                    v = c.Scalar<long?>($"SELECT SyncVersion FROM {Q(rt.Table)} WHERE Id = @id AND InstanceId = @i", null, ("@id", rt.Pk), ("@i", _instance));
                if (v is null)
                {
                    _archiveRetry.Remove(rt);
                    continue;
                }
                bool ok = pass == "changed"
                    ? v > rt.Version
                    : !_archiveRetry.Any(o => o != rt && IsChild(o, rt));
                if (!ok)
                    continue;
                _archiveRetry.Remove(rt);
                if (rt.Tries >= 3)
                {
                    using SqliteConnection c = _store.Open();
                    ClientStore.Note(c, null, _instance!, $"{ClientStore.Label(c, null, _store.Model[rt.Table], rt.Pk)} лишився на власнику: автоматика постійно його змінює");
                    continue;
                }
                await ArchiveCoreAsync([(rt.Table, rt.Pk)], rt.Tries);
                return;
            }
        }
    }

    private bool IsChild(ArchiveRetry child, ArchiveRetry parent) =>
        _store.Model.ChildrenOf(parent.Table).Any(x => x.Child.Name == child.Table);

    public async ValueTask DisposeAsync()
    {
        _life.Cancel();
        _sessionCts?.Cancel();
        if (_loop is not null)
        {
            try
            {
                await _loop;
            }
            catch { }
        }

        _conn.Dispose();
    }
}
