using System.Diagnostics;
using System.Net.Sockets;
using System.Security.Cryptography;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Client;

public enum AgentState
{
    Offline,
    Connecting,
    Snapshot,
    Catchup,
    Online,

    /// <summary>The owner runs another schema version, or an older protocol without Identity: one side needs an update.</summary>
    SchemaMismatch,

    /// <summary>
    /// Another agent of this client already serves the owner's instance (the same owner, maybe at another address),
    /// or the owner's snapshot shares Ids with an instance another agent serves.
    /// </summary>
    Duplicate
}

public sealed class AgentOptions
{
    public int SchemaVersion { get; set; } = 1;
    public TimeSpan AckInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan ApplyTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Pause between Apply rounds when the outbox has nothing more to send right away.</summary>
    public TimeSpan ApplyInterval { get; set; } = TimeSpan.FromSeconds(1);
    /// <summary>The first wait after a lost link; it doubles on every failed round up to <see cref="ReconnectMax"/> and starts over once the owner accepts Start.</summary>
    public TimeSpan ReconnectMin { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>The longest wait between reconnect attempts.</summary>
    public TimeSpan ReconnectMax { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>An Apply batch is sized by send time, not by count: ~15 s at the measured rate, within 16 KB-1 MB (6.5).</summary>
    public TimeSpan ApplyTargetTime { get; set; } = TimeSpan.FromSeconds(15);
    public int ApplyMinBytes { get; set; } = 16 * 1024;
    public int ApplyMaxBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Start asks the owner for catch-up from zero cursors instead of a snapshot for a new replica (<c>skip_snapshot_on_empty_replica</c>).
    /// When the owner still answers SnapshotRequired (cursor behind purged_version or ahead of head), the replica is emptied
    /// and refilled from the stream instead of a snapshot file.
    /// </summary>
    public bool SkipSnapshotOnEmptyReplica { get; set; }

    /// <summary>"The link does not keep up" when the catch-up backlog keeps growing this long (12).</summary>
    public TimeSpan LagWarningWindow { get; set; } = TimeSpan.FromMinutes(3);

    public int Http2StreamWindowBytes { get; set; } = 1024 * 1024;
}

public sealed record AgentStatus(
    AgentState State, bool LinkEnabled, string? InstanceId, long Remaining, bool LagWarning, DateTimeOffset? OfflineSince,
    int Pending, double? SnapshotProgress, string? LastError, DateTimeOffset? LastReceivedAt, IReadOnlyDictionary<string, string> Cursors);

/// <summary>
/// One per owner address (4); the instance it serves is learned from the owner's identity: offline → handshake → snapshot if needed → change stream (catch-up, then online),
/// in parallel with sending the outbox. The client always initiates; it reconnects with exponential back-off.
/// </summary>
public sealed class SyncAgent : IAsyncDisposable
{
    private static readonly Metadata s_gzip = new() { { "grpc-internal-encoding-request", "gzip" } };

    private readonly ClientStore _store;
    private readonly ILogger _log;
    private readonly Func<Stream, Stream>? _wrapStream;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SqliteConnection _conn;
    private readonly AsyncSignal _linkSignal = new();
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
    private long _offlineSinceMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private long _remaining;
    private bool _lagWarning;
    private string? _lastError;
    private Download? _download;
    private double? _snapshotProgress;
    private double _applyRate;
    private readonly Dictionary<string, int> _archiveTries = [];
    private TimeSpan _backoff;

    /// <summary>Unix ms of the last sign of life from the owner: a handshake or a stream message; 0 = never.</summary>
    private long _lastReceivedMs;

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>0 = not set.</summary>
    private static DateTimeOffset? FromMs(long ms) => ms == 0 ? null : DateTimeOffset.FromUnixTimeMilliseconds(ms);

    private sealed record Download(string Id, string Path, long Total);

    private enum ArchiveLeftover { Changed, Children }

    private sealed record ArchiveRetry(string Table, string Pk, ArchiveLeftover Why, long Version, int Tries);

    /// <param name="wrapStream">
    /// Decorates the TCP stream of each connection, reconnects included, for example to count bytes or emulate a slow link;
    /// the returned stream owns the one it wraps. None when null.
    /// </param>
    public SyncAgent(ClientStore store, ILogger<SyncAgent> log, string address, string name, AgentOptions? options = null, Func<Stream, Stream>? wrapStream = null)
    {
        _store = store;
        Address = address;
        Name = name;
        Options = options ?? new AgentOptions();
        _wrapStream = wrapStream;
        _log = log;
        // Children may arrive before parents during catch-up (7).
        _conn = store.Open(foreignKeys: false);
        _cursors = NewCursors();
    }

    public string Address { get; set; }

    /// <summary>The name of this agent in the log and in the app.</summary>
    public string Name { get; set; }

    public AgentOptions Options { get; }
    public string? InstanceId => _instance;

    /// <summary>Drop the next ApplyReply as if it was lost in the network (the demo's "lost reply").</summary>
    public bool LoseNextApplyReply { get; set; }

    /// <summary>Archive actions of the current instance waiting for a retry, kept in memory (<c>_archiveRetry</c>), not outbox rows (11.2, step 6).</summary>
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

    /// <summary>A hint that the status may have changed; it fires per batch, so a UI should throttle it or read <see cref="GetStatus"/> on a timer.</summary>
    public event System.Action? StatusChanged;

    /// <summary>Rows of these tables changed in the replica (a batch, a reply, a snapshot).</summary>
    public event System.Action<IReadOnlyCollection<string>>? DataChanged;

    /// <summary>Text built ahead of time, under the <c>[agent name]</c> prefix every line of this agent has.</summary>
    private void Log(LogLevel level, string text) => _log.Log(level, "[{Agent}] {Text}", Name, text);

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
            Log(value ? LogLevel.Information : LogLevel.Warning, value ? "link on" : "link off");
            if (!value)
                _sessionCts?.Cancel();
            _linkSignal.Set();
            StatusChanged?.Invoke();
        }
    }

    /// <summary>
    /// Starts the sync loop. Every session asks the owner who it is first, so the owner does not have to be reachable yet.
    /// On <see cref="AgentState.SchemaMismatch"/> and <see cref="AgentState.Duplicate"/> the agent turns its link off and
    /// keeps the state; the app turns <see cref="LinkEnabled"/> on again once the cause is gone.
    /// </summary>
    public void Start() => _loop ??= Task.Run(() => RunAsync(_life.Token));

    /// <summary>Tables open in the UI go first in catch-up (6.4).</summary>
    public void SetOpenTables(IEnumerable<string> tables)
    {
        string[] open = [.. tables];
        _open = open;
        if (_write is { } w && _state == AgentState.Catchup)
        {
            var m = new SubscribeMessage { OpenTables = new OpenTables() };
            m.OpenTables.Tables.AddRange(open);
            _ = w(m);
        }
    }

    private void TouchReceived() => _lastReceivedMs = NowMs();

    private void SetState(AgentState s)
    {
        if (_state == s)
            return;
        _state = s;
        if (s == AgentState.Offline)
        {
            if (_offlineSinceMs == 0)
                _offlineSinceMs = NowMs();
        }
        else if (s is AgentState.Catchup or AgentState.Online)
        {
            _offlineSinceMs = 0;
        }
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
        if (_instance is not null)
        {
            using SqliteConnection c = _store.Open();
            // sent: 0 waiting, 1 in flight, 2 confirmed delete held for the cursor (< 2 = not done).
            pending = (int)c.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i AND sent < 2", null, ("@i", _instance));
        }
        Dictionary<string, string> cursors;
        lock (_cursorsLock)
            cursors = _cursors.ToDictionary(x => x.Key, x => x.Value.ToString());
        return new AgentStatus(_state, _link, _instance, _remaining, _lagWarning, _link && _state is AgentState.Catchup or AgentState.Online ? null : FromMs(_offlineSinceMs),
            pending, _snapshotProgress, _lastError, FromMs(_lastReceivedMs), cursors);
    }

    /// <summary>Applies a schema version change (the demo's "new schema") and reconnects; after SchemaMismatch turn the link on again.</summary>
    public void SetSchemaVersion(int v)
    {
        Options.SchemaVersion = v;
        _sessionCts?.Cancel();
        _linkSignal.Set();
    }

    // Main loop.

    private async Task RunAsync(CancellationToken life)
    {
        _backoff = Options.ReconnectMin;
        while (!life.IsCancellationRequested)
        {
            if (!_link)
            {
                // A stop reason stays visible until the app turns the link on again.
                if (_state is not (AgentState.SchemaMismatch or AgentState.Duplicate))
                    SetState(AgentState.Offline);
                await _linkSignal.WaitAsync(TimeSpan.FromSeconds(1), life);
                continue;
            }
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(life);
            _sessionCts = cts;
            AgentState? stop = null;
            try
            {
                stop = await SessionAsync(cts.Token);
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
                SetState(AgentState.Offline);
                _log.LogWarning("[{Agent}] link lost ({Error}), retry in {Seconds} s", Name, _lastError, _backoff.TotalSeconds.ToString("0.#"));
                await _linkSignal.WaitAsync(_backoff, life);
                _backoff = TimeSpan.FromTicks(Math.Min(_backoff.Ticks * 2, Options.ReconnectMax.Ticks));
            }
            finally
            {
                _sessionCts = null;
                _write = null;
            }
            if (stop is { } s)
            {
                // Off before the state is told, so a StatusChanged handler can already turn it on again.
                _link = false;
                SetState(s);
                _log.LogWarning("[{Agent}] link off: turn it on when the cause is fixed", Name);
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
                Stream tcp = new NetworkStream(s, ownsSocket: true);
                return _wrapStream?.Invoke(tcp) ?? tcp;
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

    /// <summary>One link: throws when it breaks; returns the state the link is turned off in (SchemaMismatch, also for an owner without Identity; Duplicate).</summary>
    private async Task<AgentState> SessionAsync(CancellationToken ct)
    {
        using GrpcChannel channel = CreateChannel();
        var client = new Sync.SyncClient(channel);
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            SetState(AgentState.Connecting);
            IdentityReply owner;
            try
            {
                owner = await client.IdentityAsync(new IdentityRequest(), cancellationToken: ct);
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.Unimplemented)
            {
                // An owner older than Identity: a retry gets the same answer until the owner is updated.
                _lastError = "the owner does not answer Identity: update the owner";
                _log.LogWarning("[{Agent}] {Error}. Sync stopped, the queue is kept", Name, _lastError);
                return AgentState.SchemaMismatch;
            }
            TouchReceived();
            if (!await AdoptInstanceAsync(owner.InstanceId, ct))
                return AgentState.Duplicate;

            using AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> call = client.Subscribe(s_gzip, cancellationToken: ct);
            var writeGate = new SemaphoreSlim(1, 1);

            async Task Write(SubscribeMessage m)
            {
                await writeGate.WaitAsync(ct);
                try
                {
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
            _log.LogInformation("[{Agent}] → Start: schema {Schema}, instance_id {InstanceId}, {Cursors}", Name, start.SchemaVersion, start.InstanceId, cursors);
            await Write(new SubscribeMessage { Start = start });
            if (!await call.ResponseStream.MoveNext(ct))
                throw new IOException("the owner closed the stream");
            ChangeMessage first = call.ResponseStream.Current;
            TouchReceived();

            if (first.BodyCase == ChangeMessage.BodyOneofCase.SchemaMismatch)
            {
                _lastError = $"owner schema {first.SchemaMismatch.OwnerSchemaVersion}, mine {Options.SchemaVersion}";
                _log.LogWarning("[{Agent}] ← SchemaMismatch: {Error}. Sync stopped, the queue is kept", Name, _lastError);
                return AgentState.SchemaMismatch;
            }
            if (first.BodyCase == ChangeMessage.BodyOneofCase.SnapshotRequired)
            {
                SnapshotRequired sr = first.SnapshotRequired;
                _log.LogWarning("[{Agent}] ← SnapshotRequired: {Reason}, snapshot ~{SizeKb} KB", Name, sr.Reason, sr.SizeBytes / 1024);
                // Another owner answered since Identity: the next round adopts it, the replica of this one stays.
                if (sr.InstanceId == _instance)
                {
                    try
                    {
                        await RefillAsync(client, sr.Reason, ct);
                    }
                    catch (InstanceHeldException e)
                    {
                        // Two live owners with shared Ids: retrying would download the same snapshot again.
                        _lastError = e.Message;
                        _log.LogError("[{Agent}] {Error}: the snapshot is not installed", Name, _lastError);
                        return AgentState.Duplicate;
                    }
                }
                // A new Identity and Start with the fresh replica.
                continue;
            }

            SetState(AgentState.Catchup);
            // The owner accepted Start: the next lost link waits ReconnectMin again, not the wait the failed rounds grew to.
            _backoff = Options.ReconnectMin;
            _log.LogInformation("[{Agent}] Start accepted: stream started, Apply in parallel", Name);
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
            await Task.WhenAll(tasks).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await done;
            throw new IOException("the owner closed the stream");
        }
    }

    /// <summary>
    /// Takes the replica of instance <paramref name="id"/>, asked from the owner at the start of every session: the address
    /// may change between runs, the instance id does not (4, 6.2). A new one is registered with zero cursors, the data then
    /// arrives by catch-up or a snapshot. False when another agent of this client already serves the instance: this one
    /// then serves none.
    /// </summary>
    private async Task<bool> AdoptInstanceAsync(string id, CancellationToken ct)
    {
        if (id == _instance)
            return true;
        bool known;
        await _gate.WaitAsync(ct);
        try
        {
            if (_instance is not null)
            {
                _store.Release(_instance, this);
                ForgetInstanceWork();
            }
            if (_store.Claim(id, this) is var holder && holder != this)
            {
                // Duplicate: the claim is held by another agent, so this one serves no instance (its Release must not touch the holder's claim).
                _instance = null;
                lock (_cursorsLock)
                    _cursors = NewCursors();
                _lastError = $"instance {id} is already held by agent «{holder.Name}»";
                _log.LogError("[{Agent}] {Error}: a second agent for the same replica does not connect", Name, _lastError);
                return false;
            }
            known = ClientStore.HasInstance(_conn, id);
            if (!known)
            {
                using SqliteTransaction tx = _conn.BeginTransaction();
                ClientStore.SaveInstance(_conn, tx, id, null);
                foreach (SyncTable t in _store.Model.Tables)
                    ClientStore.SaveCursor(_conn, tx, id, t.Name, new CursorState(0));
                tx.Commit();
            }
            _instance = id;
            lock (_cursorsLock)
                _cursors = _store.LoadCursors(_conn, id);
        }
        finally
        {
            _gate.Release();
        }
        _log.LogInformation(known ? "[{Agent}] owner instance {InstanceId}: replica exists, cursors from the database" : "[{Agent}] owner instance {InstanceId}: no replica, zero cursors", Name, id);
        StatusChanged?.Invoke();
        return true;
    }

    /// <summary>
    /// In-memory work of the replica being left: NeedFull, archive retries, a partial snapshot download. It belongs to that
    /// instance and must not reach the next owner. Called under the gate.
    /// </summary>
    private void ForgetInstanceWork()
    {
        lock (_needFull)
            _needFull.Clear();
        _archiveRetry.Clear();
        _archiveTries.Clear();
        if (_download is { } d)
        {
            File.Delete(d.Path);
            _download = null;
        }
    }

    /// <summary>The answer to SnapshotRequired: a snapshot, or with <see cref="AgentOptions.SkipSnapshotOnEmptyReplica"/> an emptied replica filled from the stream (6.3, 10.3).</summary>
    private async Task RefillAsync(Sync.SyncClient client, string why, CancellationToken ct)
    {
        if (Options.SkipSnapshotOnEmptyReplica)
        {
            _log.LogWarning("[{Agent}] {Reason}: SkipSnapshotOnEmptyReplica, replica cleared, data will come in the stream, newest first", Name, why);
            await ResetReplicaAsync(ct);
            return;
        }
        _log.LogWarning("[{Agent}] → Snapshot: {Reason}", Name, why);
        await SnapshotAsync(client, ct);
    }

    private Start BuildStart()
    {
        var s = new Start { ClientId = _store.ClientId, SchemaVersion = Options.SchemaVersion, InstanceId = _instance, SkipSnapshotOnEmptyReplica = Options.SkipSnapshotOnEmptyReplica };
        lock (_cursorsLock)
        {
            foreach ((string t, CursorState k) in _cursors)
                s.Cursors.Add(k.ToWire(t));
        }

        s.OpenTables.AddRange(_open);
        return s;
    }

    private async Task ReadLoopAsync(AsyncDuplexStreamingCall<SubscribeMessage, ChangeMessage> call, CancellationToken ct)
    {
        while (await call.ResponseStream.MoveNext(ct))
        {
            ChangeMessage m = call.ResponseStream.Current;
            TouchReceived();
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
                _log.LogInformation("[{Agent}] → Ack with NeedFull: {Rows} rows", Name, nf.Count);
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
            w = new ReplicaWriter(_store.Model, _instance!, _log, Name);
            // The cursors change on a copy that is published only after the commit (6.4, 14).
            Dictionary<string, CursorState> next;
            lock (_cursorsLock)
                next = _cursors.ToDictionary(x => x.Key, x => x.Value.Clone(), StringComparer.Ordinal);
            using SqliteTransaction tx = _conn.BeginTransaction();
            foreach (RangeExtra x in m.Extra)
                next[x.Tbl].AddRange(x.Lo, x.Hi);
            switch (m.BodyCase)
            {
                case ChangeMessage.BodyOneofCase.Batch:
                    Batch b = m.Batch;
                    int n = w.ApplyBatch(_conn, tx, b);
                    if (next.TryGetValue(b.Tbl, out CursorState? k))
                    {
                        foreach (RangeExtra c in b.Covers)
                            k.AddRange(c.Lo, c.Hi);
                    }

                    if (!b.Online && b.Covers.Count > 0)
                        TrackRemaining(b.Remaining);
                    text = $"← Batch {b.Tbl}{(b.Online ? " online" : "")}: applied {n} of {b.Rows.Count + b.Tombstones.Count}"
                           + (b.Covers.Count > 0 ? $", cursor {next[b.Tbl]}" : "");
                    break;
                case ChangeMessage.BodyOneofCase.TableSynced:
                    next[m.TableSynced.Tbl].Reset(m.TableSynced.Version);
                    text = $"← TableSynced {m.TableSynced.Tbl}, cursor = {m.TableSynced.Version}";
                    break;
                case ChangeMessage.BodyOneofCase.Head:
                    foreach (CursorState kc in next.Values)
                        kc.LiftTo(m.Head.Version);
                    break;
                case ChangeMessage.BodyOneofCase.Progress:
                    TrackRemaining(m.Progress.Remaining);
                    if (m.Progress.Done)
                        text = "all tables caught up, the replica is consistent";
                    else if (_state == AgentState.Online)
                        text = $"online cannot keep up, the owner falls back to catch-up, {m.Progress.Remaining} changes remaining";
                    break;
            }
            foreach ((string t, CursorState k) in next)
                ClientStore.SaveCursor(_conn, tx, _instance!, t, k);
            // A confirmed delete waits until the cursor passes its tombstone: a late batch cannot bring the row back (9.1).
            foreach ((string t, CursorState k) in next)
            {
                _conn.Exec("DELETE FROM _sync_outbox WHERE instance = @i AND tbl = @t AND sent = 2 AND expected_version <= @c", tx,
                    ("@i", _instance), ("@t", t), ("@c", k.Cursor));
            }

            tx.Commit();
            lock (_cursorsLock)
                _cursors = next;
            lock (_needFull)
                _needFull.AddRange(w.NeedFull);
            _ackDirty = true;
        }
        finally
        {
            _gate.Release();
        }
        if (text is not null)
            Log(LogLevel.Information, text);
        if (w.NeedFull.Count > 0)
            _log.LogWarning("[{Agent}] partial rows without a row in the replica: {Rows}, asking for full ones via NeedFull", Name, w.NeedFull.Count);
        if (m.Progress?.Done == true)
            SetState(AgentState.Online);
        else if (m.Progress is not null && _state == AgentState.Online)
            SetState(AgentState.Catchup);
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
            _log.LogWarning("[{Agent}] the link cannot keep up: {Remaining} changes behind and growing", Name, remaining);
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
            bool more = false;
            try
            {
                // A full batch means the outbox has more: the next one goes at once. A partial or empty one waits ApplyInterval.
                more = await SendOnceAsync(client, ct);
                if (!more && _state == AgentState.Online)
                    await RetryArchiveAsync();
            }
            catch (RpcException e) when (!ct.IsCancellationRequested)
            {
                _log.LogWarning("[{Agent}] Apply: {Status}, will retry later", Name, e.StatusCode);
            }
            if (!more)
                await Task.Delay(Options.ApplyInterval, ct);
        }
    }

    private int ApplyBudget() =>
        _applyRate <= 0 ? 64 * 1024 : (int)Math.Clamp(_applyRate * Options.ApplyTargetTime.TotalSeconds, Options.ApplyMinBytes, Options.ApplyMaxBytes);

    /// <summary>
    /// Sends one batch: retries first, then interactive actions, then bulk ones, closed over FK dependencies.
    /// True when the batch was full and the outbox has more; false when it was partial or empty, or the reply did not come.
    /// </summary>
    private async Task<bool> SendOnceAsync(Sync.SyncClient client, CancellationToken ct)
    {
        (ApplyRequest? req, Dictionary<long, OutboxEntry> sent, bool more) = await BuildBatchAsync();
        if (req is null)
            return sent.Count > 0 && more;
        int size = req.CalculateSize();
        Log(LogLevel.Information, $"→ Apply: {string.Join("; ", req.Actions.Select(a => Describe(a, sent)))}");
        Stopwatch sw = Stopwatch.StartNew();
        ApplyReply reply;
        try
        {
            reply = await client.ApplyAsync(req, s_gzip, DateTime.UtcNow + Options.ApplyTimeout, ct);
        }
        catch (RpcException e) when (e.StatusCode is StatusCode.DeadlineExceeded or StatusCode.Unavailable && !ct.IsCancellationRequested)
        {
            _log.LogWarning("[{Agent}] no reply to Apply, resending the same actions with the same seq", Name);
            return false;
        }
        double secs = Math.Max(sw.Elapsed.TotalSeconds, 0.05);
        _applyRate = _applyRate <= 0 ? size / secs : _applyRate * 0.5 + size / secs * 0.5;
        if (LoseNextApplyReply)
        {
            LoseNextApplyReply = false;
            _log.LogError("[{Agent}] ✕ ApplyReply lost in the network; the actions stay queued with sent = 1", Name);
            // The loop's ApplyInterval wait is the timeout before a retry.
            return false;
        }
        await ProcessReplyAsync(reply, sent);
        return more;
    }

    private static string Describe(Protocol.Action a, IReadOnlyDictionary<long, OutboxEntry> bySeq)
    {
        string cls = bySeq.GetValueOrDefault(a.Seq)?.Class == OutboxClass.Bulk ? "[bulk] " : "";
        return a.Kind switch
        {
            ActionKind.Archive => $"#{a.Seq} {cls}archive {a.Rows.Count} rows, SyncVersion ≤ {a.ExpectedVersion}",
            ActionKind.PredicateDelete => $"#{a.Seq} {cls}delete {a.Tbl} where {string.Join(" AND ", a.Predicate.Select(p => $"{p.Column} = {FromValue(p.Value)}"))}, SyncVersion ≤ {a.ExpectedVersion}",
            _ => $"#{a.Seq} {cls}{a.Kind} {a.Tbl} {Short(PkText(a.Pk))}{(a.Columns.Count > 0 && a.Kind == ActionKind.Patch ? " " + string.Join(", ", a.Columns.Select((c, i) => $"{c}={FromValue(a.Values[i])}")) : "")}",
        };
    }

    private async Task<(ApplyRequest?, Dictionary<long, OutboxEntry>, bool More)> BuildBatchAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_instance is null)
                return (null, [], false);
            (ApplyRequest? req, Dictionary<long, OutboxEntry> sent, bool more) = ApplyBatchBuilder.Build(_conn, _store.Model, _store.ClientId, _instance, ApplyBudget);
            if (sent.Count > 0)
                StatusChanged?.Invoke();
            return (req, sent, more);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task ProcessReplyAsync(ApplyReply reply, IReadOnlyDictionary<long, OutboxEntry> bySeq)
    {
        await _gate.WaitAsync();
        ReplicaWriter w;
        var logs = new List<(LogLevel Level, string Text)>();
        try
        {
            w = new ReplicaWriter(_store.Model, _instance!, _log, Name);
            using SqliteTransaction tx = _conn.BeginTransaction();
            var versions = new List<string>();
            foreach (ActionResult r in reply.Results)
            {
                if (!bySeq.TryGetValue(r.Seq, out OutboxEntry? e))
                    continue;
                SyncTable? t = _store.Model.TryGet(e.Table, out SyncTable? tt) ? tt : null;

                string LabelOf() => t is not null && e.Pk is not null ? ClientStore.Label(_conn, tx, t, e.Pk) : "";

                switch (r.Status)
                {
                    case ResultStatus.Rejected when e.Kind == OutboxKind.Patch && t is not null && e.Pk is not null:
                        // The owner keeps its row as it was: the edit goes, and the owner's full row comes back (8.4, 9).
                        Log(LogLevel.Warning, $"the edit of {LabelOf()} is not saved ({RejectedWhy(r.Reason)}), the row returns to its state on the instance");
                        w.Revert(_conn, tx, t, e.Pk);
                        break;
                    case ResultStatus.Rejected when r.Reason == ApplyReasons.Unique:
                        Log(LogLevel.Warning, $"{LabelOf()} not saved: {RejectedWhy(r.Reason)}");
                        break;
                    case ResultStatus.Rejected when t is not null && e.Pk is not null:
                    {
                        var gone = new List<string>();
                        w.RemoveWithChildren(_conn, tx, t, e.Pk, gone);
                        Log(LogLevel.Warning, $"not saved: {string.Join(", ", gone.Count > 0 ? gone : [LabelOf()])} ({RejectedWhy(r.Reason)})");
                        break;
                    }
                    case ResultStatus.Ignored when t is not null && e.Pk is not null:
                    {
                        // Delete wins: the record goes here right away, without waiting for the tombstone (6.5, 9.1).
                        string label = LabelOf();
                        var gone = new List<string>();
                        w.RemoveWithChildren(_conn, tx, t, e.Pk, gone);
                        Log(LogLevel.Warning, $"the edit of {label} is lost: it was deleted on the instance");
                        break;
                    }
                    case ResultStatus.Applied or ResultStatus.Skipped:
                        switch (e.Kind)
                        {
                            case OutboxKind.Create or OutboxKind.Patch when r.HasVersion && t is not null:
                                w.SetVersion(_conn, tx, t, e.Pk!, r.Version);
                                versions.Add($"{LabelOf()} v{r.Version}");
                                break;
                            case OutboxKind.Delete when r.HasVersion:
                                long cur;
                                lock (_cursorsLock)
                                    cur = _cursors[e.Table].Cursor;
                                if (cur < r.Version)
                                    _conn.Exec("UPDATE _sync_outbox SET sent = 2, expected_version = @v WHERE id = @id", tx, ("@v", r.Version), ("@id", e.Id));
                                break;
                            case OutboxKind.PredicateDelete when KeptAfterBulkDelete(tx, r) is > 0 and int kept:
                                Log(LogLevel.Warning, $"{kept} not deleted: changed or created on the instance after you saw the data; they stay and arrive with the stream");
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
                logs.Add((LogLevel.Information, $"versions from ApplyReply: {string.Join(", ", versions)}; these changes will not echo back"));
            logs.Add((LogLevel.Information, $"← ApplyReply: applied_up_to_seq = {reply.AppliedUpToSeq}"));
        }
        finally
        {
            _gate.Release();
        }
        foreach ((LogLevel level, string text) in logs)
            Log(level, text);
        if (w.TouchedTables.Count > 0)
            DataChanged?.Invoke(w.TouchedTables);
        StatusChanged?.Invoke();
    }

    /// <summary>Rows the owner kept from a bulk delete; a row that also went by key is gone all the same (8.6, 11.6).</summary>
    private int KeptAfterBulkDelete(SqliteTransaction tx, ActionResult r) =>
        r.Changed.Count(x => ClientStore.FindEntry(_conn, tx, _instance!, x.Tbl, PkText(x.Pk)) is not { Kind: OutboxKind.Delete });

    private static string RejectedWhy(string reason) => reason switch
    {
        ApplyReasons.Unique => "the instance already has a row with this value",
        ApplyReasons.ParentDeleted => "the parent row is not saved or was deleted",
        _ => "the row was deleted on the instance",
    };

    // Snapshot (6.3, 10.3).

    /// <summary>Downloads the owner's snapshot and installs it in one transaction; the unsent outbox of the instance goes with the old replica.</summary>
    private async Task SnapshotAsync(Sync.SyncClient client, CancellationToken ct)
    {
        SetState(AgentState.Snapshot);
        string path = await DownloadAsync(client, ct);
        await _gate.WaitAsync(ct);
        try
        {
            // Another owner may answer at the address since the handshake: its snapshot is refused, the next round adopts it with a claim.
            SnapshotInstall done = ReplicaWriter.InstallSnapshot(_store, path, _instance!, this);
            long cursor;
            lock (_cursorsLock)
            {
                _cursors = _store.LoadCursors(_conn, _instance!);
                cursor = _cursors.Values.First().Cursor;
            }

            _log.LogInformation("[{Agent}] snapshot verified and merged in one transaction: rows with InstanceId = {InstanceId} replaced, cursors = {Cursor}", Name, _instance, cursor);
            if (done.Dropped > 0)
                _log.LogError("[{Agent}] the queue of the old replica is dropped: {Dropped} actions not sent", Name, done.Dropped);
            foreach ((string old, int dropped) in done.Replaced)
                _log.LogError("[{Agent}] the replica of instance {Old} is replaced by the snapshot of {InstanceId}: {Dropped} actions not sent", Name, old, _instance, dropped);
        }
        finally
        {
            _gate.Release();
            File.Delete(path);
            _download = null;
            _snapshotProgress = null;
        }
        DataChanged?.Invoke(_store.Model.Tables.Select(t => t.Name).ToList());
        StatusChanged?.Invoke();
    }

    /// <summary>
    /// Empties the replica rows of the instance (archives, <c>InstanceId</c> = "X:archive", stay) and sets zero cursors: the data then arrives by catch-up, newest first (6.3).
    /// The outbox goes with the old rows, as with a snapshot: its actions were made against the state that is replaced.
    /// </summary>
    private async Task ResetReplicaAsync(CancellationToken ct)
    {
        int dropped;
        await _gate.WaitAsync(ct);
        try
        {
            using SqliteTransaction tx = _conn.BeginTransaction();
            dropped = ReplicaWriter.DropReplica(_conn, tx, _store.Model, _instance!);
            foreach (SyncTable t in _store.Model.Tables)
                ClientStore.SaveCursor(_conn, tx, _instance!, t.Name, new CursorState(0));
            ClientStore.SaveInstance(_conn, tx, _instance!, null);
            tx.Commit();
            lock (_cursorsLock)
                _cursors = NewCursors();
        }
        finally
        {
            _gate.Release();
        }
        if (dropped > 0)
            _log.LogError("[{Agent}] the queue of the old replica is dropped: {Dropped} actions not sent", Name, dropped);
        DataChanged?.Invoke(_store.Model.Tables.Select(t => t.Name).ToList());
        StatusChanged?.Invoke();
    }

    /// <summary>Downloads the snapshot file, resuming from the same offset by snapshot_id after a break (6.3).</summary>
    private async Task<string> DownloadAsync(Sync.SyncClient client, CancellationToken ct)
    {
        long offset = _download is { } d && File.Exists(d.Path) ? new FileInfo(d.Path).Length : 0;
        if (offset > 0 && _download is { } resumed)
            _log.LogInformation("[{Agent}] snapshot {SnapshotId}: resuming from {OffsetKb} KB", Name, resumed.Id, offset / 1024);
        using AsyncServerStreamingCall<SnapshotChunk> call = client.Snapshot(new SnapshotRequest { ClientId = _store.ClientId, SnapshotId = _download?.Id ?? "", Offset = offset }, cancellationToken: ct);
        byte[]? sha = null;
        while (await call.ResponseStream.MoveNext(ct))
        {
            SnapshotChunk ch = call.ResponseStream.Current;
            if (_download is null || _download.Id != ch.SnapshotId)
            {
                if (_download is not null)
                    File.Delete(_download.Path);
                _download = new Download(ch.SnapshotId, _store.DbPath + "." + ch.SnapshotId + ".part", ch.TotalSize);
                File.Delete(_download.Path);
                _log.LogInformation("[{Agent}] snapshot {SnapshotId}: V = {Version}, {SizeKb} KB", Name, ch.SnapshotId, ch.Version, ch.TotalSize / 1024);
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
            throw new IOException("the snapshot is empty");
        await using (FileStream f = File.OpenRead(_download.Path))
        {
            if (f.Length != _download.Total)
                throw new IOException("the snapshot was cut off");
            if (sha is not null && !(await SHA256.HashDataAsync(f, ct)).SequenceEqual(sha))
            {
                f.Close();
                File.Delete(_download.Path);
                _download = null;
                throw new IOException("the snapshot hash does not match");
            }
        }
        return _download.Path;
    }

    // Bulk actions (8.6) and archive (11).

    /// <summary>
    /// Deletes the rows matching <paramref name="predicate"/> locally and queues one predicate action with the version V the
    /// replica is complete up to. If the replica has ranges above the cursor, the keys go instead. Rows with their own
    /// outbox actions and rows moved to the archive (11.6) go by key as well.
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
            List<string> ids = c.Query($"SELECT Id FROM {Q(t.Name)} WHERE InstanceId = @inst AND {where}", tx, r => r.GetString(0), [("@inst", _instance), .. args]);
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
                _log.LogInformation("[{Agent}] bulk delete: replica «{Table}» is incomplete (ranges above the cursor), keys queued: {Count}", Name, table, count);
            }
            else
            {
                // Rows with their own actions in the outbox go by key, the rest as one predicate.
                List<string> own = ids.Where(id => ClientStore.FindEntry(c, tx, _instance, table, id) is not null).ToList();
                var archived = new List<string>();
                foreach (string id in ids)
                {
                    // A row moved to the archive misses the stream's updates, so the owner must not keep it: its delete goes by key, as a local one (11.6).
                    if (ArchiveGuard.DeleteOrArchive(c, tx, _store.Model, _instance, t, id) && !own.Contains(id))
                        archived.Add(id);
                }

                foreach (string id in own.Concat(archived))
                    ClientStore.Put(c, tx, _instance, table, id, OutboxKind.Delete, null, OutboxClass.Bulk);
                ClientStore.Insert(c, tx, _instance, table, null, OutboxKind.PredicateDelete, OutboxClass.Bulk, predicate: predicate.Serialize(), expectedVersion: k.Cursor);
                Log(LogLevel.Information, $"bulk delete of {count} rows: one queued action {predicate} and SyncVersion ≤ {k.Cursor}"
                    + (own.Count > 0 ? $", {own.Count} more by key (they have their own queued actions)" : "")
                    + (archived.Count > 0 ? $", {archived.Count} more by key (moved to the archive because archived rows reference them)" : ""));
            }
            ClientStore.DropOrphanEntries(c, tx, _store.Model, _instance);
            tx.Commit();
        }
        finally
        {
            _gate.Release();
        }
        DataChanged?.Invoke(_store.Model.Tables.Select(x => x.Name).ToList());
        StatusChanged?.Invoke();
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
                return "no replica";
            if (tries == 0)
            {
                long pending = _conn.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i", null, ("@i", _instance));
                if (_state != AgentState.Online || pending > 0)
                {
                    text = "archiving is available when the instance is online, fully caught up and the queue is empty";
                    Log(LogLevel.Warning, text);
                    return text;
                }
            }
            using SqliteTransaction tx = _conn.BeginTransaction();
            var set = new List<(string Table, string Pk)>();
            var members = new HashSet<(string Table, string Pk)>();

            void Add(string table, string pk)
            {
                if (members.Contains((table, pk)))
                    return;
                if (_conn.Scalar<string>($"SELECT InstanceId FROM {Q(table)} WHERE Id = @id AND InstanceId = @i", tx, ("@id", pk), ("@i", _instance)) is null)
                    return;
                set.Add((table, pk));
                members.Add((table, pk));
                // FK closure: the owner's cascade would delete the children with the parent (11.2, step 3).
                foreach ((SyncTable child, SyncForeignKey fk) in _store.Model.ChildrenOf(table))
                {
                    foreach (string id in ClientStore.ChildIds(_conn, tx, _instance, child, fk, pk))
                        Add(child.Name, id);
                }
            }
            foreach ((string table, string pk) in rows)
                Add(table, pk);
            if (set.Count == 0)
                return "nothing to archive";
            string archive = SyncColumns.ArchiveOf(_instance);
            List<string> labels = set.Select(x => ClientStore.Label(_conn, tx, _store.Model[x.Table], x.Pk)).ToList();
            foreach ((string table, string pk) in set)
                _conn.Exec($"UPDATE {Q(table)} SET InstanceId = @a WHERE Id = @id AND InstanceId = @i", tx, ("@a", archive), ("@id", pk), ("@i", _instance));
            long v;
            lock (_cursorsLock)
                v = _cursors.Values.Min(k => k.Cursor);
            ClientStore.Insert(_conn, tx, _instance, set[0].Table, null, OutboxKind.Archive, OutboxClass.Bulk, predicate: ArchiveSet.Serialize(set), expectedVersion: v);
            tx.Commit();
            text = $"{(tries > 0 ? "archive retry" : "archive")}: {string.Join(", ", labels)} got InstanceId = {archive} in one transaction; one action in the queue: Id ∈ set and SyncVersion ≤ {v}";
            _archiveRetry.RemoveAll(x => members.Contains((x.Table, x.Pk)));
            foreach ((string _, string pk) in set)
            {
                if (tries > 0)
                    _archiveTries[pk] = tries;
                else
                    _archiveTries.Remove(pk);
            }
        }
        finally
        {
            _gate.Release();
        }
        Log(LogLevel.Information, text);
        DataChanged?.Invoke(_store.Model.Tables.Select(x => x.Name).ToList());
        StatusChanged?.Invoke();
        return text;
    }

    /// <summary>
    /// Rows the owner kept: changed after V, or parents of kept children. They go back from the archive to the replica;
    /// changed ones also into NeedFull, since their updates bypassed the archive (11.2, step 6).
    /// </summary>
    private void ArchiveLeftovers(SqliteTransaction tx, ReplicaWriter w, ActionResult r, OutboxEntry e, List<(LogLevel Level, string Text)> logs)
    {
        string archive = SyncColumns.ArchiveOf(_instance!);
        var names = new List<string>();
        foreach ((ArchiveLeftover why, Google.Protobuf.Collections.RepeatedField<RowRef> list) in new[] { (ArchiveLeftover.Changed, r.Changed), (ArchiveLeftover.Children, r.Children) })
        {
            foreach (RowRef x in list)
            {
                string pk = PkText(x.Pk);
                long v = _conn.Scalar<long>($"SELECT SyncVersion FROM {Q(x.Tbl)} WHERE Id = @id", tx, ("@id", pk));
                _conn.Exec($"UPDATE {Q(x.Tbl)} SET InstanceId = @i WHERE Id = @id AND InstanceId = @a", tx, ("@i", _instance), ("@id", pk), ("@a", archive));
                names.Add(ClientStore.Label(_conn, tx, _store.Model[x.Tbl], pk));
                if (why == ArchiveLeftover.Changed)
                    w.NeedFull.Add(x);
                _archiveRetry.Add(new ArchiveRetry(x.Tbl, pk, why, v, _archiveTries.GetValueOrDefault(pk) + 1));
                w.TouchedTables.Add(x.Tbl);
            }
        }

        logs.Add((LogLevel.Warning, $"kept by the owner: {string.Join(", ", names)} (changed after V or have children); returned from the archive to the replica, changed ones in NeedFull, will retry the archive"));
    }

    private async Task RetryArchiveAsync()
    {
        if (_archiveRetry.Count == 0)
            return;
        ArchiveRetry? next;
        await _gate.WaitAsync();
        try
        {
            next = TakeReadyArchiveRetry();
        }
        finally
        {
            _gate.Release();
        }
        if (next is not null)
            await ArchiveCoreAsync([(next.Table, next.Pk)], next.Tries);
    }

    /// <summary>Removes and returns the first retry that is ready; one that keeps failing gets a note instead. Called under the gate.</summary>
    private ArchiveRetry? TakeReadyArchiveRetry()
    {
        foreach (ArchiveLeftover pass in new[] { ArchiveLeftover.Changed, ArchiveLeftover.Children })
        {
            foreach (ArchiveRetry rt in _archiveRetry.Where(x => x.Why == pass).ToList())
            {
                long? v = _conn.Scalar<long?>($"SELECT SyncVersion FROM {Q(rt.Table)} WHERE Id = @id AND InstanceId = @i", null, ("@id", rt.Pk), ("@i", _instance));
                if (v is null)
                {
                    _archiveRetry.Remove(rt);
                    continue;
                }
                bool ok = pass == ArchiveLeftover.Changed
                    ? v > rt.Version
                    : !_archiveRetry.Any(o => o != rt && IsChild(o, rt));
                if (!ok)
                    continue;
                _archiveRetry.Remove(rt);
                if (rt.Tries >= 3)
                {
                    Log(LogLevel.Warning, $"{ClientStore.Label(_conn, null, _store.Model[rt.Table], rt.Pk)} stayed on the owner: automation keeps changing it");
                    continue;
                }
                return rt;
            }
        }
        return null;
    }

    private bool IsChild(ArchiveRetry child, ArchiveRetry parent) =>
        _store.Model.ChildrenOf(parent.Table).Any(x => x.Child.Name == child.Table);

    public async ValueTask DisposeAsync()
    {
        _life.Cancel();
        _sessionCts?.Cancel();
        if (_loop is not null)
            await _loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
        if (_instance is not null)
            _store.Release(_instance, this);
        _conn.Dispose();
    }
}
