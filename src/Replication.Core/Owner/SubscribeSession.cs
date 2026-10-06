using System.Collections.Concurrent;
using Grpc.Core;
using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Owner;

/// <summary>
/// One Subscribe stream: handshake on the first message, catch-up newest first, then online (6.2, 6.4).
/// </summary>
internal sealed class SubscribeSession
{
    private readonly OwnerStore _store;
    private readonly IAsyncStreamReader<SubscribeMessage> _in;
    private readonly IServerStreamWriter<ChangeMessage> _out;
    private readonly CancellationToken _ct;
    private readonly AsyncSignal _wake = new();
    private readonly ConcurrentQueue<RowRef> _needFull = new();
    private readonly Dictionary<string, CursorState> _mirror = new(StringComparer.Ordinal);
    private readonly HashSet<string> _synced = new(StringComparer.Ordinal);
    private readonly List<RangeExtra> _extra = [];
    private readonly object _lock = new();
    private string[] _open = [];
    private string _clientId = "";
    private string _who = "";
    private bool _online;
    private long _sentUpTo;
    private long _catchupHead;

    public SubscribeSession(OwnerStore store, IAsyncStreamReader<SubscribeMessage> input, IServerStreamWriter<ChangeMessage> output, CancellationToken ct)
    {
        _store = store;
        _in = input;
        _out = output;
        _ct = ct;
    }

    private OwnerOptions Opt => _store.Options;
    private SyncModel Model => _store.Model;

    private void Log(string text, SyncLogLevel level = SyncLogLevel.Info) => Opt.Log.Write("owner", $"[{_who}] {text}", level);

    public async Task RunAsync()
    {
        if (!await _in.MoveNext(_ct))
            return;
        if (_in.Current.BodyCase != SubscribeMessage.BodyOneofCase.Start)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "the first Subscribe message must be Start"));
        Start start = _in.Current.Start;
        _clientId = start.ClientId;
        _who = start.ClientId.Length > 8 ? start.ClientId[..8] : start.ClientId;

        using SqliteConnection conn = _store.Open();
        if (!await HandshakeAsync(conn, start))
            return;

        _store.Committed += _wake.Set;
        Task reader = Task.Run(ReadLoopAsync);
        try
        {
            await MainLoopAsync(conn);
        }
        finally
        {
            _store.Committed -= _wake.Set;
            _store.MarkSubscribed(_clientId, false);
            try
            {
                await reader;
            }
            catch
            {
                // Stream closed.
            }
        }
    }

    // Handshake (6.2).

    private async Task<bool> HandshakeAsync(SqliteConnection conn, Start start)
    {
        if (start.SchemaVersion != Opt.SchemaVersion)
        {
            Log($"Start: схема клієнта {start.SchemaVersion}, власника {Opt.SchemaVersion} → SchemaMismatch, потік закрито", SyncLogLevel.Warn);
            await SendAsync(new ChangeMessage { SchemaMismatch = new SchemaMismatch { OwnerSchemaVersion = Opt.SchemaVersion } });
            return false;
        }

        Dictionary<string, CursorState> cursors = start.Cursors.ToDictionary(c => c.Tbl, CursorState.FromWire);
        long head;
        string? reason = null;
        // Checked and registered in one write transaction, as is every purge batch (10.2): either a batch ran first and
        // the check sees its purged_version, or every later batch sees this client subscribed with its acked_version.
        using (SqliteTransaction tx = conn.BeginTransaction())
        {
            head = _store.Head(conn, tx);
            long purged = _store.Purged(conn, tx);
            long minC = Model.Tables.Select(t => cursors.TryGetValue(t.Name, out CursorState? c) ? c.Cursor : 0).DefaultIfEmpty(0).Min();
            // A table that is still empty (cursor 0, no ranges) has no rows whose deletion it could miss, so forgotten
            // tombstones do not matter for it; this lets an empty replica (6.3) start after a purge (6.2).
            (string Name, CursorState K) behind = Model.Tables
                .Select(t => (t.Name, K: cursors.TryGetValue(t.Name, out CursorState? c) ? c : new CursorState(0)))
                .FirstOrDefault(x => !(x.K.Cursor == 0 && x.K.Ranges.Count == 0) && x.K.Cursor < purged);
            if (string.IsNullOrEmpty(start.InstanceId))
                reason = "репліки нема";
            else if (start.InstanceId != _store.InstanceId)
                reason = "інший instance_id";
            else if (behind.Name is not null)
                reason = $"курсор {behind.Name} {behind.K.Cursor} < purged_version {purged}";
            else if (minC > head)
                reason = $"курсор {minC} > version {head}";
            if (reason is null)
            {
                foreach (SyncTable t in Model.Tables)
                    _mirror[t.Name] = cursors.TryGetValue(t.Name, out CursorState? c) ? c : new CursorState(0);
                _open = [.. start.OpenTables];
                _store.SaveCursors(conn, tx, _clientId, _mirror.ToDictionary(x => x.Key, x => x.Value.Cursor));
                _store.MarkSubscribed(_clientId, true);
                try
                {
                    tx.Commit();
                }
                catch
                {
                    _store.MarkSubscribed(_clientId, false);
                    throw;
                }
            }
        }

        if (reason is not null)
        {
            long size = _store.FileSizeBytes(conn);
            Log($"Start: {reason} → SnapshotRequired, знімок ~{size / 1024} КБ, потік закрито", SyncLogLevel.Warn);
            await SendAsync(new ChangeMessage
            {
                SnapshotRequired = new SnapshotRequired { Reason = reason, SizeBytes = size, InstanceId = _store.InstanceId },
            });
            return false;
        }

        Log($"Start прийнято, голова {head}; курсори {string.Join(", ", _mirror.Select(x => $"{x.Key}={x.Value}"))}", SyncLogLevel.Ok);
        return true;
    }

    // Client messages.

    private async Task ReadLoopAsync()
    {
        while (await _in.MoveNext(_ct))
        {
            SubscribeMessage m = _in.Current;
            switch (m.BodyCase)
            {
                case SubscribeMessage.BodyOneofCase.OpenTables:
                    lock (_lock)
                        _open = [.. m.OpenTables.Tables];
                    Log($"OpenTables: {string.Join(", ", m.OpenTables.Tables)}");
                    _wake.Set();
                    break;
                case SubscribeMessage.BodyOneofCase.Ack:
                    using (SqliteConnection c = _store.Open())
                    using (SqliteTransaction tx = c.BeginTransaction())
                    {
                        _store.SaveCursors(c, tx, _clientId, m.Ack.Cursors.ToDictionary(x => x.Tbl, x => x.Cursor));
                        tx.Commit();
                    }
                    foreach (RowRef r in m.Ack.NeedFull)
                        _needFull.Enqueue(r);
                    if (m.Ack.NeedFull.Count > 0)
                        _wake.Set();
                    break;
            }
        }
    }

    // Main loop.

    private async Task MainLoopAsync(SqliteConnection conn)
    {
        long dataVersion = conn.Scalar<long>("PRAGMA data_version");
        DateTimeOffset lastOnline = DateTimeOffset.MinValue;
        while (!_ct.IsCancellationRequested)
        {
            if (!_needFull.IsEmpty)
            {
                await SendNeedFullAsync(conn);
                continue;
            }
            if (!_online)
            {
                if (await CatchupStepAsync(conn))
                    continue;
                _online = true;
                _sentUpTo = _catchupHead;
                await SendAsync(new ChangeMessage { Progress = new Progress { Done = true, OwnerTimeUnix = _store.Now(conn) } });
                Log($"досинхронізацію завершено, онлайн від v{_sentUpTo}", SyncLogLevel.Ok);
                continue;
            }

            // Online: poll PRAGMA data_version (triggers cannot notify the process), or wake on our own commits.
            bool woke = await _wake.WaitAsync(Opt.PollInterval, _ct);
            long dv = conn.Scalar<long>("PRAGMA data_version");
            if (!woke && dv == dataVersion)
                continue;
            dataVersion = dv;
            TimeSpan wait = lastOnline + Opt.OnlineInterval - DateTimeOffset.UtcNow;
            if (wait > TimeSpan.Zero)
                await Task.Delay(wait, _ct);
            if (await OnlineStepAsync(conn))
                lastOnline = DateTimeOffset.UtcNow;
        }
    }

    private IEnumerable<SyncTable> PriorityTables()
    {
        string[] open;
        lock (_lock)
            open = _open;
        // Open tables first, together with the tables they reference by FK (6.4).
        bool First(SyncTable t) => open.Contains(t.Name) || open.Any(o => Model.TryGet(o, out SyncTable? ot) && ot.ForeignKeys.Any(f => f.ParentTable == t.Name));
        return Model.Tables.OrderBy(t => First(t) ? 0 : 1);
    }

    /// <summary>One step of catch-up: one Batch or TableSynced. Returns false when nothing is left.</summary>
    private async Task<bool> CatchupStepAsync(SqliteConnection conn)
    {
        ChangeMessage? msg = null;
        string? log = null;
        using (SqliteTransaction tx = conn.BeginTransaction(deferred: true))
        {
            long head = _store.Head(conn, tx);
            _catchupHead = head;
            foreach (SyncTable t in PriorityTables())
            {
                CursorState k = _mirror[t.Name];
                List<(long Lo, long Hi)> gaps = k.Gaps(head);
                List<(long Lo, long Hi)> nonEmpty = gaps.Where(g => OwnerReader.Any(conn, tx, t, g.Lo, g.Hi)).ToList();
                if (nonEmpty.Count == 0)
                {
                    if (!_synced.Contains(t.Name))
                    {
                        k.Reset(head);
                        _synced.Add(t.Name);
                        msg = new ChangeMessage { TableSynced = new TableSynced { Tbl = t.Name, Version = head } };
                        log = $"TableSynced {t.Name}, курсор = {head}";
                        break;
                    }
                    // The table is synced, the head moved because of other tables: close empty ranges on the way.
                    foreach ((long Lo, long Hi) g in gaps)
                    {
                        _extra.Add(new RangeExtra { Tbl = t.Name, Lo = g.Lo, Hi = g.Hi });
                        k.AddRange(g.Lo, g.Hi);
                    }
                    continue;
                }
                _synced.Remove(t.Name);

                // The highest unsent versions, newest first; a fresh tail smaller than a batch rides in the same message.
                int n = Opt.CatchupBatchRows;
                (long Lo, long Hi) main = nonEmpty[0];
                (long Lo, long Hi)? tail = null;
                if (nonEmpty.Count > 1 && nonEmpty[0].Hi == head && OwnerReader.Count(conn, tx, t, nonEmpty[0].Lo, nonEmpty[0].Hi) < n)
                {
                    tail = nonEmpty[0];
                    main = nonEmpty[1];
                }
                List<OwnerItem> all = OwnerReader.Range(conn, tx, t, main.Lo, main.Hi, n + 1, newestFirst: true);
                List<OwnerItem> items = all.Count > n ? all.GetRange(0, n) : all;
                long lo = all.Count > n ? items[^1].Version - 1 : main.Lo;
                List<OwnerItem> tailItems = tail is { } tl ? OwnerReader.Range(conn, tx, t, tl.Lo, tl.Hi, n, newestFirst: true) : [];

                // The client is complete up to cur0: this decides whether a partial row is enough.
                long cur0 = k.Cursor;
                var covers = new List<(long Lo, long Hi)> { (lo, main.Hi) };
                covers.AddRange(gaps.Where(g => g.Hi > main.Hi));
                foreach ((long a, long b) in covers)
                    k.AddRange(a, b);

                var batch = new Batch { Tbl = t.Name };
                batch.Columns.AddRange(t.Columns);
                foreach ((long a, long b) in covers)
                    batch.Covers.Add(new RangeExtra { Tbl = t.Name, Lo = a, Hi = b });
                var own = new List<long>();
                foreach (OwnerItem it in tailItems.Concat(items))
                {
                    if (it.Origin == _clientId)
                    {
                        // Its own change: not sent, the range still closes.
                        own.Add(it.Version);
                        continue;
                    }

                    if (it.IsTombstone)
                        batch.Tombstones.Add(new Protocol.Tombstone { Pk = PkBytes(it.Pk), Version = it.Version });
                    else
                        batch.Rows.Add(OwnerReader.ToWire(t, it, cur0));
                }
                batch.Remaining = Remaining(conn, tx, head);
                msg = new ChangeMessage { Batch = batch };
                log = $"Batch {t.Name}: {Describe(batch)}{(tail is null ? "" : $" + свіжий хвіст ({tailItems.Count})")}"
                    + (own.Count > 0 ? $"; пропущено як власні зміни: {string.Join(", ", own.Select(v => "v" + v))}" : "")
                    + $", закриває {string.Join(" ", covers.Select(c => $"({c.Lo},{c.Hi}]"))}, лишилось {batch.Remaining}";
                break;
            }
            tx.Commit();
        }
        if (msg is null)
            return false;
        Log(log!);
        await SendAsync(msg);
        return true;
    }

    private long Remaining(SqliteConnection conn, SqliteTransaction tx, long head)
    {
        long n = 0;
        foreach (SyncTable t in Model.Tables)
        {
            foreach ((long Lo, long Hi) g in _mirror[t.Name].Gaps(head))
                n += OwnerReader.Count(conn, tx, t, g.Lo, g.Hi);
        }

        return n;
    }

    /// <summary>Online: everything after the last sent version, oldest first, then Head(V) (6.4).</summary>
    private async Task<bool> OnlineStepAsync(SqliteConnection conn)
    {
        var messages = new List<ChangeMessage>();
        var logs = new List<string>();
        long upTo;
        using (SqliteTransaction tx = conn.BeginTransaction(deferred: true))
        {
            long head = _store.Head(conn, tx);
            if (head <= _sentUpTo)
            {
                tx.Commit();
                return false;
            }
            int n = Opt.OnlineBatchRows;
            List<(SyncTable Table, OwnerItem Item)> all = Model.Tables
                .SelectMany(t => OwnerReader.Range(conn, tx, t, _sentUpTo, head, n + 1, newestFirst: false).Select(it => (Table: t, Item: it)))
                .OrderBy(x => x.Item.Version)
                .ToList();
            upTo = all.Count > n ? all[n - 1].Item.Version : head;
            List<(SyncTable Table, OwnerItem Item)> taken = all.Where(x => x.Item.Version <= upTo).ToList();
            List<(SyncTable Table, OwnerItem Item)> own = taken.Where(x => x.Item.Origin == _clientId).ToList();
            foreach (IGrouping<SyncTable, (SyncTable Table, OwnerItem Item)> g in taken.Where(x => x.Item.Origin != _clientId).GroupBy(x => x.Table))
            {
                SyncTable t = g.Key;
                var batch = new Batch { Tbl = t.Name, Online = true };
                batch.Columns.AddRange(t.Columns);
                batch.Covers.Add(new RangeExtra { Tbl = t.Name, Lo = _sentUpTo, Hi = upTo });
                foreach ((SyncTable _, OwnerItem it) in g)
                {
                    if (it.IsTombstone)
                        batch.Tombstones.Add(new Protocol.Tombstone { Pk = PkBytes(it.Pk), Version = it.Version });
                    else
                        batch.Rows.Add(OwnerReader.ToWire(t, it, _mirror[t.Name].Cursor));
                }
                messages.Add(new ChangeMessage { Batch = batch });
                logs.Add($"{t.Name} {Describe(batch)}");
            }
            if (own.Count > 0)
                logs.Add($"пропущено як власні зміни: {string.Join(", ", own.Select(x => $"{x.Table.Name} v{x.Item.Version}"))}");
            tx.Commit();
        }
        messages.Add(new ChangeMessage { Head = new Head { Version = upTo } });
        Log($"онлайн: {(logs.Count > 0 ? string.Join("; ", logs) : "нових рядків нема")}; Head({upTo})");
        foreach (ChangeMessage m in messages)
            await SendAsync(m);
        foreach (CursorState k in _mirror.Values)
        {
            k.AddRange(_sentUpTo, upTo);
            k.LiftTo(upTo);
        }
        _sentUpTo = upTo;
        return true;
    }

    /// <summary>
    /// NeedFull from Ack: the full row; if the row is gone, its tombstone even when the tombstone is this client's own;
    /// if neither exists, nothing (9.1).
    /// </summary>
    private async Task SendNeedFullAsync(SqliteConnection conn)
    {
        var refs = new List<RowRef>();
        while (_needFull.TryDequeue(out RowRef? r))
            refs.Add(r);
        var messages = new List<ChangeMessage>();
        foreach (IGrouping<string, RowRef> g in refs.GroupBy(r => r.Tbl))
        {
            if (!Model.TryGet(g.Key, out SyncTable? t))
                continue;
            var batch = new Batch { Tbl = t.Name, Online = _online };
            batch.Columns.AddRange(t.Columns);
            foreach (RowRef r in g)
            {
                string pk = PkText(r.Pk);
                OwnerItem? row = OwnerReader.Row(conn, null, t, pk);
                if (row is not null)
                    batch.Rows.Add(OwnerReader.ToWire(t, row, 0, forceFull: true));
                else if (OwnerReader.Tombstone(conn, null, t, pk) is { } tomb)
                    batch.Tombstones.Add(new Protocol.Tombstone { Pk = r.Pk, Version = tomb.Version });
            }
            if (batch.Rows.Count + batch.Tombstones.Count > 0)
            {
                messages.Add(new ChangeMessage { Batch = batch });
                Log($"NeedFull {t.Name}: повних рядків {batch.Rows.Count}, tombstones {batch.Tombstones.Count}");
            }
        }
        foreach (ChangeMessage m in messages)
            await SendAsync(m);
    }

    private async Task SendAsync(ChangeMessage m)
    {
        if (_extra.Count > 0 && m.BodyCase is not (ChangeMessage.BodyOneofCase.SnapshotRequired or ChangeMessage.BodyOneofCase.SchemaMismatch))
        {
            m.Extra.AddRange(_extra);
            _extra.Clear();
        }
        TimeSpan delay = Opt.Faults.StreamDelay(_clientId);
        if (delay > TimeSpan.Zero)
            await Task.Delay(delay, _ct);
        await _out.WriteAsync(m, _ct);
    }

    private static string Describe(Batch b)
    {
        IEnumerable<string> parts = b.Rows.Select(r => r.Full ? $"v{r.Version} повний" : $"v{r.Version} [{Mask(b, r.Mask)}]")
            .Concat(b.Tombstones.Select(t => $"v{t.Version} ✕"));
        string s = string.Join(", ", parts);
        return s.Length == 0 ? "рядків нема" : s;
    }

    private static string Mask(Batch b, long mask) =>
        string.Join(", ", b.Columns.Where((_, i) => (mask & (1L << i)) != 0));
}
