using Microsoft.Data.Sqlite;
using Replication.Model;
using static Replication.Model.Wire;

namespace Replication.Client;

public enum OutboxKind
{
    Create = 1,
    Patch = 2,
    Delete = 3,
    Archive = 4,
    PredicateDelete = 5
}

public enum OutboxClass
{
    Interactive = 1,
    Bulk = 2
}

/// <summary>The <c>sent</c> column of the outbox: waiting, sent (in flight), or a delete confirmed and held until the cursor passes its tombstone (8.3, 9.1).</summary>
public enum OutboxSendState
{
    Waiting = 0,
    InFlight = 1,
    DeleteConfirmed = 2
}

/// <summary>A row of <c>_sync_outbox</c>: what was done and with which columns, never the values (5.4).</summary>
public sealed record OutboxEntry(
    long Id, string Instance, long? Seq, string Table, string? Pk, OutboxKind Kind, OutboxClass Class,
    IReadOnlyList<string> Columns, string? Predicate, long? ExpectedVersion, OutboxSendState Sent);

public sealed record ClientNote(long Id, string Instance, DateTimeOffset At, string Text, bool Info);

/// <summary>
/// The client database: one file for all instances, rows tagged by <c>InstanceId</c> (5.4), plus cursors,
/// received ranges, the outbox and notes. The outbox lives next to the data, so an edit and its outbox row commit together.
/// </summary>
public sealed class ClientStore
{
    private const string EntryColumns = "id, instance, seq, tbl, pk, kind, cls, columns, predicate, expected_version, sent";

    /// <summary>Columns that identify a row for a person reading a note.</summary>
    public static readonly string[] LabelColumns = ["Name", "Title", "Text"];

    public ClientStore(string dbPath, SyncModel model)
    {
        DbPath = Path.GetFullPath(dbPath);
        Model = model;
        ConnectionString = new SqliteConnectionStringBuilder { DataSource = DbPath, Pooling = true, DefaultTimeout = 30 }.ToString();
    }

    public string DbPath { get; }
    public SyncModel Model { get; }
    public string ConnectionString { get; }
    public string ClientId { get; private set; } = "";

    /// <param name="foreignKeys">The agent applies batches with FK off: children may arrive before parents (7).</param>
    public SqliteConnection Open(bool foreignKeys = true)
    {
        var c = new SqliteConnection(ConnectionString);
        c.Open();
        c.Exec(foreignKeys ? "PRAGMA foreign_keys = ON;" : "PRAGMA foreign_keys = OFF;");
        return c;
    }

    /// <summary>Creates the client service tables. Call after EF migrations / EnsureCreated. No sync triggers on the client (5.4).</summary>
    public void Install()
    {
        using SqliteConnection c = Open();
        c.Exec("PRAGMA journal_mode = WAL;");
        using SqliteTransaction tx = c.BeginTransaction();
        c.Exec("""
            CREATE TABLE IF NOT EXISTS _sync_local (
              key   TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS _sync_instances (
              address          TEXT PRIMARY KEY,   -- ip:port of the owner
              instance         TEXT,               -- instance_id of the owner, NULL before the first snapshot
              name             TEXT NOT NULL,
              snapshot_version INTEGER,
              next_seq         INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS _sync_cursors (
              instance TEXT    NOT NULL,
              tbl      TEXT    NOT NULL,
              cursor   INTEGER NOT NULL,
              PRIMARY KEY (instance, tbl)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS _sync_ranges (
              instance TEXT    NOT NULL,
              tbl      TEXT    NOT NULL,
              lo       INTEGER NOT NULL,
              hi       INTEGER NOT NULL,
              PRIMARY KEY (instance, tbl, hi)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS _sync_outbox (
              id               INTEGER PRIMARY KEY AUTOINCREMENT,
              instance         TEXT    NOT NULL,
              seq              INTEGER,
              tbl              TEXT    NOT NULL,
              pk               TEXT,               -- NULL for archive and predicate actions
              kind             INTEGER NOT NULL,
              cls              INTEGER NOT NULL,
              columns          TEXT,
              predicate        TEXT,
              expected_version INTEGER,
              sent             INTEGER NOT NULL DEFAULT 0,
              UNIQUE (instance, tbl, pk),
              UNIQUE (instance, seq)
            );
            CREATE TABLE IF NOT EXISTS _sync_notes (
              id       INTEGER PRIMARY KEY AUTOINCREMENT,
              instance TEXT    NOT NULL,
              at       INTEGER NOT NULL,
              text     TEXT    NOT NULL,
              info     INTEGER NOT NULL DEFAULT 0
            );
            """, tx);
        if (c.Scalar<string>("SELECT value FROM _sync_local WHERE key = 'client_id'", tx) is null)
            c.Exec("INSERT INTO _sync_local(key, value) VALUES ('client_id', @id)", tx, ("@id", "cl-" + Guid.NewGuid().ToString("N")[..12]));
        ClientId = c.Scalar<string>("SELECT value FROM _sync_local WHERE key = 'client_id'", tx)!;
        tx.Commit();
    }

    // Instances.

    public void EnsureInstance(string address, string name)
    {
        using SqliteConnection c = Open();
        c.Exec("INSERT INTO _sync_instances(address, name) VALUES (@a, @n) ON CONFLICT(address) DO UPDATE SET name = @n", null, ("@a", address), ("@n", name));
    }

    public string? InstanceOf(SqliteConnection c, string address, SqliteTransaction? tx = null) =>
        c.Scalar<string>("SELECT instance FROM _sync_instances WHERE address = @a", tx, ("@a", address));

    // Cursors.

    public Dictionary<string, CursorState> LoadCursors(SqliteConnection c, string instance, SqliteTransaction? tx = null)
    {
        Dictionary<string, CursorState> result = Model.Tables.ToDictionary(t => t.Name, _ => new CursorState(0), StringComparer.Ordinal);
        foreach ((string tbl, long cursor) in c.Query("SELECT tbl, cursor FROM _sync_cursors WHERE instance = @i", tx, r => (r.GetString(0), r.GetInt64(1)), ("@i", instance)))
        {
            if (result.ContainsKey(tbl))
                result[tbl] = new CursorState(cursor);
        }
        foreach ((string tbl, long lo, long hi) in c.Query("SELECT tbl, lo, hi FROM _sync_ranges WHERE instance = @i", tx, r => (r.GetString(0), r.GetInt64(1), r.GetInt64(2)), ("@i", instance)))
        {
            if (result.TryGetValue(tbl, out CursorState? k))
                k.AddRange(lo, hi);
        }
        return result;
    }

    public void SaveCursor(SqliteConnection c, SqliteTransaction tx, string instance, string table, CursorState k)
    {
        c.Exec("INSERT INTO _sync_cursors(instance, tbl, cursor) VALUES (@i, @t, @c) ON CONFLICT(instance, tbl) DO UPDATE SET cursor = @c",
            tx, ("@i", instance), ("@t", table), ("@c", k.Cursor));
        c.Exec("DELETE FROM _sync_ranges WHERE instance = @i AND tbl = @t", tx, ("@i", instance), ("@t", table));
        foreach ((long lo, long hi) in k.Ranges)
            c.Exec("INSERT INTO _sync_ranges(instance, tbl, lo, hi) VALUES (@i, @t, @l, @h)", tx, ("@i", instance), ("@t", table), ("@l", lo), ("@h", hi));
    }

    // Outbox.

    private static OutboxEntry ReadEntry(SqliteDataReader r) => new OutboxEntry(
        r.GetInt64(0), r.GetString(1), r.IsDBNull(2) ? null : r.GetInt64(2), r.GetString(3), r.IsDBNull(4) ? null : r.GetString(4),
        (OutboxKind)r.GetInt64(5), (OutboxClass)r.GetInt64(6),
        r.IsDBNull(7) || r.GetString(7).Length == 0 ? [] : r.GetString(7).Split(','),
        r.IsDBNull(8) ? null : r.GetString(8), r.IsDBNull(9) ? null : r.GetInt64(9), (OutboxSendState)r.GetInt64(10));

    public static OutboxEntry? FindEntry(SqliteConnection c, SqliteTransaction? tx, string instance, string table, string pk)
    {
        using SqliteCommand cmd = c.Cmd($"SELECT {EntryColumns} FROM _sync_outbox WHERE instance = @i AND tbl = @t AND pk = @p", tx, ("@i", instance), ("@t", table), ("@p", pk));
        using SqliteDataReader r = cmd.ExecuteReader();
        return r.Read() ? ReadEntry(r) : null;
    }

    public static List<OutboxEntry> Entries(SqliteConnection c, SqliteTransaction? tx, string? instance = null, bool pendingOnly = false) =>
        c.Query($"SELECT {EntryColumns} FROM _sync_outbox WHERE (@i IS NULL OR instance = @i){(pendingOnly ? " AND sent IN (0, 1)" : "")} ORDER BY id", tx, ReadEntry, ("@i", instance));

    public List<OutboxEntry> Entries(string? instance = null)
    {
        using SqliteConnection c = Open();
        return Entries(c, null, instance);
    }

    internal static void Insert(SqliteConnection c, SqliteTransaction? tx, string instance, string table, string? pk, OutboxKind kind, OutboxClass cls,
        IEnumerable<string>? columns = null, string? predicate = null, long? expectedVersion = null) =>
        c.Exec("""
            INSERT INTO _sync_outbox(instance, tbl, pk, kind, cls, columns, predicate, expected_version, sent)
            VALUES (@i, @t, @p, @k, @c, @cols, @pred, @ev, 0)
            """, tx, ("@i", instance), ("@t", table), ("@p", pk), ("@k", (int)kind), ("@c", (int)cls),
            ("@cols", columns is null ? "" : string.Join(',', columns)), ("@pred", predicate), ("@ev", expectedVersion));

    internal static void Remove(SqliteConnection c, SqliteTransaction? tx, long id) =>
        c.Exec("DELETE FROM _sync_outbox WHERE id = @id", tx, ("@id", id));

    /// <summary>
    /// Puts an action into the outbox with collapsing (8.2): create+patch = create, create+delete = nothing,
    /// patch+patch = patch with merged columns, patch+delete = delete. An action already on the way (sent = 1)
    /// is replaced by a new row without seq, so the reply to the old send does not drop the new edit (8.3).
    /// </summary>
    /// <returns>A short description for the log.</returns>
    public static string Put(SqliteConnection c, SqliteTransaction? tx, string instance, string table, string pk, OutboxKind kind,
        IEnumerable<string>? columns, OutboxClass cls)
    {
        List<string> cols = columns?.ToList() ?? [];
        OutboxEntry? e = FindEntry(c, tx, instance, table, pk);
        if (e is null)
        {
            Insert(c, tx, instance, table, pk, kind, cls, cols);
            return $"{kind}";
        }
        if (e.Sent is OutboxSendState.DeleteConfirmed || e.Kind is OutboxKind.Delete)
        {
            // The row is gone locally.
            return "вже видалено";
        }

        var newCls = (OutboxClass)Math.Min((int)e.Class, (int)cls);
        OutboxKind nk;
        List<string> nc = [];
        switch (e.Kind)
        {
            case OutboxKind.Create when kind == OutboxKind.Patch:
                nk = OutboxKind.Create;
                break;
            case OutboxKind.Create when kind == OutboxKind.Delete:
                if (e.Sent == OutboxSendState.Waiting)
                {
                    Remove(c, tx, e.Id);
                    return "створення і видалення схлопнулись: власник нічого не дізнається";
                }
                nk = OutboxKind.Delete;
                break;
            case OutboxKind.Patch when kind == OutboxKind.Patch:
                nk = OutboxKind.Patch;
                nc = [.. e.Columns.Union(cols)];
                break;
            case OutboxKind.Patch when kind == OutboxKind.Delete:
                nk = OutboxKind.Delete;
                break;
            default:
                return "без змін";
        }
        string what = nc.Count > 0 ? $"{nk} {string.Join(", ", nc)}" : $"{nk}";
        if (e.Sent == OutboxSendState.InFlight)
        {
            Remove(c, tx, e.Id);
            Insert(c, tx, instance, table, pk, nk, newCls, nc);
            return $"дія #{e.Seq} уже в дорозі: рядок черги замінено новим ({what})";
        }
        c.Exec("UPDATE _sync_outbox SET kind = @k, columns = @cols, cls = @c WHERE id = @id", tx,
            ("@k", (int)nk), ("@cols", string.Join(',', nc)), ("@c", (int)newCls), ("@id", e.Id));
        return what;
    }

    /// <summary>A local cascade removed child rows: their pending creates and patches have nothing left to send.</summary>
    internal static void DropOrphanEntries(SqliteConnection c, SqliteTransaction? tx, SyncModel model, string instance)
    {
        foreach (SyncTable t in model.Tables)
        {
            c.Exec($"""
                DELETE FROM _sync_outbox WHERE instance = @i AND tbl = @t AND kind IN (1, 2) AND sent = 0
                AND NOT EXISTS (SELECT 1 FROM {Q(t.Name)} x WHERE x.Id = _sync_outbox.pk AND x.InstanceId = @i)
                """, tx, ("@i", instance), ("@t", t.Name));
        }
    }

    // Notes.

    public static void Note(SqliteConnection c, SqliteTransaction? tx, string instance, string text, bool info = false) =>
        c.Exec("INSERT INTO _sync_notes(instance, at, text, info) VALUES (@i, @a, @t, @f)", tx,
            ("@i", instance), ("@a", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()), ("@t", text), ("@f", info ? 1 : 0));

    public List<ClientNote> Notes(string? instance = null, int limit = 50)
    {
        using SqliteConnection c = Open();
        return c.Query("SELECT id, instance, at, text, info FROM _sync_notes WHERE @i IS NULL OR instance = @i ORDER BY id DESC LIMIT @n", null,
            r => new ClientNote(r.GetInt64(0), r.GetString(1), DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(2)), r.GetString(3), r.GetInt64(4) != 0), ("@i", instance), ("@n", limit));
    }

    // Rows.

    public static string Label(SqliteConnection c, SqliteTransaction? tx, SyncTable t, string pk)
    {
        string? col = LabelColumns.FirstOrDefault(t.HasColumn);
        if (col is not null && c.Scalar<string>($"SELECT {Q(col)} FROM {Q(t.Name)} WHERE Id = @id", tx, ("@id", pk)) is { } name)
            return $"«{name}»";
        return Short(pk);
    }

    /// <summary>Ids of the live rows of the instance that point to the row through the FK.</summary>
    internal static List<string> ChildIds(SqliteConnection c, SqliteTransaction? tx, string instance, SyncTable child, SyncForeignKey fk, string pk) =>
        c.Query($"SELECT Id FROM {Q(child.Name)} WHERE {Q(fk.Column)} = @p AND InstanceId = @i", tx, r => r.GetString(0), ("@p", pk), ("@i", instance));
}
