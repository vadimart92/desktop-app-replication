using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;
using Replication.Model;
using static Replication.Model.Wire;

namespace Replication.Owner;

/// <summary>
/// The owner database: service tables, triggers, version counter, floor and tombstone cleanup (5.2, 5.3, 10).
/// Every write path of the application (SaveChanges, ExecuteUpdate/Delete, raw SQL) is caught by triggers.
/// </summary>
public sealed class OwnerStore
{
    public const long NoFloor = long.MaxValue;

    private readonly ConcurrentDictionary<string, int> _subscribed = new();
    private string _instanceId = "";

    public OwnerStore(string dbPath, SyncModel model, OwnerOptions? options = null)
    {
        DbPath = Path.GetFullPath(dbPath);
        Model = model;
        Options = options ?? new OwnerOptions();
        ConnectionString = new SqliteConnectionStringBuilder { DataSource = DbPath, Pooling = true, DefaultTimeout = 30 }.ToString();
    }

    public string DbPath { get; }
    public SyncModel Model { get; }
    public OwnerOptions Options { get; }
    public string ConnectionString { get; }
    public string InstanceId => _instanceId;

    /// <summary>Raised after the sync service committed a write (Apply, purge), so streams wake without waiting for the poll.</summary>
    public event Action? Committed;

    internal void NotifyCommitted() => Committed?.Invoke();

    public SqliteConnection Open()
    {
        var c = new SqliteConnection(ConnectionString);
        c.Open();
        c.Exec("PRAGMA foreign_keys = ON;");
        return c;
    }

    // Install.

    /// <summary>
    /// Creates the service tables, (re)creates the triggers (a table rebuild in SQLite drops them, 5.2),
    /// stamps rows that have no version yet and checks the version file for a rolled-back database (5.3).
    /// Call after EF migrations / EnsureCreated.
    /// </summary>
    public void Install()
    {
        using var c = new SqliteConnection(ConnectionString);
        c.Open();
        c.Exec("PRAGMA journal_mode = WAL;");
        if (c.Scalar<long>("PRAGMA auto_vacuum;") != 2)
        {
            // One-off, needs a full VACUUM (11.5).
            c.Exec("PRAGMA auto_vacuum = INCREMENTAL;");
            c.Exec("VACUUM;");
        }
        c.Exec("PRAGMA foreign_keys = ON;");

        using (SqliteTransaction tx = c.BeginTransaction())
        {
            c.Exec("""
                CREATE TABLE IF NOT EXISTS _sync_meta (
                  instance_id    TEXT    NOT NULL,
                  version        INTEGER NOT NULL,
                  purged_version INTEGER NOT NULL DEFAULT 0,
                  clock_offset   INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS _sync_tombstones (
                  tbl        TEXT    NOT NULL,
                  pk         TEXT    NOT NULL,
                  version    INTEGER NOT NULL,
                  deleted_at INTEGER NOT NULL,
                  origin     TEXT,
                  PRIMARY KEY (tbl, pk)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS IX_sync_tombstones_version ON _sync_tombstones(tbl, version);
                CREATE INDEX IF NOT EXISTS IX_sync_tombstones_deleted ON _sync_tombstones(deleted_at);
                CREATE TABLE IF NOT EXISTS _sync_clients (
                  client_id     TEXT    PRIMARY KEY,
                  acked_version INTEGER NOT NULL,
                  applied_seq   INTEGER NOT NULL,
                  last_seen     INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS _sync_client_cursors (
                  client_id TEXT    NOT NULL,
                  tbl       TEXT    NOT NULL,
                  cursor    INTEGER NOT NULL,
                  PRIMARY KEY (client_id, tbl)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS _sync_floor (
                  tbl   TEXT    PRIMARY KEY,
                  floor INTEGER NOT NULL
                ) WITHOUT ROWID;
                """, tx);
            if (c.Scalar<long>("SELECT COUNT(*) FROM _sync_meta", tx) == 0)
                c.Exec("INSERT INTO _sync_meta(instance_id, version) VALUES (@id, 0)", tx, ("@id", NewInstanceId()));
            foreach (SyncTable t in Model.Tables)
                c.Exec("INSERT OR IGNORE INTO _sync_floor(tbl, floor) VALUES (@t, @f)", tx, ("@t", t.Name), ("@f", NoFloor));

            foreach (SyncTable t in Model.Tables)
            {
                foreach (string sql in TriggerSql.For(t))
                    c.Exec(sql, tx);
            }

            StampUnversioned(c, tx);
            tx.Commit();
        }

        CheckVersionFile(c);
        _instanceId = c.Scalar<string>("SELECT instance_id FROM _sync_meta")!;
        WriteVersionFile(c);
    }

    /// <summary>The first start after enabling sync gives every existing row a unique version (15.2).</summary>
    private void StampUnversioned(SqliteConnection c, SqliteTransaction tx)
    {
        foreach (SyncTable t in Model.Tables)
        {
            var ids = new List<string>();
            using (SqliteCommand cmd = c.Cmd($"SELECT Id FROM {Q(t.Name)} WHERE {SyncColumns.Version} = 0", tx))
            using (SqliteDataReader r = cmd.ExecuteReader())
            {
                while (r.Read())
                    ids.Add(r.GetString(0));
            }

            foreach (string id in ids)
            {
                c.Exec("UPDATE _sync_meta SET version = version + 1", tx);
                c.Exec($"UPDATE {Q(t.Name)} SET SyncVersion = (SELECT version FROM _sync_meta), SyncBase = 0, SyncMask = @m WHERE Id = @id",
                    tx, ("@m", t.FullMask), ("@id", id));
            }
        }
    }

    private static string NewInstanceId() => "inst-" + Guid.NewGuid().ToString("N")[..12];

    // Version file outside the database (5.3, "version went back").

    private string VersionFilePath => DbPath + ".syncstate";

    private void CheckVersionFile(SqliteConnection c)
    {
        if (!File.Exists(VersionFilePath))
            return;
        string[] parts = File.ReadAllText(VersionFilePath).Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2 || !long.TryParse(parts[1], out long fileVersion))
            return;
        (string id, long version) = (c.Scalar<string>("SELECT instance_id FROM _sync_meta")!, c.Scalar<long>("SELECT version FROM _sync_meta"));
        if (parts[0] == id && version < fileVersion)
        {
            // The database was restored or replaced: clients must take a new snapshot.
            c.Exec("UPDATE _sync_meta SET instance_id = @id", null, ("@id", NewInstanceId()));
            c.Exec("DELETE FROM _sync_client_cursors; DELETE FROM _sync_clients;");
        }
    }

    public void WriteVersionFile()
    {
        using SqliteConnection c = Open();
        WriteVersionFile(c);
    }

    private void WriteVersionFile(SqliteConnection c)
    {
        (string id, long version) = (c.Scalar<string>("SELECT instance_id FROM _sync_meta")!, c.Scalar<long>("SELECT version FROM _sync_meta"));
        string tmp = VersionFilePath + ".tmp";
        File.WriteAllText(tmp, $"{id} {version}");
        File.Move(tmp, VersionFilePath, overwrite: true);
    }

    // Reading.

    public long Head(SqliteConnection c, SqliteTransaction? tx = null) => c.Scalar<long>("SELECT version FROM _sync_meta", tx);

    public long Purged(SqliteConnection c, SqliteTransaction? tx = null) => c.Scalar<long>("SELECT purged_version FROM _sync_meta", tx);

    /// <summary>Owner time in unix seconds, including the demo clock offset.</summary>
    public long Now(SqliteConnection c, SqliteTransaction? tx = null) =>
        DateTimeOffset.UtcNow.ToUnixTimeSeconds() + c.Scalar<long>("SELECT clock_offset FROM _sync_meta", tx);

    /// <summary>
    /// Moves the owner clock forward (the demo's "+31 days"). Triggers use the same offset for <c>deleted_at</c>.
    /// Only clients with an open stream are seen at the new time; the others age by the jump (5.2, 10.2).
    /// </summary>
    public void AdvanceClock(TimeSpan by)
    {
        using SqliteConnection c = Open();
        using SqliteTransaction tx = c.BeginTransaction();
        c.Exec("UPDATE _sync_meta SET clock_offset = clock_offset + @s", tx, ("@s", (long)by.TotalSeconds));
        TouchSubscribed(c, tx);
        RecomputeFloor(c, tx);
        tx.Commit();
    }

    public long FileSizeBytes(SqliteConnection c) => c.Scalar<long>("PRAGMA page_count") * c.Scalar<long>("PRAGMA page_size");

    // Clients.

    internal void MarkSubscribed(string clientId, bool on)
    {
        if (on)
            _subscribed.AddOrUpdate(clientId, 1, (_, n) => n + 1);
        else
            _subscribed.AddOrUpdate(clientId, 0, (_, n) => Math.Max(0, n - 1));
    }

    public bool IsSubscribed(string clientId) => _subscribed.TryGetValue(clientId, out int n) && n > 0;

    internal void SaveCursors(SqliteConnection c, SqliteTransaction tx, string clientId, IReadOnlyDictionary<string, long> cursors)
    {
        long now = Now(c, tx);
        long min = cursors.Count > 0 ? cursors.Values.Min() : 0;
        c.Exec("""
            INSERT INTO _sync_clients(client_id, acked_version, applied_seq, last_seen) VALUES (@c, @a, 0, @n)
            ON CONFLICT(client_id) DO UPDATE SET acked_version = @a, last_seen = @n
            """, tx, ("@c", clientId), ("@a", min), ("@n", now));
        foreach ((string tbl, long cur) in cursors)
        {
            c.Exec("INSERT INTO _sync_client_cursors(client_id, tbl, cursor) VALUES (@c, @t, @v) ON CONFLICT(client_id, tbl) DO UPDATE SET cursor = @v",
                tx, ("@c", clientId), ("@t", tbl), ("@v", cur));
        }

        RecomputeFloor(c, tx);
    }

    /// <summary>A client with an open Subscribe stream is seen now (5.2).</summary>
    private void TouchSubscribed(SqliteConnection c, SqliteTransaction tx)
    {
        long now = Now(c, tx);
        foreach ((string id, int n) in _subscribed)
        {
            if (n > 0)
                c.Exec("UPDATE _sync_clients SET last_seen = @n WHERE client_id = @c", tx, ("@n", now), ("@c", id));
        }
    }

    /// <summary>floor = MIN(cursor) of active clients per table; with no active client the base moves on every update (5.2).</summary>
    internal void RecomputeFloor(SqliteConnection c, SqliteTransaction tx)
    {
        long cutoff = Now(c, tx) - (long)Options.ActivityWindow.TotalSeconds;
        foreach (SyncTable t in Model.Tables)
        {
            long? min = c.Scalar<long?>("""
                SELECT MIN(cc.cursor) FROM _sync_client_cursors cc JOIN _sync_clients cl ON cl.client_id = cc.client_id
                WHERE cc.tbl = @t AND cl.last_seen >= @cut
                """, tx, ("@t", t.Name), ("@cut", cutoff));
            c.Exec("INSERT INTO _sync_floor(tbl, floor) VALUES (@t, @f) ON CONFLICT(tbl) DO UPDATE SET floor = @f",
                tx, ("@t", t.Name), ("@f", min ?? NoFloor));
        }
    }

    public void RecomputeFloor()
    {
        using SqliteConnection c = Open();
        using SqliteTransaction tx = c.BeginTransaction();
        TouchSubscribed(c, tx);
        RecomputeFloor(c, tx);
        tx.Commit();
    }

    // Tombstone cleanup (10.2).

    public sealed record PurgeResult(int Tombstones, int Clients, long PurgedVersion, long Floor);

    public PurgeResult Purge()
    {
        using SqliteConnection c = Open();
        long now = Now(c);
        long cutoff = now - (long)Options.Retention.TotalSeconds;
        int clients;
        using (SqliteTransaction tx = c.BeginTransaction())
        {
            TouchSubscribed(c, tx);
            c.Exec("DELETE FROM _sync_client_cursors WHERE client_id IN (SELECT client_id FROM _sync_clients WHERE last_seen < @cut)", tx, ("@cut", cutoff));
            clients = c.Exec("DELETE FROM _sync_clients WHERE last_seen < @cut", tx, ("@cut", cutoff));
            RecomputeFloor(c, tx);
            tx.Commit();
        }

        long floor;
        int total = 0;
        while (true)
        {
            // Short transactions of ~1000 rows so the application's writer is not held. Both bounds are read inside the
            // batch's write transaction, which serializes with the handshake (6.2): a client registered since the
            // previous batch already counts.
            using SqliteTransaction tx = c.BeginTransaction();
            floor = c.Scalar<long?>("SELECT MIN(acked_version) FROM _sync_clients", tx) ?? NoFloor;
            long guard = SubscribedFloor(c, tx);
            var batch = new List<(string Tbl, string Pk, long V)>();
            using (SqliteCommand cmd = c.Cmd("""
                SELECT tbl, pk, version FROM _sync_tombstones
                WHERE version <= @f OR (deleted_at < @cut AND version <= @g) LIMIT 1000
                """, tx, ("@f", floor), ("@cut", cutoff), ("@g", guard)))
            using (SqliteDataReader r = cmd.ExecuteReader())
            {
                while (r.Read())
                    batch.Add((r.GetString(0), r.GetString(1), r.GetInt64(2)));
            }

            if (batch.Count == 0)
                break;
            foreach ((string tbl, string pk, long _) in batch)
                c.Exec("DELETE FROM _sync_tombstones WHERE tbl = @t AND pk = @p", tx, ("@t", tbl), ("@p", pk));
            c.Exec("UPDATE _sync_meta SET purged_version = MAX(purged_version, @v)", tx, ("@v", batch.Max(x => x.V)));
            tx.Commit();
            total += batch.Count;
        }
        if (total > 0)
            NotifyCommitted();
        return new PurgeResult(total, clients, Purged(c), floor);
    }

    /// <summary>
    /// MIN(acked_version) of the clients with an open stream. Such a client may still be catching up on tombstones above
    /// it (its bottom gap goes last), so the age rule leaves them alone until it acknowledges them (10.2, rule 2).
    /// </summary>
    private long SubscribedFloor(SqliteConnection c, SqliteTransaction tx)
    {
        long min = NoFloor;
        foreach ((string id, int n) in _subscribed)
        {
            if (n > 0 && c.Scalar<long?>("SELECT acked_version FROM _sync_clients WHERE client_id = @c", tx, ("@c", id)) is long acked)
                min = Math.Min(min, acked);
        }

        return min;
    }

    /// <summary>Returns free pages to the file system in small steps (11.5).</summary>
    public long IncrementalVacuum(int pages)
    {
        using SqliteConnection c = Open();
        c.Exec($"PRAGMA incremental_vacuum({pages});");
        return c.Scalar<long>("PRAGMA freelist_count");
    }
}
