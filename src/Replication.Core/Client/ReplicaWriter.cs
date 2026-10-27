using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Client;

/// <summary>
/// Applies what the owner sends to the rows of one instance (7). Writes carry <c>InstanceId = X</c>,
/// so the client's own rows (<c>InstanceId</c> NULL) and archives are never touched (exceptions: the NeedFull probe in <c>ApplyRow</c>
/// reads by Id only, and <c>InstallSnapshot</c> deletes rows of other instances by Id). Idempotent and order-independent: an older version never
/// overwrites a newer one, and the outbox protects local edits.
/// </summary>
/// <param name="log">Logger of the agent; lines carry its <paramref name="agent"/> name.</param>
internal sealed class ReplicaWriter(SyncModel model, string instance, ILogger log, string agent)
{
    public string Instance { get; } = instance;

    /// <summary>Rows to request in full: partial rows that arrived for a row the replica does not have (7), reverted local edits (<see cref="Revert"/>) and archive leftovers.</summary>
    public List<RowRef> NeedFull { get; } = [];

    public HashSet<string> TouchedTables { get; } = [];

    public int ApplyBatch(SqliteConnection c, SqliteTransaction tx, Batch b)
    {
        if (!model.TryGet(b.Tbl, out SyncTable? t))
            return 0;
        int n = 0;
        foreach (Row r in b.Rows)
            n += ApplyRow(c, tx, t, b.Columns, r);
        foreach (Tombstone tb in b.Tombstones)
            n += ApplyTombstone(c, tx, t, PkText(tb.Pk), tb.Version);
        if (n > 0)
            TouchedTables.Add(t.Name);
        return n;
    }

    private int ApplyRow(SqliteConnection c, SqliteTransaction tx, SyncTable t, IList<string> columns, Row r)
    {
        string pk = PkText(r.Pk);
        OutboxEntry? e = ClientStore.FindEntry(c, tx, Instance, t.Name, pk);
        // Create in the outbox: the local state is newer; delete in the outbox (also a confirmed one, sent = 2): never re-insert (9.1).
        if (e is { Kind: OutboxKind.Create or OutboxKind.Delete })
            return 0;
        HashSet<string> protectedCols = e is { Kind: OutboxKind.Patch } ? e.Columns.ToHashSet() : [];

        if (r.Full && ParentDeleted(c, tx, t, columns, r))
            return 0;
        var cols = new List<(string Name, object Value)>();
        if (r.Full)
        {
            for (int i = 0; i < columns.Count && i < r.Values.Count; i++)
            {
                if (t.HasColumn(columns[i]))
                    cols.Add((columns[i], FromValue(r.Values[i])));
            }
        }
        else
        {
            int vi = 0;
            for (int i = 0; i < columns.Count; i++)
            {
                if ((r.Mask & (1L << i)) != 0)
                {
                    if (t.HasColumn(columns[i]))
                        cols.Add((columns[i], FromValue(r.Values[vi])));
                    vi++;
                }
            }
        }

        string table = Q(t.Name);
        if (r.Full)
        {
            string names = string.Concat(cols.Select(x => ", " + Q(x.Name)));
            string pars = string.Concat(cols.Select((_, i) => $", @v{i}"));
            string set = string.Concat(cols.Where(x => !protectedCols.Contains(x.Name)).Select(x => $"{Q(x.Name)} = excluded.{Q(x.Name)}, "));
            using SqliteCommand cmd = c.Cmd($"""
                INSERT INTO {table} (Id, InstanceId, SyncVersion{names}) VALUES (@id, @inst, @ver{pars})
                ON CONFLICT(Id) DO UPDATE SET {set}SyncVersion = excluded.SyncVersion
                WHERE excluded.SyncVersion > {table}.SyncVersion AND {table}.InstanceId = excluded.InstanceId
                """, tx, ("@id", pk), ("@inst", Instance), ("@ver", r.Version));
            for (int i = 0; i < cols.Count; i++)
                cmd.Parameters.AddWithValue($"@v{i}", cols[i].Value);
            return cmd.ExecuteNonQuery();
        }
        else
        {
            List<(string Name, object Value)> upd = cols.Where(x => !protectedCols.Contains(x.Name)).ToList();
            string set = string.Concat(upd.Select((x, i) => $"{Q(x.Name)} = @v{i}, "));
            using SqliteCommand cmd = c.Cmd($"UPDATE {table} SET {set}SyncVersion = @ver WHERE Id = @id AND InstanceId = @inst AND SyncVersion < @ver",
                tx, ("@id", pk), ("@inst", Instance), ("@ver", r.Version));
            for (int i = 0; i < upd.Count; i++)
                cmd.Parameters.AddWithValue($"@v{i}", upd[i].Value);
            int n = cmd.ExecuteNonQuery();
            if (n == 0 && c.Scalar<string>($"SELECT InstanceId FROM {table} WHERE Id = @id", tx, ("@id", pk)) is null)
            {
                // The owner believed we have the row: ask for it in full, never invent it.
                // Also taken when a row with this Id exists with InstanceId NULL (own row): the probe is null in both cases.
                NeedFull.Add(Ref(t.Name, pk));
            }

            return n;
        }
    }

    /// <summary>A tombstone wins over a patch or a create in the outbox: the row and the action go, with a note (7, 9.1).</summary>
    public int ApplyTombstone(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, long version)
    {
        OutboxEntry? e = ClientStore.FindEntry(c, tx, Instance, t.Name, pk);
        string label = ClientStore.Label(c, tx, t, pk);
        int n = 0;
        if (c.Scalar<long>($"SELECT COUNT(*) FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @inst AND SyncVersion < @v", tx, ("@id", pk), ("@inst", Instance), ("@v", version)) > 0)
        {
            // A parent that archived rows point to goes to the archive instead (11.6).
            if (ArchiveGuard.DeleteOrArchive(c, tx, model, Instance, t, pk))
                log.LogInformation("[{Agent}] {Label} was deleted on the instance; archived rows point to it, so it was moved to the archive", agent, label);
            n = 1;
        }
        if (e is { Kind: OutboxKind.Patch or OutboxKind.Create })
        {
            ClientStore.Remove(c, tx, e.Id);
            var gone = new List<string>();
            // Creates that depend on it (8.4); the row itself is not its own child.
            RemoveChildren(c, tx, t, pk, gone, [(t.Name, pk)]);
            if (gone.Count > 0)
                log.LogWarning("[{Agent}] the edit of {Label} is lost: it was deleted on the instance; dependent rows not saved: {Gone}", agent, label, string.Join(", ", gone));
            else
                log.LogWarning("[{Agent}] the edit of {Label} is lost: it was deleted on the instance", agent, label);
        }
        return n;
    }

    /// <summary>
    /// A child row whose parent has a delete in the outbox is not inserted: a late batch would otherwise bring back
    /// an orphan before the cascaded tombstone arrives (7).
    /// </summary>
    private bool ParentDeleted(SqliteConnection c, SqliteTransaction tx, SyncTable t, IList<string> columns, Row r)
    {
        foreach (SyncForeignKey fk in t.ForeignKeys)
        {
            int i = columns.IndexOf(fk.Column);
            if (i < 0 || i >= r.Values.Count || FromValue(r.Values[i]) is not string pid)
                continue;
            if (ClientStore.FindEntry(c, tx, Instance, fk.ParentTable, pid) is { Kind: OutboxKind.Delete })
                return true;
        }
        return false;
    }

    /// <summary>
    /// Deletes a row of this instance with its children and their outbox entries, adding their labels to <paramref name="gone"/>.
    /// A row that archived rows point to goes to the archive instead (11.6).
    /// </summary>
    public void RemoveWithChildren(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, List<string> gone) =>
        RemoveWithChildren(c, tx, t, pk, gone, []);

    private void RemoveWithChildren(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, List<string> gone, HashSet<(string Table, string Pk)> seen)
    {
        // An FK cycle leads back to a row that is already being removed further up.
        if (!seen.Add((t.Name, pk)))
            return;
        if (c.Scalar<string>($"SELECT InstanceId FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @inst", tx, ("@id", pk), ("@inst", Instance)) is null)
            return;
        gone.Add(ClientStore.Label(c, tx, t, pk));
        RemoveChildren(c, tx, t, pk, gone, seen);
        ArchiveGuard.DeleteOrArchive(c, tx, model, Instance, t, pk);
        if (ClientStore.FindEntry(c, tx, Instance, t.Name, pk) is { } e)
            ClientStore.Remove(c, tx, e.Id);
        TouchedTables.Add(t.Name);
    }

    private void RemoveChildren(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, List<string> gone, HashSet<(string Table, string Pk)> seen)
    {
        foreach ((SyncTable child, SyncForeignKey fk) in model.ChildrenOf(t.Name))
        {
            foreach (string id in ClientStore.ChildIds(c, tx, Instance, child, fk, pk))
                RemoveWithChildren(c, tx, child, id, gone, seen);
        }
    }

    /// <summary>
    /// Drops a local edit the owner rejected while keeping its own row: the row loses its version and asks for the
    /// owner's full row, which then overwrites it (7, 9).
    /// </summary>
    public void Revert(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk)
    {
        c.Exec($"UPDATE {Q(t.Name)} SET SyncVersion = 0 WHERE Id = @id AND InstanceId = @inst", tx, ("@id", pk), ("@inst", Instance));
        NeedFull.Add(Ref(t.Name, pk));
    }

    public void SetVersion(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, long version) =>
        c.Exec($"UPDATE {Q(t.Name)} SET SyncVersion = @v WHERE Id = @id AND InstanceId = @inst AND SyncVersion < @v",
            tx, ("@v", version), ("@id", pk), ("@inst", Instance));

    /// <summary>
    /// Merges a downloaded snapshot into the single client database (6.3, 10.3): rows of the instance are replaced in one
    /// transaction. The snapshot is the owner's whole database, so the outbox of the instance is dropped with the old rows:
    /// pending actions were made against a state the snapshot replaces. Rows of another instance with the same Ids (a restored
    /// or cloned owner) are replaced too; its other rows, cursors and outbox stay, the app decides what to do with them.
    /// </summary>
    /// <returns>The instance id of the snapshot and the number of outbox actions dropped.</returns>
    public static (string Instance, int Dropped) InstallSnapshot(ClientStore store, string path)
    {
        using SqliteConnection c = store.Open(foreignKeys: false);
        c.Exec("ATTACH DATABASE @p AS snap", null, ("@p", path));
        try
        {
            using SqliteTransaction tx = c.BeginTransaction();
            string newInst = c.Scalar<string>("SELECT instance_id FROM snap._sync_meta", tx)!;
            long v = c.Scalar<long>("SELECT version FROM snap._sync_meta", tx);
            int dropped = (int)c.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @new AND sent < 2", tx, ("@new", newInst));
            c.Exec("DELETE FROM _sync_outbox WHERE instance = @new", tx, ("@new", newInst));

            foreach (SyncTable t in store.Model.Tables)
            {
                string table = Q(t.Name);
                string cols = string.Concat(t.Columns.Select(x => ", " + Q(x)));
                c.Exec($"DELETE FROM main.{table} WHERE InstanceId = @new", tx, ("@new", newInst));
                // The snapshot wins over another instance's replica: a restored or cloned owner has the same Ids, and Id is the only key.
                c.Exec($"DELETE FROM main.{table} WHERE InstanceId IS NOT NULL AND InstanceId <> @new AND InstanceId NOT LIKE '%:archive' AND Id IN (SELECT Id FROM snap.{table})", tx, ("@new", newInst));
                // INSERT OR IGNORE: an archived row with the same Id keeps its place in the archive.
                // Likewise a snapshot row whose Id collides with an own row (InstanceId NULL) is dropped: Id is the only key and own data wins.
                c.Exec($"INSERT OR IGNORE INTO main.{table} (Id, InstanceId, SyncVersion{cols}) SELECT Id, @new, SyncVersion{cols} FROM snap.{table}", tx, ("@new", newInst));
            }

            c.Exec("DELETE FROM _sync_ranges WHERE instance = @new", tx, ("@new", newInst));
            foreach (SyncTable t in store.Model.Tables)
                ClientStore.SaveCursor(c, tx, newInst, t.Name, new CursorState(v));
            ClientStore.SaveInstance(c, tx, newInst, v);
            tx.Commit();
            return (newInst, dropped);
        }
        finally
        {
            c.Exec("DETACH DATABASE snap");
        }
    }
}
