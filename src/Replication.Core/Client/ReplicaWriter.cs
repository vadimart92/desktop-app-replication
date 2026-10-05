using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Client;

/// <summary>
/// Applies what the owner sends to the rows of one instance (7). Every statement carries <c>InstanceId = X</c>,
/// so <c>local</c> rows and archives are never touched. Idempotent and order-independent: an older version never
/// overwrites a newer one, and the outbox protects local edits.
/// </summary>
internal sealed class ReplicaWriter(SyncModel model, string instance)
{
    public string Instance { get; } = instance;

    /// <summary>Partial rows that arrived for a row the replica does not have (7).</summary>
    public List<RowRef> NeedFull { get; } = [];

    public List<string> Notes { get; } = [];

    public HashSet<string> TouchedTables { get; } = [];

    public int ApplyBatch(SqliteConnection c, SqliteTransaction tx, Batch b)
    {
        if (!model.TryGet(b.Tbl, out var t)) return 0;
        var n = 0;
        foreach (var r in b.Rows) n += ApplyRow(c, tx, t, b.Columns, r);
        foreach (var tb in b.Tombstones) n += ApplyTombstone(c, tx, t, PkText(tb.Pk), tb.Version);
        if (n > 0) TouchedTables.Add(t.Name);
        return n;
    }

    private int ApplyRow(SqliteConnection c, SqliteTransaction tx, SyncTable t, IList<string> columns, Row r)
    {
        var pk = PkText(r.Pk);
        var e = ClientStore.FindEntry(c, tx, Instance, t.Name, pk);
        // create in the outbox: the local state is newer; delete in the outbox (also a confirmed one, sent = 2): never re-insert (9.1)
        if (e is { Kind: OutboxKind.Create or OutboxKind.Delete }) return 0;
        var protectedCols = e is { Kind: OutboxKind.Patch } ? e.Columns.ToHashSet() : [];

        var cols = new List<(string Name, object Value)>();
        if (r.Full)
        {
            for (var i = 0; i < columns.Count && i < r.Values.Count; i++)
                if (t.HasColumn(columns[i])) cols.Add((columns[i], FromValue(r.Values[i])));
        }
        else
        {
            var vi = 0;
            for (var i = 0; i < columns.Count; i++)
                if ((r.Mask & (1L << i)) != 0)
                {
                    if (t.HasColumn(columns[i])) cols.Add((columns[i], FromValue(r.Values[vi])));
                    vi++;
                }
        }

        var T = Q(t.Name);
        if (r.Full)
        {
            var names = string.Concat(cols.Select(x => ", " + Q(x.Name)));
            var pars = string.Concat(cols.Select((_, i) => $", @v{i}"));
            var set = string.Concat(cols.Where(x => !protectedCols.Contains(x.Name)).Select(x => $"{Q(x.Name)} = excluded.{Q(x.Name)}, "));
            using var cmd = c.Cmd($"""
                INSERT INTO {T} (Id, InstanceId, SyncVersion{names}) VALUES (@id, @inst, @ver{pars})
                ON CONFLICT(Id) DO UPDATE SET {set}SyncVersion = excluded.SyncVersion
                WHERE excluded.SyncVersion > {T}.SyncVersion AND {T}.InstanceId = excluded.InstanceId
                """, tx, ("@id", pk), ("@inst", Instance), ("@ver", r.Version));
            for (var i = 0; i < cols.Count; i++) cmd.Parameters.AddWithValue($"@v{i}", cols[i].Value);
            return cmd.ExecuteNonQuery();
        }
        else
        {
            var upd = cols.Where(x => !protectedCols.Contains(x.Name)).ToList();
            var set = string.Concat(upd.Select((x, i) => $"{Q(x.Name)} = @v{i}, "));
            using var cmd = c.Cmd($"UPDATE {T} SET {set}SyncVersion = @ver WHERE Id = @id AND InstanceId = @inst AND SyncVersion < @ver",
                tx, ("@id", pk), ("@inst", Instance), ("@ver", r.Version));
            for (var i = 0; i < upd.Count; i++) cmd.Parameters.AddWithValue($"@v{i}", upd[i].Value);
            var n = cmd.ExecuteNonQuery();
            if (n == 0 && c.Scalar<string>($"SELECT InstanceId FROM {T} WHERE Id = @id", tx, ("@id", pk)) is null)
                NeedFull.Add(Ref(t.Name, pk)); // the owner believed we have the row: ask for it in full, never invent it
            return n;
        }
    }

    /// <summary>A tombstone wins over a patch or a create in the outbox: the row and the action go, with a note (7, 9.1).</summary>
    public int ApplyTombstone(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, long version)
    {
        var e = ClientStore.FindEntry(c, tx, Instance, t.Name, pk);
        var label = ClientStore.Label(c, tx, t, pk);
        var n = c.Exec($"DELETE FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @inst AND SyncVersion < @v", tx, ("@id", pk), ("@inst", Instance), ("@v", version));
        if (e is { Kind: OutboxKind.Patch or OutboxKind.Create })
        {
            ClientStore.Remove(c, tx, e.Id);
            var gone = new List<string>();
            RemoveChildren(c, tx, t, pk, gone); // creates that depend on it (8.4)
            Note(c, tx, $"правку запису {label} втрачено: його видалено на інстансі{(gone.Count > 0 ? $"; не збережено залежні: {string.Join(", ", gone)}" : "")}");
        }
        return n;
    }

    /// <summary>Deletes a row of this instance with its children and their outbox entries; returns their labels.</summary>
    public void RemoveWithChildren(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, List<string> gone)
    {
        if (c.Scalar<string>($"SELECT InstanceId FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @inst", tx, ("@id", pk), ("@inst", Instance)) is null) return;
        gone.Add(ClientStore.Label(c, tx, t, pk));
        RemoveChildren(c, tx, t, pk, gone);
        c.Exec($"DELETE FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @inst", tx, ("@id", pk), ("@inst", Instance));
        if (ClientStore.FindEntry(c, tx, Instance, t.Name, pk) is { } e) ClientStore.Remove(c, tx, e.Id);
        TouchedTables.Add(t.Name);
    }

    private void RemoveChildren(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, List<string> gone)
    {
        foreach (var (child, fk) in model.ChildrenOf(t.Name))
        {
            var ids = new List<string>();
            using (var cmd = c.Cmd($"SELECT Id FROM {Q(child.Name)} WHERE {Q(fk.Column)} = @p AND InstanceId = @inst", tx, ("@p", pk), ("@inst", Instance)))
            using (var r = cmd.ExecuteReader())
                while (r.Read()) ids.Add(r.GetString(0));
            foreach (var id in ids) RemoveWithChildren(c, tx, child, id, gone);
        }
    }

    public void Note(SqliteConnection c, SqliteTransaction tx, string text, bool info = false)
    {
        ClientStore.Note(c, tx, Instance, text, info);
        Notes.Add(text);
    }

    public void SetVersion(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, long version) =>
        c.Exec($"UPDATE {Q(t.Name)} SET SyncVersion = @v WHERE Id = @id AND InstanceId = @inst AND SyncVersion < @v",
            tx, ("@v", version), ("@id", pk), ("@inst", Instance));

    /// <summary>
    /// Merges a downloaded snapshot into the single client database (6.3, 10.3): rows of the instance are replaced in one
    /// transaction and the outbox carries over with the values saved before the delete. A patch or delete for a row that
    /// is not in the snapshot is dropped with a note (9.1).
    /// </summary>
    public static (string Instance, int Carried, List<string> Notes) InstallSnapshot(ClientStore store, string address, string path)
    {
        using var c = store.Open(foreignKeys: false);
        c.Exec("ATTACH DATABASE @p AS snap", null, ("@p", path));
        try
        {
            using var tx = c.BeginTransaction();
            var newInst = c.Scalar<string>("SELECT instance_id FROM snap._sync_meta", tx)!;
            var v = c.Scalar<long>("SELECT version FROM snap._sync_meta", tx);
            var oldInst = store.InstanceOf(c, address, tx) ?? newInst;
            var w = new ReplicaWriter(store.Model, newInst);
            var entries = ClientStore.Entries(c, tx, oldInst).Where(e => e.Sent != 2).ToList();
            c.Exec("DELETE FROM _sync_outbox WHERE instance = @i AND sent = 2", tx, ("@i", oldInst));

            foreach (var t in store.Model.Tables)
            {
                var T = Q(t.Name);
                var cols = string.Concat(t.Columns.Select(x => ", " + Q(x)));
                c.Exec($"DROP TABLE IF EXISTS temp.\"_keep_{t.Name}\"", tx);
                c.Exec($"""
                    CREATE TEMP TABLE "_keep_{t.Name}" AS SELECT * FROM main.{T}
                    WHERE InstanceId = @old AND Id IN (SELECT pk FROM _sync_outbox WHERE instance = @old AND tbl = @t AND kind IN (1, 2))
                    """, tx, ("@old", oldInst), ("@t", t.Name));
                c.Exec($"DELETE FROM main.{T} WHERE InstanceId IN (@old, @new)", tx, ("@old", oldInst), ("@new", newInst));
                // INSERT OR IGNORE: an archived row with the same Id keeps its place in the archive
                c.Exec($"INSERT OR IGNORE INTO main.{T} (Id, InstanceId, SyncVersion{cols}) SELECT Id, @new, SyncVersion{cols} FROM snap.{T}", tx, ("@new", newInst));
            }

            var carried = 0;
            foreach (var e in entries)
            {
                if (!store.Model.TryGet(e.Table, out var t)) continue;
                var T = Q(t.Name);
                var keep = $"temp.\"_keep_{t.Name}\"";
                switch (e.Kind)
                {
                    case OutboxKind.Create:
                        var cols = string.Concat(t.Columns.Select(x => ", " + Q(x)));
                        c.Exec($"INSERT OR REPLACE INTO main.{T} (Id, InstanceId, SyncVersion{cols}) SELECT Id, @new, 0{cols} FROM {keep} WHERE Id = @id", tx, ("@new", newInst), ("@id", e.Pk));
                        carried++;
                        break;
                    case OutboxKind.Patch:
                        if (c.Scalar<long>($"SELECT COUNT(*) FROM main.{T} WHERE Id = @id AND InstanceId = @new", tx, ("@id", e.Pk), ("@new", newInst)) == 0)
                        {
                            ClientStore.Remove(c, tx, e.Id);
                            w.Note(c, tx, $"правку запису {Short(e.Pk!)} втрачено: його видалено на інстансі");
                            break;
                        }
                        var set = string.Join(", ", e.Columns.Where(t.HasColumn).Select(x => $"{Q(x)} = (SELECT {Q(x)} FROM {keep} k WHERE k.Id = @id)"));
                        if (set.Length > 0) c.Exec($"UPDATE main.{T} SET {set} WHERE Id = @id AND InstanceId = @new", tx, ("@id", e.Pk), ("@new", newInst));
                        carried++;
                        break;
                    case OutboxKind.Delete:
                        if (c.Exec($"DELETE FROM main.{T} WHERE Id = @id AND InstanceId = @new", tx, ("@id", e.Pk), ("@new", newInst)) == 0)
                            ClientStore.Remove(c, tx, e.Id); // nothing to delete on the owner either
                        carried++;
                        break;
                    case OutboxKind.PredicateDelete when Predicate.Parse(e.Predicate) is { } p:
                        var (where, args) = p.ToSql(t);
                        c.Exec($"DELETE FROM main.{T} WHERE InstanceId = @new AND SyncVersion <= @V AND {where}", tx, [("@new", newInst), ("@V", e.ExpectedVersion), .. args]);
                        carried++;
                        break;
                }
            }
            foreach (var t in store.Model.Tables) c.Exec($"DROP TABLE IF EXISTS temp.\"_keep_{t.Name}\"", tx);

            if (oldInst != newInst)
            {
                c.Exec("UPDATE _sync_outbox SET instance = @new WHERE instance = @old", tx, ("@new", newInst), ("@old", oldInst));
                c.Exec("DELETE FROM _sync_cursors WHERE instance = @old; DELETE FROM _sync_ranges WHERE instance = @old;", tx, ("@old", oldInst));
            }
            c.Exec("DELETE FROM _sync_ranges WHERE instance = @new", tx, ("@new", newInst));
            foreach (var t in store.Model.Tables)
                store.SaveCursor(c, tx, newInst, t.Name, new CursorState(v));
            c.Exec("UPDATE _sync_instances SET instance = @new, snapshot_version = @v WHERE address = @a", tx, ("@new", newInst), ("@v", v), ("@a", address));
            tx.Commit();
            return (newInst, carried, w.Notes);
        }
        finally
        {
            c.Exec("DETACH DATABASE snap");
        }
    }
}
