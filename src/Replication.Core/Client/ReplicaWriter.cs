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
    /// pending actions were made against a state the snapshot replaces. The cursors of every table become the snapshot's
    /// version V without ranges: the owner registered this client at V when it made the snapshot, so catch-up goes on from there.
    /// The <c>_sync_instances</c> row of the instance stays, as next_seq must keep growing past the owner's applied_seq.
    /// Another instance whose rows have the same Ids is an older life of this owner (restored from a backup, 5.3): its whole
    /// replica goes as well (rows, cursors, ranges, outbox and its <c>_sync_instances</c> row). Archives stay.
    /// Only agents that have connected hold a claim, so the replica of a live owner whose agent has not connected yet is not protected;
    /// two live owners with shared Ids are not supported.
    /// </summary>
    /// <param name="agent">The installing agent: it holds the claim on an old instance while its replica is dropped.</param>
    /// <exception cref="IOException">The snapshot is of another instance than <paramref name="expected"/>; nothing is changed.</exception>
    /// <exception cref="InstanceHeldException">The snapshot shares Ids with an instance another agent serves; nothing is changed.</exception>
    public static SnapshotInstall InstallSnapshot(ClientStore store, string path, string expected, SyncAgent agent)
    {
        using SqliteConnection c = store.Open(foreignKeys: false);
        c.Exec("ATTACH DATABASE @p AS snap", null, ("@p", path));
        var claimed = new List<string>();
        try
        {
            using SqliteTransaction tx = c.BeginTransaction();
            string newInst = c.Scalar<string>("SELECT instance_id FROM snap._sync_meta", tx)!;
            if (newInst != expected)
                throw new IOException($"the snapshot is of instance {newInst}, the agent serves {expected}");
            long v = c.Scalar<long>("SELECT version FROM snap._sync_meta", tx);
            // Its _sync_instances row stays: next_seq must keep growing, the owner remembers applied_seq of this client.
            int dropped = DropReplica(c, tx, store.Model, newInst);

            var replaced = new List<(string Instance, int Dropped)>();
            foreach (string old in store.Model.Tables.SelectMany(t => c.Query(
                    $"SELECT DISTINCT InstanceId FROM main.{Q(t.Name)} WHERE InstanceId IS NOT NULL AND InstanceId <> @new AND InstanceId NOT LIKE '%:archive' AND Id IN (SELECT Id FROM snap.{Q(t.Name)})",
                    tx, r => r.GetString(0), [("@new", newInst)])).Distinct().ToList())
            {
                // The claim keeps agents off the old instance until the transaction ends.
                if (store.Claim(old, agent) is var holder && holder != agent)
                    throw new InstanceHeldException(old, holder);
                claimed.Add(old);
                replaced.Add((old, DropReplica(c, tx, store.Model, old)));
                c.Exec("DELETE FROM _sync_instances WHERE instance = @old", tx, ("@old", old));
            }

            foreach (SyncTable t in store.Model.Tables)
            {
                string table = Q(t.Name);
                string cols = string.Concat(t.Columns.Select(x => ", " + Q(x)));
                // INSERT OR IGNORE: an archived row with the same Id keeps its place in the archive.
                // Likewise a snapshot row whose Id collides with an own row (InstanceId NULL) is dropped: Id is the only key and own data wins.
                c.Exec($"INSERT OR IGNORE INTO main.{table} (Id, InstanceId, SyncVersion{cols}) SELECT Id, @new, SyncVersion{cols} FROM snap.{table}", tx, ("@new", newInst));
            }

            foreach (SyncTable t in store.Model.Tables)
                ClientStore.SaveCursor(c, tx, newInst, t.Name, new CursorState(v));
            ClientStore.SaveInstance(c, tx, newInst, v);
            tx.Commit();
            return new SnapshotInstall(dropped, replaced);
        }
        finally
        {
            foreach (string old in claimed)
                store.Release(old, agent);
            c.Exec("DETACH DATABASE snap");
        }
    }

    /// <summary>
    /// Deletes the replica of <paramref name="instance"/>: rows, cursors, ranges and outbox; its archive and its
    /// <c>_sync_instances</c> row stay. Returns the number of outbox actions not sent.
    /// </summary>
    internal static int DropReplica(SqliteConnection c, SqliteTransaction tx, SyncModel model, string instance)
    {
        int dropped = (int)c.Scalar<long>("SELECT COUNT(*) FROM _sync_outbox WHERE instance = @i AND sent < 2", tx, ("@i", instance));
        c.Exec("DELETE FROM _sync_outbox WHERE instance = @i; DELETE FROM _sync_cursors WHERE instance = @i; DELETE FROM _sync_ranges WHERE instance = @i;", tx, ("@i", instance));
        foreach (SyncTable t in model.Tables)
            c.Exec($"DELETE FROM main.{Q(t.Name)} WHERE InstanceId = @i", tx, ("@i", instance));
        return dropped;
    }
}

/// <summary>What a snapshot install dropped: the outbox actions of its instance, and the old instances replaced with their outbox actions.</summary>
internal sealed record SnapshotInstall(int Dropped, IReadOnlyList<(string Instance, int Dropped)> Replaced);

/// <summary>A snapshot shares Ids with <see cref="Instance"/>, which <see cref="Holder"/> serves: two live owners with shared Ids.</summary>
internal sealed class InstanceHeldException(string instance, SyncAgent holder)
    : Exception($"the snapshot shares Ids with instance {instance}, which agent «{holder.Name}» serves")
{
    public string Instance { get; } = instance;
    public SyncAgent Holder { get; } = holder;
}
