using Microsoft.Data.Sqlite;
using Replication.Model;
using static Replication.Model.Wire;

namespace Replication.Client;

/// <summary>
/// A parent that archived rows point to is not deleted from the replica but moved into the archive (11.6):
/// the FK cascade would otherwise take the archived rows, the only copy, with it.
/// </summary>
internal static class ArchiveGuard
{
    /// <summary>Whether archived rows of the instance point to this row, directly or through live children.</summary>
    public static bool HasArchiveBelow(SqliteConnection c, SqliteTransaction? tx, SyncModel model, string instance, SyncTable t, string pk)
    {
        var archive = SyncColumns.ArchiveOf(instance);
        foreach (var (child, fk) in model.ChildrenOf(t.Name))
        {
            if (c.Scalar<long>($"SELECT EXISTS(SELECT 1 FROM {Q(child.Name)} WHERE {Q(fk.Column)} = @p AND InstanceId = @a)", tx, ("@p", pk), ("@a", archive)) != 0)
                return true;
            foreach (var id in LiveChildren(c, tx, instance, child, fk, pk))
                if (HasArchiveBelow(c, tx, model, instance, child, id)) return true;
        }
        return false;
    }

    /// <summary>
    /// Deletes a replica row with its live children, children first; a row that archived rows point to gets
    /// <c>InstanceId = X:archive</c> instead. Works the same with FK on (no cascade reaches the archive) and off.
    /// </summary>
    /// <returns>true when the row itself was moved to the archive.</returns>
    public static bool DeleteOrArchive(SqliteConnection c, SqliteTransaction? tx, SyncModel model, string instance, SyncTable t, string pk)
    {
        if (!HasArchiveBelow(c, tx, model, instance, t, pk))
        {
            c.Exec($"DELETE FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @i", tx, ("@id", pk), ("@i", instance));
            return false;
        }
        foreach (var (child, fk) in model.ChildrenOf(t.Name))
            foreach (var id in LiveChildren(c, tx, instance, child, fk, pk))
                DeleteOrArchive(c, tx, model, instance, child, id);
        c.Exec($"UPDATE {Q(t.Name)} SET InstanceId = @a WHERE Id = @id AND InstanceId = @i", tx,
            ("@a", SyncColumns.ArchiveOf(instance)), ("@id", pk), ("@i", instance));
        return true;
    }

    private static List<string> LiveChildren(SqliteConnection c, SqliteTransaction? tx, string instance, SyncTable child, SyncForeignKey fk, string pk)
    {
        var ids = new List<string>();
        using var cmd = c.Cmd($"SELECT Id FROM {Q(child.Name)} WHERE {Q(fk.Column)} = @p AND InstanceId = @i", tx, ("@p", pk), ("@i", instance));
        using var r = cmd.ExecuteReader();
        while (r.Read()) ids.Add(r.GetString(0));
        return ids;
    }
}
