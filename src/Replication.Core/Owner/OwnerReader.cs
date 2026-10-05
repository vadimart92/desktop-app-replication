using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Owner;

/// <summary>A live row or a tombstone of one table, as read for a batch.</summary>
internal sealed record OwnerItem(long Version, string Pk, string? Origin, bool IsTombstone, long Base = 0, long Mask = 0, object?[]? Values = null);

/// <summary>Range queries over <c>SyncVersion</c> and tombstones (6.4).</summary>
internal static class OwnerReader
{
    private static string RowSelect(SyncTable t) =>
        $"SELECT Id, SyncVersion, SyncBase, SyncMask, SyncOrigin{string.Concat(t.Columns.Select(c => ", " + Q(c)))} FROM {Q(t.Name)}";

    private static OwnerItem ReadRow(SyncTable t, SqliteDataReader r)
    {
        var values = new object?[t.Columns.Count];
        for (var i = 0; i < values.Length; i++) values[i] = r.Raw(5 + i);
        return new OwnerItem(r.GetInt64(1), r.GetString(0), r.IsDBNull(4) ? null : r.GetString(4), false, r.GetInt64(2), r.GetInt64(3), values);
    }

    /// <summary>Rows and tombstones with lo &lt; version &lt;= hi, at most <paramref name="limit"/>, newest first or oldest first.</summary>
    public static List<OwnerItem> Range(SqliteConnection c, SqliteTransaction? tx, SyncTable t, long lo, long hi, int limit, bool newestFirst)
    {
        var order = newestFirst ? "DESC" : "ASC";
        var items = new List<OwnerItem>();
        using (var cmd = c.Cmd($"{RowSelect(t)} WHERE SyncVersion > @lo AND SyncVersion <= @hi ORDER BY SyncVersion {order} LIMIT @n",
                   tx, ("@lo", lo), ("@hi", hi), ("@n", limit)))
        using (var r = cmd.ExecuteReader())
            while (r.Read()) items.Add(ReadRow(t, r));
        using (var cmd = c.Cmd($"SELECT pk, version, origin FROM _sync_tombstones WHERE tbl = @t AND version > @lo AND version <= @hi ORDER BY version {order} LIMIT @n",
                   tx, ("@t", t.Name), ("@lo", lo), ("@hi", hi), ("@n", limit)))
        using (var r = cmd.ExecuteReader())
            while (r.Read()) items.Add(new OwnerItem(r.GetInt64(1), r.GetString(0), r.IsDBNull(2) ? null : r.GetString(2), true));
        items = newestFirst ? [.. items.OrderByDescending(x => x.Version)] : [.. items.OrderBy(x => x.Version)];
        return items.Count > limit ? items.GetRange(0, limit) : items;
    }

    public static long Count(SqliteConnection c, SqliteTransaction? tx, SyncTable t, long lo, long hi) =>
        c.Scalar<long>($"""
            SELECT (SELECT COUNT(*) FROM {Q(t.Name)} WHERE SyncVersion > @lo AND SyncVersion <= @hi)
                 + (SELECT COUNT(*) FROM _sync_tombstones WHERE tbl = @t AND version > @lo AND version <= @hi)
            """, tx, ("@t", t.Name), ("@lo", lo), ("@hi", hi));

    public static bool Any(SqliteConnection c, SqliteTransaction? tx, SyncTable t, long lo, long hi) =>
        c.Scalar<long>($"""
            SELECT EXISTS(SELECT 1 FROM {Q(t.Name)} WHERE SyncVersion > @lo AND SyncVersion <= @hi)
                OR EXISTS(SELECT 1 FROM _sync_tombstones WHERE tbl = @t AND version > @lo AND version <= @hi)
            """, tx, ("@t", t.Name), ("@lo", lo), ("@hi", hi)) != 0;

    public static OwnerItem? Row(SqliteConnection c, SqliteTransaction? tx, SyncTable t, string pk)
    {
        using var cmd = c.Cmd($"{RowSelect(t)} WHERE Id = @id", tx, ("@id", pk));
        using var r = cmd.ExecuteReader();
        return r.Read() ? ReadRow(t, r) : null;
    }

    public static OwnerItem? Tombstone(SqliteConnection c, SqliteTransaction? tx, SyncTable t, string pk)
    {
        using var cmd = c.Cmd("SELECT pk, version, origin FROM _sync_tombstones WHERE tbl = @t AND pk = @p", tx, ("@t", t.Name), ("@p", pk));
        using var r = cmd.ExecuteReader();
        return r.Read() ? new OwnerItem(r.GetInt64(1), r.GetString(0), r.IsDBNull(2) ? null : r.GetString(2), true) : null;
    }

    /// <summary>
    /// Full or partial form, decided per row by this client's cursor for the table (6.4):
    /// full mask, or the client's cursor below <c>SyncBase</c> → full row; otherwise only the mask columns.
    /// </summary>
    public static Row ToWire(SyncTable t, OwnerItem it, long clientCursor, bool forceFull = false)
    {
        var row = new Row { Pk = PkBytes(it.Pk), Version = it.Version };
        var full = forceFull || it.Mask == t.FullMask || clientCursor < it.Base;
        if (full)
        {
            row.Full = true;
            foreach (var v in it.Values!) row.Values.Add(ToValue(v));
        }
        else
        {
            row.Mask = it.Mask;
            for (var i = 0; i < t.Columns.Count; i++)
                if ((it.Mask & (1L << i)) != 0) row.Values.Add(ToValue(it.Values![i]));
        }
        return row;
    }
}
