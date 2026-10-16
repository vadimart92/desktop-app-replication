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
        object?[] values = new object?[t.Columns.Count];
        for (int i = 0; i < values.Length; i++)
            values[i] = r.Raw(5 + i);
        return new OwnerItem(r.GetInt64(1), r.GetString(0), r.IsDBNull(4) ? null : r.GetString(4), false, r.GetInt64(2), r.GetInt64(3), values);
    }

    /// <summary>Rows and tombstones with lo &lt; version &lt;= hi, at most <paramref name="limit"/>, newest first or oldest first.</summary>
    public static List<OwnerItem> Range(SqliteConnection c, SqliteTransaction? tx, SyncTable t, long lo, long hi, int limit, bool newestFirst)
    {
        string order = newestFirst ? "DESC" : "ASC";
        var items = new List<OwnerItem>();
        using (SqliteCommand cmd = c.Cmd($"{RowSelect(t)} WHERE SyncVersion > @lo AND SyncVersion <= @hi ORDER BY SyncVersion {order} LIMIT @n",
                   tx, ("@lo", lo), ("@hi", hi), ("@n", limit)))
        using (SqliteDataReader r = cmd.ExecuteReader())
        {
            while (r.Read())
                items.Add(ReadRow(t, r));
        }

        using (SqliteCommand cmd = c.Cmd($"SELECT pk, version, origin FROM _sync_tombstones WHERE tbl = @t AND version > @lo AND version <= @hi ORDER BY version {order} LIMIT @n",
                   tx, ("@t", t.Name), ("@lo", lo), ("@hi", hi), ("@n", limit)))
        using (SqliteDataReader r = cmd.ExecuteReader())
        {
            while (r.Read())
                items.Add(new OwnerItem(r.GetInt64(1), r.GetString(0), r.IsDBNull(2) ? null : r.GetString(2), true));
        }

        items = newestFirst ? [.. items.OrderByDescending(x => x.Version)] : [.. items.OrderBy(x => x.Version)];
        return items.Count > limit ? items.GetRange(0, limit) : items;
    }

    /// <summary>
    /// Rows and tombstones in any of the ranges (lo, hi], in one statement: a cursor with points pulled by key (6.4)
    /// has thousands of gaps. The bounds are numbers and are inlined, so their count is not bound by the parameter limit.
    /// </summary>
    public static long Count(SqliteConnection c, SqliteTransaction? tx, SyncTable t, IReadOnlyList<(long Lo, long Hi)> ranges)
    {
        if (ranges.Count == 0)
            return 0;
        string values = string.Join(", ", ranges.Select(g => FormattableString.Invariant($"({g.Lo}, {g.Hi})")));
        return c.Scalar<long>($"""
            WITH g(lo, hi) AS (VALUES {values})
            SELECT (SELECT COUNT(*) FROM g JOIN {Q(t.Name)} x ON x.SyncVersion > g.lo AND x.SyncVersion <= g.hi)
                 + (SELECT COUNT(*) FROM g JOIN _sync_tombstones s ON s.tbl = @t AND s.version > g.lo AND s.version <= g.hi)
            """, tx, ("@t", t.Name));
    }

    public static OwnerItem? Row(SqliteConnection c, SqliteTransaction? tx, SyncTable t, string pk)
    {
        using SqliteCommand cmd = c.Cmd($"{RowSelect(t)} WHERE Id = @id", tx, ("@id", pk));
        using SqliteDataReader r = cmd.ExecuteReader();
        return r.Read() ? ReadRow(t, r) : null;
    }

    /// <summary>The live rows among <paramref name="pks"/>; a key with no row is skipped.</summary>
    public static List<OwnerItem> Rows(SqliteConnection c, SqliteTransaction? tx, SyncTable t, IReadOnlyCollection<string> pks)
    {
        var items = new List<OwnerItem>();
        foreach (string[] chunk in pks.Chunk(500))
        {
            (string, object?)[] args = chunk.Select((pk, i) => ($"@p{i}", (object?)pk)).ToArray();
            using SqliteCommand cmd = c.Cmd($"{RowSelect(t)} WHERE Id IN ({string.Join(", ", args.Select(a => a.Item1))})", tx, args);
            using SqliteDataReader r = cmd.ExecuteReader();
            while (r.Read())
                items.Add(ReadRow(t, r));
        }
        return items;
    }

    public static OwnerItem? Tombstone(SqliteConnection c, SqliteTransaction? tx, SyncTable t, string pk)
    {
        using SqliteCommand cmd = c.Cmd("SELECT pk, version, origin FROM _sync_tombstones WHERE tbl = @t AND pk = @p", tx, ("@t", t.Name), ("@p", pk));
        using SqliteDataReader r = cmd.ExecuteReader();
        return r.Read() ? new OwnerItem(r.GetInt64(1), r.GetString(0), r.IsDBNull(2) ? null : r.GetString(2), true) : null;
    }

    /// <summary>
    /// Full or partial form, decided per row by this client's cursor for the table (6.4):
    /// full mask, or the client's cursor below <c>SyncBase</c> → full row; otherwise only the mask columns.
    /// </summary>
    public static Row ToWire(SyncTable t, OwnerItem it, long clientCursor, bool forceFull = false)
    {
        var row = new Row { Pk = PkBytes(it.Pk), Version = it.Version };
        bool full = forceFull || it.Mask == t.FullMask || clientCursor < it.Base;
        if (full)
        {
            row.Full = true;
            foreach (object? v in it.Values!)
                row.Values.Add(ToValue(v));
        }
        else
        {
            row.Mask = it.Mask;
            for (int i = 0; i < t.Columns.Count; i++)
            {
                if ((it.Mask & (1L << i)) != 0)
                    row.Values.Add(ToValue(it.Values![i]));
            }
        }
        return row;
    }
}
