using Microsoft.Data.Sqlite;
using Replication.Client;
using Replication.Model;

namespace Sample.Lab;

/// <summary>A row as the lab panels show it.</summary>
public sealed record RowView(
    string Table, string Id, string Label, long? Price, string? Status, string? Category, long Version,
    long Base, string Mask, string? Origin, string? InstanceId, string Mark);

public sealed record TombstoneView(string Table, string Pk, long Version, long DeletedAt, string? Origin);

public sealed record ClientView(string ClientId, long Acked, long AppliedSeq, long LastSeen, string Cursors);

public sealed record OwnerMeta(string InstanceId, long Version, long Purged, long ClockOffset, string Floor, long Pages, long FreePages);

/// <summary>Raw SQL reads for the panels and for checks; they never go through the sync code.</summary>
public static class Views
{
    private static SqliteConnection Open(string path)
    {
        var c = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = true }.ToString());
        c.Open();
        return c;
    }

    private static string MaskText(SyncTable t, long mask) =>
        mask == t.FullMask ? "усі" : mask == 0 ? "—" : string.Join(", ", t.ColumnsOf(mask));

    public static List<RowView> OwnerRows(OwnerNode o, string table, int limit = 200)
    {
        var t = o.Store.Model[table];
        using var c = Open(o.DbPath);
        return ReadRows(c, t, "1", limit, owner: true);
    }

    /// <summary>The client's rows of its instance and of the instance's archive, with outbox marks.</summary>
    public static List<RowView> ClientRows(ClientNode n, string table, int limit = 200)
    {
        var inst = n.Agent.InstanceId;
        if (inst is null) return [];
        var t = n.Replication.Model[table];
        using var c = Open(n.DbPath);
        var rows = ReadRows(c, t, $"x.InstanceId IN ('{inst.Replace("'", "''")}', '{SyncColumns.ArchiveOf(inst).Replace("'", "''")}')", limit, owner: false);
        var entries = ClientStore.Entries(c, null, inst);
        return [.. rows.Select(r =>
        {
            if (r.InstanceId != inst) return r with { Mark = "архів" };
            var e = entries.FirstOrDefault(x => x.Table == table && x.Pk == r.Id);
            return e is null ? r : r with { Mark = e.Sent == 1 ? $"в дорозі ({e.Kind})" : $"черга ({e.Kind})" };
        })];
    }

    private static List<RowView> ReadRows(SqliteConnection c, SyncTable t, string where, int limit, bool owner)
    {
        var hasName = t.HasColumn("Name");
        var label = hasName ? "x.Name" : t.HasColumn("Text") ? "x.Text" : "x.Id";
        var price = t.HasColumn("Price") ? "x.Price" : "NULL";
        var status = t.HasColumn("Status") ? "x.Status" : "NULL";
        var cat = t.HasColumn("CategoryId") ? "(SELECT k.Name FROM Category k WHERE k.Id = x.CategoryId)" : "NULL";
        var order = t.Name == "Log" ? "x.SyncVersion DESC" : "x.Id";
        using var cmd = c.CreateCommand();
        cmd.CommandText = $"""
            SELECT x.Id, {label}, {price}, {status}, {cat}, x.SyncVersion, x.SyncBase, x.SyncMask, x.SyncOrigin, x.InstanceId
            FROM "{t.Name}" x WHERE {where} ORDER BY {order} LIMIT {limit}
            """;
        using var r = cmd.ExecuteReader();
        var list = new List<RowView>();
        while (r.Read())
            list.Add(new RowView(t.Name, r.GetString(0), r.IsDBNull(1) ? "" : r.GetString(1), r.IsDBNull(2) ? null : r.GetInt64(2),
                r.IsDBNull(3) ? null : r.GetString(3), r.IsDBNull(4) ? null : r.GetString(4), r.GetInt64(5), r.GetInt64(6),
                owner ? MaskText(t, r.GetInt64(7)) : "", r.IsDBNull(8) ? null : r.GetString(8), r.IsDBNull(9) ? null : r.GetString(9), ""));
        return list;
    }

    public static List<TombstoneView> Tombstones(OwnerNode o, int limit = 100)
    {
        using var c = Open(o.DbPath);
        using var cmd = c.CreateCommand();
        cmd.CommandText = $"SELECT tbl, pk, version, deleted_at, origin FROM _sync_tombstones ORDER BY version DESC LIMIT {limit}";
        using var r = cmd.ExecuteReader();
        var list = new List<TombstoneView>();
        while (r.Read()) list.Add(new(r.GetString(0), r.GetString(1), r.GetInt64(2), r.GetInt64(3), r.IsDBNull(4) ? null : r.GetString(4)));
        return list;
    }

    public static List<ClientView> Clients(OwnerNode o)
    {
        using var c = Open(o.DbPath);
        using var cmd = c.CreateCommand();
        cmd.CommandText = """
            SELECT cl.client_id, cl.acked_version, cl.applied_seq, cl.last_seen,
                   (SELECT group_concat(tbl || '=' || cursor, ', ') FROM _sync_client_cursors cc WHERE cc.client_id = cl.client_id)
            FROM _sync_clients cl ORDER BY cl.client_id
            """;
        using var r = cmd.ExecuteReader();
        var list = new List<ClientView>();
        while (r.Read()) list.Add(new(r.GetString(0), r.GetInt64(1), r.GetInt64(2), r.GetInt64(3), r.IsDBNull(4) ? "" : r.GetString(4)));
        return list;
    }

    public static OwnerMeta Meta(OwnerNode o)
    {
        using var c = Open(o.DbPath);
        using var cmd = c.CreateCommand();
        cmd.CommandText = """
            SELECT instance_id, version, purged_version, clock_offset,
                   (SELECT group_concat(tbl || '=' || CASE WHEN floor = 9223372036854775807 THEN '∞' ELSE floor END, ', ') FROM _sync_floor),
                   (SELECT page_count FROM pragma_page_count()), (SELECT freelist_count FROM pragma_freelist_count())
            FROM _sync_meta
            """;
        using var r = cmd.ExecuteReader();
        r.Read();
        return new OwnerMeta(r.GetString(0), r.GetInt64(1), r.GetInt64(2), r.GetInt64(3), r.IsDBNull(4) ? "" : r.GetString(4), r.GetInt64(5), r.GetInt64(6));
    }

    /// <summary>
    /// Differences between the owner's rows and a client's replica (InstanceId = X): ids, data columns and versions.
    /// Empty when the replica equals the owner (design 17, "convergence").
    /// </summary>
    public static List<string> Diff(OwnerNode o, ClientNode n)
    {
        var diffs = new List<string>();
        var inst = n.Agent.InstanceId;
        if (inst is null) return ["репліки нема"];
        using var oc = Open(o.DbPath);
        using var cc = Open(n.DbPath);
        foreach (var t in o.Store.Model.Tables)
        {
            var cols = string.Join(", ", new[] { "Id", "SyncVersion" }.Concat(t.Columns).Select(Wire.Q));
            Dictionary<string, string> Read(SqliteConnection c, string where)
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = $"SELECT {cols} FROM {Wire.Q(t.Name)} WHERE {where}";
                cmd.Parameters.AddWithValue("@i", inst);
                using var r = cmd.ExecuteReader();
                var d = new Dictionary<string, string>();
                while (r.Read())
                    d[r.GetString(0)] = string.Join("|", Enumerable.Range(1, r.FieldCount - 1).Select(i => r.IsDBNull(i) ? "∅" : Convert.ToString(r.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)));
                return d;
            }
            var a = Read(oc, "1");
            var b = Read(cc, "InstanceId = @i");
            foreach (var (id, v) in a)
                if (!b.TryGetValue(id, out var w)) diffs.Add($"{t.Name} {Wire.Short(id)}: нема в репліці");
                else if (v != w) diffs.Add($"{t.Name} {Wire.Short(id)}: власник [{v}], репліка [{w}]");
            foreach (var id in b.Keys.Where(k => !a.ContainsKey(k))) diffs.Add($"{t.Name} {Wire.Short(id)}: зайвий у репліці");
        }
        return diffs;
    }

    public static bool OwnerHas(OwnerNode o, string itemName)
    {
        using var c = Open(o.DbPath);
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Item WHERE Name = @n";
        cmd.Parameters.AddWithValue("@n", itemName);
        return (long)cmd.ExecuteScalar()! > 0;
    }

    public static bool ClientHas(ClientNode n, string itemName, bool archive = false)
    {
        var inst = n.Agent.InstanceId!;
        using var c = Open(n.DbPath);
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Item WHERE Name = @n AND InstanceId = @i";
        cmd.Parameters.AddWithValue("@n", itemName);
        cmd.Parameters.AddWithValue("@i", archive ? SyncColumns.ArchiveOf(inst) : inst);
        return (long)cmd.ExecuteScalar()! > 0;
    }

    public static (long Price, string Status, string Name)? OwnerItem(OwnerNode o, string itemName)
    {
        using var c = Open(o.DbPath);
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT Price, Status, Name FROM Item WHERE Name = @n";
        cmd.Parameters.AddWithValue("@n", itemName);
        using var r = cmd.ExecuteReader();
        return r.Read() ? (r.GetInt64(0), r.GetString(1), r.GetString(2)) : null;
    }
}
