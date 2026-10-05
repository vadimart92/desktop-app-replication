using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Owner;

/// <summary>
/// Applies a batch of client actions in one short transaction (6.5): seq/applied_seq dedup, explicit FK checks,
/// delete wins (9.1), SyncOrigin for no-echo (5.2), conditional deletes for archive and bulk delete (8.6, 11).
/// </summary>
internal sealed class ApplyHandler(OwnerStore store)
{
    public static class Reasons
    {
        public const string Deleted = "deleted";
        public const string ParentDeleted = "parent deleted";
        public const string Unique = "unique";
        public const string AlreadyApplied = "already applied";
    }

    private SyncModel Model => store.Model;

    public ApplyReply Apply(ApplyRequest req)
    {
        using SqliteConnection c = store.Open();
        using SqliteTransaction tx = c.BeginTransaction(); // BEGIN IMMEDIATE
        c.Exec("PRAGMA defer_foreign_keys = ON;", tx); // the order of actions inside a batch does not matter

        string cid = req.ClientId;
        long now = store.Now(c, tx);
        c.Exec("INSERT OR IGNORE INTO _sync_clients(client_id, acked_version, applied_seq, last_seen) VALUES (@c, @a, 0, @n)",
            tx, ("@c", cid), ("@a", store.Purged(c, tx)), ("@n", now));
        long applied = c.Scalar<long>("SELECT applied_seq FROM _sync_clients WHERE client_id = @c", tx, ("@c", cid));
        long acked = c.Scalar<long>("SELECT acked_version FROM _sync_clients WHERE client_id = @c", tx, ("@c", cid));
        var cursors = new Dictionary<string, long>();
        using (SqliteCommand cmd = c.Cmd("SELECT tbl, cursor FROM _sync_client_cursors WHERE client_id = @c", tx, ("@c", cid)))
        using (SqliteDataReader r = cmd.ExecuteReader())
        {
            while (r.Read())
                cursors[r.GetString(0)] = r.GetInt64(1);
        }

        long CursorOf(string t) => cursors.TryGetValue(t, out long v) ? v : acked;

        List<Protocol.Action> actions = req.Actions.OrderBy(a => a.Seq).ToList();
        var results = new Dictionary<long, ActionResult>();
        ActionResult Res(Protocol.Action a, ResultStatus s, string reason = "") =>
            results[a.Seq] = new ActionResult { Seq = a.Seq, Status = s, Reason = reason };

        List<Protocol.Action> fresh = actions.Where(a => a.Seq > applied).ToList();
        Dictionary<(string Tbl, string), Protocol.Action> creates = fresh.Where(a => a.Kind == ActionKind.Create).ToDictionary(a => (a.Tbl, PkText(a.Pk)));
        HashSet<(string Tbl, string)> deletes = fresh.Where(a => a.Kind == ActionKind.Delete).Select(a => (a.Tbl, PkText(a.Pk))).ToHashSet();

        foreach (Protocol.Action? a in actions)
        {
            if (!Model.TryGet(a.Tbl, out SyncTable? t) && a.Kind is not ActionKind.Archive)
            {
                Res(a, ResultStatus.Rejected, "unknown table");
                continue;
            }
            if (a.Seq <= applied)
            {
                // a retry after a lost reply: the version is returned only while it is still this client's (6.5)
                ActionResult res = Res(a, ResultStatus.Skipped, Reasons.AlreadyApplied);
                if (a.Pk.Length > 0 && a.Kind is ActionKind.Create or ActionKind.Patch && OwnerReader.Row(c, tx, t, PkText(a.Pk)) is { } row && row.Origin == cid)
                    (res.HasVersion, res.Version) = (true, row.Version);
                else if (a.Kind == ActionKind.Delete && OwnerReader.Tombstone(c, tx, t, PkText(a.Pk)) is { } tomb)
                    (res.HasVersion, res.Version) = (true, tomb.Version);
                continue;
            }
            string? pk = a.Pk.Length > 0 ? PkText(a.Pk) : null;
            if (a.Kind == ActionKind.Create && OwnerReader.Tombstone(c, tx, t, pk!) is not null)
                Res(a, ResultStatus.Rejected, Reasons.Deleted); // delete wins over a create retry (9.1)
            else if (a.Kind == ActionKind.Patch && OwnerReader.Row(c, tx, t, pk!) is null && !creates.ContainsKey((a.Tbl, pk!)))
                Res(a, ResultStatus.Ignored, Reasons.Deleted);
        }

        foreach (Protocol.Action? a in fresh.Where(a => a.Kind == ActionKind.Archive && !results.ContainsKey(a.Seq)))
            results[a.Seq] = Archive(c, tx, a);
        foreach (Protocol.Action? a in fresh.Where(a => a.Kind == ActionKind.PredicateDelete && !results.ContainsKey(a.Seq)))
            results[a.Seq] = PredicateDelete(c, tx, a, cid);

        // explicit FK check by the EF model, counting creates in the same batch; a rejected parent rejects its children
        for (bool changed = true; changed;)
        {
            changed = false;
            foreach (Protocol.Action? a in fresh.Where(a => a.Kind is ActionKind.Create or ActionKind.Patch && !results.ContainsKey(a.Seq)))
            {
                SyncTable t = Model[a.Tbl];
                for (int i = 0; i < a.Columns.Count; i++)
                {
                    SyncForeignKey? fk = t.ForeignKeys.FirstOrDefault(f => f.Column == a.Columns[i]);
                    if (fk is null || FromValue(a.Values[i]) is not string pid)
                        continue;
                    SyncTable parent = Model[fk.ParentTable];
                    bool exists = OwnerReader.Row(c, tx, parent, pid) is not null && !deletes.Contains((parent.Name, pid));
                    bool inBatch = creates.TryGetValue((parent.Name, pid), out Protocol.Action? pc) && !results.ContainsKey(pc.Seq);
                    if (!exists && !inBatch)
                    {
                        Res(a, ResultStatus.Rejected, Reasons.ParentDeleted);
                        changed = true;
                        break;
                    }
                }
            }
        }

        foreach (Protocol.Action? a in fresh.Where(a => !results.ContainsKey(a.Seq)))
        {
            SyncTable t = Model[a.Tbl];
            string pk = PkText(a.Pk);
            c.Exec("SAVEPOINT act", tx);
            try
            {
                ActionResult res = Res(a, ResultStatus.Applied);
                switch (a.Kind)
                {
                    case ActionKind.Create:
                        Upsert(c, tx, t, pk, a, cid); // a new row always carries this client's origin
                        break;
                    case ActionKind.Patch:
                    {
                        // SyncOrigin = client only if it already had the previous state of the row (5.2, "no echo")
                        OwnerItem? prev = OwnerReader.Row(c, tx, t, pk);
                        string? origin = prev is null || prev.Version <= CursorOf(t.Name) ? cid : null;
                        Patch(c, tx, t, pk, a, origin);
                        if (origin is null)
                            res.Reason = "echo";
                        break;
                    }
                    case ActionKind.Delete:
                        c.Exec($"DELETE FROM {Q(t.Name)} WHERE Id = @id", tx, ("@id", pk));
                        c.Exec("UPDATE _sync_tombstones SET origin = @c WHERE tbl = @t AND pk = @p", tx, ("@c", cid), ("@t", t.Name), ("@p", pk));
                        if (OwnerReader.Tombstone(c, tx, t, pk) is { } tomb)
                            (res.HasVersion, res.Version) = (true, tomb.Version);
                        break;
                }
                if (a.Kind is ActionKind.Create or ActionKind.Patch && OwnerReader.Row(c, tx, t, pk) is { } row && row.Origin == cid)
                    (res.HasVersion, res.Version) = (true, row.Version);
                c.Exec("RELEASE act", tx);
            }
            catch (SqliteException e) when (e.SqliteErrorCode == 19) // SQLITE_CONSTRAINT: a unique index on a business column (9)
            {
                c.Exec("ROLLBACK TO act; RELEASE act;", tx);
                Res(a, ResultStatus.Rejected, Reasons.Unique);
            }
        }

        long upTo = Math.Max(applied, actions.Count > 0 ? actions.Max(a => a.Seq) : 0);
        c.Exec("UPDATE _sync_clients SET applied_seq = @s, last_seen = @n WHERE client_id = @c", tx, ("@s", upTo), ("@n", now), ("@c", cid));
        tx.Commit();
        store.NotifyCommitted();

        var reply = new ApplyReply { AppliedUpToSeq = upTo };
        reply.Results.AddRange(actions.Select(a => results[a.Seq]));
        return reply;
    }

    private static void Upsert(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, Protocol.Action a, string origin)
    {
        List<string> cols = a.Columns.Where(t.HasColumn).ToList();
        string names = string.Concat(cols.Select(x => ", " + Q(x)));
        string pars = string.Concat(cols.Select((_, i) => $", @v{i}"));
        string set = string.Concat(cols.Select(x => $"{Q(x)} = excluded.{Q(x)}, "));
        using SqliteCommand cmd = c.Cmd($"""
            INSERT INTO {Q(t.Name)} (Id{names}, SyncOrigin) VALUES (@id{pars}, @o)
            ON CONFLICT(Id) DO UPDATE SET {set}SyncOrigin = excluded.SyncOrigin
            """, tx, ("@id", pk), ("@o", origin));
        Bind(cmd, a, cols);
        cmd.ExecuteNonQuery();
    }

    private static void Patch(SqliteConnection c, SqliteTransaction tx, SyncTable t, string pk, Protocol.Action a, string? origin)
    {
        List<string> cols = a.Columns.Where(t.HasColumn).ToList();
        string set = string.Concat(cols.Select((x, i) => $"{Q(x)} = @v{i}, "));
        using SqliteCommand cmd = c.Cmd($"UPDATE {Q(t.Name)} SET {set}SyncOrigin = @o WHERE Id = @id", tx, ("@id", pk), ("@o", origin));
        Bind(cmd, a, cols);
        cmd.ExecuteNonQuery();
    }

    private static void Bind(SqliteCommand cmd, Protocol.Action a, List<string> cols)
    {
        for (int i = 0; i < cols.Count; i++)
        {
            int idx = a.Columns.IndexOf(cols[i]);
            cmd.Parameters.AddWithValue($"@v{i}", FromValue(a.Values[idx]));
        }
    }

    /// <summary>
    /// Archive (11.2, step 6): delete the set where SyncVersion &lt;= V. Rows changed after V stay ("changed"),
    /// and so do parents whose children stay ("children"). No tombstone origin: the client does not remove these rows itself.
    /// </summary>
    private ActionResult Archive(SqliteConnection c, SqliteTransaction tx, Protocol.Action a)
    {
        List<(string Tbl, string Pk)> items = a.Rows.Select(r => (Tbl: r.Tbl, Pk: PkText(r.Pk))).ToList();
        var why = new Dictionary<(string, string), string>();
        foreach ((string Tbl, string Pk) it in items)
        {
            if (!Model.TryGet(it.Tbl, out SyncTable? t))
            {
                why[it] = "gone";
                continue;
            }
            OwnerItem? row = OwnerReader.Row(c, tx, t, it.Pk);
            if (row is null)
                why[it] = "gone";
            else if (row.Version > a.ExpectedVersion)
                why[it] = "changed";
        }
        HashSet<(string Tbl, string Pk)> set = items.ToHashSet();
        for (bool changed = true; changed;)
        {
            changed = false;
            foreach ((string Tbl, string Pk) it in items.Where(x => !why.ContainsKey(x)))
            {
                foreach ((SyncTable? child, SyncForeignKey? fk) in Model.ChildrenOf(it.Tbl))
                {
                    using SqliteCommand cmd = c.Cmd($"SELECT Id FROM {Q(child.Name)} WHERE {Q(fk.Column)} = @p", tx, ("@p", it.Pk));
                    using SqliteDataReader r = cmd.ExecuteReader();
                    while (r.Read())
                    {
                        (string Name, string) k = (child.Name, r.GetString(0));
                        if (!set.Contains(k) || why.ContainsKey(k))
                        {
                            why[it] = "children";
                            changed = true;
                            break;
                        }
                    }
                    if (why.ContainsKey(it))
                        break;
                }
            }
        }
        List<(string Tbl, string Pk)> toDelete = items.Where(x => !why.ContainsKey(x)).OrderBy(x => Model.Tables.ToList().FindIndex(t => t.Name == x.Tbl) * -1).ToList();
        int deleted = 0;
        foreach ((string? tbl, string? pk) in toDelete)
            deleted += c.Exec($"DELETE FROM {Q(tbl)} WHERE Id = @id AND SyncVersion <= @v", tx, ("@id", pk), ("@v", a.ExpectedVersion));
        var res = new ActionResult { Seq = a.Seq, Status = ResultStatus.Applied, Deleted = deleted };
        res.Changed.AddRange(why.Where(x => x.Value == "changed").Select(x => Ref(x.Key.Item1, x.Key.Item2)));
        res.Children.AddRange(why.Where(x => x.Value == "children").Select(x => Ref(x.Key.Item1, x.Key.Item2)));
        return res;
    }

    /// <summary>Bulk delete by predicate: DELETE ... WHERE SyncVersion &lt;= V AND condition (8.6).</summary>
    private ActionResult PredicateDelete(SqliteConnection c, SqliteTransaction tx, Protocol.Action a, string cid)
    {
        SyncTable t = Model[a.Tbl];
        var where = new List<string>();
        var args = new List<(string, object?)>();
        for (int i = 0; i < a.Predicate.Count; i++)
        {
            Condition cond = a.Predicate[i];
            if (!t.HasColumn(cond.Column))
                return new ActionResult { Seq = a.Seq, Status = ResultStatus.Rejected, Reason = "unknown column" };
            where.Add($"{Q(cond.Column)} IS @p{i}");
            args.Add(($"@p{i}", FromValue(cond.Value)));
        }
        string condSql = where.Count > 0 ? string.Join(" AND ", where) : "1";
        long before = store.Head(c, tx);
        int deleted = c.Exec($"DELETE FROM {Q(t.Name)} WHERE SyncVersion <= @V AND {condSql}", tx, [.. args, ("@V", a.ExpectedVersion)]);
        // the client already removed these rows (and their cascade) itself
        c.Exec("UPDATE _sync_tombstones SET origin = @c WHERE version > @b", tx, ("@c", cid), ("@b", before));
        var res = new ActionResult { Seq = a.Seq, Status = ResultStatus.Applied, Deleted = deleted };
        using SqliteCommand cmd = c.Cmd($"SELECT Id FROM {Q(t.Name)} WHERE {condSql}", tx, [.. args]);
        using SqliteDataReader r = cmd.ExecuteReader();
        while (r.Read())
            res.Changed.Add(Ref(t.Name, r.GetString(0)));
        return res;
    }
}
