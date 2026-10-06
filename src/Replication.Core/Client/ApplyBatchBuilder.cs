using Microsoft.Data.Sqlite;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Client;

/// <summary>
/// Assembles one Apply batch from the outbox within a byte budget (6.5): retries first, then interactive actions,
/// then bulk ones, seq given at the first send (8.5), closed over FK dependencies (8.4).
/// </summary>
internal static class ApplyBatchBuilder
{
    /// <param name="applyBudget">Read only when the outbox has something to send.</param>
    public static (ApplyRequest? Request, Dictionary<long, OutboxEntry> Sent) Build(
        SqliteConnection c, SyncModel model, string clientId, string address, string instance, Func<int> applyBudget)
    {
        using SqliteTransaction tx = c.BeginTransaction();
        List<OutboxEntry> all = ClientStore.Entries(c, tx, instance, pendingOnly: true)
            .OrderBy(e => e.Seq is null ? 1 : 0).ThenBy(e => e.Seq ?? 0).ThenBy(e => (int)e.Class).ThenBy(e => e.Id).ToList();
        if (all.Count == 0)
            return (null, []);
        var unsentCreates = new Dictionary<(string Table, string Pk), OutboxEntry>();
        foreach (OutboxEntry x in all)
        {
            if (x is { Kind: OutboxKind.Create, Sent: OutboxSendState.Waiting, Pk: { } p })
                unsentCreates.TryAdd((x.Table, p), x);
        }
        HashSet<string> createTables = unsentCreates.Keys.Select(k => k.Table).ToHashSet();

        var batch = new List<OutboxEntry>();
        var actions = new Dictionary<long, Protocol.Action?>();
        var visiting = new HashSet<long>();
        int budget = applyBudget();
        int size = 0;

        void Add(OutboxEntry e)
        {
            if (actions.ContainsKey(e.Id) || !visiting.Add(e.Id))
                return;
            if (model.TryGet(e.Table, out SyncTable? t) && e.Kind is OutboxKind.Create or OutboxKind.Patch && e.Pk is not null)
            {
                foreach (SyncForeignKey fk in t.ForeignKeys.Where(f => e.Kind == OutboxKind.Create || e.Columns.Contains(f.Column)))
                {
                    // A create this row points to must go in the same or an earlier batch (8.4).
                    if (!createTables.Contains(fk.ParentTable))
                        continue;
                    string? pid = c.Scalar<string>($"SELECT {Q(fk.Column)} FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @i", tx, ("@id", e.Pk), ("@i", instance));
                    if (pid is not null && unsentCreates.TryGetValue((fk.ParentTable, pid), out OutboxEntry? pe))
                        Add(pe);
                }
            }

            Protocol.Action? a = ToAction(c, tx, model, instance, e);
            actions[e.Id] = a;
            batch.Add(e);
            size += a?.CalculateSize() ?? 0;
        }

        foreach (OutboxEntry e in all)
        {
            if (size >= budget && batch.Count > 0)
                break;
            Add(e);
        }

        long next = c.Scalar<long>("SELECT next_seq FROM _sync_instances WHERE address = @a", tx, ("@a", address));
        var sent = new Dictionary<long, OutboxEntry>();
        foreach (OutboxEntry e in batch)
        {
            // Seq is given at the first send, so the owner always sees seq ascending (8.5).
            long seq = e.Seq ?? next++;
            c.Exec("UPDATE _sync_outbox SET seq = @s, sent = 1 WHERE id = @id", tx, ("@s", seq), ("@id", e.Id));
            if (actions[e.Id] is { } a)
                a.Seq = seq;
            sent[seq] = e with { Seq = seq, Sent = OutboxSendState.InFlight };
        }
        c.Exec("UPDATE _sync_instances SET next_seq = @n WHERE address = @a", tx, ("@n", next), ("@a", address));
        tx.Commit();

        var req = new ApplyRequest { ClientId = clientId };
        req.Actions.AddRange(batch.Select(e => actions[e.Id]).OfType<Protocol.Action>().OrderBy(a => a.Seq));
        return (req.Actions.Count > 0 ? req : null, sent);
    }

    /// <summary>Values are read from the replica at send time, so the latest state goes and edits collapse by themselves (5.4).</summary>
    private static Protocol.Action? ToAction(SqliteConnection c, SqliteTransaction tx, SyncModel model, string instance, OutboxEntry e)
    {
        var a = new Protocol.Action { Tbl = e.Table, Kind = (ActionKind)e.Kind };
        switch (e.Kind)
        {
            case OutboxKind.Create or OutboxKind.Patch:
            {
                SyncTable t = model[e.Table];
                List<string> cols = e.Kind == OutboxKind.Create ? t.Columns.ToList() : e.Columns.Where(t.HasColumn).ToList();
                using SqliteCommand cmd = c.Cmd($"SELECT {string.Join(", ", cols.Select(Q).DefaultIfEmpty("1"))} FROM {Q(t.Name)} WHERE Id = @id AND InstanceId = @i", tx, ("@id", e.Pk), ("@i", instance));
                using SqliteDataReader r = cmd.ExecuteReader();
                if (!r.Read())
                {
                    // The row is gone: nothing to send, the reply's applied_up_to_seq clears the entry.
                    return null;
                }

                a.Pk = PkBytes(e.Pk!);
                for (int i = 0; i < cols.Count; i++)
                {
                    a.Columns.Add(cols[i]);
                    a.Values.Add(ToValue(r.Raw(i)));
                }
                return a;
            }
            case OutboxKind.Delete:
                a.Pk = PkBytes(e.Pk!);
                return a;
            case OutboxKind.Archive:
                a.ExpectedVersion = e.ExpectedVersion ?? 0;
                a.Rows.AddRange(ArchiveSet.Parse(e.Predicate).Select(x => Ref(x.Table, x.Pk)));
                return a;
            case OutboxKind.PredicateDelete:
                a.ExpectedVersion = e.ExpectedVersion ?? 0;
                a.Predicate.AddRange(Predicate.Parse(e.Predicate)!.ToWire());
                return a;
        }
        return null;
    }
}
