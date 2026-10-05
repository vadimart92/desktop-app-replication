using System.Text;
using Replication.Model;
using static Replication.Model.Wire;

namespace Replication.Owner;

/// <summary>Triggers generated from the EF model for every replicated table (5.2).</summary>
internal static class TriggerSql
{
    public static IEnumerable<string> For(SyncTable t)
    {
        string table = Q(t.Name);
        string name = t.Name.Replace("'", "''");
        string now = "CAST(strftime('%s','now') AS INTEGER) + (SELECT clock_offset FROM _sync_meta)";

        // (OLD.c1 IS NOT NEW.c1) << 0 | (OLD.c2 IS NOT NEW.c2) << 1 | ...; service columns are not counted.
        var changed = new StringBuilder();
        for (int i = 0; i < t.Columns.Count; i++)
        {
            if (i > 0)
                changed.Append(" | ");
            changed.Append($"((OLD.{Q(t.Columns[i])} IS NOT NEW.{Q(t.Columns[i])}) << {i})");
        }
        if (t.Columns.Count == 0)
            changed.Append('0');
        string floor = $"(SELECT floor FROM _sync_floor WHERE tbl = '{name}')";

        yield return $"DROP TRIGGER IF EXISTS \"_sync_{t.Name}_ins\";";
        yield return $"DROP TRIGGER IF EXISTS \"_sync_{t.Name}_upd\";";
        yield return $"DROP TRIGGER IF EXISTS \"_sync_{t.Name}_del\";";

        yield return $"""
            CREATE TRIGGER "_sync_{t.Name}_ins" AFTER INSERT ON {table} BEGIN
              UPDATE _sync_meta SET version = version + 1;
              UPDATE {table} SET SyncVersion = (SELECT version FROM _sync_meta), SyncBase = 0, SyncMask = {t.FullMask}
              WHERE Id = NEW.Id;
              DELETE FROM _sync_tombstones WHERE tbl = '{name}' AND pk = NEW.Id;
            END;
            """;

        // WHEN keeps the trigger from firing on its own version/base/mask update.
        yield return $"""
            CREATE TRIGGER "_sync_{t.Name}_upd" AFTER UPDATE ON {table}
            WHEN NEW.SyncVersion = OLD.SyncVersion BEGIN
              UPDATE _sync_meta SET version = version + 1;
              UPDATE {table} SET
                SyncVersion = (SELECT version FROM _sync_meta),
                SyncBase = CASE WHEN {floor} >= OLD.SyncVersion THEN OLD.SyncVersion ELSE OLD.SyncBase END,
                SyncMask = CASE WHEN {floor} >= OLD.SyncVersion THEN ({changed}) ELSE OLD.SyncMask | ({changed}) END,
                SyncOrigin = CASE WHEN NEW.SyncOrigin IS OLD.SyncOrigin THEN NULL ELSE NEW.SyncOrigin END
              WHERE Id = NEW.Id;
            END;
            """;

        yield return $"""
            CREATE TRIGGER "_sync_{t.Name}_del" AFTER DELETE ON {table} BEGIN
              UPDATE _sync_meta SET version = version + 1;
              INSERT OR REPLACE INTO _sync_tombstones(tbl, pk, version, deleted_at, origin)
              VALUES ('{name}', OLD.Id, (SELECT version FROM _sync_meta), {now}, NULL);
            END;
            """;
    }
}
