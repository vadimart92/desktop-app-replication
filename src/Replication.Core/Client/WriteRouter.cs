using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Replication.Model;
using static Replication.Model.Wire;

namespace Replication.Client;

/// <summary>
/// EF interceptor for the client's DbContext (8.1): an edit of a row with <c>InstanceId = X</c> is written to
/// the replica and, in the same transaction, to the outbox of X. Rows of <c>local</c> and archives are not touched.
/// </summary>
public sealed class WriteRouter : SaveChangesInterceptor
{
    private sealed record Change(SyncTable Table, string Instance, string Pk, OutboxKind Kind, List<string> Columns);

    private sealed class Pending
    {
        public List<Change> Changes { get; } = [];
        public IDbContextTransaction? OwnTransaction { get; set; }
        public List<(SyncTable Table, string Instance, string Pk)> ArchiveMoves { get; } = [];
        public bool OpenedConnection { get; set; }
    }

    private readonly SyncModel _model;
    private readonly ConditionalWeakTable<DbContext, Pending> _pending = new();

    public WriteRouter(SyncModel model) => _model = model;

    /// <summary>Raised after an outbox change commits, with the instance it belongs to; the agent wakes and sends.</summary>
    public event Action<string>? OutboxChanged;

    /// <summary>Raised with a description of each outbox write, for the sample's event log.</summary>
    public event Action<string, string>? Routed;

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        if (eventData.Context is { } ctx && Collect(ctx) is { } p && ctx.Database.CurrentTransaction is null)
            p.OwnTransaction = ctx.Database.BeginTransaction();
        return result;
    }

    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
    {
        if (eventData.Context is { } ctx && Collect(ctx) is { } p && ctx.Database.CurrentTransaction is null)
            p.OwnTransaction = await ctx.Database.BeginTransactionAsync(ct);
        return result;
    }

    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        if (eventData.Context is { } ctx)
            Flush(ctx);
        return result;
    }

    public override ValueTask<int> SavedChangesAsync(SaveChangesCompletedEventData eventData, int result, CancellationToken ct = default)
    {
        if (eventData.Context is { } ctx)
            Flush(ctx);
        return ValueTask.FromResult(result);
    }

    public override void SaveChangesFailed(DbContextErrorEventData eventData)
    {
        if (eventData.Context is { } ctx && _pending.TryGetValue(ctx, out Pending? p))
        {
            p.OwnTransaction?.Rollback();
            p.OwnTransaction?.Dispose();
            if (p.OpenedConnection)
                ctx.Database.CloseConnection();
            _pending.Remove(ctx);
        }
    }

    public override Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken ct = default)
    {
        SaveChangesFailed(eventData);
        return Task.CompletedTask;
    }

    private Pending? Collect(DbContext ctx)
    {
        ctx.ChangeTracker.DetectChanges();
        var entries = ctx.ChangeTracker.Entries().ToList();
        var deleted = entries.Where(e => e.State == EntityState.Deleted && _model.ForType(e.Metadata.ClrType) is not null)
            .Select(e => (_model.ForType(e.Metadata.ClrType)!.Name, PkText((Guid)e.Property(SyncColumns.Key).OriginalValue!)))
            .ToHashSet();
        var changes = new List<Change>();
        var moves = new List<(SyncTable, string, string)>();
        bool opened = false;
        foreach (EntityEntry? e in entries)
        {
            if (_model.ForType(e.Metadata.ClrType) is not { } t)
                continue;
            string? instance = (string?)(e.State == EntityState.Deleted ? e.Property(SyncColumns.InstanceId).OriginalValue : e.Property(SyncColumns.InstanceId).CurrentValue);
            if (!SyncColumns.IsRemote(instance))
                continue;
            string pk = PkText((Guid)e.Property(SyncColumns.Key).CurrentValue!);
            var store = StoreObjectIdentifier.Table(t.Name, null);
            switch (e.State)
            {
                case EntityState.Added:
                    changes.Add(new Change(t, instance!, pk, OutboxKind.Create, [.. t.Columns]));
                    break;
                case EntityState.Modified:
                    var cols = e.Properties.Where(p => p.IsModified).Select(p => p.Metadata.GetColumnName(store)!).Where(t.HasColumn).ToList();
                    if (cols.Count > 0)
                        changes.Add(new Change(t, instance!, pk, OutboxKind.Patch, cols));
                    break;
                case EntityState.Deleted:
                    // only the parent goes to the outbox; the owner cascades by its own schema (8.7)
                    bool cascaded = t.ForeignKeys.Any(fk => fk.Cascade && e.Property(PropertyOf(e.Metadata, fk.Column, store)).OriginalValue is Guid pid
                                                          && deleted.Contains((fk.ParentTable, PkText(pid))));
                    changes.Add(new Change(t, instance!, pk, cascaded ? (OutboxKind)0 : OutboxKind.Delete, []));
                    // archived rows point to it (directly or below): EF must not delete it, the cascade would take the archive (11.6)
                    if (_model.ChildrenOf(t.Name).Any())
                    {
                        if (ctx.Database.GetDbConnection().State != System.Data.ConnectionState.Open)
                        {
                            ctx.Database.OpenConnection();
                            opened = true;
                        }
                        if (ArchiveGuard.HasArchiveBelow((SqliteConnection)ctx.Database.GetDbConnection(), null, _model, instance!, t, pk))
                        {
                            moves.Add((t, instance!, pk));
                            e.State = EntityState.Unchanged;
                        }
                    }
                    break;
            }
        }
        if (changes.Count == 0)
            return null;
        Pending p = _pending.GetOrCreateValue(ctx);
        p.Changes.Clear();
        p.Changes.AddRange(changes);
        p.ArchiveMoves.Clear();
        p.ArchiveMoves.AddRange(moves);
        p.OpenedConnection = opened;
        return p;
    }

    private static string PropertyOf(IEntityType et, string column, StoreObjectIdentifier store) =>
        et.GetProperties().First(p => p.GetColumnName(store) == column).Name;

    private void Flush(DbContext ctx)
    {
        if (!_pending.TryGetValue(ctx, out Pending? p))
            return;
        _pending.Remove(ctx);
        var conn = (SqliteConnection)ctx.Database.GetDbConnection();
        var tx = (SqliteTransaction?)ctx.Database.CurrentTransaction?.GetDbTransaction();
        try
        {
            foreach ((SyncTable? t, string? inst, string? pk) in p.ArchiveMoves)
            {
                if (ArchiveGuard.DeleteOrArchive(conn, tx, _model, inst, t, pk))
                    Routed?.Invoke(inst, $"{t.Name} {Short(pk)}: на нього посилаються архівні записи, тому в репліці він перенесений в архів, а видалення йде власнику");
            }

            foreach (Change? ch in p.Changes.Where(x => x.Kind != 0))
            {
                string what = ClientStore.Put(conn, tx, ch.Instance, ch.Table.Name, ch.Pk, ch.Kind, ch.Columns, OutboxClass.Interactive);
                Routed?.Invoke(ch.Instance, $"черга: {ch.Table.Name} {Short(ch.Pk)} → {what}");
            }
            if (p.Changes.Any(x => x.Kind is OutboxKind.Delete or 0))
            {
                foreach (string? inst in p.Changes.Select(x => x.Instance).Distinct())
                    DropOrphanEntries(conn, tx, _model, inst);
            }

            p.OwnTransaction?.Commit();
        }
        catch
        {
            p.OwnTransaction?.Rollback();
            throw;
        }
        finally
        {
            p.OwnTransaction?.Dispose();
            if (p.OpenedConnection)
                ctx.Database.CloseConnection();
        }
        foreach (string? inst in p.Changes.Select(x => x.Instance).Distinct())
            OutboxChanged?.Invoke(inst);
    }

    /// <summary>A local cascade removed child rows: their pending creates and patches have nothing left to send.</summary>
    internal static void DropOrphanEntries(SqliteConnection c, SqliteTransaction? tx, SyncModel model, string instance)
    {
        foreach (SyncTable t in model.Tables)
        {
            c.Exec($"""
                DELETE FROM _sync_outbox WHERE instance = @i AND tbl = @t AND kind IN (1, 2) AND sent = 0
                AND NOT EXISTS (SELECT 1 FROM {Q(t.Name)} x WHERE x.Id = _sync_outbox.pk AND x.InstanceId = @i)
                """, tx, ("@i", instance), ("@t", t.Name));
        }
    }
}
