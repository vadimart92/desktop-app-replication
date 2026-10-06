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
public sealed class WriteRouter(SyncModel model) : SaveChangesInterceptor
{
    private readonly ConditionalWeakTable<DbContext, Pending> _pending = new();

    private sealed record Change(SyncTable Table, string Instance, string Pk, OutboxKind? Kind, List<string> Columns);

    private sealed record Pending(
        IReadOnlyList<Change> Changes, IReadOnlyList<(SyncTable Table, string Instance, string Pk)> ArchiveMoves,
        bool OpenedConnection, IDbContextTransaction? OwnTransaction);

    /// <summary>Raised after an outbox change commits, with the instance it belongs to; the agent wakes and sends.</summary>
    public event Action<string>? OutboxChanged;

    /// <summary>Raised with a description of each outbox write, for the sample's event log.</summary>
    public event Action<string, string>? Routed;

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        if (eventData.Context is { } ctx && Start(ctx) is { } p)
        {
            if (p.OwnTransaction is null && ctx.Database.CurrentTransaction is null)
                p = p with { OwnTransaction = ctx.Database.BeginTransaction() };
            _pending.AddOrUpdate(ctx, p);
        }
        return result;
    }

    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
    {
        if (eventData.Context is { } ctx && Start(ctx) is { } p)
        {
            if (p.OwnTransaction is null && ctx.Database.CurrentTransaction is null)
                p = p with { OwnTransaction = await ctx.Database.BeginTransactionAsync(ct) };
            _pending.AddOrUpdate(ctx, p);
        }
        return result;
    }

    /// <summary>
    /// EF reports a failure inside SavingChanges (an interceptor after the router, a cancelled BEGIN) to no interceptor:
    /// what such a save left behind is dropped before the next save collects its changes (8.1).
    /// </summary>
    private Pending? Start(DbContext ctx)
    {
        Abort(ctx);
        return Collect(ctx);
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
        if (eventData.Context is { } ctx)
            Abort(ctx);
    }

    public override Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken ct = default)
    {
        SaveChangesFailed(eventData);
        return Task.CompletedTask;
    }

    /// <summary>EF reports a concurrency failure here, not in SaveChangesFailed; an interceptor after the router that suppresses it finds the save rolled back (8.1).</summary>
    public override InterceptionResult ThrowingConcurrencyException(ConcurrencyExceptionEventData eventData, InterceptionResult result)
    {
        if (!result.IsSuppressed && eventData.Context is { } ctx)
            Abort(ctx);
        return result;
    }

    public override ValueTask<InterceptionResult> ThrowingConcurrencyExceptionAsync(ConcurrencyExceptionEventData eventData, InterceptionResult result, CancellationToken ct = default) =>
        ValueTask.FromResult(ThrowingConcurrencyException(eventData, result));

    public override void SaveChangesCanceled(DbContextEventData eventData)
    {
        if (eventData.Context is { } ctx)
            Abort(ctx);
    }

    public override Task SaveChangesCanceledAsync(DbContextEventData eventData, CancellationToken ct = default)
    {
        SaveChangesCanceled(eventData);
        return Task.CompletedTask;
    }

    /// <summary>Ends a save that did not complete: rolls back the router's own transaction and forgets its changes. Safe to call twice.</summary>
    private void Abort(DbContext ctx)
    {
        if (!_pending.TryGetValue(ctx, out Pending? p))
            return;
        _pending.Remove(ctx);
        p.OwnTransaction?.Rollback();
        p.OwnTransaction?.Dispose();
        if (p.OpenedConnection)
            ctx.Database.CloseConnection();
    }

    private Pending? Collect(DbContext ctx)
    {
        ctx.ChangeTracker.DetectChanges();
        List<EntityEntry> entries = ctx.ChangeTracker.Entries().ToList();
        HashSet<(string Name, string)> deleted = entries.Where(e => e.State == EntityState.Deleted && model.ForType(e.Metadata.ClrType) is not null)
            .Select(e => (model.ForType(e.Metadata.ClrType)!.Name, PkText((Guid)e.Property(SyncColumns.Key).OriginalValue!)))
            .ToHashSet();
        var changes = new List<Change>();
        var moves = new List<(SyncTable, string, string)>();
        bool opened = false;
        IDbContextTransaction? own = null;
        try
        {
            foreach (EntityEntry e in entries)
            {
                if (model.ForType(e.Metadata.ClrType) is not { } t)
                    continue;
                string? instance = (string?)(e.State == EntityState.Deleted ? e.Property(SyncColumns.InstanceId).OriginalValue : e.Property(SyncColumns.InstanceId).CurrentValue);
                if (!SyncColumns.IsRemote(instance))
                    continue;
                string pk = PkText((Guid)e.Property(SyncColumns.Key).CurrentValue!);
                StoreObjectIdentifier store = StoreObjectIdentifier.Table(t.Name, null);
                switch (e.State)
                {
                    case EntityState.Added:
                        changes.Add(new Change(t, instance!, pk, OutboxKind.Create, []));
                        break;
                    case EntityState.Modified:
                        List<string> cols = e.Properties.Where(p => p.IsModified).Select(p => p.Metadata.GetColumnName(store)!).Where(t.HasColumn).ToList();
                        if (cols.Count > 0)
                            changes.Add(new Change(t, instance!, pk, OutboxKind.Patch, cols));
                        break;
                    case EntityState.Deleted:
                        // Only the parent goes to the outbox; the owner cascades by its own schema (8.7).
                        bool cascaded = t.ForeignKeys.Any(fk => fk.Cascade && e.Property(PropertyOf(e.Metadata, fk.Column, store)).OriginalValue is Guid pid
                                                              && deleted.Contains((fk.ParentTable, PkText(pid))));
                        changes.Add(new Change(t, instance!, pk, cascaded ? null : OutboxKind.Delete, []));
                        // Archived rows point to it (directly or below): EF must not delete it, the cascade would take the archive (11.6).
                        if (model.ChildrenOf(t.Name).Any())
                        {
                            if (ctx.Database.GetDbConnection().State != System.Data.ConnectionState.Open)
                            {
                                ctx.Database.OpenConnection();
                                opened = true;
                            }
                            // The check and EF's DELETE share one write transaction, so an archive move cannot commit between them.
                            if (ctx.Database.CurrentTransaction is null)
                                own = ctx.Database.BeginTransaction();
                            var tx = (SqliteTransaction?)ctx.Database.CurrentTransaction?.GetDbTransaction();
                            if (ArchiveGuard.HasArchiveBelow((SqliteConnection)ctx.Database.GetDbConnection(), tx, model, instance!, t, pk))
                            {
                                moves.Add((t, instance!, pk));
                                e.State = EntityState.Unchanged;
                            }
                        }
                        break;
                }
            }
        }
        catch
        {
            // EF does not call SaveChangesFailed for a failure inside SavingChanges.
            own?.Rollback();
            own?.Dispose();
            if (opened)
                ctx.Database.CloseConnection();
            throw;
        }
        return changes.Count == 0 ? null : new Pending(changes, moves, opened, own);
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
            foreach ((SyncTable t, string inst, string pk) in p.ArchiveMoves)
            {
                if (ArchiveGuard.DeleteOrArchive(conn, tx, model, inst, t, pk))
                    Routed?.Invoke(inst, $"{t.Name} {Short(pk)}: на нього посилаються архівні записи, тому в репліці він перенесений в архів, а видалення йде власнику");
            }

            foreach (Change ch in p.Changes)
            {
                if (ch.Kind is { } kind)
                {
                    string what = ClientStore.Put(conn, tx, ch.Instance, ch.Table.Name, ch.Pk, kind, ch.Columns, OutboxClass.Interactive);
                    Routed?.Invoke(ch.Instance, $"черга: {ch.Table.Name} {Short(ch.Pk)} → {what}");
                }
            }
            if (p.Changes.Any(x => x.Kind is OutboxKind.Delete or null))
            {
                foreach (string inst in p.Changes.Select(x => x.Instance).Distinct())
                    ClientStore.DropOrphanEntries(conn, tx, model, inst);
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
        foreach (string inst in p.Changes.Select(x => x.Instance).Distinct())
            OutboxChanged?.Invoke(inst);
    }
}
