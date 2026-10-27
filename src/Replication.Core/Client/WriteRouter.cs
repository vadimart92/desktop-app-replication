using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Replication.Model;
using static Replication.Model.Wire;

namespace Replication.Client;

/// <summary>
/// EF interceptor for the client's DbContext (8.1): an edit of a row with <c>InstanceId = X</c> is written to
/// the replica and, in the same transaction, to the outbox of X. The client's own rows (<c>InstanceId</c> NULL) and archives are not touched.
/// </summary>
/// <remarks>
/// Register it after every other SaveChanges interceptor (after the application's own). EF tells no
/// interceptor when one throws in SavingChanges, so an interceptor after the router that rejects the save would leave
/// the router's write transaction, and with it the write lock on the client database, open until the next save on that context.
/// Each outbox write is logged at Information as <c>[instance] …</c>.
/// </remarks>
public sealed class WriteRouter(SyncModel model) : SaveChangesInterceptor
{
    private readonly ConditionalWeakTable<DbContext, Pending> _pending = new();

    private sealed record Change(SyncTable Table, string Instance, string Pk, OutboxKind? Kind, List<string> Columns);

    /// <summary>A delete kept from EF because archived rows point to the row: the router moves the row to the archive instead (11.6).</summary>
    private sealed record ArchiveMove(SyncTable Table, string Instance, string Pk, EntityEntry Entry);

    private sealed record Pending(
        IReadOnlyList<Change> Changes, IReadOnlyList<ArchiveMove> ArchiveMoves,
        bool OpenedConnection, IDbContextTransaction? OwnTransaction);

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        if (eventData.Context is { } ctx && AbortLeftoverAndCollect(ctx) is { } p)
        {
            if (p.OwnTransaction is null && ctx.Database.CurrentTransaction is null)
                p = p with { OwnTransaction = ctx.Database.BeginTransaction() };
            _pending.AddOrUpdate(ctx, p);
        }
        return result;
    }

    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
    {
        if (eventData.Context is { } ctx && AbortLeftoverAndCollect(ctx) is { } p)
        {
            if (p.OwnTransaction is null && ctx.Database.CurrentTransaction is null)
                p = p with { OwnTransaction = await ctx.Database.BeginTransactionAsync(ct) };
            _pending.AddOrUpdate(ctx, p);
        }
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
        if (eventData.Context is { } ctx)
            Abort(ctx);
    }

    public override Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken ct = default)
    {
        SaveChangesFailed(eventData);
        return Task.CompletedTask;
    }

    /// <summary>EF reports a concurrency failure here, not in SaveChangesFailed (8.1); one an earlier interceptor suppressed is no failure.</summary>
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

    /// <summary>
    /// EF reports a failure inside SavingChanges (a cancelled BEGIN, an interceptor wrongly registered after the router)
    /// to no interceptor: what such a save left behind is dropped before the next save collects its changes (8.1).
    /// </summary>
    private Pending? AbortLeftoverAndCollect(DbContext ctx)
    {
        Abort(ctx);
        return Collect(ctx);
    }

    /// <summary>
    /// Ends a save that did not complete: rolls back the router's own transaction and forgets its changes. Safe to call twice.
    /// A leftover transaction the app rolled back, or a pooled context's reset disposed, is no longer the router's to end.
    /// </summary>
    private void Abort(DbContext ctx)
    {
        if (!_pending.TryGetValue(ctx, out Pending? p))
            return;
        _pending.Remove(ctx);
        RestoreDeletes(p.ArchiveMoves);
        if (p.OwnTransaction is { } own && ctx.Database.CurrentTransaction == own)
        {
            own.Rollback();
            own.Dispose();
            // The router opens the connection only together with its own transaction.
            if (p.OpenedConnection)
                ctx.Database.CloseConnection();
        }
    }

    /// <summary>A delete kept from EF for an archive move goes back to EF, so a retry of the save collects it again (11.6).</summary>
    private static void RestoreDeletes(IEnumerable<ArchiveMove> moves)
    {
        foreach (ArchiveMove m in moves)
        {
            if (m.Entry.State == EntityState.Unchanged)
                m.Entry.State = EntityState.Deleted;
        }
    }

    private Pending? Collect(DbContext ctx)
    {
        ctx.ChangeTracker.DetectChanges();
        List<EntityEntry> entries = ctx.ChangeTracker.Entries().ToList();
        HashSet<(string Name, string)> deleted = entries.Where(e => e.State == EntityState.Deleted && model.ForType(e.Metadata.ClrType) is not null)
            .Select(e => (model.ForType(e.Metadata.ClrType)!.Name, PkText((Guid)e.Property(SyncColumns.Key).OriginalValue!)))
            .ToHashSet();
        var changes = new List<Change>();
        var moves = new List<ArchiveMove>();
        bool opened = false;
        IDbContextTransaction? own = null;
        try
        {
            foreach (EntityEntry e in entries)
            {
                if (model.ForType(e.Metadata.ClrType) is not { } t)
                    continue;
                string? instance = (string?)(e.State == EntityState.Deleted ? e.Property(SyncColumns.InstanceId).OriginalValue : e.Property(SyncColumns.InstanceId).CurrentValue);
                StoreObjectIdentifier store = StoreObjectIdentifier.Table(t.Name, null);
                // Local rows too: no source may point into another one (5.4).
                if (e.State is EntityState.Added or EntityState.Modified)
                    CheckSources(ctx, entries, t, e, instance, store);
                if (!SyncColumns.IsRemote(instance))
                    continue;
                string pk = PkText((Guid)e.Property(SyncColumns.Key).CurrentValue!);
                switch (e.State)
                {
                    case EntityState.Added:
                        changes.Add(new Change(t, instance!, pk, OutboxKind.Create, []));
                        break;
                    case EntityState.Modified:
                        List<string> cols = ModifiedProperties(e).Where(p => p.IsModified).Select(p => p.Metadata.GetColumnName(store)!).Where(t.HasColumn).ToList();
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
                            // opened / own mark what this router opened or began, so only those are closed, committed or rolled back (Pending, catch).
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
                                moves.Add(new ArchiveMove(t, instance!, pk, e));
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
            RestoreDeletes(moves);
            own?.Rollback();
            own?.Dispose();
            if (opened)
                ctx.Database.CloseConnection();
            throw;
        }
        return changes.Count == 0 ? null : new Pending(changes, moves, opened, own);
    }

    /// <summary>All properties of the entry (not only modified ones; the caller filters by <c>IsModified</c>), including the members of its complex properties at any depth.</summary>
    private static IEnumerable<PropertyEntry> ModifiedProperties(EntityEntry e)
    {
        foreach (PropertyEntry p in e.Properties)
            yield return p;
        foreach (ComplexPropertyEntry c in e.ComplexProperties)
        {
            foreach (PropertyEntry p in Members(c))
                yield return p;
        }

        static IEnumerable<PropertyEntry> Members(ComplexPropertyEntry c)
        {
            foreach (PropertyEntry p in c.Properties)
                yield return p;
            foreach (ComplexPropertyEntry n in c.ComplexProperties)
            {
                foreach (PropertyEntry p in Members(n))
                    yield return p;
            }
        }
    }

    private static string PropertyOf(IEntityType et, string column, StoreObjectIdentifier store) =>
        et.GetProperties().First(p => p.GetColumnName(store) == column).Name;

    /// <summary>
    /// A row may point only to rows of its own source (5.4): local rows (<c>InstanceId</c> NULL) to local rows, a replica's
    /// rows to that instance's rows. An owner's delete then never reaches another source's rows, by cascade or by orphaning.
    /// </summary>
    private void CheckSources(DbContext ctx, List<EntityEntry> entries, SyncTable t, EntityEntry e, string? instance, StoreObjectIdentifier store)
    {
        bool moved = e.State == EntityState.Modified && e.Property(SyncColumns.InstanceId).IsModified;
        foreach (SyncForeignKey fk in t.ForeignKeys)
        {
            PropertyEntry p = e.Property(PropertyOf(e.Metadata, fk.Column, store));
            if (p.CurrentValue is not Guid pid || (e.State == EntityState.Modified && !p.IsModified && !moved) || !model.TryGet(fk.ParentTable, out SyncTable? pt))
                continue;
            // A parent the database does not have is left to the FK constraint.
            if (ParentInstance(ctx, entries, pt, pid) is (true, var parent) && !SyncColumns.MayReference(instance, parent))
            {
                throw new InvalidOperationException(
                    $"{t.Name} {PkText((Guid)e.Property(SyncColumns.Key).CurrentValue!)} ({SourceText(instance)}) cannot point to {pt.Name} {PkText(pid)} ({SourceText(parent)}): a row may point only to rows of its own source");
            }
        }
    }

    // Display text for errors only: the client's own data has InstanceId NULL, there is no "local" value in the database.
    private static string SourceText(string? instance) => string.IsNullOrEmpty(instance) ? "local" : instance;

    /// <summary>The parent's <c>InstanceId</c>: from this save when the parent is in it, otherwise from the database.</summary>
    private (bool Found, string? Instance) ParentInstance(DbContext ctx, List<EntityEntry> entries, SyncTable parent, Guid id)
    {
        foreach (EntityEntry pe in entries)
        {
            if (pe.State != EntityState.Deleted && model.ForType(pe.Metadata.ClrType) == parent && pe.Property(SyncColumns.Key).CurrentValue is Guid k && k == id)
                return (true, (string?)pe.Property(SyncColumns.InstanceId).CurrentValue);
        }
        ctx.Database.OpenConnection();
        try
        {
            var tx = (SqliteTransaction?)ctx.Database.CurrentTransaction?.GetDbTransaction();
            using SqliteCommand cmd = ((SqliteConnection)ctx.Database.GetDbConnection()).Cmd($"SELECT InstanceId FROM {Q(parent.Name)} WHERE Id = @id", tx, ("@id", PkText(id)));
            using SqliteDataReader r = cmd.ExecuteReader();
            return r.Read() ? (true, r.IsDBNull(0) ? null : r.GetString(0)) : (false, null);
        }
        finally
        {
            ctx.Database.CloseConnection();
        }
    }

    private void Flush(DbContext ctx)
    {
        if (!_pending.TryGetValue(ctx, out Pending? p))
            return;
        _pending.Remove(ctx);

        ILogger<WriteRouter> log = ctx.GetService<ILoggerFactory>().CreateLogger<WriteRouter>();

        var conn = (SqliteConnection)ctx.Database.GetDbConnection();
        var tx = (SqliteTransaction?)ctx.Database.CurrentTransaction?.GetDbTransaction();
        try
        {
            foreach (ArchiveMove m in p.ArchiveMoves)
            {
                if (ArchiveGuard.DeleteOrArchive(conn, tx, model, m.Instance, m.Table, m.Pk))
                    log.LogInformation("[{Instance}] {Table} {Pk}: archived rows point to it, so the replica moved it to the archive and the delete goes to the owner", m.Instance, m.Table.Name, Short(m.Pk));
            }

            foreach (Change ch in p.Changes)
            {
                if (ch.Kind is { } kind)
                {
                    string what = ClientStore.Put(conn, tx, ch.Instance, ch.Table.Name, ch.Pk, kind, ch.Columns, OutboxClass.Interactive);
                    log.LogInformation("[{Instance}] queue: {Table} {Pk} → {What}", ch.Instance, ch.Table.Name, Short(ch.Pk), what);
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
    }
}
