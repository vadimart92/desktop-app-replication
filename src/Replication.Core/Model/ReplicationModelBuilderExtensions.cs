using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Replication.Model;

public static class ReplicationModelBuilderExtensions
{
    public const string ReplicatedAnnotation = "Replication:Replicated";

    /// <summary>
    /// Marks entity types as replicated and adds the service columns as shadow properties, so the
    /// entity classes (for example a BaseEntity with Id, CreatedOn, ModifiedOn) stay as they are.
    /// Call it at the end of OnModelCreating. The same DbContext is used on the owner and on the client (5.4).
    /// </summary>
    /// <remarks>
    /// <c>SyncVersion</c>, <c>SyncBase</c>, <c>SyncMask</c>, <c>SyncOrigin</c> are never written by EF: the owner's
    /// triggers and the sync agent own them (5.2). The table declares its triggers, so EF does not use RETURNING (5.2).
    /// </remarks>
    public static ModelBuilder UseReplication(this ModelBuilder modelBuilder, Func<Type, bool>? include = null)
    {
        foreach (IMutableEntityType? et in modelBuilder.Model.GetEntityTypes().ToList())
        {
            if (et.IsOwned() || et.ClrType == typeof(Dictionary<string, object>))
                continue;
            if (include is not null && !include(et.ClrType))
                continue;

            EntityTypeBuilder b = modelBuilder.Entity(et.ClrType);
            et.SetAnnotation(ReplicatedAnnotation, true);

            ServiceColumn(b.Property<long>(SyncColumns.Version).HasDefaultValue(0L));
            ServiceColumn(b.Property<long>(SyncColumns.Base).HasDefaultValue(0L));
            ServiceColumn(b.Property<long>(SyncColumns.Mask).HasDefaultValue(0L));
            ServiceColumn(b.Property<string?>(SyncColumns.Origin));
            b.Property<string?>(SyncColumns.InstanceId);

            b.HasIndex(SyncColumns.Version);
            b.HasIndex(SyncColumns.InstanceId);

            string table = et.GetTableName()!;
            b.ToTable(table, t =>
            {
                t.HasTrigger($"_sync_{table}_ins");
                t.HasTrigger($"_sync_{table}_upd");
                t.HasTrigger($"_sync_{table}_del");
            });
        }
        return modelBuilder;
    }

    private static void ServiceColumn(PropertyBuilder p)
    {
        p.ValueGeneratedOnAddOrUpdate();
        p.Metadata.SetBeforeSaveBehavior(PropertySaveBehavior.Ignore);
        p.Metadata.SetAfterSaveBehavior(PropertySaveBehavior.Ignore);
    }

    /// <summary>Sets the replica a new row belongs to on the client (an instance id, <c>local</c> or an archive).</summary>
    public static void SetInstance(this EntityEntry entry, string instanceId) =>
        entry.Property(SyncColumns.InstanceId).CurrentValue = instanceId;

    public static string? GetInstance(this EntityEntry entry) =>
        (string?)entry.Property(SyncColumns.InstanceId).CurrentValue;

    public static long GetSyncVersion(this EntityEntry entry) =>
        (long)(entry.Property(SyncColumns.Version).CurrentValue ?? 0L);
}
