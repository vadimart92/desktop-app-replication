using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Replication.Model;

namespace Sample.Domain;

/// <summary>One DbContext for the owner and the client (design 5.4).</summary>
public sealed class SampleDbContext(DbContextOptions<SampleDbContext> options) : DbContext(options)
{
    public DbSet<Category> Categories => Set<Category>();
    public DbSet<Item> Items => Set<Item>();
    public DbSet<LogEntry> Log => Set<LogEntry>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Category>(e =>
        {
            e.ToTable("Category");
            e.Property(x => x.Name).IsRequired();
        });
        b.Entity<Item>(e =>
        {
            e.ToTable("Item");
            e.Property(x => x.Name).IsRequired();
            e.HasOne(x => x.Category).WithMany(x => x.Items).HasForeignKey(x => x.CategoryId).OnDelete(DeleteBehavior.Cascade);
        });
        b.Entity<LogEntry>(e => e.ToTable("Log"));

        // The only line the replication needs in the model: every BaseEntity is replicated.
        b.UseReplication(t => typeof(BaseEntity).IsAssignableFrom(t));
    }

    public static SampleDbContext Open(string path, params IInterceptor[] interceptors)
    {
        DbContextOptions<SampleDbContext> options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseSqlite($"Data Source={path};Pooling=True;Default Timeout=30")
            .AddInterceptors([new StampInterceptor(), .. interceptors])
            .Options;
        return new SampleDbContext(options);
    }
}

/// <summary>Sets CreatedOn / ModifiedOn, as an application usually does.</summary>
public sealed class StampInterceptor : SaveChangesInterceptor
{
    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        Stamp(eventData.Context);
        return result;
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
    {
        Stamp(eventData.Context);
        return ValueTask.FromResult(result);
    }

    private static void Stamp(DbContext? ctx)
    {
        if (ctx is null)
            return;
        DateTime now = DateTime.UtcNow;
        foreach (EntityEntry<BaseEntity> e in ctx.ChangeTracker.Entries<BaseEntity>())
        {
            if (e.State == EntityState.Added)
                e.Entity.CreatedOn = e.Entity.ModifiedOn = now;
            else if (e.State == EntityState.Modified)
                e.Entity.ModifiedOn = now;
        }
    }
}
