using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Replication.Model;

namespace Replication.Tests.Integration.Migrations;

public abstract class ShopEntity
{
    public Guid Id { get; set; } = Guid.NewGuid();
}

public sealed class Shelf : ShopEntity
{
    public string Name { get; set; } = "";
}

public sealed class Product : ShopEntity
{
    public string Name { get; set; } = "";
    public long Price { get; set; }

    /// <summary>Schema 2. It sorts before Name, Price and ShelfId, so their bits in <c>SyncMask</c> move up by one.</summary>
    public long Discount { get; set; }

    public Guid ShelfId { get; set; }
    public Shelf? Shelf { get; set; }
}

/// <summary>Schema 2: a new replicated table.</summary>
public sealed class Tag : ShopEntity
{
    public string Label { get; set; } = "";
    public Guid ProductId { get; set; }
    public Product? Product { get; set; }
}

/// <summary>
/// An application whose schema evolves through EF migrations while it replicates. <see cref="Schema"/> picks the
/// model of one application version: 1 is the <c>Initial</c> migration, 2 is <c>DiscountAndTags</c>, which adds
/// <see cref="Product.Discount"/>, adds the <see cref="Tag"/> table and alters <c>Product.Name</c>, an operation
/// SQLite carries out by rebuilding the table (which drops its triggers, design 5.2).
/// </summary>
/// <remarks>
/// The migrations in this folder were generated with <c>dotnet ef migrations add</c> against <see cref="DesignTimeFactory"/>,
/// with <c>SHOP_SCHEMA=1</c> for <c>Initial</c> and the default (latest) for <c>DiscountAndTags</c>.
/// </remarks>
public sealed class ShopDbContext(DbContextOptions<ShopDbContext> options, int schema) : DbContext(options)
{
    public const int Latest = 2;

    /// <summary>The last migration of each schema version, for <c>IMigrator.Migrate</c>.</summary>
    public static string MigrationOf(int schema) => schema switch
    {
        1 => "Initial",
        2 => "DiscountAndTags",
        _ => throw new ArgumentOutOfRangeException(nameof(schema)),
    };

    public int Schema { get; } = schema;

    public DbSet<Shelf> Shelves => Set<Shelf>();
    public DbSet<Product> Products => Set<Product>();

    /// <summary>Schema 2. A method, not a DbSet property: EF would add the type of a DbSet property to every schema.</summary>
    public DbSet<Tag> Tags() => Set<Tag>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Shelf>(e =>
        {
            e.ToTable("Shelf");
            e.Property(x => x.Name).IsRequired();
        });
        b.Entity<Product>(e =>
        {
            e.ToTable("Product");
            // Schema 2 makes Name optional: SQLite cannot alter a column, so EF rebuilds the table.
            e.Property(x => x.Name).IsRequired(Schema < 2);
            e.HasOne(x => x.Shelf).WithMany().HasForeignKey(x => x.ShelfId).OnDelete(DeleteBehavior.Cascade);
            if (Schema < 2)
                e.Ignore(x => x.Discount);
        });
        if (Schema >= 2)
        {
            b.Entity<Tag>(e =>
            {
                e.ToTable("Tag");
                e.Property(x => x.Label).IsRequired();
                e.HasOne(x => x.Product).WithMany().HasForeignKey(x => x.ProductId).OnDelete(DeleteBehavior.Cascade);
            });
        }

        b.UseReplication();
    }

    public static ShopDbContext Open(string path, int schema, params IInterceptor[] interceptors) =>
        new(new DbContextOptionsBuilder<ShopDbContext>()
            .UseSqlite($"Data Source={path};Pooling=True;Default Timeout=30")
            .ReplaceService<IModelCacheKeyFactory, SchemaCacheKey>()
            // An older application version runs with a model behind the latest migration snapshot, as it would in the field.
            .ConfigureWarnings(w => w.Ignore(RelationalEventId.PendingModelChangesWarning))
            .AddInterceptors(interceptors)
            .Options, schema);

    /// <summary>One cached EF model per schema version.</summary>
    private sealed class SchemaCacheKey : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime) => (context.GetType(), ((ShopDbContext)context).Schema, designTime);
    }

    /// <summary>For <c>dotnet ef migrations add</c>; <c>SHOP_SCHEMA</c> picks the model.</summary>
    public sealed class DesignTimeFactory : IDesignTimeDbContextFactory<ShopDbContext>
    {
        public ShopDbContext CreateDbContext(string[] args) =>
            Open("design.db", int.TryParse(Environment.GetEnvironmentVariable("SHOP_SCHEMA"), out int s) ? s : Latest);
    }
}
