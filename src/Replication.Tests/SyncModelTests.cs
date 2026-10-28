using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using NUnit.Framework;
using Replication.Model;
using Sample.Domain;

namespace Replication.Tests;

/// <summary>Design 5.2: the INTEGER mask holds 63 data columns; the model is built from the EF model, complex types flattened.</summary>
public sealed class SyncModelTests
{
    [Test]
    public void Table_with_63_data_columns_is_accepted_and_its_full_mask_has_63_bits()
    {
        SyncModel model = ModelWithColumns(63);

        SyncTable t = model["Wide"];
        Assert.That(t.Columns, Has.Count.EqualTo(63));
        Assert.That(t.FullMask, Is.EqualTo(long.MaxValue));
        string last = t.Columns[62];
        Assert.That(t.MaskOf([last]), Is.EqualTo(1L << 62), "the last column is bit 62");
        Assert.That(t.ColumnsOf(1L << 62), Is.EqualTo(new[] { last }));
    }

    [Test]
    public void Table_with_64_data_columns_is_refused()
    {
        Assert.That(() => ModelWithColumns(64), Throws.TypeOf<NotSupportedException>().With.Message.Contain("64 data columns"));
    }

    [Test]
    public void Complex_type_members_are_columns_of_the_owner_table_and_count_toward_the_63()
    {
        SyncModel model = SampleModel();

        Assert.That(model["StockObject"].Columns, Is.EquivalentTo(new[] { "CreatedOn", "ModifiedOn", "Name", "Settings_Description", "Settings_StockLevel" }));
    }

    [Test]
    public void Mask_of_unknown_columns_is_zero_and_the_mask_round_trips_through_ColumnsOf()
    {
        SyncTable item = SampleModel()["Item"];

        Assert.That(item.MaskOf(["NoSuchColumn"]), Is.Zero);
        Assert.That(item.IndexOf("NoSuchColumn"), Is.EqualTo(-1));
        long mask = item.MaskOf(["Status", "Price", "NoSuchColumn"]);
        Assert.That(item.ColumnsOf(mask), Is.EquivalentTo(new[] { "Price", "Status" }));
        Assert.That(item.ColumnsOf(item.FullMask), Is.EqualTo(item.Columns));
    }

    [Test]
    public void Tables_come_parents_first_and_ChildrenOf_names_the_foreign_key()
    {
        SyncModel model = SampleModel();

        List<string> names = [.. model.Tables.Select(t => t.Name)];
        Assert.That(names.IndexOf("Category"), Is.LessThan(names.IndexOf("Item")));
        (SyncTable child, SyncForeignKey fk) = model.ChildrenOf("Category").Single();
        Assert.That((child.Name, fk.Column, fk.Cascade), Is.EqualTo(("Item", "CategoryId", true)));
        Assert.That(model.ChildrenOf("Item"), Is.Empty);
    }

    [Test]
    public void Lookups_by_name_and_type_find_replicated_tables_only()
    {
        SyncModel model = SampleModel();

        Assert.That(model.TryGet("Item", out SyncTable? item), Is.True);
        Assert.That(model.ForType(typeof(Item)), Is.SameAs(item));
        Assert.That(model.TryGet("Nope", out _), Is.False);
        Assert.That(model.ForType(typeof(string)), Is.Null);
    }

    private static SyncModel SampleModel()
    {
        using SampleDbContext db = SampleDbContext.Open(":memory:");
        return SyncModel.From(db);
    }

    private static SyncModel ModelWithColumns(int columns)
    {
        using var db = new WideDbContext(columns);
        return SyncModel.From(db);
    }

    private sealed class WidthKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime) => (context.GetType(), ((WideDbContext)context).Columns, designTime);
    }

    public sealed class Wide
    {
        public Guid Id { get; set; }
    }

    private sealed class WideDbContext(int columns) : DbContext
    {
        public int Columns => columns;

        // EF caches the model per context type: without this key the second width would get the first model.
        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseSqlite("Data Source=:memory:").ReplaceService<IModelCacheKeyFactory, WidthKeyFactory>();

        protected override void OnModelCreating(ModelBuilder b)
        {
            b.Entity<Wide>(e =>
            {
                e.ToTable("Wide");
                for (int i = 0; i < columns; i++)
                    e.Property<int>($"C{i}");
            });
            b.UseReplication();
        }
    }
}
