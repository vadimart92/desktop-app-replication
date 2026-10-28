using System.ComponentModel.DataAnnotations.Schema;

namespace Sample.Domain;

/// <summary>The application's base entity, unchanged: the sync columns are shadow properties added by the core.</summary>
public abstract class BaseEntity
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public DateTime CreatedOn { get; set; }
    public DateTime ModifiedOn { get; set; }
}

public sealed class Category : BaseEntity
{
    public string Name { get; set; } = "";
    public List<Item> Items { get; set; } = [];
}

public sealed class Item : BaseEntity
{
    public string Name { get; set; } = "";
    public long Price { get; set; }
    public string Status { get; set; } = "новий";
    public Guid CategoryId { get; set; }
    public Category? Category { get; set; }
}

public sealed class LogEntry : BaseEntity
{
    public string Text { get; set; } = "";
}

/// <summary>A value object stored in the columns of its owner's table (<c>Settings_Description</c>, <c>Settings_StockLevel</c>).</summary>
[ComplexType]
public sealed record ObjectSettings(string? Description, int StockLevel);

public sealed class StockObject : BaseEntity
{
    public string Name { get; set; } = "";
    public ObjectSettings Settings { get; set; } = new(null, 0);
}
