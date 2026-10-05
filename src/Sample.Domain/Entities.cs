namespace Sample.Domain;

/// <summary>The application's base entity, unchanged: the sync columns are shadow properties added by the core.</summary>
public abstract class BaseEntity
{
    public Guid Id { get; set; } = Guid.CreateVersion7();
    public DateTime CreatedOn { get; set; }
    public DateTime ModifiedOn { get; set; }
}

public class Category : BaseEntity
{
    public string Name { get; set; } = "";
    public List<Item> Items { get; set; } = [];
}

public class Item : BaseEntity
{
    public string Name { get; set; } = "";
    public long Price { get; set; }
    public string Status { get; set; } = "новий";
    public Guid CategoryId { get; set; }
    public Category? Category { get; set; }
}

public class LogEntry : BaseEntity
{
    public string Text { get; set; } = "";
}
