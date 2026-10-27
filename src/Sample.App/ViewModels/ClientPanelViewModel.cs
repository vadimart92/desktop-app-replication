using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Sample.Lab;

namespace Sample.App.ViewModels;

/// <summary>Where new rows of the panel go: an owner's replica, or the client's own data when <see cref="Agent"/> is null.</summary>
public sealed record WriteTarget(string Name, AgentViewModel? Agent)
{
    public override string ToString() => Name;
}

/// <summary>
/// A client: one database with a replica per owner (4) and, optionally, its own data (<c>InstanceId</c> NULL). Per owner its
/// agent (<see cref="AgentViewModel"/>); the grids show the rows of every source together, and an edit goes to the agent
/// of the row's owner, or stays in the client for its own rows.
/// </summary>
public sealed partial class ClientPanelViewModel : ObservableObject
{
    public static readonly string[] Statuses = ["новий", "активний", "архів"];

    [ObservableProperty]
    private int _selectedTab;

    [ObservableProperty]
    private RowView? _selectedItem;

    [ObservableProperty]
    private RowView? _selectedCategory;

    [ObservableProperty]
    private WriteTarget _target;

    [ObservableProperty]
    private string _editName = "";

    [ObservableProperty]
    private string _editPrice = "";

    [ObservableProperty]
    private string _editStatus = "новий";

    [ObservableProperty]
    private string? _error;

    public ClientPanelViewModel(ClientNode node)
        : this(node.Label, [node])
    {
    }

    /// <param name="nodes">The client's replicas, one per owner, all on the same <see cref="ClientReplication"/>.</param>
    /// <param name="local">The client's own data, shown and edited next to the replicas.</param>
    public ClientPanelViewModel(string title, IReadOnlyList<ClientNode> nodes, LocalData? local = null)
    {
        Title = title;
        Local = local;
        Agents = [.. nodes.Select(n => new AgentViewModel(n, nodes.Count > 1))];
        Targets = [.. Agents.Select(a => new WriteTarget(a.Owner, a)), .. local is null ? Array.Empty<WriteTarget>() : [new WriteTarget(OwnSource, null)]];
        _target = Targets[0];
    }

    /// <summary>The owner column of the client's own rows.</summary>
    public const string OwnSource = "власні";

    public LocalData? Local { get; }

    public IReadOnlyList<WriteTarget> Targets { get; }

    public string Title { get; }

    public IReadOnlyList<AgentViewModel> Agents { get; }

    /// <summary>Several sources (owners, own data): the grids get an owner column, new rows a choice of source.</summary>
    public bool ManyOwners => Targets.Count > 1;

    private ClientNode First => Agents[0].Node;

    public IReadOnlyList<string> StatusOptions => Statuses;

    public ObservableCollection<RowView> Items { get; } = [];
    public ObservableCollection<RowView> Categories { get; } = [];
    public ObservableCollection<RowView> Log { get; } = [];

    partial void OnSelectedTabChanged(int value)
    {
        string table = value switch { 1 => "Category", 2 => "Log", _ => "Item" };
        foreach (AgentViewModel a in Agents)
            a.Node.Agent.SetOpenTables([table]);
    }

    partial void OnSelectedItemChanged(RowView? value)
    {
        if (value is null || value.IsArchived)
            return;
        EditName = value.Label;
        EditPrice = value.Price?.ToString() ?? "";
        EditStatus = value.Status ?? "новий";
    }

    public void Refresh()
    {
        try
        {
            foreach (AgentViewModel a in Agents)
                a.Refresh();
            Items.SyncTo(Rows("Item"));
            Categories.SyncTo(Rows("Category"));
            Log.SyncTo(Rows("Log", 30));
        }
        catch (Exception e)
        {
            Error = e.Message;
        }
    }

    /// <summary>One source: as the replica has them. Several: one list of the client's database, the latest change on top.</summary>
    private List<RowView> Rows(string table, int limit = 200) =>
        ManyOwners
            ? [.. Agents.SelectMany(a => Inspect.ClientRows(a.Node, table, limit, newestFirst: true).Select(r => r with { Owner = a.Owner }))
                .Concat(Local is null ? [] : Inspect.LocalRows(Local, table, limit, newestFirst: true).Select(r => r with { Owner = OwnSource }))
                .OrderByDescending(r => r.ModifiedOn).ThenByDescending(r => r.Version).Take(limit)]
            : Inspect.ClientRows(First, table, limit);

    /// <summary>
    /// The source a row of the grid belongs to and its id: the replica's node, or no node for the client's own row.
    /// Archived rows are read-only.
    /// </summary>
    private (ClientNode? Node, Guid Id)? Live(RowView? r) =>
        r is not { IsArchived: false } ? null
        : r.InstanceId is null ? (Local is null ? null : (null, Guid.Parse(r.Id)))
        : Agents.FirstOrDefault(a => a.Node.Agent.InstanceId == r.InstanceId) is { } a ? (a.Node, Guid.Parse(r.Id)) : null;

    private LocalData Own => Local ?? throw new InvalidOperationException("у клієнта нема власних даних");

    private async Task Run(Func<Task> action)
    {
        try
        {
            Error = null;
            await action();
            Refresh();
        }
        catch (Exception e)
        {
            Error = e.InnerException?.Message ?? e.Message;
        }
    }

    [RelayCommand]
    private Task Save() => Run(() => Live(SelectedItem) is { } x && long.TryParse(EditPrice, out long p)
        ? x.Node?.UpdateItemAsync(x.Id, EditName, p, EditStatus) ?? Own.UpdateItemAsync(x.Id, EditName, p, EditStatus)
        : Task.CompletedTask);

    [RelayCommand]
    private Task Delete() => Run(() => Live(SelectedItem) is { } x ? x.Node?.DeleteAsync("Item", x.Id) ?? Own.DeleteAsync("Item", x.Id) : Task.CompletedTask);

    [RelayCommand]
    private Task Archive() => Run(() => Live(SelectedItem) is { } x ? ArchiveAsync(x.Node, "Item", x.Id) : Task.CompletedTask);

    /// <summary>The archive belongs to an owner's replica (11); the client's own rows have none.</summary>
    private static Task ArchiveAsync(ClientNode? node, string table, Guid id) =>
        node?.ArchiveByIdAsync(table, id) ?? throw new InvalidOperationException("власні дані не мають архіву: архів є тільки у реплік власників");

    [RelayCommand]
    private Task AddItem() => Run(() => Target.Agent?.Node.AddItemAsync() ?? Own.AddItemAsync());

    [RelayCommand]
    private Task AddCategory() => Run(() => Target.Agent?.Node.CreateCategoryAsync($"Категорія {Categories.Count + 1}") ?? Own.CreateCategoryAsync($"Моя категорія {Categories.Count + 1}"));

    [RelayCommand]
    private Task DeleteCategory() => Run(() => Live(SelectedCategory) is { } x ? x.Node?.DeleteAsync("Category", x.Id) ?? Own.DeleteAsync("Category", x.Id) : Task.CompletedTask);

    [RelayCommand]
    private Task ArchiveCategory() => Run(() => Live(SelectedCategory) is { } x ? ArchiveAsync(x.Node, "Category", x.Id) : Task.CompletedTask);

    /// <summary>One predicate action per owner: each replica deletes its own rows; the client's own rows go locally.</summary>
    [RelayCommand]
    private Task DeleteArchived() => Run(async () =>
    {
        foreach (AgentViewModel a in Agents.Where(a => a.Node.Agent.InstanceId is not null))
            await a.Node.DeleteWhereStatusAsync("архів");
        if (Local is not null)
            await Local.DeleteWhereStatusAsync("архів");
    });
}
