using System.Collections.ObjectModel;
using Avalonia.Media;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Replication.Client;
using Replication.Net;
using Sample.Lab;

namespace Sample.App.ViewModels;

public sealed record NetworkChoice(string Title, NetworkProfile Profile)
{
    public override string ToString() => Title;
}

/// <summary>A client: status, link switch, byte counters per direction, network emulation, its replica and outbox.</summary>
public sealed partial class ClientPanelViewModel : ObservableObject
{
    public static readonly string[] Statuses = ["новий", "активний", "архів"];

    public ClientPanelViewModel(ClientNode node)
    {
        Node = node;
        _selectedNetwork = Networks[0];
    }

    public ClientNode Node { get; }
    public string Title => Node.Label;

    public IReadOnlyList<NetworkChoice> Networks { get; } =
    [
        new("Локально, без обмежень", NetworkProfile.Local()),
        new("Цільова: 1,5 с в кожен бік, ↑ 70 кбіт/с, ↓ 2 Мбіт/с", NetworkProfile.Target()),
        new("Затримка 0,7 с в кожен бік", new NetworkProfile { Latency = TimeSpan.FromMilliseconds(700) }),
        new("Повільна: ↑ 20 кбіт/с, ↓ 100 кбіт/с", new NetworkProfile { UpBitsPerSecond = 20_000, DownBitsPerSecond = 100_000 }),
    ];

    public IReadOnlyList<string> StatusOptions => Statuses;

    public ObservableCollection<RowView> Items { get; } = [];
    public ObservableCollection<RowView> Categories { get; } = [];
    public ObservableCollection<RowView> Log { get; } = [];
    public ObservableCollection<string> Outbox { get; } = [];
    public ObservableCollection<string> Cursors { get; } = [];
    public ObservableCollection<string> Notes { get; } = [];
    public ObservableCollection<string> MessageStats { get; } = [];

    [ObservableProperty] private string _statusText = "";
    [ObservableProperty] private IBrush _statusBrush = Brushes.Gray;
    [ObservableProperty] private string _pendingText = "";
    [ObservableProperty] private string _bytesUp = "0 Б";
    [ObservableProperty] private string _bytesDown = "0 Б";
    [ObservableProperty] private string _rateUp = "";
    [ObservableProperty] private string _rateDown = "";
    [ObservableProperty] private NetworkChoice _selectedNetwork;
    [ObservableProperty] private bool _newSchema;
    [ObservableProperty] private int _selectedTab;
    [ObservableProperty] private RowView? _selectedItem;
    [ObservableProperty] private RowView? _selectedCategory;
    [ObservableProperty] private string _editName = "";
    [ObservableProperty] private string _editPrice = "";
    [ObservableProperty] private string _editStatus = "новий";
    [ObservableProperty] private string? _error;

    public bool Link
    {
        get => Node.Link;
        set
        {
            Node.Link = value;
            OnPropertyChanged();
        }
    }

    public bool LoseNextReply
    {
        get => Node.Agent.LoseNextApplyReply;
        set
        {
            Node.Agent.LoseNextApplyReply = value;
            OnPropertyChanged();
        }
    }

    partial void OnSelectedNetworkChanged(NetworkChoice value) => Node.Agent.Network.CopyFrom(value.Profile);

    partial void OnNewSchemaChanged(bool value) => Node.Agent.SetSchemaVersion(value ? 2 : 1);

    partial void OnSelectedTabChanged(int value) => Node.Agent.SetOpenTables(value switch { 1 => "Category", 2 => "Log", _ => "Item" });

    partial void OnSelectedItemChanged(RowView? value)
    {
        if (value is null || value.Mark == "архів") return;
        EditName = value.Label;
        EditPrice = value.Price?.ToString() ?? "";
        EditStatus = value.Status ?? "новий";
    }

    public void Refresh()
    {
        try
        {
            var s = Node.Agent.GetStatus();
            (StatusText, StatusBrush) = Describe(s);
            PendingText = s.Pending == 0 ? "усе відправлено" : $"{s.Pending} змін очікують відправки{(s.InFlight > 0 ? $", {s.InFlight} у дорозі" : "")}{(s.PendingDeletes > 0 ? $"; видалень: {s.PendingDeletes}" : "")}";
            var m = Node.Agent.Meter;
            var (up, down) = m.SampleRate();
            BytesUp = WireMeter.Format(m.BytesUp);
            BytesDown = WireMeter.Format(m.BytesDown);
            RateUp = up > 0 ? $"{WireMeter.Format(up)}/с" : "";
            RateDown = down > 0 ? $"{WireMeter.Format(down)}/с" : "";
            MessageStats.SyncTo([.. m.Messages.OrderBy(x => x.Key).Select(x => $"{x.Key}: {x.Value.Count} шт., {WireMeter.Format(x.Value.Bytes)} без стиснення")]);
            OnPropertyChanged(nameof(Link));
            OnPropertyChanged(nameof(LoseNextReply));

            Items.SyncTo(Inspect.ClientRows(Node, "Item"));
            Categories.SyncTo(Inspect.ClientRows(Node, "Category"));
            Log.SyncTo(Inspect.ClientRows(Node, "Log", 30));
            Cursors.SyncTo([.. s.Cursors.Select(x => $"{x.Key}: {x.Value}")]);
            if (s.InstanceId is { } inst)
                Outbox.SyncTo([.. Node.Replication.Store.Entries(inst).Select(Describe)]);
            Notes.SyncTo([.. Node.Replication.Store.Notes(limit: 20).Select(n => $"{n.At.ToLocalTime():HH:mm:ss} {n.Text}")]);
        }
        catch (Exception e)
        {
            Error = e.Message;
        }
    }

    private static (string, IBrush) Describe(AgentStatus s)
    {
        if (!s.LinkEnabled) return (s.OfflineSince is { } t ? $"Нема зв'язку з {t:HH:mm:ss}" : "Нема зв'язку", Brushes.IndianRed);
        return s.State switch
        {
            AgentState.SchemaMismatch => ("Оновіть додаток на інстансі або клієнті", Brushes.IndianRed),
            AgentState.Connecting => ("Start…", Brushes.SteelBlue),
            AgentState.Flushing => ("Відправка черги перед знімком", Brushes.DarkOrange),
            AgentState.Snapshot => ($"Завантаження, {s.SnapshotProgress ?? 0:P0}", Brushes.SteelBlue),
            AgentState.Catchup when s.LagWarning => ($"Канал не встигає: відстаємо на {s.Remaining} змін, росте", Brushes.IndianRed),
            AgentState.Catchup => ($"Досинхронізація, лишилось {s.Remaining} змін", Brushes.DarkOrange),
            AgentState.Online => ("✓ Синхронно", Brushes.SeaGreen),
            _ => (s.OfflineSince is { } t ? $"Нема зв'язку з {t:HH:mm:ss}{(s.LastError is { } e ? $" ({e})" : "")}" : "Нема зв'язку", Brushes.IndianRed),
        };
    }

    private static string Describe(OutboxEntry e)
    {
        var state = e.Sent switch { 1 => "в дорозі", 2 => $"підтверджено, чекає курсор ≥ {e.ExpectedVersion}", _ => "чекає" };
        var what = e.Kind switch
        {
            OutboxKind.Archive => $"архів {ArchiveCount(e)} записів, SyncVersion ≤ {e.ExpectedVersion}",
            OutboxKind.PredicateDelete => $"видалення де {Predicate.Parse(e.Predicate)}, SyncVersion ≤ {e.ExpectedVersion}",
            _ => $"{e.Kind} {e.Table} …{Replication.Model.Wire.Short(e.Pk ?? "")}{(e.Columns.Count > 0 && e.Kind == OutboxKind.Patch ? " [" + string.Join(", ", e.Columns) + "]" : "")}",
        };
        return $"seq {(e.Seq?.ToString() ?? "—")} · {(e.Class == OutboxClass.Bulk ? "масова" : "інтерактивна")} · {what} · {state}";
    }

    private static int ArchiveCount(OutboxEntry e) => e.Predicate?.Split("],[").Length ?? 0;

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
    private Task Save() => Run(() => SelectedItem is { Mark: not "архів" } r && long.TryParse(EditPrice, out var p)
        ? Node.UpdateItemAsync(Guid.Parse(r.Id), EditName, p, EditStatus)
        : Task.CompletedTask);

    [RelayCommand] private Task Delete() => Run(() => SelectedItem is { Mark: not "архів" } r ? Node.DeleteAsync("Item", Guid.Parse(r.Id)) : Task.CompletedTask);
    [RelayCommand] private Task Archive() => Run(() => SelectedItem is { Mark: not "архів" } r ? Node.ArchiveByIdAsync("Item", Guid.Parse(r.Id)) : Task.CompletedTask);
    [RelayCommand] private Task AddItem() => Run(Node.AddItemAsync);
    [RelayCommand] private Task AddCategory() => Run(() => Node.CreateCategoryAsync($"Категорія {Categories.Count + 1}"));
    [RelayCommand] private Task DeleteCategory() => Run(() => SelectedCategory is { Mark: not "архів" } r ? Node.DeleteAsync("Category", Guid.Parse(r.Id)) : Task.CompletedTask);
    [RelayCommand] private Task ArchiveCategory() => Run(() => SelectedCategory is { Mark: not "архів" } r ? Node.ArchiveByIdAsync("Category", Guid.Parse(r.Id)) : Task.CompletedTask);
    [RelayCommand] private Task DeleteArchived() => Run(() => Node.DeleteWhereStatusAsync("архів"));
    [RelayCommand] private void ResetBytes() => Node.Agent.Meter.Reset();
}
