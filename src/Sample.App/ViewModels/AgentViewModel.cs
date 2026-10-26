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

/// <summary>One owner of a client: the agent's status, link switch, byte counters, network emulation, outbox and cursors.</summary>
public sealed partial class AgentViewModel : ObservableObject
{
    [ObservableProperty]
    private string _statusText = "";

    [ObservableProperty]
    private IBrush _statusBrush = Brushes.Gray;

    [ObservableProperty]
    private string _pendingText = "";

    [ObservableProperty]
    private string _bytesUp = "0 Б";

    [ObservableProperty]
    private string _bytesDown = "0 Б";

    [ObservableProperty]
    private string _rateUp = "";

    [ObservableProperty]
    private string _rateDown = "";

    [ObservableProperty]
    private NetworkChoice _selectedNetwork;

    [ObservableProperty]
    private bool _newSchema;

    /// <param name="showOwner">The client has several owners: headers name this one.</param>
    public AgentViewModel(ClientNode node, bool showOwner)
    {
        Node = node;
        _selectedNetwork = Networks[0];
        Owner = node.OwnerName;
        Prefix = showOwner ? Owner + ": " : "";
    }

    public ClientNode Node { get; }

    /// <summary>The owner's name, as the client knows it.</summary>
    public string Owner { get; }

    /// <summary>Empty for a client with one owner, otherwise "owner: " before the agent's headers.</summary>
    public string Prefix { get; }

    public bool ShowOwner => Prefix.Length > 0;
    public string OutboxHeader => Prefix + "_sync_outbox (черга)";
    public string CursorsHeader => Prefix + "_sync_cursors і _sync_ranges";

    public IReadOnlyList<NetworkChoice> Networks { get; } =
    [
        new NetworkChoice("Локально, без обмежень", NetworkProfile.Local()),
        new NetworkChoice("Цільова: 1,5 с в кожен бік, ↑ 70 кбіт/с, ↓ 2 Мбіт/с", NetworkProfile.Target()),
        new NetworkChoice("Затримка 0,7 с в кожен бік", new NetworkProfile { Latency = TimeSpan.FromMilliseconds(700) }),
        new NetworkChoice("Повільна: ↑ 20 кбіт/с, ↓ 100 кбіт/с", new NetworkProfile { UpBitsPerSecond = 20_000, DownBitsPerSecond = 100_000 }),
    ];

    public ObservableCollection<string> Outbox { get; } = [];
    public ObservableCollection<string> Cursors { get; } = [];
    public ObservableCollection<string> MessageStats { get; } = [];

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

    /// <summary>The demo's app update: a new schema version; the link the agent turned off on SchemaMismatch goes on again.</summary>
    partial void OnNewSchemaChanged(bool value)
    {
        bool mismatch = Node.Agent.GetStatus().State == AgentState.SchemaMismatch;
        Node.Agent.SetSchemaVersion(value ? 2 : 1);
        if (mismatch)
            Node.Agent.LinkEnabled = true;
    }

    public override string ToString() => Owner;

    public void Refresh()
    {
        AgentStatus s = Node.Agent.GetStatus();
        (StatusText, StatusBrush) = Describe(s);
        PendingText = Prefix + (s.Pending == 0 ? "усе відправлено" : $"{s.Pending} змін очікують відправки");
        WireMeter m = Node.Agent.Meter;
        (double up, double down) = m.SampleRate();
        BytesUp = WireMeter.Format(m.BytesUp);
        BytesDown = WireMeter.Format(m.BytesDown);
        RateUp = up > 0 ? $"{WireMeter.Format(up)}/с" : "";
        RateDown = down > 0 ? $"{WireMeter.Format(down)}/с" : "";
        MessageStats.SyncTo([.. m.Messages.OrderBy(x => x.Key).Select(x => $"{x.Key}: {x.Value.Count} шт., {WireMeter.Format(x.Value.Bytes)} без стиснення")]);
        OnPropertyChanged(nameof(Link));
        OnPropertyChanged(nameof(LoseNextReply));
        Cursors.SyncTo([.. s.Cursors.Select(x => $"{x.Key}: {x.Value}")]);
        if (s.InstanceId is { } inst)
            Outbox.SyncTo([.. Node.Replication.Store.Entries(inst).Select(Describe)]);
    }

    private static (string, IBrush) Describe(AgentStatus s)
    {
        // The agent turns the link off on these, the reason stays.
        if (s.State == AgentState.SchemaMismatch)
            return ("Оновіть додаток на інстансі або клієнті", Brushes.IndianRed);
        if (s.State == AgentState.Duplicate)
            return ($"Цей власник уже підключений: {s.LastError}", Brushes.IndianRed);
        if (!s.LinkEnabled)
            return (s.OfflineSince is { } t ? $"Нема зв'язку з {t:HH:mm:ss}" : "Нема зв'язку", Brushes.IndianRed);
        return s.State switch
        {
            AgentState.Connecting => ("Start…", Brushes.SteelBlue),
            AgentState.Snapshot => ($"Завантаження, {s.SnapshotProgress ?? 0:P0}", Brushes.SteelBlue),
            AgentState.Catchup when s.LagWarning => ($"Канал не встигає: відстаємо на {s.Remaining} змін, росте", Brushes.IndianRed),
            AgentState.Catchup => ($"Досинхронізація, лишилось {s.Remaining} змін", Brushes.DarkOrange),
            AgentState.Online => ("✓ Синхронно", Brushes.SeaGreen),
            _ => (s.OfflineSince is { } t ? $"Нема зв'язку з {t:HH:mm:ss}{(s.LastError is { } e ? $" ({e})" : "")}" : "Нема зв'язку", Brushes.IndianRed),
        };
    }

    private static string Describe(OutboxEntry e)
    {
        string state = e.Sent switch { OutboxSendState.InFlight => "в дорозі", OutboxSendState.DeleteConfirmed => $"підтверджено, чекає курсор ≥ {e.ExpectedVersion}", _ => "чекає" };
        string what = e.Kind switch
        {
            OutboxKind.Archive => $"архів {ArchiveCount(e)} записів, SyncVersion ≤ {e.ExpectedVersion}",
            OutboxKind.PredicateDelete => $"видалення де {Predicate.Parse(e.Predicate)}, SyncVersion ≤ {e.ExpectedVersion}",
            _ => $"{e.Kind} {e.Table} …{Replication.Model.Wire.Short(e.Pk ?? "")}{(e.Columns.Count > 0 && e.Kind == OutboxKind.Patch ? " [" + string.Join(", ", e.Columns) + "]" : "")}",
        };
        return $"seq {(e.Seq?.ToString() ?? "—")} · {(e.Class == OutboxClass.Bulk ? "масова" : "інтерактивна")} · {what} · {state}";
    }

    private static int ArchiveCount(OutboxEntry e) => e.Predicate?.Split("],[").Length ?? 0;

    [RelayCommand]
    private void ResetBytes() => Node.Agent.Meter.Reset();
}
