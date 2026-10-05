using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Replication.Net;
using Sample.Lab;

namespace Sample.App.ViewModels;

/// <summary>The owner: its rows with the sync columns, tombstones, known clients, and the automation buttons.</summary>
public sealed partial class OwnerPanelViewModel(OwnerNode owner) : ObservableObject
{
    [ObservableProperty]
    private RowView? _selectedItem;

    [ObservableProperty]
    private RowView? _selectedCategory;

    [ObservableProperty]
    private string _meta = "";

    [ObservableProperty]
    private string _traffic = "";

    [ObservableProperty]
    private string? _error;

    public OwnerNode Owner { get; } = owner;

    public ObservableCollection<RowView> Items { get; } = [];
    public ObservableCollection<RowView> Categories { get; } = [];
    public ObservableCollection<RowView> Log { get; } = [];
    public ObservableCollection<TombstoneView> Tombstones { get; } = [];
    public ObservableCollection<ClientView> Clients { get; } = [];

    public string Title => $"Власник · {Owner.Address}";

    /// <summary>Byte counters of the clients connected in this process (the lab); empty for a standalone owner.</summary>
    public Func<IEnumerable<WireMeter>>? Meters { get; set; }

    public void Refresh()
    {
        try
        {
            OwnerMeta m = Inspect.Meta(Owner);
            Meta = $"instance_id {m.InstanceId} · version {m.Version} · purged_version {m.Purged} · floor {m.Floor} · "
                   + $"годинник +{TimeSpan.FromSeconds(m.ClockOffset).TotalDays:0.#} дн. · файл {m.Pages} стор., вільних {m.FreePages}";
            Items.SyncTo(Inspect.OwnerRows(Owner, "Item"));
            Categories.SyncTo(Inspect.OwnerRows(Owner, "Category"));
            Log.SyncTo(Inspect.OwnerRows(Owner, "Log", 30));
            Tombstones.SyncTo(Inspect.Tombstones(Owner));
            Clients.SyncTo(Inspect.Clients(Owner));
            if (Meters?.Invoke().ToList() is { Count: > 0 } meters)
                Traffic = $"усього на дроті: від клієнтів ↑ {WireMeter.Format(meters.Sum(x => x.BytesUp))}, до клієнтів ↓ {WireMeter.Format(meters.Sum(x => x.BytesDown))}";
        }
        catch (Exception e)
        {
            Error = e.Message;
        }
    }

    private async Task Run(Func<Task> action)
    {
        try
        {
            Error = null;
            await action();
        }
        catch (Exception e)
        {
            Error = e.Message;
        }
    }

    [RelayCommand]
    private Task NewItem() => Run(() => Owner.NewItemAsync());

    [RelayCommand]
    private Task AddLog() => Run(() => Owner.AddLogAsync(5));

    [RelayCommand]
    private Task Burst() => Run(Owner.BurstAsync);

    [RelayCommand]
    private Task Price() => Run(() => SelectedItem is { } r ? Owner.SetPriceAsync(r.Label) : Task.CompletedTask);

    [RelayCommand]
    private Task Status() => Run(() => SelectedItem is { } r ? Owner.SetStatusAsync(r.Label) : Task.CompletedTask);

    [RelayCommand]
    private Task DeleteItem() => Run(() => SelectedItem is { } r ? Owner.DeleteItemAsync(r.Label) : Task.CompletedTask);

    [RelayCommand]
    private Task DeleteCategory() => Run(() => SelectedCategory is { } r ? Owner.DeleteCategoryAsync(r.Label) : Task.CompletedTask);

    [RelayCommand]
    private Task Purge() => Run(Owner.PurgeAsync);

    [RelayCommand]
    private void PlusDay() => Owner.AdvanceClock(TimeSpan.FromDays(1));

    [RelayCommand]
    private void Plus31Days() => Owner.AdvanceClock(TimeSpan.FromDays(31));

    [RelayCommand]
    private void Vacuum() => Owner.Store.IncrementalVacuum(64);
}
