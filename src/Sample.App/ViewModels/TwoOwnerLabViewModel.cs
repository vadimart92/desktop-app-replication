using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Microsoft.Extensions.Logging;
using Replication;
using Sample.Lab;

namespace Sample.App.ViewModels;

/// <summary>
/// The two-owner tab: two owners with different data and one client with its own data (<c>InstanceId</c> NULL) that gets
/// a replica of each owner (4). Both links start off; the client joins each owner by catch-up instead of a snapshot.
/// </summary>
public sealed partial class TwoOwnerLabViewModel : ObservableObject
{
    private readonly DispatcherTimer _timer;
    private TwoOwnerLab? _lab;

    [ObservableProperty]
    private OwnerPanelViewModel? _ownerA;

    [ObservableProperty]
    private OwnerPanelViewModel? _ownerB;

    [ObservableProperty]
    private ClientPanelViewModel? _client;

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(RestartCommand), nameof(VerifyCommand))]
    private bool _busy;

    [ObservableProperty]
    private string _verdict = "";

    [ObservableProperty]
    private string _state = "";

    [ObservableProperty]
    private string _labDir = "";

    public TwoOwnerLabViewModel() =>
        _timer = new DispatcherTimer(TimeSpan.FromMilliseconds(400), DispatcherPriority.Background, (_, _) => Refresh());

    public LogFeed Feed { get; } = new LogFeed();

    public string Intro =>
        $"Один клієнт з власними даними ({Describe(LocalData.Seed)}; InstanceId NULL, власникам не йдуть) і два власники з різними даними: «{TwoOwnerLab.OwnerA}» ({Describe(OwnerSeed.Default)}) і «{TwoOwnerLab.OwnerB}» ({Describe(OwnerSeed.Second)}). "
        + "У клієнта по агенту на кожного власника, рядки кожного власника мають свій InstanceId в одній БД. Обидва зв'язки вимкнені: увімкніть їх, "
        + "і клієнт досинхронізується без знімка (skip_snapshot_on_empty_replica), а далі отримує онлайн-зміни. Правка рядка йде тільки власнику, з якого рядок прийшов; "
        + "власні рядки ніколи не потрапляють у чергу, і рядок може посилатися тільки на рядки свого джерела.";

    private static string Describe(OwnerSeed s) => $"категорій {s.Categories.Count}, товарів {s.Items.Count}, записів журналу {s.LogEntries}";

    /// <summary>Starts the lab the first time the tab is shown.</summary>
    public Task EnsureStartedAsync() => _lab is null && !Busy ? StartAsync() : Task.CompletedTask;

    private async Task StartAsync()
    {
        Busy = true;
        Verdict = "";
        State = "запускаю двох власників і клієнта…";
        try
        {
            _timer.Stop();
            (OwnerA, OwnerB, Client) = (null, null, null);
            TwoOwnerLab? old = _lab;
            _lab = null;
            if (old is not null)
                await old.DisposeAsync();
            Feed.Clear();
            TwoOwnerLab lab = await Task.Run(() => TwoOwnerLab.StartAsync(localSeed: LocalData.Seed,
                logging: b => b.AddProvider(new LogFeedLoggerProvider(Feed)), ownerLogging: (name, b) => b.AddProvider(new LogFeedLoggerProvider(Feed, name))));
            _lab = lab;
            LabDir = lab.Dir;
            OwnerA = new OwnerPanelViewModel(lab.A) { Name = TwoOwnerLab.OwnerA, Meters = [lab.ToA.Agent.Meter] };
            OwnerB = new OwnerPanelViewModel(lab.B) { Name = TwoOwnerLab.OwnerB, Meters = [lab.ToB.Agent.Meter] };
            Client = new ClientPanelViewModel("Клієнт · одна БД: власні дані і два власники", [lab.ToA, lab.ToB], lab.Local);
            _timer.Start();
            State = "";
        }
        catch (Exception e)
        {
            State = "помилка: " + e.Message;
        }
        finally
        {
            Busy = false;
        }
    }

    private bool NotBusy() => !Busy;

    [RelayCommand(CanExecute = nameof(NotBusy))]
    private Task Restart() => StartAsync();

    /// <summary>Waits for the connected replicas to settle and compares each with its owner.</summary>
    [RelayCommand(CanExecute = nameof(NotBusy))]
    private async Task Verify()
    {
        if (_lab is not { } lab)
            return;
        Busy = true;
        Verdict = "чекаю, поки все доїде…";
        try
        {
            await Task.Run(() => lab.SettleAsync(TimeSpan.FromSeconds(60)));
            List<string> problems = [.. lab.Pairs.Where(p => p.Client.Link).SelectMany(p => Inspect.Diff(p.Owner, p.Client).Select(d => $"{p.Client.Label}: {d}"))];
            Verdict = lab.Pairs.All(p => !p.Client.Link) ? "обидва зв'язки вимкнені, нема що порівнювати"
                : problems.Count == 0 ? "✓ кожна підключена репліка дорівнює своєму власнику"
                : "✕ " + string.Join("; ", problems);
        }
        catch (Exception e)
        {
            Verdict = "✕ " + e.Message;
        }
        finally
        {
            Busy = false;
        }
    }

    private void Refresh()
    {
        OwnerA?.Refresh();
        OwnerB?.Refresh();
        Client?.Refresh();
    }
}
