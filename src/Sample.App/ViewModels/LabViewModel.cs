using System.Collections.ObjectModel;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Microsoft.Extensions.Logging;
using Replication;
using Sample.Lab;

namespace Sample.App.ViewModels;

public sealed record StepView(int Number, string Title, string Explain, bool Done)
{
    public string Header => $"{Number}. {Title}";
    public double Opacity => Done ? 1 : 0.55;
}

/// <summary>The lab window: an owner and two clients over real gRPC, plus the demo-page scenarios.</summary>
public sealed partial class LabViewModel : ObservableObject
{
    private readonly DispatcherTimer _timer;
    private Sample.Lab.Lab? _lab;
    private ScenarioRunner? _runner;
    private Scenario? _started;

    [ObservableProperty]
    private Scenario _selectedScenario;

    [ObservableProperty]
    private OwnerPanelViewModel? _owner;

    [ObservableProperty]
    private ClientPanelViewModel? _client1;

    [ObservableProperty]
    private ClientPanelViewModel? _client2;

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(NextStepCommand), nameof(RestartCommand), nameof(VerifyCommand))]
    private bool _busy;

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(NextStepCommand))]
    private bool _done;

    [ObservableProperty]
    private string _nextTitle = "";

    [ObservableProperty]
    private string _verdict = "";

    [ObservableProperty]
    private string _state = "";

    [ObservableProperty]
    private string _labDir = "";

    [ObservableProperty]
    private int _selectedTab;

    public LabViewModel()
    {
        _selectedScenario = Scenarios.All[0];
        _timer = new DispatcherTimer(TimeSpan.FromMilliseconds(400), DispatcherPriority.Background, (_, _) => Refresh());
        _timer.Start();
        _ = StartScenarioAsync(_selectedScenario);
    }

    public IReadOnlyList<Scenario> ScenarioList => Scenarios.All;
    public LogFeed Feed { get; } = new LogFeed();
    public ObservableCollection<StepView> Steps { get; } = [];

    /// <summary>The second tab: one client on two owners.</summary>
    public TwoOwnerLabViewModel TwoOwners { get; } = new TwoOwnerLabViewModel();

    partial void OnSelectedTabChanged(int value)
    {
        if (value == 1)
            _ = TwoOwners.EnsureStartedAsync();
    }

    partial void OnSelectedScenarioChanged(Scenario value) => _ = StartScenarioAsync(value);

    private async Task StartScenarioAsync(Scenario scenario)
    {
        if (Busy)
            return;
        _started = scenario;
        Busy = true;
        Verdict = "";
        State = "запускаю власника і двох клієнтів…";
        try
        {
            Owner = null;
            Client1 = null;
            Client2 = null;
            Sample.Lab.Lab? old = _lab;
            _lab = null;
            _runner = null;
            if (old is not null)
                await old.DisposeAsync();
            Feed.Clear();
            Sample.Lab.Lab lab = await Task.Run(() => Sample.Lab.Lab.StartAsync(logging: b => b.AddProvider(new LogFeedLoggerProvider(Feed))));
            _lab = lab;
            LabDir = lab.Dir;
            var c1 = new ClientPanelViewModel(lab.C1);
            var c2 = new ClientPanelViewModel(lab.C2);
            Owner = new OwnerPanelViewModel(lab.Owner) { Meters = [.. lab.Clients.Select(c => c.Meter)] };
            Client1 = c1;
            Client2 = c2;
            _runner = new ScenarioRunner(lab, scenario);
            State = "готую початковий стан сценарію…";
            await Task.Run(_runner.SetupAsync);
            State = "";
        }
        catch (Exception e)
        {
            State = "помилка: " + e.Message;
        }
        finally
        {
            Busy = false;
            UpdateSteps();
        }
        await FollowSelectionAsync();
    }

    /// <summary>The scenario was switched while busy (starting, a step, a check): start the selected one now.</summary>
    private Task FollowSelectionAsync() => SelectedScenario != _started ? StartScenarioAsync(SelectedScenario) : Task.CompletedTask;

    private void UpdateSteps()
    {
        Steps.Clear();
        if (_runner is null)
        {
            Done = false;
            NextTitle = "";
            return;
        }
        for (int i = 0; i < _runner.Scenario.Steps.Count; i++)
            Steps.Add(new StepView(i + 1, _runner.Scenario.Steps[i].Title, _runner.Scenario.Steps[i].Explain, i < _runner.Step));
        Done = _runner.Done;
        NextTitle = _runner.Done ? "Усі кроки виконано" : $"Виконати крок {_runner.Step + 1}";
    }

    private bool CanStep() => !Busy && !Done;

    [RelayCommand(CanExecute = nameof(CanStep))]
    private async Task NextStep()
    {
        if (_runner is not { } runner)
            return;
        Busy = true;
        try
        {
            await Task.Run(runner.NextAsync);
        }
        catch (Exception e)
        {
            State = "крок не вдався: " + e.Message;
        }
        finally
        {
            Busy = false;
            UpdateSteps();
        }
        await FollowSelectionAsync();
    }

    private bool NotBusy() => !Busy;

    [RelayCommand(CanExecute = nameof(NotBusy))]
    private Task Restart() => StartScenarioAsync(SelectedScenario);

    /// <summary>Waits for the system to settle and checks that every replica equals the owner.</summary>
    [RelayCommand(CanExecute = nameof(NotBusy))]
    private async Task Verify()
    {
        if (_runner is not { } runner)
            return;
        Busy = true;
        Verdict = "чекаю, поки все доїде…";
        try
        {
            List<string> problems = await Task.Run(() => runner.VerifyAsync(TimeSpan.FromSeconds(60)));
            Verdict = problems.Count == 0 ? "✓ репліки дорівнюють власнику, результат як у дизайні" : "✕ " + string.Join("; ", problems);
        }
        catch (Exception e)
        {
            Verdict = "✕ " + e.Message;
        }
        finally
        {
            Busy = false;
        }
        await FollowSelectionAsync();
    }

    private void Refresh()
    {
        Owner?.Refresh();
        Client1?.Refresh();
        Client2?.Refresh();
    }
}

/// <summary>A standalone owner window (--owner).</summary>
public sealed partial class OwnerWindowViewModel : ObservableObject
{
    [ObservableProperty]
    private OwnerPanelViewModel? _owner;

    [ObservableProperty]
    private string _state = "запускаю власника…";

    public OwnerWindowViewModel(StartOptions o)
    {
        _ = StartAsync(o);
    }

    public LogFeed Feed { get; } = new LogFeed();

    private async Task StartAsync(StartOptions o)
    {
        try
        {
            OwnerNode node = await Task.Run(() => OwnerNode.StartAsync(o.DbPath, o.Port, o.ListenAnywhere, logging: b => b.AddProvider(new LogFeedLoggerProvider(Feed))));
            Owner = new OwnerPanelViewModel(node);
            State = $"gRPC-сервер на порту {node.Host.Port}, БД {node.DbPath}";
            var timer = new DispatcherTimer(TimeSpan.FromMilliseconds(500), DispatcherPriority.Background, (_, _) => Owner.Refresh());
            timer.Start();
        }
        catch (Exception e)
        {
            State = "не вдалося запустити: " + e.Message;
        }
    }
}

/// <summary>A standalone client window (--client --connect http://host:port).</summary>
public sealed partial class ClientWindowViewModel : ObservableObject
{
    [ObservableProperty]
    private ClientPanelViewModel? _client;

    [ObservableProperty]
    private string _state = "";

    public ClientWindowViewModel(StartOptions o)
    {
        Start(o);
    }

    public LogFeed Feed { get; } = new LogFeed();

    private void Start(StartOptions o)
    {
        try
        {
            // Lives as long as the window, that is the process.
            ILoggerFactory loggers = LoggerFactory.Create(b => b.AddProvider(new LogFeedLoggerProvider(Feed)));
            ClientNode node = ClientNode.Connect(ClientNode.Create(o.DbPath, loggers), o.Name, o.Connect);
            node.Link = true;
            Client = new ClientPanelViewModel(node);
            State = $"підключення до {o.Connect}, БД {node.DbPath}";
            var timer = new DispatcherTimer(TimeSpan.FromMilliseconds(400), DispatcherPriority.Background, (_, _) => Client.Refresh());
            timer.Start();
        }
        catch (Exception e)
        {
            State = "не вдалося запустити: " + e.Message;
        }
    }
}
