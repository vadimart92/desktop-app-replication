using System.Collections.ObjectModel;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
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

    public LabViewModel()
    {
        _selectedScenario = Scenarios.All[0];
        _timer = new DispatcherTimer(TimeSpan.FromMilliseconds(400), DispatcherPriority.Background, (_, _) => Refresh());
        _timer.Start();
        _ = StartScenarioAsync(_selectedScenario);
    }

    public IReadOnlyList<Scenario> ScenarioList => Scenarios.All;
    public LogFeed Feed { get; } = new();
    public ObservableCollection<StepView> Steps { get; } = [];

    [ObservableProperty] private Scenario _selectedScenario;
    [ObservableProperty] private OwnerPanelViewModel? _owner;
    [ObservableProperty] private ClientPanelViewModel? _client1;
    [ObservableProperty] private ClientPanelViewModel? _client2;
    [ObservableProperty][NotifyCanExecuteChangedFor(nameof(NextStepCommand), nameof(RestartCommand), nameof(VerifyCommand))] private bool _busy;
    [ObservableProperty][NotifyCanExecuteChangedFor(nameof(NextStepCommand))] private bool _done;
    [ObservableProperty] private string _nextTitle = "";
    [ObservableProperty] private string _verdict = "";
    [ObservableProperty] private string _state = "";
    [ObservableProperty] private string _labDir = "";

    partial void OnSelectedScenarioChanged(Scenario value) => _ = StartScenarioAsync(value);

    private async Task StartScenarioAsync(Scenario scenario)
    {
        if (Busy)
            return;
        Busy = true;
        Verdict = "";
        State = "запускаю власника і двох клієнтів…";
        try
        {
            Owner = null;
            Client1 = null;
            Client2 = null;
            if (_lab is not null)
                await _lab.DisposeAsync();
            Feed.Clear();
            var log = new SyncLog();
            Feed.Attach(log);
            _lab = await Task.Run(() => Sample.Lab.Lab.StartAsync(log: log));
            LabDir = _lab.Dir;
            var c1 = new ClientPanelViewModel(_lab.C1);
            var c2 = new ClientPanelViewModel(_lab.C2);
            Owner = new OwnerPanelViewModel(_lab.Owner) { Meters = () => [_lab.C1.Agent.Meter, _lab.C2.Agent.Meter] };
            Client1 = c1;
            Client2 = c2;
            _runner = new ScenarioRunner(_lab, scenario);
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
        // the scenario was switched while the previous one was starting
        if (_runner?.Scenario != SelectedScenario)
            await StartScenarioAsync(SelectedScenario);
    }

    private void UpdateSteps()
    {
        if (_runner is null)
            return;
        Steps.Clear();
        for (int i = 0; i < _runner.Scenario.Steps.Count; i++)
            Steps.Add(new StepView(i + 1, _runner.Scenario.Steps[i].Title, _runner.Scenario.Steps[i].Explain, i < _runner.Step));
        Done = _runner.Done;
        NextTitle = _runner.Done ? "Усі кроки виконано" : $"Виконати крок {_runner.Step + 1}";
    }

    private bool CanStep() => !Busy && !Done;

    [RelayCommand(CanExecute = nameof(CanStep))]
    private async Task NextStep()
    {
        if (_runner is null)
            return;
        Busy = true;
        try
        {
            await Task.Run(_runner.NextAsync);
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
    }

    private bool NotBusy() => !Busy;

    [RelayCommand(CanExecute = nameof(NotBusy))]
    private Task Restart() => StartScenarioAsync(SelectedScenario);

    /// <summary>Waits for the system to settle and checks that every replica equals the owner.</summary>
    [RelayCommand(CanExecute = nameof(NotBusy))]
    private async Task Verify()
    {
        if (_runner is null)
            return;
        Busy = true;
        Verdict = "чекаю, поки все доїде…";
        try
        {
            List<string> problems = await Task.Run(() => _runner.VerifyAsync(TimeSpan.FromSeconds(60)));
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
    public OwnerWindowViewModel(StartOptions o)
    {
        var log = new SyncLog();
        Feed.Attach(log);
        _ = StartAsync(o, log);
    }

    public LogFeed Feed { get; } = new();

    [ObservableProperty] private OwnerPanelViewModel? _owner;
    [ObservableProperty] private string _state = "запускаю власника…";

    private async Task StartAsync(StartOptions o, SyncLog log)
    {
        try
        {
            OwnerNode node = await Task.Run(() => OwnerNode.StartAsync(o.Db ?? "owner.db", log, o.Port, o.ListenAnywhere));
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
    public ClientWindowViewModel(StartOptions o)
    {
        var log = new SyncLog();
        Feed.Attach(log);
        try
        {
            ClientNode node = ClientNode.Create("c", o.Name ?? "Клієнт", o.Db ?? "client.db", o.Connect!, log);
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

    public LogFeed Feed { get; } = new();

    [ObservableProperty] private ClientPanelViewModel? _client;
    [ObservableProperty] private string _state = "";
}
