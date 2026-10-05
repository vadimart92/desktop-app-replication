using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Sample.App.ViewModels;
using Sample.App.Views;

namespace Sample.App;

public partial class App : Application
{
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            var o = Program.Options;
            desktop.MainWindow = o.Mode switch
            {
                AppMode.Owner => new OwnerWindow { DataContext = new OwnerWindowViewModel(o) },
                AppMode.Client => new ClientWindow { DataContext = new ClientWindowViewModel(o) },
                _ => new LabWindow { DataContext = new LabViewModel() },
            };
        }
        base.OnFrameworkInitializationCompleted();
    }
}
