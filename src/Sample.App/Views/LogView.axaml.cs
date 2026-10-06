using Avalonia.Controls;
using Avalonia.Threading;
using Sample.App.ViewModels;

namespace Sample.App.Views;

public partial class LogView : UserControl
{
    private LogFeed? _feed;

    public LogView()
    {
        InitializeComponent();
        DataContextChanged += (_, _) =>
        {
            if (_feed is not null)
                _feed.Appended -= ScrollToEnd;
            _feed = DataContext as LogFeed;
            if (_feed is not null)
                _feed.Appended += ScrollToEnd;
        };
    }

    private void ScrollToEnd()
    {
        if (_feed is { Lines.Count: > 0 } f)
            Dispatcher.UIThread.Post(() => List.ScrollIntoView(f.Lines[^1]), DispatcherPriority.Background);
    }
}
