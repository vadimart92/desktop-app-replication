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
        if (_feed is not { } f)
            return;
        Dispatcher.UIThread.Post(() =>
        {
            // Feed.Clear() can run between the Add that posted this job and the job itself.
            if (f.Lines.Count > 0)
                List.ScrollIntoView(f.Lines[^1]);
        }, DispatcherPriority.Background);
    }
}
