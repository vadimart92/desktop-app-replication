using System.Collections.ObjectModel;
using Avalonia.Media;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using Replication;

namespace Sample.App.ViewModels;

public sealed record LogLine(string Time, string Source, string Text, IBrush Brush);

/// <summary>The trace of the sync code as a list for the window, newest at the bottom.</summary>
public sealed class LogFeed : ObservableObject
{
    private const int Max = 600;

    public ObservableCollection<LogLine> Lines { get; } = [];

    public event Action? Appended;

    public void Attach(SyncLog log) => log.Written += e => Dispatcher.UIThread.Post(() => Add(e));

    private void Add(SyncLogEntry e)
    {
        IBrush brush = e.Level switch
        {
            SyncLogLevel.Ok => Brushes.SeaGreen,
            SyncLogLevel.Warn => Brushes.DarkOrange,
            SyncLogLevel.Bad => Brushes.IndianRed,
            _ => e.Source == "owner" ? Brushes.SteelBlue : e.Source == "сценарій" ? Brushes.MediumPurple : Brushes.Gray,
        };
        Lines.Add(new LogLine(e.At.ToString("HH:mm:ss.fff"), e.Source, e.Text, brush));
        while (Lines.Count > Max) Lines.RemoveAt(0);
        Appended?.Invoke();
    }

    public void Clear() => Lines.Clear();
}

internal static class CollectionSync
{
    /// <summary>Updates the list in place so the grid keeps scroll position and selection where it can.</summary>
    public static void SyncTo<T>(this ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        for (var i = 0; i < source.Count; i++)
        {
            if (i < target.Count)
            {
                if (!Equals(target[i], source[i])) target[i] = source[i];
            }
            else target.Add(source[i]);
        }
        while (target.Count > source.Count) target.RemoveAt(target.Count - 1);
    }
}
