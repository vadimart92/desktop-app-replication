using System.Collections.ObjectModel;
using Avalonia.Media;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using Microsoft.Extensions.Logging;

namespace Sample.App.ViewModels;

public sealed record LogLine(string Time, string Source, string Text, IBrush Brush);

/// <summary>The trace of the sync code as a list for the window, newest at the bottom.</summary>
public sealed class LogFeed : ObservableObject
{
    private const int Max = 600;

    public ObservableCollection<LogLine> Lines { get; } = [];

    public event Action? Appended;

    /// <summary>A line from <see cref="LogFeedLoggerProvider"/>; <paramref name="owner"/> marks the owner's lines.</summary>
    public void Add(string source, string text, LogLevel level, bool owner)
    {
        DateTimeOffset at = DateTimeOffset.Now;
        IBrush brush = level switch
        {
            LogLevel.Warning => Brushes.DarkOrange,
            >= LogLevel.Error => Brushes.IndianRed,
            _ => owner ? Brushes.SteelBlue : source == LogFeedLoggerProvider.Scenario ? Brushes.MediumPurple : Brushes.Gray,
        };
        Dispatcher.UIThread.Post(() => Add(at, source, text, brush));
    }

    private void Add(DateTimeOffset at, string source, string text, IBrush brush)
    {
        Lines.Add(new LogLine(at.ToString("HH:mm:ss.fff"), source, text, brush));
        while (Lines.Count > Max)
            Lines.RemoveAt(0);
        Appended?.Invoke();
    }

    public void Clear() => Lines.Clear();
}

internal static class CollectionSync
{
    /// <summary>Updates the list in place so the grid keeps scroll position and selection where it can.</summary>
    public static void SyncTo<T>(this ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        for (int i = 0; i < source.Count; i++)
        {
            if (i < target.Count)
            {
                if (!Equals(target[i], source[i]))
                    target[i] = source[i];
            }
            else
            {
                target.Add(source[i]);
            }
        }
        while (target.Count > source.Count)
            target.RemoveAt(target.Count - 1);
    }
}
