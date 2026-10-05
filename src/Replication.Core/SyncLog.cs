namespace Replication;

public enum SyncLogLevel { Info, Ok, Warn, Bad }

public sealed record SyncLogEntry(DateTimeOffset At, string Source, string Text, SyncLogLevel Level);

/// <summary>Human-readable trace of what the sync does, for the sample UI and for diagnostics.</summary>
public sealed class SyncLog
{
    public static readonly SyncLog Null = new();

    public event Action<SyncLogEntry>? Written;

    public void Write(string source, string text, SyncLogLevel level = SyncLogLevel.Info) =>
        Written?.Invoke(new SyncLogEntry(DateTimeOffset.Now, source, text, level));
}
