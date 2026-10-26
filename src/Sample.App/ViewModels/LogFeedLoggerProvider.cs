using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Sample.App.ViewModels;

/// <summary>
/// Writes the sync code's lines (not ASP.NET Core's or gRPC's) to <paramref name="feed"/>. A line with an <c>{Agent}</c>
/// prefix goes under that name; the owner's lines go under <paramref name="owner"/>, the lab's under "сценарій".
/// </summary>
public sealed class LogFeedLoggerProvider(LogFeed feed, string owner = "owner") : ILoggerProvider
{
    public const string Scenario = "сценарій";

    public ILogger CreateLogger(string categoryName)
    {
        if (categoryName.StartsWith("Replication.Owner.", StringComparison.Ordinal) || categoryName.StartsWith("Sample.Lab.Owner", StringComparison.Ordinal))
            return new FeedLogger(feed, owner, true);
        if (categoryName is "Sample.Lab.Lab" or "Sample.Lab.ScenarioRunner")
            return new FeedLogger(feed, Scenario, false);
        if (categoryName.StartsWith("Replication.", StringComparison.Ordinal) || categoryName.StartsWith("Sample.", StringComparison.Ordinal))
            return new FeedLogger(feed, categoryName[(categoryName.LastIndexOf('.') + 1)..], false);
        return NullLogger.Instance;
    }

    public void Dispose() { }

    private sealed class FeedLogger(LogFeed feed, string source, bool owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;
            string text = formatter(state, exception);
            // A client line: "[agent] text" goes under the agent's name.
            if (state is IReadOnlyList<KeyValuePair<string, object?>> props && props.FirstOrDefault(p => p.Key == "Agent").Value is { } agent)
            {
                string name = agent.ToString() ?? "";
                string prefix = $"[{name}] ";
                feed.Add(name, text.StartsWith(prefix, StringComparison.Ordinal) ? text[prefix.Length..] : text, logLevel, false);
                return;
            }
            feed.Add(source, text, logLevel, owner);
        }
    }
}
