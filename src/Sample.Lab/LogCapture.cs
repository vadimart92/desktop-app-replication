using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Replication.Client;

namespace Sample.Lab;

/// <summary>Keeps what the agents log, so scenarios can check the messages a client got (lost edits, rejected rows).</summary>
public sealed class LogCapture : ILoggerProvider
{
    private readonly List<string> _lines = [];

    public ILogger CreateLogger(string categoryName) =>
        categoryName == typeof(SyncAgent).FullName ? new Capture(this) : NullLogger.Instance;

    /// <summary>Lines of the agent named <paramref name="agent"/>, without the <c>[agent] </c> prefix.</summary>
    public List<string> Of(string agent)
    {
        string prefix = $"[{agent}] ";
        lock (_lines)
            return [.. _lines.Where(l => l.StartsWith(prefix, StringComparison.Ordinal)).Select(l => l[prefix.Length..])];
    }

    /// <summary>Whether the agent named <paramref name="agent"/> logged a line containing <paramref name="fragment"/>.</summary>
    public bool Has(string agent, string fragment) => Of(agent).Any(l => l.Contains(fragment, StringComparison.Ordinal));

    public void Dispose()
    {
    }

    private sealed class Capture(LogCapture owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;
            string line = formatter(state, exception);
            lock (owner._lines)
                owner._lines.Add(line);
        }
    }
}
