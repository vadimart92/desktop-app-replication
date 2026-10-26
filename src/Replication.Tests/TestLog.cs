using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Replication.Client;

namespace Replication.Tests;

/// <summary>What the sync code logged during one test: for the few asserts that need it, and for the test output.</summary>
internal sealed class TestLog : IDisposable
{
    private readonly FakeLogCollector _collector = new();

    public TestLog()
    {
        Loggers = LoggerFactory.Create(AddTo);
    }

    /// <summary>Loggers that write here, for clients; a host takes <see cref="AddTo"/>.</summary>
    public ILoggerFactory Loggers { get; }

    public void AddTo(ILoggingBuilder b) => b.AddProvider(new FakeLoggerProvider(_collector));

    /// <summary>Lines of the categories starting with <paramref name="category"/> that start with <c>[<paramref name="who"/>] </c>, without that prefix.</summary>
    public List<string> About(string category, string who)
    {
        string prefix = $"[{who}] ";
        return [.. _collector.GetSnapshot()
            .Where(r => r.Category?.StartsWith(category, StringComparison.Ordinal) == true && r.Message.StartsWith(prefix, StringComparison.Ordinal))
            .Select(r => r.Message[prefix.Length..])];
    }

    /// <summary>Lines of the agent named <paramref name="agent"/>.</summary>
    public List<string> OfAgent(string agent) => About(typeof(SyncAgent).FullName!, agent);

    public void WriteTo(TextWriter w)
    {
        foreach (FakeLogRecord r in _collector.GetSnapshot())
            w.WriteLine($"{r.Timestamp.ToLocalTime():HH:mm:ss.fff} {r.Level} [{r.Category?[(r.Category.LastIndexOf('.') + 1)..]}] {r.Message}");
    }

    public void Dispose() => Loggers.Dispose();
}
