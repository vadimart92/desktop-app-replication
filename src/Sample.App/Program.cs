using Avalonia;
using Replication;
using Sample.Lab;

namespace Sample.App;

/// <summary>
/// Modes:
///   (no arguments)                                   lab: an owner and two clients in one window
///   --owner [--port 5005] [--db owner.db] [--any]    owner with a window
///   --owner --headless [--port 5005] [--db ...]      owner without UI (console)
///   --client --connect http://host:5005 [--db client.db] [--name "Клієнт"]
/// </summary>
public static class Program
{
    public static StartOptions Options { get; private set; } = new();

    [STAThread]
    public static int Main(string[] args)
    {
        Options = StartOptions.Parse(args);
        if (Options.Mode == AppMode.Owner && Options.Headless)
            return RunHeadlessOwner(Options).GetAwaiter().GetResult();
        return BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
    }

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .LogToTrace();

    /// <summary>The owner can run headless (design 2): the same application, no window, the gRPC server inside.</summary>
    private static async Task<int> RunHeadlessOwner(StartOptions o)
    {
        var log = new SyncLog();
        log.Written += e => Console.WriteLine($"{e.At:HH:mm:ss.fff} [{e.Source}] {e.Text}");
        await using OwnerNode owner = await OwnerNode.StartAsync(o.Db ?? "owner.db", log, o.Port, o.ListenAnywhere);
        Console.WriteLine($"Власник працює: {owner.Address} (БД {owner.DbPath}). Ctrl+C для зупинки.");
        var done = new TaskCompletionSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            done.TrySetResult();
        };
        await done.Task;
        return 0;
    }
}

public enum AppMode { Lab, Owner, Client }

public sealed class StartOptions
{
    public AppMode Mode { get; private set; } = AppMode.Lab;
    public bool Headless { get; private set; }
    public bool ListenAnywhere { get; private set; }
    public int Port { get; private set; } = 5005;
    public string? Db { get; private set; }
    public string? Connect { get; private set; }
    public string? Name { get; private set; }

    public static StartOptions Parse(string[] args)
    {
        var o = new StartOptions();
        for (int i = 0; i < args.Length; i++)
        {
            string Next() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"{args[i]}: потрібне значення");
            switch (args[i])
            {
                case "--owner": o.Mode = AppMode.Owner; break;
                case "--client": o.Mode = AppMode.Client; break;
                case "--headless": o.Headless = true; break;
                case "--any": o.ListenAnywhere = true; break;
                case "--port": o.Port = int.Parse(Next()); break;
                case "--db": o.Db = Next(); break;
                case "--connect": o.Connect = Next(); break;
                case "--name": o.Name = Next(); break;
            }
        }
        if (o.Mode == AppMode.Client && o.Connect is null)
            o.Connect = "http://127.0.0.1:5005";
        return o;
    }
}
