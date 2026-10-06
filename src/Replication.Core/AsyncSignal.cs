namespace Replication;

/// <summary>An auto-reset signal for async loops: Set wakes the next (or current) WaitAsync.</summary>
internal sealed class AsyncSignal
{
    private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Set() => Volatile.Read(ref _tcs).TrySetResult();

    /// <returns>true when signalled, false on timeout.</returns>
    public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        TaskCompletionSource tcs = Volatile.Read(ref _tcs);
        if (!tcs.Task.IsCompleted)
        {
            await tcs.Task.WaitAsync(timeout, ct).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            ct.ThrowIfCancellationRequested();
        }
        if (!tcs.Task.IsCompleted)
            return false;
        Interlocked.CompareExchange(ref _tcs, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), tcs);
        return true;
    }
}
