using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Replication.Model;

namespace Replication.Owner;

/// <summary>
/// Snapshot files made with VACUUM INTO, served in chunks and resumable by snapshot_id and offset (6.3).
/// </summary>
internal sealed class SnapshotStore(OwnerStore store)
{
    private readonly ConcurrentDictionary<string, Snap> _snaps = new();
    private readonly SemaphoreSlim _gate = new(1, 1);

    internal sealed record Snap(string Id, string Path, long Version, string InstanceId, long Size, byte[] Sha256, DateTimeOffset Created);

    private string Dir
    {
        get
        {
            string d = store.Options.SnapshotDirectory ?? System.IO.Path.Combine(System.IO.Path.GetDirectoryName(store.DbPath)!, "snapshots");
            Directory.CreateDirectory(d);
            return d;
        }
    }

    /// <summary>A tracked snapshot whose file is still there; another owner sharing the folder may have swept it.</summary>
    public Snap? Find(string id) => _snaps.TryGetValue(id, out Snap? s) && File.Exists(s.Path) ? s : null;

    /// <summary>
    /// VACUUM INTO is a consistent copy as of the start of reading and does not block the writer.
    /// In the same operation the client is registered with acked_version = V, so cleanup keeps the tombstones it needs next.
    /// </summary>
    public async Task<Snap> CreateAsync(string clientId)
    {
        await _gate.WaitAsync();
        try
        {
            Cleanup();
            string id = "snap-" + Guid.NewGuid().ToString("N")[..10];
            string path = System.IO.Path.Combine(Dir, id + ".db");
            Snap snap;
            try
            {
                using (SqliteConnection c = store.Open())
                    c.Exec("VACUUM INTO @p", null, ("@p", path));

                long v;
                string instance;
                using (var s = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ToString()))
                {
                    s.Open();
                    v = s.Scalar<long>("SELECT version FROM _sync_meta");
                    instance = s.Scalar<string>("SELECT instance_id FROM _sync_meta")!;
                }

                using (SqliteConnection c = store.Open())
                    store.SaveCursors(c, clientId, store.Model.Tables.ToDictionary(t => t.Name, _ => v));

                byte[] hash;
                await using (FileStream f = File.OpenRead(path))
                    hash = await SHA256.HashDataAsync(f);
                snap = new Snap(id, path, v, instance, new FileInfo(path).Length, hash, DateTimeOffset.UtcNow);
            }
            catch
            {
                // Not tracked yet, and a failed VACUUM INTO can leave a partial file.
                TryDelete(path);
                throw;
            }

            _snaps[id] = snap;
            store.Options.Log.Write("owner", $"VACUUM INTO → {id}, V = {snap.Version}, {snap.Size / 1024} КБ; клієнт зареєстровано з acked_version = {snap.Version}");
            return snap;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Deletes snapshots older than SnapshotKeep: tracked ones, which stay tracked while a delete fails (a download may
    /// still hold the file), and files left by an earlier run of the owner. A fresh untracked file may belong to another
    /// owner sharing the folder, so it stays.
    /// </summary>
    private void Cleanup()
    {
        DateTimeOffset cutoff = DateTimeOffset.UtcNow - store.Options.SnapshotKeep;
        foreach (Snap s in _snaps.Values.Where(s => s.Created < cutoff).ToList())
        {
            if (TryDelete(s.Path))
                _snaps.TryRemove(s.Id, out _);
        }

        foreach (string f in Directory.EnumerateFiles(Dir, "snap-*.db"))
        {
            if (!_snaps.ContainsKey(System.IO.Path.GetFileNameWithoutExtension(f)) && File.GetLastWriteTimeUtc(f) < cutoff.UtcDateTime)
                TryDelete(f);
        }
    }

    private bool TryDelete(string path)
    {
        try
        {
            File.Delete(path);
            return true;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            store.Options.Log.Write("owner", $"знімок {System.IO.Path.GetFileName(path)}: не вдалося видалити ({e.Message})", SyncLogLevel.Warn);
            return false;
        }
    }
}
