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

    public Snap? Find(string id) => _snaps.GetValueOrDefault(id);

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
            using (SqliteTransaction tx = c.BeginTransaction())
            {
                store.SaveCursors(c, tx, clientId, store.Model.Tables.ToDictionary(t => t.Name, _ => v));
                tx.Commit();
            }

            byte[] hash;
            await using (FileStream f = File.OpenRead(path))
                hash = await SHA256.HashDataAsync(f);
            var snap = new Snap(id, path, v, instance, new FileInfo(path).Length, hash, DateTimeOffset.UtcNow);
            _snaps[id] = snap;
            store.Options.Log.Write("owner", $"VACUUM INTO → {id}, V = {v}, {snap.Size / 1024} КБ; клієнт зареєстровано з acked_version = {v}");
            return snap;
        }
        finally
        {
            _gate.Release();
        }
    }

    private void Cleanup()
    {
        foreach (Snap s in _snaps.Values.Where(s => DateTimeOffset.UtcNow - s.Created > store.Options.SnapshotKeep).ToList())
        {
            _snaps.TryRemove(s.Id, out _);
            try
            {
                File.Delete(s.Path);
            }
            catch (IOException) { }
        }
    }
}
