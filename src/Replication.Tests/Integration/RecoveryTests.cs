using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using NUnit.Framework;
using Replication.Client;
using Sample.Lab;

namespace Replication.Tests.Integration;

/// <summary>
/// Restarts and recovery (5.3, 10.3, 14): either side restarts and sync resumes from what is in the databases;
/// a restored owner database and a client behind the retention period both end in a new snapshot that replaces the replica and drops the outbox.
/// </summary>
[TestFixture]
public sealed class RecoveryTests
{
    private TestLog _log = null!;
    private string _dir = "";
    private OwnerNode? _owner;
    private ClientNode? _client;
    private int _port;

    private string OwnerPath => Path.Combine(_dir, "owner.db");
    private string ClientPath => Path.Combine(_dir, "client.db");

    [SetUp]
    public void SetUp()
    {
        _log = new TestLog();
        _dir = TestDb.NewDir();
    }

    [TearDown]
    public async Task TearDown()
    {
        await StopClientAsync();
        await StopOwnerAsync();
        _log.WriteTo(TestContext.Out);
        _log.Dispose();
    }

    [Test]
    public async Task Client_restart_resumes_from_its_cursors_and_keeps_its_outbox()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();
        string instance = _client!.Instance;
        long snapshot = SnapshotVersion();
        await GoOfflineAsync();
        await _client.SetPriceAsync("Степлер", 150);

        await StopClientAsync();
        await _owner!.SetPriceAsync("Палета", 470);
        StartClient(link: false);

        // The agent learns who the owner is on the first link.
        Assert.That(_client.Agent.InstanceId, Is.Null);

        _client.Link = true;
        await Lab.WaitAsync(() => _client.Agent.InstanceId is not null, TimeSpan.FromSeconds(30), "no Identity");
        Assert.That(_client.Agent.InstanceId, Is.EqualTo(instance), "the owner's instance id found the replica");
        await SettleAsync();
        Assert.That(Inspect.OwnerItem(_owner, "Степлер")?.Price, Is.EqualTo(150));
        Assert.That(Inspect.Diff(_owner, _client), Is.Empty);
        Assert.That(SnapshotVersion(), Is.EqualTo(snapshot), "a catch-up, not a new snapshot");
    }

    [Test]
    public async Task Another_owner_at_the_same_address_gets_its_own_replica_and_the_first_one_stays()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();
        string first = _owner!.Store.InstanceId;
        int firstRows = Rows.All(ClientPath, "Item", first).Count;

        // A new owner database answers at the same port while the agent keeps running.
        await GoOfflineAsync();
        await StopOwnerAsync();
        foreach (string f in new[] { OwnerPath, OwnerPath + "-wal", OwnerPath + "-shm" })
            File.Delete(f);
        await StartOwnerAsync();
        _client!.Link = true;
        await SettleAsync();

        Assert.That(_owner!.Store.InstanceId, Is.Not.EqualTo(first));
        Assert.That(_client.Agent.InstanceId, Is.EqualTo(_owner.Store.InstanceId), "Identity told the agent who is at the address now");
        Assert.That(Rows.All(ClientPath, "Item", first), Has.Count.EqualTo(firstRows), "the replica of the first owner is untouched");
        Assert.That(Rows.Scalar(ClientPath, "SELECT COUNT(*) FROM _sync_instances"), Is.EqualTo(2L));
        Assert.That(Inspect.Diff(_owner, _client), Is.Empty);
    }

    [Test]
    public async Task Client_reconnects_to_the_same_owner_at_another_address_and_resumes_the_replica()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();
        string instance = _owner!.Store.InstanceId;
        long snapshot = SnapshotVersion();
        long cursor = _client!.Agent.CursorOf("Item").Cursor;
        string oldAddress = _client.Agent.Address;
        int snapshotsBefore = SnapshotRequests();

        // The owner comes back on the same database at another port; the client learns the address from its app.
        await StopClientAsync();
        await StopOwnerAsync();
        _port = 0;
        await StartOwnerAsync();
        await _owner!.SetPriceAsync("Палета", 470);
        StartClient();
        await SettleAsync();

        Assert.That(_client!.Agent.Address, Is.Not.EqualTo(oldAddress));
        Assert.That(_owner.Store.InstanceId, Is.EqualTo(instance));
        Assert.That(_client.Agent.InstanceId, Is.EqualTo(instance), "the owner's instance id matched the row in _sync_instances");
        Assert.That(Rows.Scalar(ClientPath, "SELECT COUNT(*) FROM _sync_instances"), Is.EqualTo(1L));
        Assert.That(SnapshotVersion(), Is.EqualTo(snapshot), "a catch-up, not a new snapshot");
        Assert.That(SnapshotRequests(), Is.EqualTo(snapshotsBefore));
        Assert.That(_client.Agent.CursorOf("Item").Cursor, Is.GreaterThan(cursor), "resumed from the stored cursor and moved on");
        Assert.That(Inspect.OwnerItem(_owner, "Палета")?.Price, Is.EqualTo(470));
        Assert.That(Inspect.Diff(_owner, _client), Is.Empty);
    }

    [Test]
    public async Task Owner_restart_keeps_its_instance_and_clients_catch_up_on_what_the_app_wrote_meanwhile()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();
        string instance = _owner!.Store.InstanceId;
        long snapshot = SnapshotVersion();

        await StopOwnerAsync();
        // The application writes while its sync server is down: the triggers still version every write.
        await using (Sample.Domain.SampleDbContext db = Sample.Domain.SampleDbContext.Open(OwnerPath))
        {
            db.Log.Add(new Sample.Domain.LogEntry { Text = "без сервера синку" });
            await db.SaveChangesAsync();
        }
        await StartOwnerAsync();

        Assert.That(_owner.Store.InstanceId, Is.EqualTo(instance));
        await SettleAsync();
        Assert.That(Inspect.Diff(_owner, _client!), Is.Empty);
        Assert.That(SnapshotVersion(), Is.EqualTo(snapshot));
    }

    [Test]
    public async Task Owner_restored_from_a_backup_gets_a_new_instance_and_the_client_takes_a_snapshot_that_replaces_the_old_replica()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();
        string oldInstance = _owner!.Store.InstanceId;
        await StopOwnerAsync();
        string backup = Path.Combine(_dir, "owner.backup.db");
        File.Copy(OwnerPath, backup);

        // The owner moves on, the client follows, then the database file is replaced by the older backup.
        await StartOwnerAsync();
        await _owner!.AddLogAsync(5);
        await SettleAsync();
        await GoOfflineAsync();
        await _client!.SetPriceAsync("Степлер", 150);
        await StopOwnerAsync();
        File.Copy(backup, OwnerPath, overwrite: true);
        await StartOwnerAsync();

        // Its version is below the one recorded next to the database: a new instance id (5.3).
        Assert.That(_owner!.Store.InstanceId, Is.Not.EqualTo(oldInstance));
        _client.Link = true;
        await SettleAsync();

        Assert.That(_client.Instance, Is.EqualTo(_owner.Store.InstanceId));
        Assert.That(Rows.All(ClientPath, "Item", oldInstance), Is.Empty, "the snapshot of the restored owner replaced the old replica");
        Assert.That(Rows.Scalar(ClientPath, "SELECT COUNT(*) FROM _sync_instances"), Is.EqualTo(1L));
        Assert.That(Rows.Scalar(ClientPath, $"SELECT COUNT(*) FROM _sync_cursors WHERE instance = '{oldInstance}'"), Is.EqualTo(0L));
        Assert.That(Inspect.OwnerItem(_owner, "Степлер")?.Price, Is.Not.EqualTo(150), "the offline edit belonged to the old replica and is never sent");
        Assert.That(_client.Replication.Store.Entries(oldInstance), Is.Empty, "dropped with the old replica");
        Assert.That(_client.Outbox(), Is.Empty);
        Assert.That(Inspect.Diff(_owner, _client), Is.Empty);
    }

    [Test]
    public async Task Archive_retries_of_the_old_instance_do_not_reach_the_owner_restored_from_a_backup()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();
        await StopOwnerAsync();
        string backup = Path.Combine(_dir, "owner.backup.db");
        File.Copy(OwnerPath, backup);

        // The owner's change is held back from the client, so the client archives an older version and the owner keeps the row (11.2, step 6).
        await StartOwnerAsync();
        await SettleAsync();
        _owner!.Store.Options.Faults.DelayStream(_client!.Replication.ClientId, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(30));
        await _owner.SetPriceAsync("Палета", 470);
        await _client.ArchiveAsync(("Item", "Палета"));
        await Lab.WaitAsync(() => _client.Agent.HasPendingArchiveWork, TimeSpan.FromSeconds(30), "the owner did not keep the changed row");

        // The restored owner holds the same Ids under a new instance id (5.3): the retry belonged to the old instance.
        await GoOfflineAsync();
        await StopOwnerAsync();
        File.Copy(backup, OwnerPath, overwrite: true);
        await StartOwnerAsync();
        _client.Link = true;
        await SettleAsync();

        Assert.That(_client.Instance, Is.EqualTo(_owner!.Store.InstanceId));
        Assert.That(_client.Agent.HasPendingArchiveWork, Is.False, "the archive retry of the old instance is forgotten");
        Assert.That(Inspect.OwnerItem(_owner, "Палета"), Is.Not.Null);
        Assert.That(Inspect.ClientHas(_client, "Палета"), Is.True);
        Assert.That(Inspect.Diff(_owner, _client), Is.Empty);
    }

    [Test]
    public async Task Back_off_starts_over_once_the_owner_accepts_Start()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();

        // Failed rounds double the wait up to ReconnectMax (1 s).
        await StopOwnerAsync();
        await Lab.WaitAsync(() => Retries().Contains(1.0), TimeSpan.FromSeconds(30), "the back-off did not reach ReconnectMax");
        await StartOwnerAsync();
        await SettleAsync();
        int before = Retries().Count;

        await StopOwnerAsync();
        await Lab.WaitAsync(() => Retries().Count > before, TimeSpan.FromSeconds(30), "the client did not lose the link");
        Assert.That(Retries()[before], Is.EqualTo(0.2), "the first retry after an accepted Start waits ReconnectMin");
    }

    [Test]
    public async Task Client_behind_the_retention_period_takes_a_snapshot_and_drops_its_outbox()
    {
        await StartOwnerAsync();
        StartClient();
        await SettleAsync();
        long snapshot = SnapshotVersion();
        await GoOfflineAsync();
        await _client!.SetPriceAsync("Палета", 470);
        await _owner!.DeleteItemAsync("Скотч");
        await _owner.DeleteItemAsync("Факс");

        // A month later the owner forgets the silent client and purges the tombstones it never got (10.2).
        _owner.AdvanceClock(TimeSpan.FromDays(31));
        Replication.Owner.OwnerStore.PurgeResult purged = await _owner.PurgeAsync();
        Assert.That(purged.Tombstones, Is.EqualTo(2));
        Assert.That(purged.PurgedVersion, Is.GreaterThan(_client.Agent.CursorOf("Item").Cursor));

        _client.Link = true;
        await SettleAsync();

        Assert.That(SnapshotVersion(), Is.GreaterThan(snapshot), "cursor behind purged_version, a new snapshot");
        Assert.That(Inspect.OwnerItem(_owner, "Палета")?.Price, Is.Not.EqualTo(470), "the offline edit belonged to the old replica and is dropped");
        Assert.That(_client.Outbox(), Is.Empty);
        Assert.That(Inspect.ClientHas(_client, "Скотч"), Is.False);
        Assert.That(Inspect.Diff(_owner, _client), Is.Empty);
    }

    private async Task StartOwnerAsync()
    {
        _owner = await OwnerNode.StartAsync(OwnerPath, _port, configure: o => o.OnlineInterval = TimeSpan.FromMilliseconds(300), logging: _log.AddTo);
        _port = _owner.Host.Port;
    }

    private async Task StopOwnerAsync()
    {
        if (_owner is not null)
            await _owner.DisposeAsync();
        _owner = null;
        SqliteConnection.ClearAllPools();
    }

    private void StartClient(bool link = true)
    {
        var options = new AgentOptions
        {
            AckInterval = TimeSpan.FromMilliseconds(300),
            ReconnectMin = TimeSpan.FromMilliseconds(200),
            ReconnectMax = TimeSpan.FromSeconds(1),
        };
        _client = ClientNode.Connect(ClientNode.Create(ClientPath, _log.Loggers), "клієнт", $"http://127.0.0.1:{_port}", options);
        _client.Link = link;
    }

    private async Task StopClientAsync()
    {
        if (_client is not null)
            await _client.Replication.DisposeAsync();
        _client = null;
    }

    private async Task GoOfflineAsync()
    {
        _client!.Link = false;
        await Lab.WaitAsync(() => _client.Agent.GetStatus().State == AgentState.Offline, TimeSpan.FromSeconds(30), "the client did not go offline");
    }

    private Task SettleAsync() =>
        Lab.WaitAsync(() => _client!.Agent.GetStatus() is { State: AgentState.Online, Pending: 0 } && _client.Agent.MinCursor >= _owner!.Head(),
            TimeSpan.FromSeconds(30), "the client did not catch up with the owner");

    private int SnapshotRequests()
    {
        return _log.OfAgent("клієнт").Count(l => l.StartsWith("→ Snapshot", StringComparison.Ordinal));
    }

    /// <summary>The waits of the agent's "link lost …, retry in N s" lines, in seconds, oldest first.</summary>
    private List<double> Retries() =>
        _log.OfAgent("клієнт")
            .Select(l => Regex.Match(l, @"link lost .*, retry in ([\d.,]+) s"))
            .Where(m => m.Success)
            .Select(m => double.Parse(m.Groups[1].Value.Replace(',', '.'), CultureInfo.InvariantCulture))
            .ToList();

    private long SnapshotVersion() => (long)Rows.Scalar(ClientPath, "SELECT snapshot_version FROM _sync_instances")!;
}
