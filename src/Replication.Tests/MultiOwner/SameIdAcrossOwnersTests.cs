using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Sample.Domain;

namespace Replication.Tests.MultiOwner;

/// <summary>
/// Id is the only key of a table in the client database (5.4), so owners must hold disjoint sets of Ids: rows cannot be told
/// apart otherwise. When they do collide (a cloned owner) the first owner's row keeps its place and the second owner's row is
/// not loaded; this is expected, not a bug.
/// </summary>
[TestFixture]
public sealed class SameIdAcrossOwnersTests
{
    private TwoOwnerRig? _rig;

    [TearDown]
    public async Task StopAsync()
    {
        if (_rig is not null)
            await _rig.DisposeAsync();
        _rig = null;
    }

    [Test]
    public async Task First_owners_row_keeps_its_place_and_the_second_owners_catch_up_still_completes()
    {
        Guid shared = await StartWithSharedCategoryAsync();

        Assert.That(Sql.Scalar<string>(_rig!.ClientPath, "SELECT Name FROM Category WHERE Id = @i", ("@i", Wire.PkText(shared))), Is.EqualTo("Спільна А"));
        Assert.That(Sql.Scalar<string>(_rig.ClientPath, "SELECT InstanceId FROM Category WHERE Id = @i", ("@i", Wire.PkText(shared))), Is.EqualTo(_rig.ToA.InstanceId));
        Assert.That(_rig.ToB.MinCursor, Is.GreaterThanOrEqualTo(_rig.B.Head()), "the cursor of owner 2 moved on");
    }

    [Test]
    public async Task Row_whose_id_another_owner_already_holds_is_not_loaded_into_the_replica_of_the_second_owner()
    {
        Guid shared = await StartWithSharedCategoryAsync();

        long inReplicaOfB = Sql.Scalar<long>(_rig!.ClientPath, "SELECT COUNT(*) FROM Category WHERE Id = @i AND InstanceId = @b", ("@i", Wire.PkText(shared)), ("@b", _rig.ToB.InstanceId));
        Assert.That(inReplicaOfB, Is.Zero, "Id is the only key on the client; Ids must not repeat across owners");
    }

    [Test]
    public async Task Snapshot_that_shares_ids_with_an_instance_another_agent_serves_is_refused_and_the_link_stops_as_Duplicate()
    {
        Guid shared = await StartWithSharedCategoryAsync(joinB: false);
        SyncAgent toB = _rig!.Client.Connect(_rig.B, snapshot: true);

        toB.LinkEnabled = true;
        await Wait.UntilAsync(() => toB.GetStatus().State == AgentState.Duplicate, TimeSpan.FromSeconds(30), "the snapshot of owner 2 was not refused");

        Assert.That(toB.LinkEnabled, Is.False, "no retry: the same snapshot would be refused again");
        Assert.That(toB.GetStatus().LastError, Does.StartWith($"the snapshot shares Ids with instance {_rig.ToA.InstanceId}"));

        Assert.That(Sql.Scalar<string>(_rig.ClientPath, "SELECT Name FROM Category WHERE Id = @i", ("@i", Wire.PkText(shared))), Is.EqualTo("Спільна А"));
        Assert.That(Sql.Scalar<long>(_rig.ClientPath, "SELECT COUNT(*) FROM _sync_instances WHERE snapshot_version IS NOT NULL AND instance = @b", ("@b", toB.InstanceId)), Is.Zero);
    }

    /// <summary>Both owners hold a category with the same Id and different names; the client joins owner 1, then owner 2.</summary>
    private async Task<Guid> StartWithSharedCategoryAsync(bool joinB = true)
    {
        _rig = await TwoOwnerRig.StartAsync();
        var shared = Guid.NewGuid();
        foreach ((TestOwner owner, string name) in new[] { (_rig.A, "Спільна А"), (_rig.B, "Спільна Б") })
        {
            await using SampleDbContext db = owner.Db();
            db.Categories.Add(new Category { Id = shared, Name = name });
            await db.SaveChangesAsync();
        }
        await TwoOwnerRig.SyncNowAsync(_rig.ToA);
        await _rig.SettleAsync();
        if (joinB)
        {
            await TwoOwnerRig.SyncNowAsync(_rig.ToB);
            await _rig.SettleAsync();
        }
        return shared;
    }
}
