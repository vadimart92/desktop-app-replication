using Microsoft.Data.Sqlite;
using NUnit.Framework;
using Sample.App.ViewModels;
using Sample.Lab;

namespace Replication.Tests;

public sealed class OwnerPanelViewModelTests
{
    [Test]
    public async Task Clock_command_shows_a_database_error_instead_of_throwing()
    {
        string dir = TestDb.NewDir();
        await using OwnerNode owner = await OwnerNode.StartAsync(Path.Combine(dir, "owner.db"));
        using (SqliteConnection c = owner.Store.Open())
            TestDb.Exec(c, "CREATE TRIGGER clock_fails BEFORE UPDATE OF clock_offset ON _sync_meta BEGIN SELECT RAISE(ABORT, 'clock is broken'); END");
        var panel = new OwnerPanelViewModel(owner);

        Assert.That(() => panel.PlusDayCommand.Execute(null), Throws.Nothing);
        Assert.That(panel.Error, Does.Contain("clock is broken"));
    }
}
