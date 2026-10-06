using Microsoft.Data.Sqlite;
using Sample.App.ViewModels;
using Sample.Lab;
using Xunit;

namespace Replication.Tests;

public sealed class OwnerPanelViewModelTests
{
    [Fact]
    public async Task Clock_command_shows_a_database_error_instead_of_throwing()
    {
        string dir = TestDb.NewDir();
        await using OwnerNode owner = await OwnerNode.StartAsync(Path.Combine(dir, "owner.db"), new SyncLog());
        using (SqliteConnection c = owner.Store.Open())
            TestDb.Exec(c, "CREATE TRIGGER clock_fails BEFORE UPDATE OF clock_offset ON _sync_meta BEGIN SELECT RAISE(ABORT, 'clock is broken'); END");
        var panel = new OwnerPanelViewModel(owner);

        Exception? thrown = Record.Exception(() => panel.PlusDayCommand.Execute(null));

        Assert.Null(thrown);
        Assert.Contains("clock is broken", panel.Error);
    }
}
