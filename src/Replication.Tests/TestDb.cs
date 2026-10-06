using Microsoft.Data.Sqlite;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Database plumbing the tests share.</summary>
internal static class TestDb
{
    /// <summary>A new directory for the databases of one test, named like the lab's.</summary>
    public static string NewDir() => Directory.CreateDirectory(Lab.NewDir()).FullName;

    /// <summary>Runs raw SQL on the connection, past EF and the router.</summary>
    public static int Exec(SqliteConnection c, string sql, params (string Name, object Value)[] args)
    {
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach ((string name, object value) in args)
            cmd.Parameters.AddWithValue(name, value);
        return cmd.ExecuteNonQuery();
    }
}
