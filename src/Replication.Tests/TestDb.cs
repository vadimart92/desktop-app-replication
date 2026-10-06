using Microsoft.Data.Sqlite;

namespace Replication.Tests;

/// <summary>Database plumbing the tests share.</summary>
internal static class TestDb
{
    /// <summary>A new directory under the temp path for the databases of one test.</summary>
    public static string NewDir()
    {
        string dir = Path.Combine(Path.GetTempPath(), "replication-tests", Guid.NewGuid().ToString("N")[..12]);
        Directory.CreateDirectory(dir);
        return dir;
    }

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
