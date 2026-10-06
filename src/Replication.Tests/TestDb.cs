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
        using SqliteCommand cmd = Command(c, sql, args);
        return cmd.ExecuteNonQuery();
    }

    /// <summary>The first column of the first row of raw SQL, or null when there is no row.</summary>
    public static object? Scalar(SqliteConnection c, string sql, params (string Name, object Value)[] args)
    {
        using SqliteCommand cmd = Command(c, sql, args);
        return cmd.ExecuteScalar();
    }

    private static SqliteCommand Command(SqliteConnection c, string sql, (string Name, object Value)[] args)
    {
        SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach ((string name, object value) in args)
            cmd.Parameters.AddWithValue(name, value);
        return cmd;
    }
}
