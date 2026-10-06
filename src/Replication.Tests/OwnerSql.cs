using Microsoft.Data.Sqlite;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Raw SQL on the owner's database, for state the lab's API does not write and for counts it does not show.</summary>
internal static class OwnerSql
{
    public static void Exec(Lab lab, string sql) => Run(lab, sql, cmd => cmd.ExecuteNonQuery());

    public static long Scalar(Lab lab, string sql) => Run(lab, sql, cmd => (long)cmd.ExecuteScalar()!);

    private static T Run<T>(Lab lab, string sql, Func<SqliteCommand, T> execute)
    {
        using SqliteConnection c = lab.Owner.Store.Open();
        using SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return execute(cmd);
    }
}
