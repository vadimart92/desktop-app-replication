using Microsoft.Data.Sqlite;
using Sample.Lab;

namespace Replication.Tests;

/// <summary>Raw SQL on the lab owner's database, for state the lab's API does not write and for counts it does not show.</summary>
internal static class OwnerSql
{
    public static void Exec(Lab lab, string sql)
    {
        using SqliteConnection c = lab.Owner.Store.Open();
        TestDb.Exec(c, sql);
    }

    public static long Scalar(Lab lab, string sql)
    {
        using SqliteConnection c = lab.Owner.Store.Open();
        return (long)TestDb.Scalar(c, sql)!;
    }
}
