using Google.Protobuf;
using Microsoft.Data.Sqlite;
using Replication.Protocol;

namespace Replication.Model;

/// <summary>Conversions between SQLite values, GUID keys and protobuf messages.</summary>
public static class Wire
{
    /// <summary>EF Core stores Guid in SQLite as upper-case TEXT; the core keeps the same format everywhere.</summary>
    public static string PkText(Guid id) => id.ToString().ToUpperInvariant();

    public static ByteString PkBytes(Guid id) => ByteString.CopyFrom(id.ToByteArray());

    public static ByteString PkBytes(string text) => PkBytes(Guid.Parse(text));

    public static Guid PkGuid(ByteString b) => new Guid(b.Span);

    public static string PkText(ByteString b) => PkText(PkGuid(b));

    public static Value ToValue(object? v) => v switch
    {
        null or DBNull => new Value(),
        long l => new Value { I = l },
        int i => new Value { I = i },
        bool b => new Value { I = b ? 1 : 0 },
        double d => new Value { D = d },
        float f => new Value { D = f },
        string s => new Value { S = s },
        byte[] bytes => new Value { B = ByteString.CopyFrom(bytes) },
        Guid g => new Value { S = PkText(g) },
        _ => new Value { S = Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture) },
    };

    public static object FromValue(Value v) => v.VCase switch
    {
        Value.VOneofCase.I => v.I,
        Value.VOneofCase.D => v.D,
        Value.VOneofCase.S => v.S,
        Value.VOneofCase.B => v.B.ToByteArray(),
        _ => DBNull.Value,
    };

    /// <summary>The tail of a GUID for logs: v7 GUIDs share their leading (time) digits.</summary>
    public static string Short(string pk) => pk.Length > 8 ? pk[^8..] : pk;

    public static string Q(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    public static RowRef Ref(string tbl, string pk) => new RowRef { Tbl = tbl, Pk = PkBytes(pk) };
}

internal static class SqliteExtensions
{
    public static SqliteCommand Cmd(this SqliteConnection c, string sql, SqliteTransaction? tx = null, params (string, object?)[] args)
    {
        SqliteCommand cmd = c.CreateCommand();
        cmd.CommandText = sql;
        cmd.Transaction = tx;
        foreach ((string n, object? v) in args)
            cmd.Parameters.AddWithValue(n, v ?? DBNull.Value);
        return cmd;
    }

    public static int Exec(this SqliteConnection c, string sql, SqliteTransaction? tx = null, params (string, object?)[] args)
    {
        using SqliteCommand cmd = c.Cmd(sql, tx, args);
        return cmd.ExecuteNonQuery();
    }

    public static T? Scalar<T>(this SqliteConnection c, string sql, SqliteTransaction? tx = null, params (string, object?)[] args)
    {
        using SqliteCommand cmd = c.Cmd(sql, tx, args);
        object? r = cmd.ExecuteScalar();
        if (r is null or DBNull)
            return default;
        return (T)Convert.ChangeType(r, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }

    public static List<T> Query<T>(this SqliteConnection c, string sql, SqliteTransaction? tx, Func<SqliteDataReader, T> read, params (string, object?)[] args)
    {
        using SqliteCommand cmd = c.Cmd(sql, tx, args);
        using SqliteDataReader r = cmd.ExecuteReader();
        var list = new List<T>();
        while (r.Read())
            list.Add(read(r));
        return list;
    }

    public static object? Raw(this SqliteDataReader r, int i) => r.IsDBNull(i) ? null : r.GetValue(i);
}
