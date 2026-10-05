using System.Text.Json;
using Replication.Model;
using Replication.Protocol;
using static Replication.Model.Wire;

namespace Replication.Client;

/// <summary>
/// A simple serializable filter for bulk actions (8.6): equalities on columns, ANDed.
/// Anything more complex falls back to sending the keys the user saw.
/// </summary>
public sealed class Predicate
{
    public Predicate(IEnumerable<KeyValuePair<string, object?>> equals) => Equals_ = [.. equals];

    public Predicate(string column, object? value) : this([new KeyValuePair<string, object?>(column, value)]) { }

    public IReadOnlyList<KeyValuePair<string, object?>> Equals_ { get; }

    public string Serialize() =>
        JsonSerializer.Serialize(Equals_.Select(kv => new Dictionary<string, object?> { ["c"] = kv.Key, ["v"] = kv.Value }));

    public static Predicate? Parse(string? json)
    {
        if (string.IsNullOrEmpty(json))
            return null;
        List<Dictionary<string, JsonElement>> items = JsonSerializer.Deserialize<List<Dictionary<string, JsonElement>>>(json)!;
        return new Predicate(items.Select(d => new KeyValuePair<string, object?>(d["c"].GetString()!, d["v"].ValueKind switch
        {
            JsonValueKind.Number when d["v"].TryGetInt64(out long l) => l,
            JsonValueKind.Number => d["v"].GetDouble(),
            JsonValueKind.String => d["v"].GetString(),
            JsonValueKind.True => 1L,
            JsonValueKind.False => 0L,
            _ => null,
        })));
    }

    public (string Where, (string, object?)[] Args) ToSql(SyncTable t)
    {
        var where = new List<string>();
        var args = new List<(string, object?)>();
        for (int i = 0; i < Equals_.Count; i++)
        {
            if (!t.HasColumn(Equals_[i].Key))
                throw new ArgumentException($"{t.Name} has no column {Equals_[i].Key}");
            where.Add($"{Q(Equals_[i].Key)} IS @q{i}");
            args.Add(($"@q{i}", Equals_[i].Value));
        }
        return (where.Count > 0 ? string.Join(" AND ", where) : "1", [.. args]);
    }

    public IEnumerable<Condition> ToWire() => Equals_.Select(kv => new Condition { Column = kv.Key, Value = ToValue(kv.Value) });

    public override string ToString() => string.Join(" AND ", Equals_.Select(kv => $"{kv.Key} = '{kv.Value}'"));
}

/// <summary>The row set of an archive action, stored in <c>_sync_outbox.predicate</c>.</summary>
internal static class ArchiveSet
{
    public static string Serialize(IEnumerable<(string Table, string Pk)> rows) =>
        JsonSerializer.Serialize(rows.Select(r => new[] { r.Table, r.Pk }));

    public static List<(string Table, string Pk)> Parse(string? json) =>
        string.IsNullOrEmpty(json) ? [] : [.. JsonSerializer.Deserialize<List<string[]>>(json)!.Select(a => (a[0], a[1]))];
}
