using Microsoft.Data.Sqlite;
using NUnit.Framework;
using Replication.Client;
using Replication.Model;
using Replication.Protocol;
using Sample.Domain;

namespace Replication.Tests;

/// <summary>The predicate of a bulk delete (8.6): equalities ANDed, stored as JSON in the outbox, sent as conditions.</summary>
public sealed class PredicateTests
{
    [Test]
    public void Json_round_trip_keeps_strings_integers_doubles_and_NULL()
    {
        var p = new Predicate(new KeyValuePair<string, object?>[] { new("Status", "архів"), new("Price", 150L), new("Rate", 1.5), new("Note", null) });

        Predicate? back = Predicate.Parse(p.Serialize());

        Assert.That(back, Is.Not.Null);
        Assert.That(back!.Equalities.Select(e => (e.Key, e.Value)), Is.EqualTo(new (string, object?)[] { ("Status", "архів"), ("Price", 150L), ("Rate", 1.5), ("Note", null) }));
    }

    [Test]
    public void Booleans_come_back_as_one_and_zero()
    {
        var p = new Predicate(new KeyValuePair<string, object?>[] { new("A", true), new("B", false) });

        Predicate back = Predicate.Parse(p.Serialize())!;

        Assert.That(back.Equalities.Select(e => e.Value), Is.EqualTo(new object[] { 1L, 0L }));
    }

    [TestCase(null)]
    [TestCase("")]
    public void Nothing_to_parse_gives_no_predicate(string? json)
    {
        Assert.That(Predicate.Parse(json), Is.Null);
    }

    [Test]
    public void Predicate_without_conditions_matches_every_row()
    {
        var p = new Predicate(Array.Empty<KeyValuePair<string, object?>>());

        (string where, (string, object?)[] args) = p.ToSql(Item());

        Assert.That(where, Is.EqualTo("1"));
        Assert.That(args, Is.Empty);
        Assert.That(Predicate.Parse(p.Serialize())!.Equalities, Is.Empty);
    }

    [Test]
    public void Sql_uses_IS_so_that_NULL_matches_NULL_and_numbers_the_parameters()
    {
        var p = new Predicate(new KeyValuePair<string, object?>[] { new("Status", "архів"), new("Price", null) });

        (string where, (string, object?)[] args) = p.ToSql(Item());

        Assert.That(where, Is.EqualTo("\"Status\" IS @q0 AND \"Price\" IS @q1"));
        Assert.That(args, Is.EqualTo(new (string, object?)[] { ("@q0", "архів"), ("@q1", null) }));
    }

    [Test]
    public void Column_the_table_does_not_have_is_refused_and_not_put_into_the_SQL()
    {
        var p = new Predicate("Status\" OR 1=1 --", "x");

        Assert.That(() => p.ToSql(Item()), Throws.ArgumentException.With.Message.Contain("has no column"));
    }

    [Test]
    public void Wire_conditions_carry_column_and_typed_value()
    {
        var p = new Predicate(new KeyValuePair<string, object?>[] { new("Status", "архів"), new("Price", 5L), new("Note", null) });

        List<Condition> wire = [.. p.ToWire()];

        Assert.That(wire.Select(c => c.Column), Is.EqualTo(new[] { "Status", "Price", "Note" }));
        Assert.That(wire[0].Value.S, Is.EqualTo("архів"));
        Assert.That(wire[1].Value.I, Is.EqualTo(5L));
        Assert.That(wire[2].Value.VCase, Is.EqualTo(Value.VOneofCase.None));
    }

    [Test]
    public void Predicate_matches_NULL_columns_in_SQLite_as_the_owner_deletes_them()
    {
        using var c = new SqliteConnection("Data Source=:memory:");
        c.Open();
        TestDb.Exec(c, "CREATE TABLE Item (Id TEXT, Status TEXT, Price INTEGER)");
        TestDb.Exec(c, "INSERT INTO Item VALUES ('1','архів',1), ('2','архів',NULL), ('3',NULL,NULL), ('4','новий',NULL)");
        var p = new Predicate(new KeyValuePair<string, object?>[] { new("Status", "архів"), new("Price", null) });
        (string where, (string, object?)[] args) = p.ToSql(Item());
        (string, object)[] named = [.. args.Select(a => (a.Item1, a.Item2 ?? DBNull.Value))];

        object? ids = TestDb.Scalar(c, $"SELECT group_concat(Id) FROM Item WHERE {where}", named);

        Assert.That(ids, Is.EqualTo("2"));
    }

    private static SyncTable Item()
    {
        using SampleDbContext db = SampleDbContext.Open(":memory:");
        return SyncModel.From(db)["Item"];
    }
}
