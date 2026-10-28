using System.Globalization;
using NUnit.Framework;
using Replication.Model;
using Replication.Protocol;

namespace Replication.Tests;

/// <summary>Conversions between SQLite values and the protocol's <see cref="Value"/>, and GUID keys (5.1).</summary>
public sealed class WireTests
{
    [Test]
    public void Null_and_DBNull_are_an_unset_value_that_reads_back_as_DBNull()
    {
        Assert.That(Wire.ToValue(null).VCase, Is.EqualTo(Value.VOneofCase.None));
        Assert.That(Wire.ToValue(DBNull.Value).VCase, Is.EqualTo(Value.VOneofCase.None));
        Assert.That(Wire.FromValue(new Value()), Is.SameAs(DBNull.Value));
    }

    [Test]
    public void Integers_and_booleans_go_as_int64_and_floats_as_double()
    {
        Assert.That(Wire.ToValue(7L).I, Is.EqualTo(7L));
        Assert.That(Wire.ToValue(int.MinValue).I, Is.EqualTo(int.MinValue));
        Assert.That(Wire.ToValue(true).I, Is.EqualTo(1L));
        Assert.That(Wire.ToValue(false).I, Is.Zero);
        Assert.That(Wire.ToValue(1.5).D, Is.EqualTo(1.5));
        Assert.That(Wire.ToValue(1.5f).D, Is.EqualTo(1.5));
        Assert.That(Wire.ToValue(long.MaxValue).I, Is.EqualTo(long.MaxValue));
    }

    [Test]
    public void Empty_string_and_empty_blob_stay_set_values_not_NULL()
    {
        Value s = Wire.ToValue("");
        Value b = Wire.ToValue(Array.Empty<byte>());

        Assert.That(s.VCase, Is.EqualTo(Value.VOneofCase.S));
        Assert.That(Wire.FromValue(s), Is.EqualTo(""));
        Assert.That(b.VCase, Is.EqualTo(Value.VOneofCase.B));
        Assert.That((byte[])Wire.FromValue(b), Is.Empty);
    }

    [Test]
    public void Blob_round_trips_byte_for_byte()
    {
        byte[] bytes = [0, 255, 1, 2, 0];

        Assert.That((byte[])Wire.FromValue(Wire.ToValue(bytes)), Is.EqualTo(bytes));
    }

    [Test]
    public void Guid_goes_as_the_upper_case_text_EF_stores()
    {
        var g = Guid.Parse("0a1b2c3d-0000-4000-8000-00000000abcd");

        Value v = Wire.ToValue(g);

        Assert.That(v.S, Is.EqualTo("0A1B2C3D-0000-4000-8000-00000000ABCD"));
        Assert.That(Wire.PkText(g), Is.EqualTo(v.S));
    }

    [Test]
    public void DateTime_and_decimal_go_as_invariant_text_whatever_the_current_culture()
    {
        CultureInfo saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("uk-UA");
            var at = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc);

            Assert.That(Wire.ToValue(at).S, Is.EqualTo(at.ToString(CultureInfo.InvariantCulture)));
            Assert.That(Wire.ToValue(1234.5m).S, Is.EqualTo("1234.5"));
            Assert.That(Wire.ToValue(-0.25m).S, Is.EqualTo("-0.25"));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    [Test]
    public void DateTime_carries_only_its_invariant_text_so_kind_and_sub_second_ticks_are_lost()
    {
        DateTime at = new DateTime(2026, 3, 4, 5, 6, 7, 123, DateTimeKind.Utc).AddTicks(4567);

        Assert.That(Wire.ToValue(at).S, Is.EqualTo("03/04/2026 05:06:07"));
    }

    [Test]
    public void Key_bytes_and_text_round_trip()
    {
        var g = Guid.NewGuid();

        Assert.That(Wire.PkGuid(Wire.PkBytes(g)), Is.EqualTo(g));
        Assert.That(Wire.PkBytes(Wire.PkText(g)), Is.EqualTo(Wire.PkBytes(g)));
        Assert.That(Wire.PkText(Wire.PkBytes(g)), Is.EqualTo(Wire.PkText(g)));
        Assert.That(Wire.PkBytes(g).Length, Is.EqualTo(16));
    }

    [Test]
    public void Short_takes_the_tail_of_a_long_key_and_keeps_a_short_one()
    {
        Assert.That(Wire.Short("0A1B2C3D-0000-4000-8000-00000000ABCD"), Is.EqualTo("0000ABCD"));
        Assert.That(Wire.Short("abc"), Is.EqualTo("abc"));
        Assert.That(Wire.Short("12345678"), Is.EqualTo("12345678"));
    }

    [Test]
    public void Quoting_doubles_the_quotes_inside_an_identifier()
    {
        Assert.That(Wire.Q("Item"), Is.EqualTo("\"Item\""));
        Assert.That(Wire.Q("a\"b"), Is.EqualTo("\"a\"\"b\""));
    }
}
