using NUnit.Framework;
using Replication.Model;

namespace Replication.Tests;

public sealed class CursorStateTests
{
    [Test]
    public void Ranges_merge_and_lift_the_cursor()
    {
        var k = new CursorState(10);
        k.AddRange(40, 50);
        k.AddRange(20, 30);
        Assert.That(k.Ranges, Is.EqualTo(new[] { (20L, 30L), (40L, 50L) }));
        Assert.That(k.Gaps(60), Is.EqualTo(new[] { (50L, 60L), (30L, 40L), (10L, 20L) }));
        k.AddRange(30, 40);
        Assert.That(k.Ranges, Is.EqualTo(new[] { (20L, 50L) }));
        k.AddRange(10, 20);
        Assert.That(k.Cursor, Is.EqualTo(50));
        Assert.That(k.Ranges, Is.Empty);
    }

    [Test]
    public void Wire_encoding_round_trips()
    {
        var k = new CursorState(1000, [(1500L, 1600L), (2000L, 2100L)]);
        CursorState back = CursorState.FromWire(k.ToWire("T"));
        Assert.That(back.Cursor, Is.EqualTo(k.Cursor));
        Assert.That(back.Ranges, Is.EqualTo(k.Ranges));
    }

    [Test]
    public void Head_lifts_only_tables_without_ranges()
    {
        var a = new CursorState(5);
        var b = new CursorState(5, [(7L, 9L)]);
        a.LiftTo(20);
        b.LiftTo(20);
        Assert.That(a.Cursor, Is.EqualTo(20));
        Assert.That(b.Cursor, Is.EqualTo(5));
    }

    [Test]
    public void A_range_spanning_several_ranges_merges_them()
    {
        var k = new CursorState(0, [(10L, 12L), (14L, 16L), (18L, 20L)]);
        k.AddRange(13, 19);
        Assert.That(k.Cursor, Is.EqualTo(0));
        Assert.That(k.Ranges, Is.EqualTo(new[] { (10L, 12L), (13L, 20L) }));
    }

    [Test]
    public void A_range_that_bridges_ranges_to_the_cursor_lifts_it_past_them()
    {
        var k = new CursorState(10, [(12L, 14L), (16L, 18L)]);
        k.AddRange(10, 17);
        Assert.That(k.Cursor, Is.EqualTo(18));
        Assert.That(k.Ranges, Is.Empty);
    }

    [Test]
    public void Random_adds_match_a_per_version_model()
    {
        const int Top = 40;
        var random = new Random(1);
        for (int run = 0; run < 500; run++)
        {
            var k = new CursorState(0);
            bool[] got = new bool[Top + 1];
            for (int step = 0; step < 20; step++)
            {
                int lo = random.Next(Top + 1);
                int hi = random.Next(Top + 1);
                k.AddRange(lo, hi);
                for (int v = lo + 1; v <= hi; v++)
                    got[v] = true;
                AssertMatches(got, k);
            }
        }
    }

    /// <summary>Version v is received when got[v]: the cursor ends the received prefix, the ranges are the received runs above it.</summary>
    private static void AssertMatches(bool[] got, CursorState k)
    {
        int cursor = 0;
        while (cursor + 1 < got.Length && got[cursor + 1])
            cursor++;
        var ranges = new List<(long Lo, long Hi)>();
        for (int v = cursor + 2; v < got.Length; v++)
        {
            if (!got[v])
                continue;
            if (got[v - 1])
                ranges[^1] = (ranges[^1].Lo, v);
            else
                ranges.Add((v - 1, v));
        }
        Assert.That(k.Cursor, Is.EqualTo(cursor));
        Assert.That(k.Ranges, Is.EqualTo(ranges));
    }

    [Test]
    public void Clone_is_a_working_copy_that_does_not_change_the_published_cursor()
    {
        var published = new CursorState(10, [(20L, 30L)]);
        CursorState copy = published.Clone();
        copy.AddRange(40, 50);
        copy.AddRange(10, 20);
        Assert.That(published.ToString(), Is.EqualTo("10 + (20,30]"));
        Assert.That(copy.ToString(), Is.EqualTo("30 + (40,50]"));
    }

    [Test]
    public void Gaps_of_a_cursor_without_ranges_is_the_stretch_up_to_the_head()
    {
        var k = new CursorState(10);

        Assert.That(k.Gaps(25), Is.EqualTo(new[] { (10L, 25L) }));
        Assert.That(k.Gaps(10), Is.Empty, "the head is at the cursor");
        Assert.That(k.Gaps(3), Is.Empty, "the head is below the cursor");
    }

    [Test]
    public void Gaps_with_the_head_inside_or_at_the_edge_of_a_range_leave_no_gap_above_it()
    {
        var k = new CursorState(10, [(20L, 30L)]);

        Assert.That(k.Gaps(25), Is.EqualTo(new[] { (10L, 20L) }), "head inside the range");
        Assert.That(k.Gaps(30), Is.EqualTo(new[] { (10L, 20L) }), "head at the top of the range");
        Assert.That(k.Gaps(20), Is.EqualTo(new[] { (10L, 20L) }), "head at the bottom of the range");
        Assert.That(k.Gaps(15), Is.EqualTo(new[] { (10L, 15L) }), "head below the range");
        Assert.That(k.Gaps(40), Is.EqualTo(new[] { (30L, 40L), (10L, 20L) }), "head above the range, highest gap first");
    }

    [Test]
    public void Reset_sets_the_cursor_and_forgets_every_range()
    {
        var k = new CursorState(10, [(20L, 30L), (40L, 50L)]);

        k.Reset(7);

        Assert.That(k.Cursor, Is.EqualTo(7));
        Assert.That(k.Ranges, Is.Empty);
        Assert.That(k.ToString(), Is.EqualTo("7"));
        k.AddRange(7, 9);
        Assert.That(k.Cursor, Is.EqualTo(9), "an empty cursor takes ranges again");
    }

    [Test]
    public void LiftTo_never_moves_the_cursor_back_and_never_over_open_ranges()
    {
        var plain = new CursorState(10);
        plain.LiftTo(3);
        Assert.That(plain.Cursor, Is.EqualTo(10));
        plain.LiftTo(10);
        Assert.That(plain.Cursor, Is.EqualTo(10));

        var open = new CursorState(10, [(20L, 30L)]);
        open.LiftTo(100);
        Assert.That(open.Cursor, Is.EqualTo(10));
        Assert.That(open.Ranges, Is.EqualTo(new[] { (20L, 30L) }));
    }

    [TestCase(30L, 20L)]
    [TestCase(20L, 20L)]
    [TestCase(5L, 10L)]
    [TestCase(-5L, 3L)]
    public void Range_that_is_empty_reversed_or_below_the_cursor_changes_nothing(long lo, long hi)
    {
        var k = new CursorState(10, [(20L, 30L)]);

        k.AddRange(lo, hi);

        Assert.That(k.ToString(), Is.EqualTo("10 + (20,30]"));
    }

    [Test]
    public void Range_that_starts_below_the_cursor_counts_from_the_cursor()
    {
        var k = new CursorState(10);

        k.AddRange(2, 15);

        Assert.That(k.Cursor, Is.EqualTo(15));
        Assert.That(k.Ranges, Is.Empty);
    }

    [Test]
    public void Constructor_merges_unsorted_and_overlapping_ranges()
    {
        var k = new CursorState(0, [(30L, 40L), (10L, 20L), (15L, 32L)]);

        Assert.That(k.Ranges, Is.EqualTo(new[] { (10L, 40L) }));
    }

    [Test]
    public void FromWire_ignores_a_trailing_odd_delta_and_empty_ranges()
    {
        var tc = new Replication.Protocol.TableCursor { Tbl = "T", Cursor = 10 };
        tc.RangeDeltas.AddRange([5L, 5L, 0L, 0L, 3L]);

        CursorState k = CursorState.FromWire(tc);

        Assert.That(k.Cursor, Is.EqualTo(10));
        Assert.That(k.Ranges, Is.EqualTo(new[] { (15L, 20L) }), "(20,20] is empty and the odd 3 has no partner");
    }

    [Test]
    public void FromWire_clips_a_range_that_a_negative_delta_puts_below_the_cursor()
    {
        var tc = new Replication.Protocol.TableCursor { Tbl = "T", Cursor = 10 };
        tc.RangeDeltas.AddRange([-5L, 10L]);

        CursorState k = CursorState.FromWire(tc);

        Assert.That(k.Cursor, Is.EqualTo(15), "(5,15] reaches the cursor and lifts it");
        Assert.That(k.Ranges, Is.Empty);
    }

    [Test]
    public void ToWire_without_ranges_carries_only_the_cursor()
    {
        var k = new CursorState(10, [(20L, 30L)]);

        Replication.Protocol.TableCursor tc = k.ToWire("T", withRanges: false);

        Assert.That(tc.Cursor, Is.EqualTo(10));
        Assert.That(tc.RangeDeltas, Is.Empty);
        Assert.That(k.ToWire("T").RangeDeltas, Is.EqualTo(new[] { 10L, 10L }), "lo - cursor, hi - lo");
    }
}
