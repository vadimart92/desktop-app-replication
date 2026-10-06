using Replication.Model;
using Xunit;

namespace Replication.Tests;

public sealed class CursorStateTests
{
    [Fact]
    public void Ranges_merge_and_lift_the_cursor()
    {
        var k = new CursorState(10);
        k.AddRange(40, 50);
        k.AddRange(20, 30);
        Assert.Equal([(20L, 30L), (40L, 50L)], k.Ranges);
        Assert.Equal([(50L, 60L), (30L, 40L), (10L, 20L)], k.Gaps(60));
        k.AddRange(30, 40);
        Assert.Equal([(20L, 50L)], k.Ranges);
        k.AddRange(10, 20);
        Assert.Equal(50, k.Cursor);
        Assert.Empty(k.Ranges);
    }

    [Fact]
    public void Wire_encoding_round_trips()
    {
        var k = new CursorState(1000, [(1500L, 1600L), (2000L, 2100L)]);
        CursorState back = CursorState.FromWire(k.ToWire("T"));
        Assert.Equal(k.Cursor, back.Cursor);
        Assert.Equal(k.Ranges, back.Ranges);
    }

    [Fact]
    public void Head_lifts_only_tables_without_ranges()
    {
        var a = new CursorState(5);
        var b = new CursorState(5, [(7L, 9L)]);
        a.LiftTo(20);
        b.LiftTo(20);
        Assert.Equal(20, a.Cursor);
        Assert.Equal(5, b.Cursor);
    }

    [Fact]
    public void A_range_spanning_several_ranges_merges_them()
    {
        var k = new CursorState(0, [(10L, 12L), (14L, 16L), (18L, 20L)]);
        k.AddRange(13, 19);
        Assert.Equal(0, k.Cursor);
        Assert.Equal([(10L, 12L), (13L, 20L)], k.Ranges);
    }

    [Fact]
    public void A_range_that_bridges_ranges_to_the_cursor_lifts_it_past_them()
    {
        var k = new CursorState(10, [(12L, 14L), (16L, 18L)]);
        k.AddRange(10, 17);
        Assert.Equal(18, k.Cursor);
        Assert.Empty(k.Ranges);
    }

    [Fact]
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
        Assert.Equal(cursor, k.Cursor);
        Assert.Equal(ranges, k.Ranges);
    }
}
