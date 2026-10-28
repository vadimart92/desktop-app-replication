using Replication.Protocol;

namespace Replication.Model;

/// <summary>
/// "Everything up to <see cref="Cursor"/> received" plus received ranges (lo, hi] above it (5.4, 6.4).
/// The same structure is kept by the client (persisted) and mirrored by the owner per stream.
/// </summary>
public sealed class CursorState
{
    private readonly List<(long Lo, long Hi)> _ranges = [];

    public CursorState(long cursor, IEnumerable<(long Lo, long Hi)>? ranges = null)
    {
        Cursor = cursor;
        if (ranges is not null)
        {
            foreach ((long lo, long hi) in ranges)
                AddRange(lo, hi);
        }
    }

    public long Cursor { get; private set; }

    /// <summary>Received ranges above the cursor, ascending, not touching each other or the cursor.</summary>
    public IReadOnlyList<(long Lo, long Hi)> Ranges => _ranges;

    /// <summary>A copy in O(ranges): they are already merged, so they are not added one by one.</summary>
    public CursorState Clone()
    {
        var k = new CursorState(Cursor);
        k._ranges.AddRange(_ranges);
        return k;
    }

    /// <summary>Adds (lo, hi] and merges: ranges that touch merge, a range that reaches the cursor lifts it.</summary>
    public void AddRange(long lo, long hi)
    {
        lo = Math.Max(lo, Cursor);
        if (hi <= lo)
            return;
        int j = _ranges.Count;
        while (j > 0 && _ranges[j - 1].Lo > hi)
            j--;
        int i = j;
        while (i > 0 && _ranges[i - 1].Hi >= lo)
        {
            i--;
            lo = Math.Min(lo, _ranges[i].Lo);
            hi = Math.Max(hi, _ranges[i].Hi);
        }
        _ranges.RemoveRange(i, j - i);
        if (lo <= Cursor)
            Cursor = hi;
        else
            _ranges.Insert(i, (lo, hi));
    }

    /// <summary>Head(V) in the online phase lifts the cursor of a table that has no open ranges (6.4).</summary>
    public void LiftTo(long v)
    {
        if (_ranges.Count == 0 && v > Cursor)
            Cursor = v;
    }

    public void Reset(long cursor)
    {
        Cursor = cursor;
        _ranges.Clear();
    }

    /// <summary>Not yet received ranges between the cursor, the received ranges and the head H, highest first.</summary>
    public IReadOnlyList<(long Lo, long Hi)> Gaps(long head)
    {
        var gaps = new List<(long, long)>();
        long top = head;
        for (int i = _ranges.Count - 1; i >= 0; i--)
        {
            (long lo, long hi) = _ranges[i];
            if (top > hi)
                gaps.Add((hi, top));
            top = Math.Min(top, lo);
        }
        if (top > Cursor)
            gaps.Add((Cursor, top));
        return gaps;
    }

    public TableCursor ToWire(string table, bool withRanges = true)
    {
        var tc = new TableCursor { Tbl = table, Cursor = Cursor };
        if (!withRanges)
            return tc;
        long prev = Cursor;
        foreach ((long lo, long hi) in _ranges)
        {
            // Compact: differences between neighbouring bounds, varint (6.4, "how many ranges accumulate").
            tc.RangeDeltas.Add(lo - prev);
            tc.RangeDeltas.Add(hi - lo);
            prev = hi;
        }
        return tc;
    }

    public static CursorState FromWire(TableCursor tc)
    {
        var s = new CursorState(tc.Cursor);
        long prev = tc.Cursor;
        for (int i = 0; i + 1 < tc.RangeDeltas.Count; i += 2)
        {
            long lo = prev + tc.RangeDeltas[i];
            long hi = lo + tc.RangeDeltas[i + 1];
            s.AddRange(lo, hi);
            prev = hi;
        }
        return s;
    }

    public override string ToString() =>
        _ranges.Count == 0 ? Cursor.ToString() : $"{Cursor} + {string.Join(" ", _ranges.Select(r => $"({r.Lo},{r.Hi}]"))}";
}
