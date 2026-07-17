"""Unit tests for break enumeration. Frozen real closes, no network."""

from clients.silver_window import find_breaks


def _rows(pairs):
    return [{"trade_date": d, "close": c} for d, c in pairs]


# Real AAPL adjusted closes (production silver, frozen 2026-07-17): continuous.
AAPL = _rows([("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])

# EQIX shape: an unexplained ~25x step at 2003-01-02 with no CA record.
EQIX = _rows([("2002-12-27", 0.20), ("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.31)])


def test_find_breaks_enumerates_every_break_not_just_the_first():
    """check_adjusted_continuity stops at the first; triage needs them all, or the
    later break never gets triaged and its real history is amputated forever."""
    rows = _rows(
        [
            ("2001-01-02", 1.00),
            ("2001-01-03", 50.00),
            ("2002-01-02", 51.00),
            ("2002-01-03", 4.00),
            ("2002-01-04", 4.05),
        ]
    )
    assert [b["date"] for b in find_breaks(rows)] == ["2001-01-03", "2002-01-03"]


def test_find_breaks_is_empty_for_a_continuous_series():
    assert find_breaks(AAPL) == []


def test_find_breaks_honours_exempt_dates():
    assert find_breaks(EQIX, exempt=frozenset({"2003-01-02"})) == []


def test_find_breaks_reports_a_non_positive_close_with_no_ratio():
    rows = _rows([("2024-01-02", 185.64), ("2024-01-03", 0.0), ("2024-01-04", 181.91)])
    breaks = find_breaks(rows)
    assert [b["date"] for b in breaks] == ["2024-01-03"]
    assert breaks[0]["ratio"] is None  # nothing to compare against a second source
