"""Unit tests for break enumeration and window resolution. Frozen real closes, no network."""

from clients.silver_window import find_breaks, resolve_window


def _rows(pairs):
    return [{"trade_date": d, "close": c} for d, c in pairs]


# Real AAPL adjusted closes (production silver, frozen 2026-07-17): continuous.
AAPL = _rows([("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])

# EQIX: an unexplained ~25x step at 2003-01-02 with no CA record (EQIX has zero
# split events in the store). Real bronze closes, frozen 2026-07-17.
EQIX = _rows([("2002-12-27", 0.22), ("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.08)])


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


def test_continuous_series_keeps_its_whole_history():
    result = resolve_window(AAPL)
    assert result["start"] == "2024-01-02"
    assert result["trimmed_at"] is None
    assert result["rows_dropped"] == 0


def test_unexplained_break_trims_to_the_break_date():
    result = resolve_window(EQIX)
    assert result["start"] == "2003-01-02"
    assert result["trimmed_at"] == "2003-01-02"
    assert result["rows_dropped"] == 2


def test_window_starts_after_the_LAST_break_not_the_first():
    rows = _rows(
        [
            ("2001-01-02", 1.00),
            ("2001-01-03", 50.00),  # break 1
            ("2002-01-02", 51.00),
            ("2002-01-03", 4.00),  # break 2 (later)
            ("2002-01-04", 4.05),
        ]
    )
    result = resolve_window(rows)
    assert result["start"] == "2002-01-03"
    assert result["rows_dropped"] == 3


def test_keep_dates_from_triage_do_not_trim():
    result = resolve_window(EQIX, keep_dates=frozenset({"2003-01-02"}))
    assert result["start"] == "2002-12-27"
    assert result["trimmed_at"] is None


def test_operator_allowlist_does_not_trim():
    result = resolve_window(EQIX, allowlist=frozenset({"2003-01-02"}))
    assert result["start"] == "2002-12-27"


def test_empty_series_has_no_window():
    assert resolve_window([])["start"] is None


def test_non_positive_close_trims_that_row_out():
    rows = _rows([("2024-01-02", 185.64), ("2024-01-03", 0.0), ("2024-01-04", 181.91)])
    result = resolve_window(rows)
    assert result["start"] == "2024-01-04"
    assert "non-positive" in result["reason"]


def test_a_break_on_the_newest_row_collapses_the_window_onto_it():
    """DANGEROUS by itself, and pinned here so it cannot regress silently.

    The suffix rule assumes the newer side of a break is trustworthy, which is false
    for a bad NEW bar: the window collapses onto the garbage row and drops all the
    real history behind it. resolve_window is pure and cannot tell the two apart —
    rebuild_silver's window-regression guard is what refuses to publish this.
    """
    rows = _rows([("2024-01-02", 185.64), ("2024-01-03", 18564.0)])
    result = resolve_window(rows)
    assert result["start"] == "2024-01-03"
    assert result["rows_dropped"] == 1


def test_a_trailing_unusable_row_leaves_no_publishable_window():
    """The suffix rule can only trim a prefix, so when the newest row is the unusable
    one there is no window that both excludes it and stays a suffix. Say so rather
    than publishing a zero close as the first bar."""
    rows = _rows([("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 0.0)])
    result = resolve_window(rows)
    assert result["start"] is None
    assert result["rows_dropped"] == 3


def test_a_non_numeric_close_is_reported_rather_than_crashing_the_scan():
    """Bronze is typed, but a hand-written or migrated parquet can carry junk. One bad
    symbol must not take down a --full rebuild."""
    rows = [{"trade_date": "2024-01-02", "close": 185.64}, {"trade_date": "2024-01-03", "close": "n/a"}]
    breaks = find_breaks(rows)
    assert [b["date"] for b in breaks] == ["2024-01-03"]
    assert breaks[0]["ratio"] is None
