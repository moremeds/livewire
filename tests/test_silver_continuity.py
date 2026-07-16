import pytest

from clients.silver_continuity import check_adjusted_continuity


def _rows(closes):
    # closes: list of (iso_date, close). Only trade_date/close are read by the gate.
    return [{"trade_date": d, "close": c} for d, c in closes]


def test_clean_series_passes():
    rows = _rows([("2021-06-10", 17.43), ("2021-06-11", 17.76), ("2021-06-14", 17.95)])
    assert check_adjusted_continuity(rows) is None


def test_double_adjusted_bar_raises_with_date_and_ratio():
    # NVDA-style: a ~40x drop into a double-adjusted bar then back up.
    rows = _rows([("2021-06-17", 18.59), ("2021-06-18", 0.4644), ("2021-06-21", 18.36)])
    with pytest.raises(ValueError) as exc:
        check_adjusted_continuity(rows, threshold=6.0)
    assert "2021-06-18" in str(exc.value)


def test_allowlisted_date_is_exempt():
    rows = _rows([("2021-06-17", 18.59), ("2021-06-18", 0.4644), ("2021-06-21", 18.36)])
    assert check_adjusted_continuity(rows, threshold=6.0, allowlist=frozenset({"2021-06-18"})) is None


def test_threshold_is_inclusive_boundary_safe():
    # exactly 6x is not a violation; just over is.
    assert check_adjusted_continuity(_rows([("2021-01-04", 10.0), ("2021-01-05", 60.0)]), threshold=6.0) is None
    with pytest.raises(ValueError):
        check_adjusted_continuity(_rows([("2021-01-04", 10.0), ("2021-01-05", 60.01)]), threshold=6.0)
