from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from clients.quality_detector import (
    QualityFlag,
    _normalize_bars_for_detection,
    detect_all,
    detect_fetch_tainting,
    detect_interior_gaps,
    detect_range_shortfall,
    detect_row_count_anomaly,
)
from clients.trading_calendar import get_nyse_holidays, previous_trading_day, trading_days_between


def test_quality_flag_is_frozen():
    f = QualityFlag(category="range_shortfall", severity="critical", detail={"x": 1}, ts="2026-05-17T00:00:00Z")
    with pytest.raises(Exception):
        f.severity = "warning"


def test_range_shortfall_clean_returns_none():
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 1, 1),
        ib_head_timestamp=date(2020, 1, 1),
    )
    assert flag is None


def test_range_shortfall_within_tolerance_returns_none():
    # 3 trading days short - under warning threshold of >5
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 1, 6),
        ib_head_timestamp=date(2020, 1, 1),
    )
    assert flag is None


def test_range_shortfall_warning_threshold():
    # 10 trading days short of expected, but head_ts matches expected (i.e., data IS available)
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 1, 15),
        ib_head_timestamp=date(2020, 1, 1),
    )
    assert flag is not None
    assert flag.severity == "warning"
    assert flag.category == "range_shortfall"


def test_range_shortfall_critical_against_head_ts():
    # SMH case: expected 1993, got 2019, head_ts says 1993 (huge gap)
    flag = detect_range_shortfall(
        expected_start=date(1993, 1, 29),
        actual_start=date(2019, 5, 20),
        ib_head_timestamp=date(1993, 1, 29),
    )
    assert flag is not None
    assert flag.severity == "critical"
    assert "shortfall_days" in flag.detail


def test_range_shortfall_head_ts_matches_actual_returns_none():
    # IB legitimately has no older data - head_ts == actual_start, so not a fault
    flag = detect_range_shortfall(
        expected_start=date(1993, 1, 29),
        actual_start=date(2019, 5, 20),
        ib_head_timestamp=date(2019, 5, 20),
    )
    assert flag is None


def test_range_shortfall_no_head_ts_uses_expected_diff_only():
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 2, 15),
        ib_head_timestamp=None,
    )
    assert flag is not None
    assert flag.severity in {"warning", "critical"}


# Use a simple BarRecord stub - match clients.historical_provider's BarRecord shape
class _Bar:
    def __init__(self, d, c=100.0):
        self.trade_date = d if isinstance(d, str) else d.isoformat()


def test_interior_gaps_no_gap():
    bars = [_Bar(f"2026-04-{day:02d}") for day in [1, 2, 3, 6, 7, 8]]  # Apr 4,5 = weekend
    assert detect_interior_gaps(bars, trading_calendar=None) is None


def test_interior_gaps_single_missing_trading_day():
    # Apr 1, 2, 3, [missing Apr 6], 7; Apr 4,5 = weekend
    bars = [_Bar(f"2026-04-{day:02d}") for day in [1, 2, 3, 7]]
    flag = detect_interior_gaps(bars, trading_calendar=None)
    assert flag is not None
    assert flag.category == "interior_gaps"
    assert flag.severity in {"warning", "critical"}
    assert flag.detail["missing_days_count"] >= 1


def test_interior_gaps_consecutive_critical():
    # 10 consecutive trading days missing - critical
    bars = [_Bar("2026-04-01")] + [_Bar(f"2026-04-{day:02d}") for day in [17, 20]]
    flag = detect_interior_gaps(bars, trading_calendar=None)
    assert flag is not None
    assert flag.severity == "critical"


def test_interior_gaps_empty_bars_returns_none():
    assert detect_interior_gaps([], trading_calendar=None) is None


def test_interior_gaps_single_bar_returns_none():
    assert detect_interior_gaps([_Bar("2026-04-01")], trading_calendar=None) is None


def test_interior_gaps_accepts_date_trade_date():
    bars = [SimpleNamespace(trade_date=date(2026, 4, 1)), SimpleNamespace(trade_date=date(2026, 4, 3))]
    flag = detect_interior_gaps(bars, trading_calendar=lambda d: True)
    assert flag is not None
    assert flag.detail["first_missing"] == "2026-04-02"


def test_interior_gaps_no_calendar_emits_info_flag(monkeypatch):
    from clients import quality_detector

    monkeypatch.setattr(quality_detector, "_default_is_trading_day", None)
    bars = [_Bar("2026-04-01"), _Bar("2026-04-10")]
    flag = detect_interior_gaps(bars, trading_calendar=None)
    assert flag is not None
    assert flag.category == "interior_gaps"
    assert flag.severity == "info"
    assert flag.detail.get("reason") == "no_calendar"


def test_interior_gaps_calendar_failure_emits_info_flag(monkeypatch):
    # Simulate trading-calendar import/raise
    from clients import quality_detector

    def boom(d):
        raise RuntimeError("calendar broken")

    monkeypatch.setattr(quality_detector, "_default_is_trading_day", boom)
    bars = [_Bar("2026-04-01"), _Bar("2026-04-10")]
    flag = detect_interior_gaps(bars, trading_calendar=None)
    assert flag is not None
    assert flag.category == "interior_gaps"
    assert flag.severity == "info"
    assert flag.detail.get("status") == "gap_detection_unavailable"


def test_extracted_trading_calendar_sunday_observed_and_ranges():
    assert date(2023, 1, 2) in get_nyse_holidays(2023)
    assert previous_trading_day(date(2025, 1, 6)) == date(2025, 1, 3)
    assert trading_days_between(date(2025, 1, 3), date(2025, 1, 6)) == 1


def test_normalize_bars_for_detection_accepts_common_shapes():
    rows = _normalize_bars_for_detection([
        SimpleNamespace(trade_date="2026-04-01"),
        SimpleNamespace(date=date(2026, 4, 2)),
        {"bar_timestamp": datetime(2026, 4, 3, tzinfo=timezone.utc)},
        {"trade_date": "2026-04-06"},
        object(),
    ])
    assert [r.trade_date for r in rows] == [
        "2026-04-01",
        "2026-04-02",
        "2026-04-03",
        "2026-04-06",
    ]


def test_fetch_tainting_no_errors_returns_none():
    assert detect_fetch_tainting([]) is None


def test_fetch_tainting_one_error_warning():
    flag = detect_fetch_tainting([{"code": 162, "count": 1, "message": "no data"}])
    assert flag is not None
    assert flag.severity == "warning"
    assert flag.category == "fetch_tainted"


def test_fetch_tainting_aggregated_count_critical():
    flag = detect_fetch_tainting([
        {"code": 162, "count": 4},
        {"code": 2105, "count": 2},
    ])
    assert flag.severity == "critical"
    assert flag.detail["error_count"] == 6


def test_fetch_tainting_codes_recorded():
    flag = detect_fetch_tainting([
        {"code": 162, "count": 2},
        {"code": 2105, "count": 1},
    ])
    assert set(flag.detail["codes"]) == {162, 2105}


def test_row_count_anomaly_stub_returns_none():
    assert detect_row_count_anomaly([], reference_source=None) is None


def test_detect_all_clean_returns_empty():
    bars = [_Bar("2026-04-01"), _Bar("2026-04-02")]
    flags = detect_all(
        bars=bars,
        metadata={
            "expected_start": date(2026, 4, 1),
            "ib_head_timestamp": date(2026, 4, 1),
            "errors_during_fetch": [],
        },
        trading_calendar=lambda d: True,
    )
    assert flags == []


def test_detect_all_returns_multiple_flags():
    bars = [_Bar("2020-01-10"), _Bar("2020-01-11")]    # actual_start 2020-01-10
    flags = detect_all(
        bars=bars,
        metadata={
            "expected_start": date(1993, 1, 1),
            "ib_head_timestamp": date(1993, 1, 1),
            "errors_during_fetch": [{"code": 2105, "count": 6}],
        },
        trading_calendar=lambda d: True,
    )
    categories = {f.category for f in flags}
    assert "range_shortfall" in categories
    assert "fetch_tainted" in categories


def test_detect_all_includes_interior_gap_flag():
    bars = [_Bar("2026-04-01"), _Bar("2026-04-04")]
    flags = detect_all(
        bars=bars,
        metadata={
            "expected_start": date(2026, 4, 1),
            "ib_head_timestamp": date(2026, 4, 1),
            "errors_during_fetch": [],
        },
        trading_calendar=lambda d: True,
    )
    assert any(f.category == "interior_gaps" for f in flags)


def test_detect_all_handles_missing_metadata_keys():
    flags = detect_all(
        bars=[_Bar("2026-04-01")],
        metadata={},
        trading_calendar=lambda d: True,
    )
    assert flags == []    # No expected_start -> can't compute range_shortfall


def test_detect_all_isolates_detector_failures(monkeypatch):
    from clients import quality_detector

    monkeypatch.setattr(
        quality_detector,
        "detect_range_shortfall",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    bars = [_Bar("2020-01-10")]
    # Should NOT raise; failed detector logs and continues
    flags = detect_all(
        bars=bars,
        metadata={"expected_start": date(2020, 1, 1), "ib_head_timestamp": None, "errors_during_fetch": []},
        trading_calendar=lambda d: True,
    )
    # detector_error is recorded as a flag itself
    assert any(f.category == "detector_error" for f in flags)


def test_detect_all_includes_row_count_anomaly_when_detector_returns_flag(monkeypatch):
    from clients import quality_detector

    monkeypatch.setattr(
        quality_detector,
        "detect_row_count_anomaly",
        lambda *a, **kw: QualityFlag(category="row_count_anomaly", severity="warning"),
    )
    flags = detect_all(
        bars=[_Bar("2026-04-01")],
        metadata={"reference_source": object(), "errors_during_fetch": []},
        trading_calendar=lambda d: True,
    )
    assert any(f.category == "row_count_anomaly" for f in flags)
