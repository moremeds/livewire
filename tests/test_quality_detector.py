from datetime import date

import pytest

from clients.quality_detector import QualityFlag, detect_range_shortfall


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
