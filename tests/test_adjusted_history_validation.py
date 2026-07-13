from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from clients.adjusted_history_validation import (
    build_split_only_rows,
    build_total_return_rows,
    compare_series,
    find_mechanical_split_jumps,
    merge_reference_rows,
    rolling_sma,
)
from clients.corporate_action_store import CorporateAction


def _bar(trade_date: date, close: float, **changes) -> dict:
    row = {
        "trade_date": trade_date,
        "symbol_id": 1,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "adj_close": close,
        "volume": 100,
        "source": "massive",
        "price_basis": "raw",
        "currency": "USD",
    }
    row.update(changes)
    return row


def _action(action_id: str, action_type: str, ex_date: date, **changes) -> CorporateAction:
    values = {
        "action_id": action_id,
        "provider": "massive",
        "provider_event_id": action_id,
        "event_revision": 1,
        "supersedes_action_id": None,
        "symbol": "TEST",
        "action_type": action_type,
        "ex_date": ex_date,
        "split_from": None,
        "split_to": None,
        "cash_amount": None,
        "currency": None,
        "declaration_date": None,
        "record_date": None,
        "pay_date": None,
        "status": "active",
        "fetched_at": datetime(2024, 1, 1, tzinfo=UTC),
        "payload_hash": action_id,
    }
    values.update(changes)
    return CorporateAction(**values)


def test_merge_reference_rows_uses_massive_first_and_reports_unresolved() -> None:
    dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
    coverage = merge_reference_rows(
        dates,
        massive=[_bar(dates[1], 20), _bar(dates[2], 30)],
        ib=[_bar(dates[0], 10), _bar(dates[2], 300)],
    )

    assert [row["close"] for row in coverage.rows] == [10, 20, 30]
    assert coverage.sources == {
        dates[0]: "ib",
        dates[1]: "massive",
        dates[2]: "massive+ib",
    }
    assert coverage.unresolved == (dates[3],)


def test_merge_reference_rows_rejects_duplicate_provider_dates() -> None:
    trade_date = date(2024, 1, 2)
    with pytest.raises(ValueError, match="duplicate massive trade date"):
        merge_reference_rows([trade_date], massive=[_bar(trade_date, 1), _bar(trade_date, 2)], ib=[])


def test_rolling_sma_uses_ordered_sessions_and_requires_full_window() -> None:
    start = date(2024, 1, 1)
    rows = [_bar(start + timedelta(days=offset * 2), float(offset + 1)) for offset in range(5)]

    values = rolling_sma(rows, 3)

    assert values == {
        rows[2]["trade_date"]: 2.0,
        rows[3]["trade_date"]: 3.0,
        rows[4]["trade_date"]: 4.0,
    }


def test_compare_series_fails_point_error_even_when_sma_error_cancels() -> None:
    start = date(2024, 1, 1)
    reference = [_bar(start + timedelta(days=offset), 100.0, high=102.0, low=98.0) for offset in range(20)]
    local = [dict(row) for row in reference]
    local[0]["close"] = local[0]["adj_close"] = 101.0
    local[1]["close"] = local[1]["adj_close"] = 99.0

    result = compare_series(local, reference, warning_bps=1, failure_bps=5, windows=(20,))

    assert result.passed is False
    assert result.point_failure_count == 2
    assert result.sma[20].failure_count == 0
    assert result.sma[20].max_error_bps == pytest.approx(0.0)


def test_compare_series_keeps_non_close_provider_drift_diagnostic() -> None:
    trade_date = date(2024, 1, 2)
    reference = [_bar(trade_date, 100.0, high=101.0)]
    local = [_bar(trade_date, 100.0, high=110.0)]

    result = compare_series(
        local,
        reference,
        windows=(),
        point_failure_columns=("close",),
        point_failure_sources=("massive",),
    )

    assert result.passed is True
    assert result.point_failure_count == 0
    assert result.point_warning_count == 1
    assert result.differences[0].column == "high"
    assert result.differences[0].severity == "warning"


def test_compare_series_keeps_ib_replay_close_drift_diagnostic() -> None:
    trade_date = date(2024, 1, 2)
    reference = [_bar(trade_date, 100.0, source="ib")]
    local = [_bar(trade_date, 101.0)]

    result = compare_series(
        local,
        reference,
        windows=(),
        point_failure_columns=("close",),
        point_failure_sources=("massive",),
    )

    assert result.passed is True
    assert result.point_failure_count == 0
    assert result.point_warning_count == 4


def test_compare_series_still_fails_massive_close_drift() -> None:
    trade_date = date(2024, 1, 2)
    reference = [_bar(trade_date, 100.0, source="massive")]
    local = [_bar(trade_date, 101.0)]

    result = compare_series(
        local,
        reference,
        windows=(),
        point_failure_columns=("close",),
        point_failure_sources=("massive",),
    )

    assert result.passed is False
    assert result.point_failure_count == 1
    assert result.point_warning_count == 3


def test_compare_series_still_fails_ib_sma_drift() -> None:
    start = date(2024, 1, 1)
    reference = [_bar(start + timedelta(days=offset), 100.0, source="ib") for offset in range(20)]
    local = [_bar(start + timedelta(days=offset), 101.0) for offset in range(20)]

    result = compare_series(
        local,
        reference,
        windows=(20,),
        point_failure_columns=("close",),
        point_failure_sources=("massive",),
    )

    assert result.passed is False
    assert result.point_failure_count == 0
    assert result.sma[20].failure_count == 1


def test_compare_series_reports_missing_reference_date() -> None:
    first = date(2024, 1, 2)
    second = date(2024, 1, 3)
    result = compare_series([_bar(first, 10), _bar(second, 11)], [_bar(first, 10)])

    assert result.passed is False
    assert result.missing_dates == (second,)


def test_reconstruction_separates_split_only_from_total_return() -> None:
    before = date(2024, 1, 2)
    ex_date = date(2024, 1, 3)
    rows = [_bar(before, 100), _bar(ex_date, 51)]
    split = _action("split", "split", ex_date, split_from=1.0, split_to=2.0)
    dividend = _action("div", "cash_dividend", ex_date, cash_amount=2.0, currency="USD")

    split_only = build_split_only_rows(rows, [split, dividend], ex_date)
    total_return = build_total_return_rows(rows, [split, dividend], ex_date)

    assert split_only[0]["close"] == pytest.approx(50.0)
    assert split_only[1]["close"] == pytest.approx(51.0)
    assert total_return[0]["close"] == pytest.approx(48.0)
    assert total_return[1]["close"] == pytest.approx(51.0)


def test_mechanical_split_jump_is_detected() -> None:
    before = date(2024, 1, 2)
    ex_date = date(2024, 1, 3)
    split = _action("split", "split", ex_date, split_from=1.0, split_to=4.0)
    rows = [_bar(before, 100), _bar(ex_date, 25)]

    jumps = find_mechanical_split_jumps(rows, [split], ex_date)

    assert len(jumps) == 1
    assert jumps[0].action_id == "split"
    assert jumps[0].observed_ratio == pytest.approx(0.25)


def test_comparison_threshold_edges_are_strictly_greater_than() -> None:
    trade_date = date(2024, 1, 2)
    reference = [_bar(trade_date, 100)]

    warning = compare_series([_bar(trade_date, 100.01)], reference, warning_bps=1, failure_bps=5, windows=())
    failure = compare_series([_bar(trade_date, 100.051)], reference, warning_bps=1, failure_bps=5, windows=())

    assert warning.point_warning_count == 0
    assert warning.passed is True
    assert failure.point_failure_count == 4
    assert failure.passed is False


def test_exact_silver_columns_fail_even_when_ohlc_matches() -> None:
    trade_date = date(2024, 1, 2)
    expected = _bar(
        trade_date,
        100,
        price_adjustment_factor=0.9,
        split_volume_factor=2.0,
        adjustment_revision=1,
    )
    actual = {**expected, "volume": 99, "price_adjustment_factor": 0.8}

    result = compare_series(
        [actual],
        [expected],
        windows=(),
        exact_columns=("adj_close", "volume", "price_adjustment_factor", "split_volume_factor"),
    )

    assert result.passed is False
    assert result.exact_failure_count == 2
