from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.corporate_action_store import CorporateAction


def _bars(closes=(100.0, 100.0, 100.0), *, start=date(2026, 1, 1), currency="USD"):
    return [
        {
            "trade_date": start + timedelta(days=index),
            "symbol_id": 1,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": 1_000,
            "currency": currency,
        }
        for index, close in enumerate(closes)
    ]


def _action(
    action_id: str,
    ex_date: date,
    *,
    action_type: str,
    split_from: float | None = None,
    split_to: float | None = None,
    cash_amount: float | None = None,
    currency: str | None = None,
) -> CorporateAction:
    return CorporateAction(
        action_id=action_id,
        provider="massive",
        provider_event_id=action_id,
        event_revision=1,
        supersedes_action_id=None,
        symbol="NVDA",
        action_type=action_type,
        ex_date=ex_date,
        split_from=split_from,
        split_to=split_to,
        cash_amount=cash_amount,
        currency=currency,
        declaration_date=None,
        record_date=None,
        pay_date=None,
        status="active",
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload_hash=action_id,
    )


def test_cumulative_four_for_one_and_ten_for_one_splits():
    bars = _bars((400.0, 100.0, 100.0, 10.0, 10.0), start=date(2020, 1, 1))
    actions = [
        _action("split-4", date(2020, 1, 2), action_type="split", split_from=1, split_to=4),
        _action("split-10", date(2020, 1, 4), action_type="split", split_from=1, split_to=10),
    ]

    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, actions), revision=7)

    assert adjusted[0]["close"] == pytest.approx(10.0)
    assert adjusted[0]["volume"] == 40_000
    assert adjusted[1]["close"] == pytest.approx(10.0)
    assert adjusted[1]["volume"] == 10_000
    assert adjusted[-1]["close"] == pytest.approx(10.0)
    assert adjusted[-1]["volume"] == 1_000
    assert all(row["adjustment_revision"] == 7 for row in adjusted)


def test_dividend_adjusts_price_not_volume():
    bars = _bars()
    dividend = _action(
        "div-1",
        date(2026, 1, 3),
        action_type="cash_dividend",
        cash_amount=1,
        currency="USD",
    )

    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, [dividend]), revision=1)

    assert adjusted[0]["close"] == pytest.approx(99.0)
    assert adjusted[0]["volume"] == 1_000
    assert adjusted[1]["close"] == pytest.approx(99.0)
    assert adjusted[2]["close"] == pytest.approx(100.0)


def test_recurring_dividends_compound_for_older_rows():
    bars = _bars((100.0, 100.0, 100.0, 100.0))
    actions = [
        _action("div-1", date(2026, 1, 3), action_type="cash_dividend", cash_amount=1, currency="USD"),
        _action("div-2", date(2026, 1, 4), action_type="cash_dividend", cash_amount=2, currency="USD"),
    ]
    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, actions), revision=1)
    assert adjusted[0]["close"] == pytest.approx(100 * 0.99 * 0.98)
    assert adjusted[2]["close"] == pytest.approx(98.0)


def test_same_day_split_is_applied_before_dividend_reference_close():
    bars = _bars((100.0, 50.0, 50.0))
    actions = [
        _action("div", date(2026, 1, 2), action_type="cash_dividend", cash_amount=1, currency="USD"),
        _action("split", date(2026, 1, 2), action_type="split", split_from=1, split_to=2),
    ]
    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, actions), revision=1)
    assert adjusted[0]["close"] == pytest.approx(49.0)
    assert adjusted[0]["volume"] == 2_000


def test_intervals_are_exhaustive_ordered_and_non_overlapping():
    bars = _bars()
    action = _action("split", date(2026, 1, 3), action_type="split", split_from=1, split_to=2)
    intervals = build_factor_intervals(bars, [action])
    assert [(item.effective_start, item.effective_end) for item in intervals] == [
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 3), date(2026, 1, 3)),
    ]


@given(
    closes=st.lists(st.floats(min_value=1, max_value=10_000, allow_nan=False), min_size=1, max_size=20),
    volumes=st.lists(st.integers(min_value=0, max_value=10_000_000), min_size=1, max_size=20),
)
def test_no_action_identity_property(closes, volumes):
    size = min(len(closes), len(volumes))
    bars = _bars(tuple(closes[:size]))
    for row, volume in zip(bars, volumes[:size], strict=True):
        row["volume"] = volume
    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, []), revision=3)
    assert [row["close"] for row in adjusted] == [row["close"] for row in bars]
    assert [row["volume"] for row in adjusted] == [row["volume"] for row in bars]


def test_duplicate_bar_dates_are_rejected():
    bars = _bars()
    bars.append(dict(bars[-1]))
    with pytest.raises(ValueError, match="duplicate"):
        build_factor_intervals(bars, [])


def test_missing_previous_close_blocks_dividend():
    dividend = _action(
        "div",
        date(2026, 1, 1),
        action_type="cash_dividend",
        cash_amount=1,
        currency="USD",
    )
    with pytest.raises(ValueError, match="previous close"):
        build_factor_intervals(_bars(), [dividend])


def test_currency_mismatch_blocks_dividend():
    dividend = _action(
        "div",
        date(2026, 1, 3),
        action_type="cash_dividend",
        cash_amount=1,
        currency="CAD",
    )
    with pytest.raises(ValueError, match="currency"):
        build_factor_intervals(_bars(currency="USD"), [dividend])


@pytest.mark.parametrize(
    "action, message",
    [
        (_action("zero", date(2026, 1, 3), action_type="split", split_from=0, split_to=2), "split"),
        (
            _action("negative", date(2026, 1, 3), action_type="cash_dividend", cash_amount=-1, currency="USD"),
            "dividend",
        ),
        (
            _action("too-large", date(2026, 1, 3), action_type="cash_dividend", cash_amount=100, currency="USD"),
            "previous close",
        ),
    ],
)
def test_invalid_event_values_are_rejected(action, message):
    with pytest.raises(ValueError, match=message):
        build_factor_intervals(_bars(), [action])
