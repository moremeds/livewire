from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.corporate_action_store import CorporateAction


def _bars(
    closes=(100.0, 100.0, 100.0),
    *,
    start=date(2026, 1, 1),
    currency="USD",
    price_basis="raw",
):
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
            "source": "massive",
            "price_basis": price_basis,
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

    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, actions, date.max), revision=7)

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

    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, [dividend], date.max), revision=1)

    assert adjusted[0]["close"] == pytest.approx(99.0)
    assert adjusted[0]["volume"] == 1_000
    assert adjusted[1]["close"] == pytest.approx(99.0)
    assert adjusted[2]["close"] == pytest.approx(100.0)


def test_future_dividend_is_excluded_until_its_ex_date():
    bars = _bars()
    dividend = _action(
        "announced-dividend",
        date(2026, 1, 3),
        action_type="cash_dividend",
        cash_amount=1,
        currency="USD",
    )

    before_ex_date = build_factor_intervals(
        bars,
        [dividend],
        as_of_date=date(2026, 1, 2),
    )
    effective_on_ex_date = build_factor_intervals(
        bars,
        [dividend],
        as_of_date=date(2026, 1, 3),
    )

    assert [item.price_adjustment_factor for item in before_ex_date] == [Decimal("1")]
    assert [item.price_adjustment_factor for item in effective_on_ex_date] == [
        Decimal("0.99"),
        Decimal("1"),
    ]
    assert effective_on_ex_date[0].effective_end == date(2026, 1, 2)
    assert effective_on_ex_date[1].effective_start == date(2026, 1, 3)


def test_future_split_is_excluded_until_its_ex_date():
    bars = _bars()
    split = _action(
        "announced-split",
        date(2026, 1, 3),
        action_type="split",
        split_from=1,
        split_to=2,
    )

    before_ex_date = build_factor_intervals(bars, [split], date(2026, 1, 2))
    effective_on_ex_date = build_factor_intervals(bars, [split], date(2026, 1, 3))

    assert [item.price_adjustment_factor for item in before_ex_date] == [Decimal("1")]
    assert [item.split_volume_factor for item in before_ex_date] == [Decimal("1")]
    assert [item.price_adjustment_factor for item in effective_on_ex_date] == [
        Decimal("0.5"),
        Decimal("1"),
    ]
    assert [item.split_volume_factor for item in effective_on_ex_date] == [
        Decimal("2"),
        Decimal("1"),
    ]


def test_split_adjusted_row_does_not_receive_second_split_factor():
    bars = _bars(price_basis="split_adjusted")
    split = _action("split", date(2026, 1, 3), action_type="split", split_from=1, split_to=2)

    intervals = build_factor_intervals(bars, [split], date(2026, 1, 3))

    assert [item.price_adjustment_factor for item in intervals] == [Decimal("1")]
    assert [item.split_volume_factor for item in intervals] == [Decimal("1")]


def test_unknown_split_affected_row_blocks_factor_construction():
    bars = _bars(price_basis="unknown")
    split = _action("split", date(2026, 1, 3), action_type="split", split_from=1, split_to=2)

    with pytest.raises(ValueError, match="unknown price_basis"):
        build_factor_intervals(bars, [split], date(2026, 1, 3))


def test_unknown_row_after_split_is_safe():
    bars = _bars((100.0,), start=date(2026, 1, 3), price_basis="unknown")
    split = _action("split", date(2026, 1, 3), action_type="split", split_from=1, split_to=2)

    intervals = build_factor_intervals(bars, [split], date(2026, 1, 3))

    assert intervals[0].price_adjustment_factor == Decimal("1")


def test_effective_action_without_ex_date_bar_adjusts_only_earlier_sessions():
    bars = _bars((100.0, 100.0), start=date(2026, 1, 2))
    bars[1]["trade_date"] = date(2026, 1, 5)
    split = _action(
        "weekend-split",
        date(2026, 1, 4),
        action_type="split",
        split_from=1,
        split_to=2,
    )

    intervals = build_factor_intervals(bars, [split], date(2026, 1, 4))

    assert [(item.effective_start, item.effective_end) for item in intervals] == [
        (date(2026, 1, 2), date(2026, 1, 2)),
        (date(2026, 1, 5), date(2026, 1, 5)),
    ]
    assert [item.price_adjustment_factor for item in intervals] == [Decimal("0.5"), Decimal("1")]


def _bulx_bars():
    """Real BULX (Bull leveraged ETF) closes, frozen from Bronze on 2026-07-17.

    BULX liquidated: its final bar is 2026-06-18 at NAV 2.96, and its only cash
    action is a 2.96 terminal distribution ex 2026-06-22 — the entire NAV paid out
    the week after trading stopped. There is no bar on or after the ex-date.
    """
    closes = {
        date(2026, 6, 15): 2.96,
        date(2026, 6, 16): 2.9604,
        date(2026, 6, 17): 2.96,
        date(2026, 6, 18): 2.96,
    }
    return [
        {
            "trade_date": day,
            "symbol_id": 1,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": 1_000,
            "currency": "USD",
            "source": "legacy",
            "price_basis": "unknown",
        }
        for day, close in closes.items()
    ]


def test_terminal_distribution_after_last_bar_is_excluded_not_quarantined():
    bars = _bulx_bars()
    terminal = _action(
        "bulx-liquidation",
        date(2026, 6, 22),  # strictly after the final 2026-06-18 bar
        action_type="cash_dividend",
        cash_amount=2.96,
        currency="USD",
    )

    intervals = build_factor_intervals(bars, [terminal], as_of_date=date(2026, 7, 17))

    # No observable ex-date drop → no adjustment, and crucially no raise: the symbol
    # stages instead of being quarantined by the "dividend < previous close" gate.
    assert [item.price_adjustment_factor for item in intervals] == [Decimal("1")]
    assert [item.split_volume_factor for item in intervals] == [Decimal("1")]


def test_oversized_dividend_within_history_still_raises():
    # Guard: the horizon bound must not disable the magnitude gate for a dividend that
    # does have a bar on/after its ex-date. A 2.96 cut against a 2.96 close on an
    # in-history date is still corrupt and must fail closed.
    bars = _bulx_bars()
    in_history = _action(
        "bulx-oversized",
        date(2026, 6, 18),  # the final bar sits on this date; previous close is 2.96
        action_type="cash_dividend",
        cash_amount=2.96,
        currency="USD",
    )

    with pytest.raises(ValueError, match="cash dividend must be less than positive previous close"):
        build_factor_intervals(bars, [in_history], as_of_date=date(2026, 7, 17))


def test_recurring_dividends_compound_for_older_rows():
    bars = _bars((100.0, 100.0, 100.0, 100.0))
    actions = [
        _action("div-1", date(2026, 1, 3), action_type="cash_dividend", cash_amount=1, currency="USD"),
        _action("div-2", date(2026, 1, 4), action_type="cash_dividend", cash_amount=2, currency="USD"),
    ]
    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, actions, date.max), revision=1)
    assert adjusted[0]["close"] == pytest.approx(100 * 0.99 * 0.98)
    assert adjusted[2]["close"] == pytest.approx(98.0)


def test_same_day_split_is_applied_before_dividend_reference_close():
    bars = _bars((100.0, 50.0, 50.0))
    actions = [
        _action("div", date(2026, 1, 2), action_type="cash_dividend", cash_amount=1, currency="USD"),
        _action("split", date(2026, 1, 2), action_type="split", split_from=1, split_to=2),
    ]
    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, actions, date.max), revision=1)
    assert adjusted[0]["close"] == pytest.approx(49.0)
    assert adjusted[0]["volume"] == 2_000


def test_intervals_are_exhaustive_ordered_and_non_overlapping():
    bars = _bars()
    action = _action("split", date(2026, 1, 3), action_type="split", split_from=1, split_to=2)
    intervals = build_factor_intervals(bars, [action], date.max)
    assert [(item.effective_start, item.effective_end) for item in intervals] == [
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 3), date(2026, 1, 3)),
    ]
    assert all(item.adjustment_revision == 0 for item in intervals)


@given(
    closes=st.lists(st.floats(min_value=1, max_value=10_000, allow_nan=False), min_size=1, max_size=20),
    volumes=st.lists(st.integers(min_value=0, max_value=10_000_000), min_size=1, max_size=20),
)
def test_no_action_identity_property(closes, volumes):
    size = min(len(closes), len(volumes))
    bars = _bars(tuple(closes[:size]))
    for row, volume in zip(bars, volumes[:size], strict=True):
        row["volume"] = volume
    adjusted = adjust_daily_rows(bars, build_factor_intervals(bars, [], date.max), revision=3)
    assert [row["close"] for row in adjusted] == [row["close"] for row in bars]
    assert [row["volume"] for row in adjusted] == [row["volume"] for row in bars]


def test_duplicate_bar_dates_are_rejected():
    bars = _bars()
    bars.append(dict(bars[-1]))
    with pytest.raises(ValueError, match="duplicate"):
        build_factor_intervals(bars, [], date.max)


@pytest.mark.parametrize(
    "action",
    [
        _action(
            "old-dividend",
            date(2025, 12, 31),
            action_type="cash_dividend",
            cash_amount=1,
            currency="USD",
        ),
        _action(
            "first-day-dividend",
            date(2026, 1, 1),
            action_type="cash_dividend",
            cash_amount=1,
            currency="USD",
        ),
        _action(
            "old-split",
            date(2025, 12, 31),
            action_type="split",
            split_from=0,
            split_to=2,
        ),
        _action(
            "first-day-split",
            date(2026, 1, 1),
            action_type="split",
            split_from=0,
            split_to=2,
        ),
    ],
)
def test_action_on_or_before_first_stored_session_is_non_impacting(action):
    bars = _bars(price_basis="unknown")

    intervals = build_factor_intervals(bars, [action], date.max)
    adjusted = adjust_daily_rows(bars, intervals, revision=1)

    assert [row["close"] for row in adjusted] == [row["close"] for row in bars]
    assert [row["volume"] for row in adjusted] == [row["volume"] for row in bars]
    assert [interval.price_adjustment_factor for interval in intervals] == [Decimal("1")]
    assert [interval.split_volume_factor for interval in intervals] == [Decimal("1")]


def test_currency_mismatch_blocks_dividend():
    dividend = _action(
        "div",
        date(2026, 1, 3),
        action_type="cash_dividend",
        cash_amount=1,
        currency="CAD",
    )
    with pytest.raises(ValueError, match="currency"):
        build_factor_intervals(_bars(currency="USD"), [dividend], date.max)


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
        build_factor_intervals(_bars(), [action], date.max)


def test_conflicting_active_splits_on_one_ex_date_fail_closed():
    """Two active splits on one ex-date used to multiply, silently.

    `latest_active()` dedupes on the provider-scoped `provider_event_id`, so one
    logical event recorded under two ids survives twice. Measured 2026-08-02
    across 16 symbols: COEP 200x, BTX 50x, FTLF 10x, and LIME/TTSH where an exact
    inverse pair (`300:1` and `1:300`) collapsed to 1.0 — the split never applied.

    They are not two events, and nothing in the store says which is right, so the
    symbol is quarantined rather than published wrong.
    """
    bars = _bars((400.0, 100.0, 100.0), start=date(2020, 1, 1))
    actions = [
        _action("split-a", date(2020, 1, 2), action_type="split", split_from=1, split_to=4),
        _action("split-b", date(2020, 1, 2), action_type="split", split_from=100, split_to=1),
    ]

    with pytest.raises(ValueError, match="conflicting active splits on 2020-01-02"):
        build_factor_intervals(bars, actions, date(2020, 1, 3))


def test_one_split_restated_at_another_scale_is_collapsed_not_doubled():
    """PGC carries `10:11` and `100:110`; CZFS `1:1.01` and `100:101`.

    Equal ratios are the same event written twice, so it applies once. The
    per-bar loop multiplies every entry it is given, so the duplicate has to be
    dropped — checking the ratio alone would still double-adjust.
    """
    bars = _bars((110.0, 100.0, 100.0), start=date(2020, 1, 1))
    single = [_action("split-a", date(2020, 1, 2), action_type="split", split_from=10, split_to=11)]
    restated = single + [_action("split-b", date(2020, 1, 2), action_type="split", split_from=100, split_to=110)]

    assert build_factor_intervals(bars, restated, date(2020, 1, 3)) == build_factor_intervals(
        bars, single, date(2020, 1, 3)
    )
