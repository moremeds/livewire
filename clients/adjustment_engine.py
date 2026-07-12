"""Pure corporate-action factor construction and daily-bar adjustment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from clients.corporate_action_store import CorporateAction

ONE = Decimal("1")


@dataclass(frozen=True)
class FactorInterval:
    effective_start: date
    effective_end: date
    price_adjustment_factor: Decimal
    split_volume_factor: Decimal
    adjustment_revision: int = 0


def _date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    return result


def build_factor_intervals(
    bars: list[dict],
    actions: list[CorporateAction],
) -> list[FactorInterval]:
    """Build exhaustive factor intervals over the supplied bronze sessions."""
    if not bars:
        return []
    ordered_bars = sorted(bars, key=lambda row: _date(row["trade_date"]))
    dates = [_date(row["trade_date"]) for row in ordered_bars]
    if len(dates) != len(set(dates)):
        raise ValueError("duplicate bronze trade dates")

    active_actions = sorted(
        (action for action in actions if action.status == "active"),
        key=lambda action: (action.ex_date, 0 if action.action_type == "split" else 1, action.action_id),
    )
    factors_by_action: dict[str, tuple[Decimal, Decimal]] = {}
    splits_by_date: dict[date, Decimal] = {}

    for action in active_actions:
        if action.action_type != "split":
            continue
        split_from = _decimal(action.split_from, "split_from")
        split_to = _decimal(action.split_to, "split_to")
        if split_from <= 0 or split_to <= 0:
            raise ValueError("split ratio must be positive")
        price_factor = split_from / split_to
        volume_factor = split_to / split_from
        splits_by_date[action.ex_date] = splits_by_date.get(action.ex_date, ONE) * price_factor
        factors_by_action[action.action_id] = (price_factor, volume_factor)

    for action in active_actions:
        if action.action_type == "split":
            continue
        if action.action_type != "cash_dividend":
            raise ValueError(f"unsupported action type: {action.action_type}")
        cash = _decimal(action.cash_amount, "cash dividend")
        if cash < 0:
            raise ValueError("cash dividend must be non-negative")
        previous = [row for row in ordered_bars if _date(row["trade_date"]) < action.ex_date]
        if not previous:
            raise ValueError(f"missing previous close for dividend {action.action_id}")
        previous_row = previous[-1]
        bar_currency = str(previous_row.get("currency", "USD")).upper()
        if not action.currency or action.currency.upper() != bar_currency:
            raise ValueError("dividend currency does not match bronze currency")
        previous_close = _decimal(previous_row["close"], "previous close")
        reference_close = previous_close * splits_by_date.get(action.ex_date, ONE)
        if reference_close <= 0 or cash >= reference_close:
            raise ValueError("cash dividend must be less than positive previous close")
        factors_by_action[action.action_id] = ((reference_close - cash) / reference_close, ONE)

    action_factors = [(action.ex_date, *factors_by_action[action.action_id]) for action in active_actions]

    factors_by_date: list[tuple[date, Decimal, Decimal]] = []
    for bar_date in dates:
        price_factor = ONE
        volume_factor = ONE
        for ex_date, action_price, action_volume in action_factors:
            if ex_date > bar_date:
                price_factor *= action_price
                volume_factor *= action_volume
        factors_by_date.append((bar_date, price_factor, volume_factor))

    intervals: list[FactorInterval] = []
    start, price_factor, volume_factor = factors_by_date[0]
    end = start
    for bar_date, next_price, next_volume in factors_by_date[1:]:
        if (next_price, next_volume) == (price_factor, volume_factor):
            end = bar_date
            continue
        intervals.append(FactorInterval(start, end, price_factor, volume_factor))
        start = end = bar_date
        price_factor, volume_factor = next_price, next_volume
    intervals.append(FactorInterval(start, end, price_factor, volume_factor))
    return intervals


def adjust_daily_rows(
    rows: list[dict],
    intervals: list[FactorInterval],
    revision: int,
) -> list[dict]:
    """Apply one exhaustive factor interval to every bronze daily row."""
    adjusted: list[dict] = []
    for row in sorted(rows, key=lambda item: _date(item["trade_date"])):
        trade_date = _date(row["trade_date"])
        matches = [item for item in intervals if item.effective_start <= trade_date <= item.effective_end]
        if len(matches) != 1:
            raise ValueError(f"expected one factor interval for {trade_date}, found {len(matches)}")
        interval = matches[0]
        output = dict(row)
        for column in ("open", "high", "low", "close"):
            output[column] = float(_decimal(row[column], column) * interval.price_adjustment_factor)
        output["adj_close"] = output["close"]
        output["volume"] = int(
            (_decimal(row["volume"], "volume") * interval.split_volume_factor).to_integral_value(rounding=ROUND_HALF_UP)
        )
        output["price_adjustment_factor"] = float(interval.price_adjustment_factor)
        output["split_volume_factor"] = float(interval.split_volume_factor)
        output["adjustment_revision"] = int(revision)
        adjusted.append(output)
    return adjusted
