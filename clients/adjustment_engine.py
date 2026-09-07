"""Pure corporate-action factor construction and daily-bar adjustment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from clients.corporate_action_store import CorporateAction
from clients.timeutils import coerce_date

ONE = Decimal("1")


@dataclass(frozen=True)
class FactorInterval:
    effective_start: date
    effective_end: date
    price_adjustment_factor: Decimal
    split_volume_factor: Decimal
    adjustment_revision: int = 0


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
    as_of_date: date,
) -> list[FactorInterval]:
    """Build exhaustive factor intervals over the supplied bronze sessions."""
    if not bars:
        return []
    ordered_bars = sorted(bars, key=lambda row: coerce_date(row["trade_date"]))
    dates = [coerce_date(row["trade_date"]) for row in ordered_bars]
    if len(dates) != len(set(dates)):
        raise ValueError("duplicate bronze trade dates")

    first_trade_date = dates[0]
    last_trade_date = dates[-1]
    # An action whose ex-date is after the final bar has no observable ex-date drop in
    # this series, so it cannot be back-adjusted for. Bounding by last_trade_date (not
    # just as_of_date) excludes those — e.g. a terminal liquidating distribution paid the
    # trading week after a fund's last bar, whose cash == NAV would otherwise trip the
    # "dividend < previous close" gate and quarantine the whole symbol. When bronze later
    # extends past the ex-date, the window is re-derived and the action re-enters.
    horizon = min(as_of_date, last_trade_date)
    active_actions = sorted(
        (action for action in actions if action.status == "active" and first_trade_date < action.ex_date <= horizon),
        key=lambda action: (action.ex_date, 0 if action.action_type == "split" else 1, action.action_id),
    )
    factors_by_action: dict[str, tuple[Decimal, Decimal]] = {}
    splits_by_date: dict[date, Decimal] = {}
    superseded: set[str] = set()

    for action in active_actions:
        if action.action_type != "split":
            continue
        split_from = _decimal(action.split_from, "split_from")
        split_to = _decimal(action.split_to, "split_to")
        if split_from <= 0 or split_to <= 0:
            raise ValueError("split ratio must be positive")
        price_factor = split_from / split_to
        volume_factor = split_to / split_from
        # Two active splits on one ex-date used to MULTIPLY, here and in the
        # per-bar loop below. `latest_active()` dedupes on the provider-scoped
        # `provider_event_id`, so one logical event recorded under two ids
        # survives twice and the symbol silently publishes wrong prices —
        # measured 2026-08-02 across 16 symbols: COEP 200x, BTX 50x, FTLF 10x,
        # LIME and TTSH collapsing to 1.0 (the split never applied at all).
        #
        # These are not two events. They are the same event disagreeing with
        # itself: exact inverses (LIME `300:1` vs `1:300`), or ratios that
        # migrated between dates across revisions (TSM 2007 and 2009 swapped).
        # Nothing in the store says which is right, so this fails closed and
        # quarantines the symbol rather than guessing — the same rule the basis
        # machinery already applies to an ambiguous split boundary.
        previous = splits_by_date.get(action.ex_date)
        if previous is not None:
            if previous != price_factor:
                raise ValueError(
                    f"conflicting active splits on {action.ex_date}: "
                    f"price factor {previous} vs {price_factor} ({action.action_id})"
                )
            # Same ratio restated at a different scale — PGC `10:11` and
            # `100:110`, CZFS `1:1.01` and `100:101`. One event, so keep one.
            superseded.add(action.action_id)
            continue
        splits_by_date[action.ex_date] = price_factor
        factors_by_action[action.action_id] = (price_factor, volume_factor)

    for action in active_actions:
        if action.action_type == "split":
            continue
        if action.action_type != "cash_dividend":
            raise ValueError(f"unsupported action type: {action.action_type}")
        cash = _decimal(action.cash_amount, "cash dividend")
        if cash < 0:
            raise ValueError("cash dividend must be non-negative")
        previous = [row for row in ordered_bars if coerce_date(row["trade_date"]) < action.ex_date]
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

    # Dropping the superseded split here is the half that actually matters: the
    # per-bar loop below multiplies every entry, so leaving a duplicate in would
    # double-adjust even when both records agree.
    action_factors = [
        (action, *factors_by_action[action.action_id])
        for action in active_actions
        if action.action_id not in superseded
    ]

    factors_by_date: list[tuple[date, Decimal, Decimal]] = []
    for row, bar_date in zip(ordered_bars, dates, strict=True):
        price_factor = ONE
        volume_factor = ONE
        for action, action_price, action_volume in action_factors:
            if action.ex_date <= bar_date:
                continue
            if action.action_type == "split":
                price_basis = str(row.get("price_basis", "unknown"))
                if price_basis == "unknown":
                    raise ValueError(f"unknown price_basis for split-affected row {bar_date}")
                if price_basis == "split_adjusted":
                    continue
                if price_basis != "raw":
                    raise ValueError(f"unsupported price_basis: {price_basis!r}")
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
    for row in sorted(rows, key=lambda item: coerce_date(item["trade_date"])):
        trade_date = coerce_date(row["trade_date"])
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
