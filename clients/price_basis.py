"""Normalize provider-adjusted equity daily rows to canonical raw basis."""

from __future__ import annotations

from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from clients.corporate_action_store import CorporateAction

ONE = Decimal("1")
VOLUME_MODES = frozenset({"raw", "split_adjusted"})


def _date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _decimal(value, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    return result


def normalize_split_adjusted_rows(
    rows: list[dict],
    actions: list[CorporateAction],
    as_of_date: date,
    *,
    volume_mode: str,
) -> list[dict]:
    """Reverse effective split adjustments in IB daily rows.

    ``volume_mode`` is intentionally required and has no permissive default.
    Use ``raw`` when IB volume is already historical raw volume, or
    ``split_adjusted`` when volume was adjusted alongside price.
    """
    if volume_mode not in VOLUME_MODES:
        raise ValueError("IB volume convention must be calibrated before normalization")

    splits: list[tuple[date, Decimal]] = []
    for action in actions:
        if action.status != "active" or action.action_type != "split" or action.ex_date > as_of_date:
            continue
        split_from = _decimal(action.split_from, "split_from")
        split_to = _decimal(action.split_to, "split_to")
        if split_from <= 0 or split_to <= 0:
            raise ValueError("split ratio must be positive")
        splits.append((action.ex_date, split_from / split_to))

    normalized: list[dict] = []
    for row in rows:
        if row.get("price_basis") != "split_adjusted":
            raise ValueError("normalization requires split_adjusted input rows")
        trade_date = _date(row["trade_date"])
        factor = ONE
        for ex_date, split_factor in splits:
            if ex_date > trade_date:
                factor *= split_factor
        if factor <= 0:
            raise ValueError("cumulative split factor must be positive")

        output = dict(row)
        for column in ("open", "high", "low", "close", "adj_close"):
            raw_price = _decimal(row[column], column) / factor
            if raw_price <= 0:
                raise ValueError(f"normalized {column} must be positive")
            output[column] = float(raw_price)
        if volume_mode == "split_adjusted":
            output["volume"] = int(
                (_decimal(row["volume"], "volume") * factor).to_integral_value(rounding=ROUND_HALF_UP)
            )
        output["source"] = "ib"
        output["price_basis"] = "raw"
        normalized.append(output)
    return normalized
