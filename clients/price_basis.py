"""Normalize provider-adjusted equity daily rows to canonical raw basis."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

from clients.corporate_action_store import CorporateAction

ONE = Decimal("1")
VOLUME_MODES = frozenset({"raw", "split_adjusted"})


@dataclass(frozen=True)
class SplitClassification:
    action_id: str
    ex_date: date
    split_factor: Decimal
    treatment: Literal["raw", "adjusted", "ambiguous"]
    observed_ratio: float | None
    raw_error: float
    adjusted_error: float
    confidence: float


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


def _effective_splits(actions: list[CorporateAction], as_of_date: date) -> list[tuple[CorporateAction, Decimal]]:
    result: list[tuple[CorporateAction, Decimal]] = []
    for action in actions:
        if action.status != "active" or action.action_type != "split" or action.ex_date > as_of_date:
            continue
        split_from = _decimal(action.split_from, "split_from")
        split_to = _decimal(action.split_to, "split_to")
        if split_from <= 0 or split_to <= 0:
            raise ValueError("split ratio must be positive")
        result.append((action, split_from / split_to))
    return sorted(result, key=lambda item: (item[0].ex_date, item[0].action_id))


def classify_split_events(
    rows: list[dict],
    actions: list[CorporateAction],
    as_of_date: date,
    *,
    tolerance: float = 0.15,
    min_margin: float = 0.10,
) -> list[SplitClassification]:
    """Classify each effective split as raw, adjusted, or ambiguous."""
    if tolerance <= 0 or min_margin < 0:
        raise ValueError("classification tolerances must be positive")
    ordered = sorted(rows, key=lambda row: _date(row["trade_date"]))
    result: list[SplitClassification] = []
    for action, factor in _effective_splits(actions, as_of_date):
        previous = [row for row in ordered if _date(row["trade_date"]) < action.ex_date]
        following = [row for row in ordered if _date(row["trade_date"]) >= action.ex_date]
        if not previous:
            continue
        if not following:
            result.append(
                SplitClassification(
                    action.action_id,
                    action.ex_date,
                    factor,
                    "ambiguous",
                    None,
                    math.inf,
                    math.inf,
                    0.0,
                )
            )
            continue
        before = float(_decimal(previous[-1]["close"], "previous close"))
        after = float(_decimal(following[0]["close"], "following close"))
        if before <= 0 or after <= 0:
            raise ValueError("split-boundary closes must be positive")
        observed = after / before
        raw_error = abs(math.log(observed / float(factor)))
        adjusted_error = abs(math.log(observed))
        best = min(raw_error, adjusted_error)
        margin = abs(raw_error - adjusted_error)
        if best > tolerance or margin < min_margin:
            treatment: Literal["raw", "adjusted", "ambiguous"] = "ambiguous"
        else:
            treatment = "raw" if raw_error < adjusted_error else "adjusted"
        result.append(
            SplitClassification(
                action.action_id,
                action.ex_date,
                factor,
                treatment,
                observed,
                raw_error,
                adjusted_error,
                margin,
            )
        )
    return result


def normalize_ib_rows(rows: list[dict], classifications: list[SplitClassification]) -> list[dict]:
    """Reverse only split events that IB already incorporated."""
    ambiguous = [item.action_id for item in classifications if item.treatment == "ambiguous"]
    if ambiguous:
        raise ValueError(f"ambiguous split classifications: {', '.join(ambiguous)}")
    normalized: list[dict] = []
    for row in rows:
        if row.get("source") != "ib" or row.get("price_basis") not in {"unknown", "split_adjusted"}:
            raise ValueError("normalization requires staged IB rows")
        trade_date = _date(row["trade_date"])
        factor = ONE
        for item in classifications:
            if item.treatment == "adjusted" and item.ex_date > trade_date:
                factor *= item.split_factor
        output = dict(row)
        for column in ("open", "high", "low", "close", "adj_close"):
            raw_price = _decimal(row[column], column) / factor
            if raw_price <= 0:
                raise ValueError(f"normalized {column} must be positive")
            output[column] = float(raw_price)
        output["volume"] = int((_decimal(row["volume"], "volume") * factor).to_integral_value(rounding=ROUND_HALF_UP))
        output["price_basis"] = "raw"
        normalized.append(output)
    return normalized


def prepare_ib_rows_for_publish(
    incoming_rows: list[dict],
    *,
    existing_rows: list[dict],
    actions: list[CorporateAction],
    as_of_date: date,
) -> list[dict]:
    """Classify IB split treatment and return canonical raw incoming rows.

    Existing canonical rows supply the opposite side of split boundaries for
    incremental and backfill requests. They are classification context only and
    are never returned or rewritten by this helper.
    """
    staged = [
        {**row, "source": "ib", "price_basis": "split_adjusted"} if row.get("source") == "ib" else dict(row)
        for row in incoming_rows
    ]
    if not any(row.get("source") == "ib" for row in staged):
        return staged
    earliest_ib_date = min(_date(row["trade_date"]) for row in staged if row.get("source") == "ib")
    relevant_actions = [action for action in actions if action.ex_date > earliest_ib_date]
    combined_by_date = {str(row["trade_date"]): row for row in existing_rows}
    combined_by_date.update({str(row["trade_date"]): row for row in staged})
    classifications = classify_split_events(list(combined_by_date.values()), relevant_actions, as_of_date)
    normalized_ib = iter(normalize_ib_rows([row for row in staged if row.get("source") == "ib"], classifications))
    return [next(normalized_ib) if row.get("source") == "ib" else row for row in staged]


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

    splits = [(action.ex_date, factor) for action, factor in _effective_splits(actions, as_of_date)]

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
