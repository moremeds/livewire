"""Normalize provider-adjusted equity daily rows to canonical raw basis."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

from clients.corporate_action_store import CorporateAction
from clients.timeutils import coerce_date
from clients.trading_calendar import trading_days_between

ONE = Decimal("1")
# Splits whose ex-date carried a real market move larger than the tolerance.
# They are classified by margin alone; each entry cites the evidence the move is real.
# ponytail: an id list in code; a restated action gets a new id and fails closed again.
TOLERANCE_EXEMPT_SPLITS = frozenset(
    {
        # UVXY 2014-01-24 1:4 reverse. IB close 386.5M -> 455.06M (+17.7%, VIX spike; open gap +5.8%),
        # macmini:~/market-warehouse/logs/uvxy-ib-2014-01-readonly-2026-09-28.txt
        "c65fe6154f36c5d954452c6549045978",
    }
)
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


def _classify_ratio(
    observed: float, raw_target: float, adjusted_target: float, tolerance: float, min_margin: float
) -> tuple[Literal["raw", "adjusted", "ambiguous"], float, float, float]:
    raw_error = abs(math.log(observed / raw_target))
    adjusted_error = abs(math.log(observed / adjusted_target))
    best = min(raw_error, adjusted_error)
    margin = abs(raw_error - adjusted_error)
    if best > tolerance or margin < min_margin:
        treatment: Literal["raw", "adjusted", "ambiguous"] = "ambiguous"
    else:
        treatment = "raw" if raw_error < adjusted_error else "adjusted"
    return treatment, raw_error, adjusted_error, margin


def classify_split_events(
    rows: list[dict],
    actions: list[CorporateAction],
    as_of_date: date,
    *,
    tolerance: float = 0.15,
    min_margin: float = 0.10,
    last_ib_date: date | None = None,
) -> list[SplitClassification]:
    """Classify each effective split as raw, adjusted, or ambiguous.

    A split whose ``ex_date`` falls after the last incoming IB row (the backfill
    case: incoming rows end before existing rows begin) cannot be measured at its
    own boundary — the rows on both sides of that boundary are existing rows, so
    the comparison only reproduces the real, already-correct raw jump and never
    observes what basis IB's incoming block was actually delivered in. Such
    splits are deferred and classified together from the seam between the last
    incoming IB row and the first existing row after it (see
    :func:`_classify_seam_group`), since IB back-adjusts an entire returned
    window for every split up to fetch time, including ones after the window.

    ``last_ib_date`` should be the last date of the *incoming* IB batch, passed
    explicitly by :func:`prepare_ib_rows_for_publish`. Recent bronze equity
    rows are commonly ``source='ib'`` too (IB is the primary daily provider),
    so inferring it from every ``source == 'ib'`` row in ``rows`` (the default,
    kept for callers like ``audit_split_basis.py`` that stage their entire
    existing history as ``source='ib'`` and have no separate incoming batch)
    would pick up an existing IB-sourced row dated after the split and never
    route it to the seam at all.
    """
    if tolerance <= 0 or min_margin < 0:
        raise ValueError("classification tolerances must be positive")
    ordered = sorted(rows, key=lambda row: coerce_date(row["trade_date"]))
    if last_ib_date is None:
        ib_dates = [coerce_date(row["trade_date"]) for row in ordered if row.get("source") == "ib"]
        last_ib_date = max(ib_dates) if ib_dates else None
    result: list[SplitClassification] = []
    seam_actions: list[tuple[CorporateAction, Decimal]] = []
    for action, factor in _effective_splits(actions, as_of_date):
        previous = [row for row in ordered if coerce_date(row["trade_date"]) < action.ex_date]
        following = [row for row in ordered if coerce_date(row["trade_date"]) >= action.ex_date]
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
        if last_ib_date is not None and action.ex_date > last_ib_date:
            seam_actions.append((action, factor))
            continue
        before = float(_decimal(previous[-1]["close"], "previous close"))
        after = float(_decimal(following[0]["close"], "following close"))
        if before <= 0 or after <= 0:
            raise ValueError("split-boundary closes must be positive")
        observed = after / before
        action_tolerance = math.inf if action.action_id in TOLERANCE_EXEMPT_SPLITS else tolerance
        treatment, raw_error, adjusted_error, margin = _classify_ratio(
            observed, float(factor), 1.0, action_tolerance, min_margin
        )
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
    if seam_actions:
        result.extend(_classify_seam_group(seam_actions, ordered, last_ib_date, tolerance, min_margin))
    return result


def _classify_seam_group(
    seam_actions: list[tuple[CorporateAction, Decimal]],
    ordered: list[dict],
    last_ib_date: date,
    tolerance: float,
    min_margin: float,
    *,
    before_is_ib: bool = True,
) -> list[SplitClassification]:
    """Classify splits after a seam from the row before it and the row after.

    Model: IB divides raw historical prices by the product of every split
    factor with ``ex_date`` after that date, ``F(t)``, evaluated as of fetch
    time — so ``adjusted_price(t) = raw_price(t) * F(t)`` (see
    :func:`normalize_ib_rows`, which reverses this by dividing by the same
    factor). A split whose ``ex_date`` falls in the gap itself (after
    ``last_ib_date`` but on or before the first row past it) has already
    actually happened by the time the other side resumes, so that side's raw
    price already carries the real jump; a split later still has NOT
    happened yet by the first row past the seam, and only appears through
    whichever side is IB being pre-adjusted for it.

    This is exact under that model, not an approximation, but it is NOT
    symmetric in which side is IB — the two derivations below differ,
    because ``F`` cancels against the real gap jump on one side and
    compounds with it on the other:

    ``before_is_ib=True`` (ib -> non-ib, e.g. an incoming backfill batch
    ending before existing data): ``before = raw(t0)*f_gap*f_later``
    (adjusted) or ``raw(t0)`` (raw); ``after = raw(t1)`` always (raw(t1) =
    raw(t0)*f_gap, the real gap jump). ``observed = after/before`` gives
    ``f_gap`` (raw) or ``1/f_later`` (adjusted) — ``f_gap`` cancels out of
    the adjusted target entirely.

    ``before_is_ib=False`` (non-ib -> ib, e.g. an existing block followed by
    a later IB fetch): ``before = raw(t0)`` always; ``after = raw(t1)``
    (raw) or ``raw(t1)*f_later`` (adjusted, since only splits after ``t1``
    are still unaccounted for in IB's own row). ``observed`` gives ``f_gap``
    (raw, same as the other direction) or ``f_gap*f_later`` (adjusted) —
    here ``f_gap`` does NOT cancel, because it is baked into ``raw(t1)``
    (a real historical fact) rather than into a factor being undone.
    """
    # Every action here was only added to seam_actions by the caller after
    # confirming it has a row at/after its own ex_date, and ex_date >
    # last_ib_date, so at least one row after the seam always exists. Look up
    # by date, not by source: combined_by_date lets the incoming row win over
    # an existing row dated the same day for the ib-before case, so the row
    # at last_ib_date is the right one regardless of what label it carries.
    before_row = next(row for row in ordered if coerce_date(row["trade_date"]) == last_ib_date)
    after_seam = [row for row in ordered if coerce_date(row["trade_date"]) > last_ib_date]
    first_existing_date = coerce_date(after_seam[0]["trade_date"])

    # A seam step spans more than a handful of trading days only when the
    # incoming batch and the existing series don't actually meet — the ratio
    # is then a multi-period return, not a single boundary step, and a
    # genuine ~50% move is indistinguishable from a 2:1 split. 5 trading days
    # is generous slack for a short weekend/holiday gap while still catching
    # a real multi-week hole.
    if trading_days_between(last_ib_date, first_existing_date) > 5:
        return [
            SplitClassification(action.action_id, action.ex_date, factor, "ambiguous", None, math.inf, math.inf, 0.0)
            for action, factor in seam_actions
        ]

    gap_factor = 1.0
    later_factor = 1.0
    for action, factor in seam_actions:
        if action.ex_date <= first_existing_date:
            gap_factor *= float(factor)
        else:
            later_factor *= float(factor)

    before = float(_decimal(before_row["close"], "seam before close"))
    after = float(_decimal(after_seam[0]["close"], "seam after close"))
    if before <= 0 or after <= 0:
        raise ValueError("split-boundary closes must be positive")
    observed = after / before
    adjusted_target = (1.0 / later_factor) if before_is_ib else (gap_factor * later_factor)
    treatment, raw_error, adjusted_error, margin = _classify_ratio(
        observed, gap_factor, adjusted_target, tolerance, min_margin
    )
    return [
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
        for action, factor in seam_actions
    ]


def classify_source_seam_breaks(
    rows: list[dict],
    actions: list[CorporateAction],
    as_of_date: date,
    *,
    tolerance: float = 0.15,
    min_margin: float = 0.10,
) -> list[tuple[date, SplitClassification]]:
    """Classify every ib/non-ib source transition in a symbol's full series.

    Generalizes the seam check in :func:`prepare_ib_rows_for_publish` — built
    for one incoming/existing boundary — to every adjacent-row pair in a
    symbol's stored history whose ``source`` crosses the ib/non-ib line,
    classified the same way against the splits active after it (see
    :func:`_classify_seam_group`). Used by the offline basis audit
    (``audit_legacy_basis.py``) to catch a mis-basis block whose splits lie
    entirely after it: a split-boundary-only check (continuity) never looks
    there, and the fixed 2021-06 seed-boundary check only looks at that one
    location. Returns ``(boundary_date, classification)`` pairs so a caller
    can report where each seam sits, not just which action it implicates.
    """
    ordered = sorted(rows, key=lambda row: coerce_date(row["trade_date"]))
    result: list[tuple[date, SplitClassification]] = []
    for previous, current in zip(ordered, ordered[1:], strict=False):
        previous_is_ib = previous.get("source") == "ib"
        if previous_is_ib == (current.get("source") == "ib"):
            continue
        boundary_date = coerce_date(previous["trade_date"])
        seam_actions = [(a, f) for a, f in _effective_splits(actions, as_of_date) if a.ex_date > boundary_date]
        if not seam_actions:
            continue
        for classification in _classify_seam_group(
            seam_actions, ordered, boundary_date, tolerance, min_margin, before_is_ib=previous_is_ib
        ):
            result.append((boundary_date, classification))
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
        trade_date = coerce_date(row["trade_date"])
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

    ``as_of_date`` gates which corporate actions are "effective" and is often
    supplied by callers as the last date in the incoming batch, not real
    calendar-today — correct for the common incremental-forward case, where the
    batch's last date already is roughly today. It is wrong for a backfill
    batch (incoming rows end before existing rows begin): a split dated after
    the batch but on or before the existing data's own latest date has
    provably already happened — the existing rows past it are the proof — so
    the effective date is widened to cover them rather than trusting the
    caller's possibly stale cutoff.
    """
    staged = [
        {**row, "source": "ib", "price_basis": "split_adjusted"} if row.get("source") == "ib" else dict(row)
        for row in incoming_rows
    ]
    incoming_ib_dates = [coerce_date(row["trade_date"]) for row in staged if row.get("source") == "ib"]
    if not incoming_ib_dates:
        return staged
    earliest_ib_date = min(incoming_ib_dates)
    latest_incoming_ib_date = max(incoming_ib_dates)
    relevant_actions = [action for action in actions if action.ex_date > earliest_ib_date]
    combined_by_date = {str(row["trade_date"]): row for row in existing_rows}
    combined_by_date.update({str(row["trade_date"]): row for row in staged})
    existing_dates = [coerce_date(row["trade_date"]) for row in existing_rows]
    effective_as_of = max(as_of_date, *existing_dates) if existing_dates else as_of_date
    classifications = classify_split_events(
        list(combined_by_date.values()),
        relevant_actions,
        effective_as_of,
        last_ib_date=latest_incoming_ib_date,
    )
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
        trade_date = coerce_date(row["trade_date"])
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
