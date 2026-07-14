"""Independent reference evidence for split-basis classification."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Literal

from clients.corporate_action_store import CorporateAction


@dataclass(frozen=True)
class SplitReferenceClassification:
    action_id: str
    treatment: Literal["raw", "adjusted", "ambiguous"]
    reason: str
    request_count: int
    pre_date: date | None
    post_date: date | None
    observed_scale_ratio: float | None
    raw_error: float
    adjusted_error: float
    confidence: float
    max_reference_spread_bps: float | None


@dataclass(frozen=True)
class OhlcReferenceCorrection:
    status: Literal["resolved", "ambiguous"]
    reason: str
    trade_date: date
    proposed_values: dict[str, float]
    max_reference_spread_bps: float | None
    close_error_bps: float | None


def _date(value: object) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _ambiguous(
    action: CorporateAction,
    reason: str,
    request_count: int,
    *,
    pre_date: date | None = None,
    post_date: date | None = None,
    observed_scale_ratio: float | None = None,
    raw_error: float = math.inf,
    adjusted_error: float = math.inf,
    confidence: float = 0.0,
    max_reference_spread_bps: float | None = None,
) -> SplitReferenceClassification:
    return SplitReferenceClassification(
        action.action_id,
        "ambiguous",
        reason,
        request_count,
        pre_date,
        post_date,
        observed_scale_ratio,
        raw_error,
        adjusted_error,
        confidence,
        max_reference_spread_bps,
    )


def classify_reference_basis(
    reference_runs: list[list[dict]],
    action: CorporateAction,
    *,
    tolerance: float = 0.15,
    min_margin: float = 0.10,
) -> Literal["raw", "adjusted", "ambiguous"]:
    """Classify repeated provider runs at their own split boundary."""
    if len(reference_runs) < 2:
        return "ambiguous"
    split_from = Decimal(str(action.split_from))
    split_to = Decimal(str(action.split_to))
    if split_from <= 0 or split_to <= 0:
        return "ambiguous"
    factor = float(split_from / split_to)
    treatments = []
    for run in reference_runs:
        rows = {_date(row["trade_date"]): float(row["close"]) for row in run}
        previous = sorted(day for day in rows if day < action.ex_date)
        following = sorted(day for day in rows if day >= action.ex_date)
        if not previous or not following:
            return "ambiguous"
        before = statistics.median(rows[day] for day in previous[-5:])
        after_values = [rows[day] for day in following[:5]]
        if before <= 0 or any(after <= 0 for after in after_values):
            return "ambiguous"
        session_treatments = []
        for after in after_values:
            observed = after / before
            raw_error = abs(math.log(observed / factor))
            adjusted_error = abs(math.log(observed))
            best = min(raw_error, adjusted_error)
            confidence = abs(raw_error - adjusted_error)
            strongly_separated = confidence >= 0.0001 and max(raw_error, adjusted_error) >= best * 10
            if confidence < min_margin or (best > tolerance and not strongly_separated):
                continue
            session_treatments.append("raw" if raw_error < adjusted_error else "adjusted")
        if not session_treatments or len(set(session_treatments)) != 1:
            return "ambiguous"
        treatments.append(session_treatments[0])
    return treatments[0] if len(set(treatments)) == 1 else "ambiguous"


def classify_split_from_reference(
    bronze_rows: list[dict],
    reference_runs: list[list[dict]],
    action: CorporateAction,
    *,
    reference_tolerance_bps: float = 10.0,
    fit_tolerance: float = 0.03,
    min_margin: float = 0.01,
    reference_basis: Literal["raw", "adjusted"] = "adjusted",
) -> SplitReferenceClassification:
    """Classify a Bronze boundary against repeated references of a known basis."""
    if len(reference_runs) < 2:
        return _ambiguous(action, "insufficient_reference_requests", len(reference_runs))
    bronze = {_date(row["trade_date"]): float(row["close"]) for row in bronze_rows}
    references = [{_date(row["trade_date"]): float(row["close"]) for row in run} for run in reference_runs]
    common_dates = set(bronze)
    for reference in references:
        common_dates &= set(reference)
    previous = sorted(day for day in common_dates if day < action.ex_date)
    following = sorted(day for day in common_dates if day >= action.ex_date)
    if not previous:
        return _ambiguous(action, "missing_reference_boundary", len(reference_runs))
    reference_dates = set(references[0])
    for reference in references[1:]:
        reference_dates &= set(reference)
    reference_following = sorted(day for day in reference_dates if day >= action.ex_date)
    pre_only = not following
    if pre_only and not reference_following:
        return _ambiguous(action, "missing_reference_boundary", len(reference_runs))
    pre_date = previous[-1]
    post_date = reference_following[0] if pre_only else following[0]
    comparison_dates = [*previous[-5:], *(reference_following[:5] if pre_only else following[:5])]
    max_spread_bps = 0.0
    for day in comparison_dates:
        values = [reference[day] for reference in references]
        if any(value <= 0 for value in values):
            return _ambiguous(
                action,
                "nonpositive_reference_close",
                len(reference_runs),
                pre_date=pre_date,
                post_date=post_date,
            )
        spread_bps = (max(values) / min(values) - 1.0) * 10_000
        max_spread_bps = max(max_spread_bps, spread_bps)
    if max_spread_bps > reference_tolerance_bps:
        return _ambiguous(
            action,
            "reference_disagreement",
            len(reference_runs),
            pre_date=pre_date,
            post_date=post_date,
            max_reference_spread_bps=max_spread_bps,
        )
    if bronze[pre_date] <= 0 or (not pre_only and bronze[post_date] <= 0):
        return _ambiguous(
            action,
            "nonpositive_bronze_close",
            len(reference_runs),
            pre_date=pre_date,
            post_date=post_date,
            max_reference_spread_bps=max_spread_bps,
        )
    scale_ratios = []
    for reference in references:
        pre_scale = statistics.median(bronze[day] / reference[day] for day in previous[-5:])
        if pre_only:
            scale_ratios.append(pre_scale)
        else:
            post_scale = statistics.median(bronze[day] / reference[day] for day in following[:5])
            scale_ratios.append(pre_scale / post_scale)
    observed = statistics.median(scale_ratios)
    split_from = Decimal(str(action.split_from))
    split_to = Decimal(str(action.split_to))
    if split_from <= 0 or split_to <= 0:
        return _ambiguous(action, "invalid_split_factor", len(reference_runs))
    if reference_basis == "adjusted":
        raw_expected = float(split_to / split_from)
        adjusted_expected = 1.0
    else:
        raw_expected = 1.0
        adjusted_expected = float(split_from / split_to)
    raw_error = abs(math.log(observed / raw_expected))
    adjusted_error = abs(math.log(observed / adjusted_expected))
    best = min(raw_error, adjusted_error)
    confidence = abs(raw_error - adjusted_error)
    if best > fit_tolerance:
        return _ambiguous(
            action,
            "neither_hypothesis_fit",
            len(reference_runs),
            pre_date=pre_date,
            post_date=post_date,
            observed_scale_ratio=observed,
            raw_error=raw_error,
            adjusted_error=adjusted_error,
            confidence=confidence,
            max_reference_spread_bps=max_spread_bps,
        )
    strongly_separated = confidence >= 0.0001 and best <= confidence / 10
    if confidence < min_margin and not strongly_separated:
        return _ambiguous(
            action,
            "low_hypothesis_margin",
            len(reference_runs),
            pre_date=pre_date,
            post_date=post_date,
            observed_scale_ratio=observed,
            raw_error=raw_error,
            adjusted_error=adjusted_error,
            confidence=confidence,
            max_reference_spread_bps=max_spread_bps,
        )
    treatment: Literal["raw", "adjusted"] = "raw" if raw_error < adjusted_error else "adjusted"
    return SplitReferenceClassification(
        action.action_id,
        treatment,
        "reference_consensus_pre_only" if pre_only else "reference_consensus",
        len(reference_runs),
        pre_date,
        post_date,
        observed,
        raw_error,
        adjusted_error,
        confidence,
        max_spread_bps,
    )


def correct_invalid_ohlc_from_reference(
    bronze_row: dict,
    reference_runs: list[list[dict]],
    *,
    reference_tolerance_bps: float = 10.0,
    anchor_tolerance_bps: float = 100.0,
) -> OhlcReferenceCorrection:
    """Recover nonpositive Bronze OHLC fields in the row's existing basis."""
    trade_date = _date(bronze_row["trade_date"])
    if len(reference_runs) < 2:
        return OhlcReferenceCorrection("ambiguous", "insufficient_reference_requests", trade_date, {}, None, None)
    references = []
    for run in reference_runs:
        matching = [row for row in run if _date(row["trade_date"]) == trade_date]
        if len(matching) != 1:
            return OhlcReferenceCorrection("ambiguous", "missing_reference_row", trade_date, {}, None, None)
        references.append(matching[0])
    max_spread_bps = 0.0
    medians: dict[str, float] = {}
    for column in ("open", "high", "low", "close", "adj_close"):
        values = [float(row[column]) for row in references]
        if any(value <= 0 for value in values):
            return OhlcReferenceCorrection("ambiguous", "nonpositive_reference_ohlc", trade_date, {}, None, None)
        spread_bps = (max(values) / min(values) - 1.0) * 10_000
        max_spread_bps = max(max_spread_bps, spread_bps)
        medians[column] = statistics.median(values)
    if max_spread_bps > reference_tolerance_bps:
        return OhlcReferenceCorrection("ambiguous", "reference_disagreement", trade_date, {}, max_spread_bps, None)
    bronze_close = float(bronze_row["close"])
    if bronze_close <= 0:
        return OhlcReferenceCorrection("ambiguous", "nonpositive_bronze_close", trade_date, {}, max_spread_bps, None)
    scale = bronze_close / medians["close"]
    anchor_errors = [
        abs((medians[column] * scale) / float(bronze_row[column]) - 1.0) * 10_000
        for column in ("open", "high", "low", "close", "adj_close")
        if float(bronze_row[column]) > 0
    ]
    close_error_bps = max(anchor_errors)
    if close_error_bps > anchor_tolerance_bps:
        return OhlcReferenceCorrection(
            "ambiguous", "reference_close_mismatch", trade_date, {}, max_spread_bps, close_error_bps
        )
    proposed = {
        column: medians[column] * scale
        for column in ("open", "high", "low", "close", "adj_close")
        if float(bronze_row[column]) <= 0
    }
    if not proposed:
        return OhlcReferenceCorrection("ambiguous", "no_invalid_ohlc", trade_date, {}, max_spread_bps, close_error_bps)
    candidate = {column: float(bronze_row[column]) for column in ("open", "high", "low", "close")}
    candidate.update(proposed)
    if candidate["high"] < max(candidate["open"], candidate["low"], candidate["close"]) or candidate["low"] > min(
        candidate["open"], candidate["high"], candidate["close"]
    ):
        return OhlcReferenceCorrection(
            "ambiguous", "invalid_corrected_ohlc", trade_date, {}, max_spread_bps, close_error_bps
        )
    return OhlcReferenceCorrection(
        "resolved", "reference_consensus", trade_date, proposed, max_spread_bps, close_error_bps
    )
