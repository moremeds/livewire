"""Pure full-history adjusted-series validation primitives."""

from __future__ import annotations

import math
import statistics
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.corporate_action_store import CorporateAction

PRICE_COLUMNS = ("open", "high", "low", "close")
DEFAULT_WINDOWS = (20, 50, 200)
_THRESHOLD_EPSILON = 1e-9


@dataclass(frozen=True)
class CoverageMap:
    rows: tuple[dict[str, Any], ...]
    sources: dict[date, str]
    unresolved: tuple[date, ...]
    extra_massive_dates: tuple[date, ...]
    extra_ib_dates: tuple[date, ...]


@dataclass(frozen=True)
class MetricSummary:
    count: int
    warning_count: int
    failure_count: int
    max_error_bps: float
    mean_error_bps: float
    median_error_bps: float
    p95_error_bps: float
    worst_date: date | None


@dataclass(frozen=True)
class PointDifference:
    trade_date: date
    column: str
    local: float
    reference: float
    error_bps: float
    severity: str


@dataclass(frozen=True)
class MechanicalJump:
    action_id: str
    ex_date: date
    previous_date: date
    observed_ratio: float
    declared_factor: float


@dataclass(frozen=True)
class SeriesComparison:
    passed: bool
    missing_dates: tuple[date, ...]
    extra_reference_dates: tuple[date, ...]
    point_warning_count: int
    point_failure_count: int
    differences: tuple[PointDifference, ...]
    sma: dict[int, MetricSummary]


def _trade_date(row: dict[str, Any]) -> date:
    value = row["trade_date"]
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _rows_by_date(rows: Iterable[dict[str, Any]], label: str) -> dict[date, dict[str, Any]]:
    result: dict[date, dict[str, Any]] = {}
    for row in rows:
        trade_date = _trade_date(row)
        if trade_date in result:
            raise ValueError(f"duplicate {label} trade date: {trade_date}")
        result[trade_date] = dict(row)
    return result


def _validate_ohlc(row: dict[str, Any], label: str) -> None:
    values: dict[str, float] = {}
    for column in PRICE_COLUMNS:
        try:
            value = float(row[column])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{label} {column} must be numeric") from exc
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{label} {column} must be finite and positive")
        values[column] = value
    if values["high"] < max(values["open"], values["low"], values["close"]):
        raise ValueError(f"{label} high violates OHLC ordering")
    if values["low"] > min(values["open"], values["high"], values["close"]):
        raise ValueError(f"{label} low violates OHLC ordering")


def merge_reference_rows(
    local_dates: Iterable[date],
    massive: Iterable[dict[str, Any]],
    ib: Iterable[dict[str, Any]],
) -> CoverageMap:
    """Choose Massive first, use IB fallback, and retain exact coverage gaps."""
    ordered_dates = tuple(sorted(set(local_dates)))
    local_set = set(ordered_dates)
    massive_by_date = _rows_by_date(massive, "massive")
    ib_by_date = _rows_by_date(ib, "ib")
    rows: list[dict[str, Any]] = []
    sources: dict[date, str] = {}
    unresolved: list[date] = []
    for trade_date in ordered_dates:
        massive_row = massive_by_date.get(trade_date)
        ib_row = ib_by_date.get(trade_date)
        if massive_row is not None:
            rows.append(massive_row)
            sources[trade_date] = "massive+ib" if ib_row is not None else "massive"
        elif ib_row is not None:
            rows.append(ib_row)
            sources[trade_date] = "ib"
        else:
            unresolved.append(trade_date)
    return CoverageMap(
        rows=tuple(rows),
        sources=sources,
        unresolved=tuple(unresolved),
        extra_massive_dates=tuple(sorted(set(massive_by_date) - local_set)),
        extra_ib_dates=tuple(sorted(set(ib_by_date) - local_set)),
    )


def rolling_sma(
    rows: Iterable[dict[str, Any]],
    window: int,
    *,
    column: str = "close",
) -> dict[date, float]:
    """Return a simple moving average over ordered session rows."""
    if window <= 0:
        raise ValueError("moving-average window must be positive")
    ordered = _rows_by_date(rows, "moving-average")
    queue: deque[float] = deque()
    total = 0.0
    result: dict[date, float] = {}
    for trade_date in sorted(ordered):
        value = float(ordered[trade_date][column])
        if not math.isfinite(value):
            raise ValueError(f"moving-average {column} must be finite")
        queue.append(value)
        total += value
        if len(queue) > window:
            total -= queue.popleft()
        if len(queue) == window:
            result[trade_date] = total / window
    return result


def _relative_error_bps(local: float, reference: float) -> float:
    if not math.isfinite(local) or not math.isfinite(reference) or reference == 0:
        return math.inf
    return abs(local - reference) / abs(reference) * 10_000


def _severity(error_bps: float, warning_bps: float, failure_bps: float) -> str:
    if error_bps > failure_bps + _THRESHOLD_EPSILON:
        return "failure"
    if error_bps > warning_bps + _THRESHOLD_EPSILON:
        return "warning"
    return "ok"


def _metric_summary(
    errors: list[tuple[date, float]],
    warning_bps: float,
    failure_bps: float,
) -> MetricSummary:
    if not errors:
        return MetricSummary(0, 0, 0, 0.0, 0.0, 0.0, 0.0, None)
    values = [value for _, value in errors]
    ordered = sorted(values)
    p95_index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    worst_date, maximum = max(errors, key=lambda item: item[1])
    severities = [_severity(value, warning_bps, failure_bps) for value in values]
    return MetricSummary(
        count=len(errors),
        warning_count=severities.count("warning"),
        failure_count=severities.count("failure"),
        max_error_bps=maximum,
        mean_error_bps=statistics.fmean(values),
        median_error_bps=statistics.median(values),
        p95_error_bps=ordered[p95_index],
        worst_date=worst_date,
    )


def _aligned_sma_errors(
    local_by_date: dict[date, dict[str, Any]],
    reference_by_date: dict[date, dict[str, Any]],
    window: int,
) -> list[tuple[date, float]]:
    """Compute SMA errors only after N consecutive aligned local sessions."""
    local_dates = sorted(local_by_date)
    local_queue: deque[float] = deque()
    reference_queue: deque[float] = deque()
    errors: list[tuple[date, float]] = []
    for trade_date in local_dates:
        reference = reference_by_date.get(trade_date)
        if reference is None:
            local_queue.clear()
            reference_queue.clear()
            continue
        local_queue.append(float(local_by_date[trade_date]["close"]))
        reference_queue.append(float(reference["close"]))
        if len(local_queue) > window:
            local_queue.popleft()
            reference_queue.popleft()
        if len(local_queue) == window:
            errors.append(
                (
                    trade_date,
                    _relative_error_bps(
                        statistics.fmean(local_queue),
                        statistics.fmean(reference_queue),
                    ),
                )
            )
    return errors


def compare_series(
    local_rows: Iterable[dict[str, Any]],
    reference_rows: Iterable[dict[str, Any]],
    *,
    warning_bps: float = 1.0,
    failure_bps: float = 5.0,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
) -> SeriesComparison:
    """Compare aligned OHLC point values and rolling close averages."""
    if warning_bps < 0 or failure_bps <= warning_bps:
        raise ValueError("thresholds require 0 <= warning_bps < failure_bps")
    local_by_date = _rows_by_date(local_rows, "local")
    reference_by_date = _rows_by_date(reference_rows, "reference")
    for trade_date, row in local_by_date.items():
        _validate_ohlc(row, f"local {trade_date}")
    for trade_date, row in reference_by_date.items():
        _validate_ohlc(row, f"reference {trade_date}")

    missing_dates = tuple(sorted(set(local_by_date) - set(reference_by_date)))
    extra_dates = tuple(sorted(set(reference_by_date) - set(local_by_date)))
    differences: list[PointDifference] = []
    for trade_date in sorted(set(local_by_date) & set(reference_by_date)):
        for column in PRICE_COLUMNS:
            local = float(local_by_date[trade_date][column])
            reference = float(reference_by_date[trade_date][column])
            error = _relative_error_bps(local, reference)
            severity = _severity(error, warning_bps, failure_bps)
            if severity != "ok":
                differences.append(PointDifference(trade_date, column, local, reference, error, severity))

    sma = {
        window: _metric_summary(
            _aligned_sma_errors(local_by_date, reference_by_date, window),
            warning_bps,
            failure_bps,
        )
        for window in windows
    }
    point_warnings = sum(item.severity == "warning" for item in differences)
    point_failures = sum(item.severity == "failure" for item in differences)
    passed = not missing_dates and point_failures == 0 and all(item.failure_count == 0 for item in sma.values())
    return SeriesComparison(
        passed=passed,
        missing_dates=missing_dates,
        extra_reference_dates=extra_dates,
        point_warning_count=point_warnings,
        point_failure_count=point_failures,
        differences=tuple(differences),
        sma=sma,
    )


def _reconstruct(
    rows: list[dict[str, Any]],
    actions: list[CorporateAction],
    as_of_date: date,
) -> list[dict[str, Any]]:
    intervals = build_factor_intervals(rows, actions, as_of_date)
    return adjust_daily_rows(rows, intervals, revision=1)


def build_split_only_rows(
    rows: list[dict[str, Any]],
    actions: list[CorporateAction],
    as_of_date: date,
) -> list[dict[str, Any]]:
    """Reconstruct split-adjusted rows without dividend factors."""
    return _reconstruct(rows, [item for item in actions if item.action_type == "split"], as_of_date)


def build_total_return_rows(
    rows: list[dict[str, Any]],
    actions: list[CorporateAction],
    as_of_date: date,
) -> list[dict[str, Any]]:
    """Reconstruct the split-plus-dividend daily series."""
    return _reconstruct(rows, actions, as_of_date)


def find_mechanical_split_jumps(
    adjusted_rows: Iterable[dict[str, Any]],
    actions: Iterable[CorporateAction],
    as_of_date: date,
    *,
    tolerance: float = 0.15,
) -> tuple[MechanicalJump, ...]:
    """Find adjusted returns that still resemble a declared split ratio."""
    if tolerance <= 0:
        raise ValueError("mechanical-jump tolerance must be positive")
    by_date = _rows_by_date(adjusted_rows, "adjusted")
    ordered_dates = sorted(by_date)
    jumps: list[MechanicalJump] = []
    for action in actions:
        if action.status != "active" or action.action_type != "split" or action.ex_date > as_of_date:
            continue
        previous_dates = [item for item in ordered_dates if item < action.ex_date]
        following_dates = [item for item in ordered_dates if item >= action.ex_date]
        if not previous_dates or not following_dates:
            continue
        previous_date = previous_dates[-1]
        following_date = following_dates[0]
        observed = float(by_date[following_date]["close"]) / float(by_date[previous_date]["close"])
        factor = float(action.split_from) / float(action.split_to)
        if observed <= 0 or factor <= 0 or math.isclose(factor, 1.0):
            continue
        mechanical_error = min(abs(math.log(observed / factor)), abs(math.log(observed * factor)))
        if mechanical_error <= tolerance:
            jumps.append(MechanicalJump(action.action_id, action.ex_date, previous_date, observed, factor))
    return tuple(jumps)
