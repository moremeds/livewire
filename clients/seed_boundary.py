"""Deterministic detector for the 2021-06 bulk-seed basis artifact.

The warehouse was bulk-seeded over 2021-06-11..2021-06-21 from one deep IB fetch
of *back-adjusted* prices labelled ``price_basis='raw'``; rows on/after the window
are genuine raw. A symbol is corrupt exactly when its stored series steps by ``P``
at the window, where ``P`` is the product of the split factors that took effect
after it — the adjustment IB had already applied.

Unlike the blind ratio heuristic in :mod:`clients.silver_continuity`, this looks at
a *known location* and compares against a *predicted* value, so it resolves the
2x-5x class the 6.0 threshold structurally cannot see. It MEASURES rather than
assumes: symbols later re-pulled raw have ``P != 1`` yet a flat boundary (KLAC,
COO) and must stay clean.
"""

from __future__ import annotations

import math

from clients.corporate_action_store import CorporateAction

SEED_WINDOW_START = "2021-06-11"
SEED_WINDOW_END = "2021-06-21"
# |ln(fold)| below this overlaps ordinary daily moves (measured p999 of |ln return|
# over 566k adjacent no-split days = 0.359), so a match there is not evidence.
MIN_CONFIDENT_LOG_FOLD = 0.55
DEFAULT_TOLERANCE = 0.25


class SeedBoundaryBreak(ValueError):
    """The stored series steps by the predicted split fold at the seed window.

    Subclasses ``ValueError`` so the rebuild staging loop's ``except Exception``
    quarantines the symbol, while exposing structured fields for callers.
    """

    def __init__(self, date: str, observed: float, predicted: float) -> None:
        self.date = date
        self.observed = observed
        self.predicted = predicted
        super().__init__(
            f"seed-boundary basis break at {date}: observed {observed:.2f}x step matches the "
            f"{predicted:.2f}x post-boundary split fold — pre-{SEED_WINDOW_START} rows are already split-adjusted"
        )


def predict_boundary_fold(actions: list[CorporateAction], *, window_end: str = SEED_WINDOW_END) -> float:
    """Product of active split magnitudes with ``ex_date`` after the seed window.

    Returns a magnitude >= 1: a 25:1 reverse and a 1:25 forward both report 25.0,
    because the boundary step's direction follows the split's.
    """
    fold = 1.0
    for action in actions:
        if action.action_type != "split" or action.status != "active":
            continue
        if action.ex_date.isoformat() <= window_end:
            continue
        if not action.split_from or not action.split_to:
            continue
        fold *= float(action.split_to) / float(action.split_from)
    if fold <= 0 or not math.isfinite(fold):
        return 1.0
    return max(fold, 1.0 / fold)


def measure_boundary_jump(
    rows: list[dict], *, window_start: str = SEED_WINDOW_START, window_end: str = SEED_WINDOW_END
) -> tuple[str, float] | None:
    """Largest adjacent-day close-ratio magnitude stepping into the seed window.

    Returns ``(date, ratio)`` for the largest step whose *later* date is inside the
    window, or ``None`` when no such adjacent pair exists (symbol seeded later, or
    no pre-window history).
    """
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    best: tuple[str, float] | None = None
    for previous, current in zip(ordered, ordered[1:], strict=False):
        current_date = str(current["trade_date"])[:10]
        if not (window_start <= current_date <= window_end):
            continue
        try:
            previous_close = float(previous["close"])
            current_close = float(current["close"])
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(previous_close) and math.isfinite(current_close)):
            continue
        if previous_close <= 0 or current_close <= 0:
            continue
        ratio = max(current_close / previous_close, previous_close / current_close)
        if best is None or ratio > best[1]:
            best = (current_date, ratio)
    return best


def classify_seed_boundary(
    rows: list[dict],
    actions: list[CorporateAction],
    *,
    window_start: str = SEED_WINDOW_START,
    window_end: str = SEED_WINDOW_END,
    tolerance: float = DEFAULT_TOLERANCE,
) -> dict:
    """Measure the boundary against the predicted fold.

    ``corrupt`` when the observed step matches the predicted fold within
    ``tolerance`` (log space) and the fold outruns daily noise; ``inconclusive``
    when the fold is too small to be evidence or the boundary is unmeasurable;
    ``clean`` otherwise — including a predicted fold with a flat boundary.
    """
    fold = predict_boundary_fold(actions, window_end=window_end)
    measured = measure_boundary_jump(rows, window_start=window_start, window_end=window_end)
    result: dict = {
        "fold": fold,
        "observed": None if measured is None else measured[1],
        "date": None if measured is None else measured[0],
        "verdict": "clean",
    }
    if measured is None:
        result["verdict"] = "inconclusive" if fold > 1.01 else "clean"
        return result
    if fold <= 1.01:
        return result
    if abs(math.log(fold)) < MIN_CONFIDENT_LOG_FOLD:
        result["verdict"] = "inconclusive"
        return result
    if abs(math.log(measured[1]) - math.log(fold)) <= tolerance:
        result["verdict"] = "corrupt"
    return result


def check_seed_boundary(
    rows: list[dict],
    actions: list[CorporateAction],
    *,
    window_start: str = SEED_WINDOW_START,
    window_end: str = SEED_WINDOW_END,
    tolerance: float = DEFAULT_TOLERANCE,
) -> None:
    """Raise :class:`SeedBoundaryBreak` when the symbol is confidently corrupt."""
    result = classify_seed_boundary(
        rows, actions, window_start=window_start, window_end=window_end, tolerance=tolerance
    )
    if result["verdict"] == "corrupt":
        raise SeedBoundaryBreak(result["date"], result["observed"], result["fold"])
