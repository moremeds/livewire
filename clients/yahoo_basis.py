"""Pure raw-close reconstruction and split reconciliation against the Yahoo reference.

Yahoo's `close` is split-adjusted, so the true raw close of a legacy unknown-basis
row is `yahoo_close * product(split multiplier for ex_date after that row)`. Silver,
however, applies split factors from OUR action store — so before a reconstructed raw
series can pass Silver, the store's splits must agree with Yahoo's. `reconcile_splits`
reports what the store is missing (Yahoo has it — add it) and what it has spuriously
(Yahoo lacks it — usually a stock-dividend/spinoff mis-recorded as a split).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from clients.yahoo_client import YahooBar, YahooSplit


def reconstruct_raw_closes(bars: list[YahooBar], splits: list[YahooSplit]) -> dict[date, float]:
    """True raw close per date: split-adjusted close times the folds still ahead of it.

    A split only affects rows BEFORE its ex-date (the ex-date row already shows the
    post-split price), matching ``build_factor_intervals``' ``action.ex_date > bar_date``.
    """
    multipliers = sorted(((s.ex_date, s.price_multiplier) for s in splits), key=lambda item: item[0])
    raw: dict[date, float] = {}
    for bar in bars:
        factor = 1.0
        for ex_date, multiplier in multipliers:
            if ex_date > bar.trade_date:
                factor *= multiplier
        raw[bar.trade_date] = bar.close * factor
    return raw


@dataclass(frozen=True)
class SplitReconciliation:
    matched: list[date] = field(default_factory=list)
    # Yahoo records a split the store lacks — the store must gain it or Silver under-adjusts.
    yahoo_only: list[tuple[date, float]] = field(default_factory=list)
    # The store records a "split" Yahoo lacks — usually a spinoff/stock-dividend mis-recorded.
    store_only: list[tuple[date, float]] = field(default_factory=list)

    @property
    def reconciled(self) -> bool:
        return not self.yahoo_only and not self.store_only


def reconcile_splits(
    yahoo_splits: list[YahooSplit],
    store_splits: list[tuple[date, float]],
    *,
    ratio_tol: float = 0.02,
    day_tol: int = 3,
    min_date: date | None = None,
) -> SplitReconciliation:
    """Match Yahoo splits to store splits by near-equal ex-date AND near-equal ratio.

    ``store_splits`` are ``(ex_date, split_to/split_from)`` — the same price multiplier
    Yahoo reports as ``numerator/denominator``. A provider can stamp the same split a day
    apart, hence ``day_tol``.

    ``min_date`` drops splits with ``ex_date <= min_date`` from BOTH sides before matching.
    Pass the first stored bronze date: a split on/before it affects no stored row (both
    ``reconstruct_raw_closes`` and ``build_factor_intervals`` apply only ``ex_date > bar_date``),
    so a Yahoo/store disagreement there is immaterial and must not fail the symbol closed.
    """
    if min_date is not None:
        yahoo_splits = [s for s in yahoo_splits if s.ex_date > min_date]
        store_splits = [(sx, sr) for sx, sr in store_splits if sx > min_date]
    used_store: set[int] = set()
    matched: list[date] = []
    yahoo_only: list[tuple[date, float]] = []
    for ys in sorted(yahoo_splits, key=lambda s: s.ex_date):
        hit = None
        for index, (sx, sratio) in enumerate(store_splits):
            if index in used_store:
                continue
            if abs((sx - ys.ex_date).days) <= day_tol and _ratio_close(ys.price_multiplier, sratio, ratio_tol):
                hit = index
                break
        if hit is None:
            yahoo_only.append((ys.ex_date, round(ys.price_multiplier, 6)))
        else:
            used_store.add(hit)
            matched.append(ys.ex_date)
    store_only = [(sx, round(sratio, 6)) for index, (sx, sratio) in enumerate(store_splits) if index not in used_store]
    return SplitReconciliation(matched=matched, yahoo_only=yahoo_only, store_only=store_only)


def _ratio_close(a: float, b: float, tol: float) -> bool:
    high = max(abs(a), abs(b))
    return high <= 0 or abs(a - b) / high <= tol


@dataclass(frozen=True)
class BasisClassification:
    """Per-row verdict for an existing bronze series against the Yahoo reference.

    `relabel`: existing close already equals the reconstructed raw — keep the value,
    only stamp price_basis='raw'. `rewrite`: existing close equals the split-adjusted
    close (Yahoo's `close`) — the row is adjusted and must be rewritten to raw.
    `mismatch`: neither — bad data or a reference disagreement; the symbol fails closed.
    A row with no split ahead has raw == adjusted, so it always classifies `relabel`.
    """

    relabel: list[date] = field(default_factory=list)
    rewrite: list[date] = field(default_factory=list)
    mismatch: list[tuple[date, float, float, float]] = field(default_factory=list)
    unmatched: list[date] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.mismatch


def classify_existing_basis(
    existing_rows: list[dict],
    yahoo_raw: dict[date, float],
    yahoo_adjusted: dict[date, float],
    *,
    tol: float = 0.02,
    abs_floor: float = 0.01,
) -> BasisClassification:
    """Decide, per existing bronze row, whether it is already raw, adjusted, or neither.

    ``abs_floor`` lets a sub-dollar penny stock match within a cent absolute — a 2%
    relative band is only a few tenths of a cent there, so ordinary vendor close
    disagreement would otherwise read as a mismatch on the whole illiquid tail.
    """
    result = BasisClassification()
    for row in existing_rows:
        day = row["trade_date"]
        day = day if isinstance(day, date) else date.fromisoformat(str(day)[:10])
        raw = yahoo_raw.get(day)
        adjusted = yahoo_adjusted.get(day)
        if raw is None or adjusted is None:
            result.unmatched.append(day)
            continue
        close = float(row["close"])
        if _close_match(close, raw, tol, abs_floor):
            result.relabel.append(day)
        elif _close_match(close, adjusted, tol, abs_floor):
            result.rewrite.append(day)
        else:
            result.mismatch.append((day, close, raw, adjusted))
    return result


def _close_match(a: float, b: float, tol: float, abs_floor: float) -> bool:
    return _ratio_close(a, b, tol) or abs(a - b) <= abs_floor


def last_split_ex_date(store_splits: list[tuple[date, float]]) -> date | None:
    """Most recent split ex-date; None when the symbol has no splits."""
    return max((ex for ex, _ in store_splits), default=None)


@dataclass(frozen=True)
class AnchorVerdict:
    """Verdict of comparing a reconstructed true-raw series against IB on the window
    AFTER the last split, where IB is definitionally raw. ``mismatches`` is a small
    sample of ``(date, corrected_close, ib_close)`` for the manifest."""

    verified: bool
    reason: str  # "verified" | "ib_insufficient_overlap" | "ib_mismatch"
    overlap: int
    window_start: date | None
    mismatches: list[tuple[date, float, float]] = field(default_factory=list)


def _as_day(value) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def anchor_window(corrected_rows: list[dict], *, last_split_ex: date | None, window_cap: int = 250) -> list[dict]:
    """The only rows IB may be compared against: those strictly after the last split
    ex-date (the whole series when there is no split), capped to the most recent
    ``window_cap``. Callers also use this to bound the IB *request* to the window —
    fetching full history for a 250-day comparison would chunk into decades of
    needless requests and hit IB pacing."""
    return sorted(
        (r for r in corrected_rows if last_split_ex is None or _as_day(r["trade_date"]) > last_split_ex),
        key=lambda r: _as_day(r["trade_date"]),
    )[-window_cap:]


def ib_anchor_verdict(
    corrected_rows: list[dict],
    ib_rows: list[dict],
    *,
    last_split_ex: date | None,
    tol: float = 0.02,
    abs_floor: float = 0.01,
    min_overlap: int = 5,
    window_cap: int = 250,
) -> AnchorVerdict:
    """Confirm the reconstruction against IB on the post-last-split window only.

    IB's basis is unreliable across split boundaries, so anything on or before the
    last split ex-date is ignored. The verdict is verified only when at least
    ``min_overlap`` dates overlap IB AND every overlapping close matches within
    tolerance.
    """
    window = anchor_window(corrected_rows, last_split_ex=last_split_ex, window_cap=window_cap)
    window_start = _as_day(window[0]["trade_date"]) if window else None
    ib_by_day = {_as_day(r["trade_date"]): float(r["close"]) for r in ib_rows}
    overlap_days = [_as_day(r["trade_date"]) for r in window if _as_day(r["trade_date"]) in ib_by_day]
    # `not overlap_days` is not redundant with the count test: min_overlap=0 would make
    # `0 < 0` false and certify a symbol IB returned nothing for. Zero overlap is never
    # a verification, whatever the caller passes.
    if not overlap_days or len(overlap_days) < min_overlap:
        return AnchorVerdict(False, "ib_insufficient_overlap", len(overlap_days), window_start)
    corrected_by_day = {_as_day(r["trade_date"]): float(r["close"]) for r in window}
    mismatches = [
        (day, corrected_by_day[day], ib_by_day[day])
        for day in overlap_days
        if not _close_match(corrected_by_day[day], ib_by_day[day], tol, abs_floor)
    ]
    if mismatches:
        return AnchorVerdict(False, "ib_mismatch", len(overlap_days), window_start, mismatches[:20])
    return AnchorVerdict(True, "verified", len(overlap_days), window_start)
