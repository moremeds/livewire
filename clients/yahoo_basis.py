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
) -> SplitReconciliation:
    """Match Yahoo splits to store splits by near-equal ex-date AND near-equal ratio.

    ``store_splits`` are ``(ex_date, split_to/split_from)`` — the same price multiplier
    Yahoo reports as ``numerator/denominator``. A provider can stamp the same split a day
    apart, hence ``day_tol``.
    """
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
