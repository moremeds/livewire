"""Post-adjustment continuity invariant for silver publication.

A correctly adjusted daily series has no large day-over-day close discontinuity —
adjustment is exactly what removes corporate-action jumps. A residual jump above
the threshold signals mixed-basis / double-adjustment (e.g. an already-adjusted
legacy row mislabeled ``price_basis='raw'`` that got divided by the split factor a
second time). Such a symbol must be quarantined, not published.
"""

from __future__ import annotations


class ContinuityBreak(ValueError):
    """A residual adjacent-day discontinuity in an adjusted series.

    Subclasses ``ValueError`` so existing ``except ValueError`` handlers (the
    rebuild staging loop) still catch it, while exposing structured ``.date`` /
    ``.ratio`` so callers (the audit) don't parse the message string.
    """

    def __init__(self, date: str, ratio: float, previous_date: str, threshold: float) -> None:
        self.date = date
        self.ratio = ratio
        self.previous_date = previous_date
        super().__init__(
            f"adjusted continuity break at {date}: {ratio:.1f}x jump from "
            f"{previous_date} (threshold {threshold:g}x) — mixed-basis suspected"
        )


def check_adjusted_continuity(
    rows: list[dict],
    *,
    threshold: float = 6.0,
    allowlist: frozenset[str] = frozenset(),
) -> None:
    """Raise ``ValueError`` on the first adjacent-day close ratio above ``threshold``.

    ``rows`` are adjusted daily rows ordered by ``trade_date`` (iso string), each
    with a positive float ``close``. ``allowlist`` holds iso dates exempt from the
    check (evidence-backed halts/relistings). Returns ``None`` when the series is
    continuous.
    """
    previous_close: float | None = None
    previous_date: str | None = None
    for row in rows:
        trade_date = str(row["trade_date"])
        close = float(row["close"])
        if close <= 0:
            raise ValueError(f"non-positive adjusted close at {trade_date}: {close}")
        if previous_close is not None and trade_date not in allowlist and previous_date not in allowlist:
            ratio = max(close / previous_close, previous_close / close)
            if ratio > threshold:
                raise ContinuityBreak(trade_date, ratio, previous_date, threshold)
        previous_close = close
        previous_date = trade_date
