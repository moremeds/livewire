"""Resolve the silver-grade window of an adjusted daily series.

Silver grade means: every row's basis is correct, every split inside the window is
recorded, and the adjusted series has no discontinuity that is not an evidenced
real market move. Deep history is not a goal — a symbol may publish a short
window; what it publishes must be right.
"""

from __future__ import annotations

import math

DEFAULT_THRESHOLD = 6.0


def find_breaks(
    rows: list[dict],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    exempt: frozenset[str] = frozenset(),
) -> list[dict]:
    """Every bad discontinuity in an adjusted series, in date order.

    Unlike :func:`clients.silver_continuity.check_adjusted_continuity`, which raises
    on the FIRST break, this enumerates all of them — the audit needs the full list
    so every break gets triaged, and the resolver needs the last one.
    """
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    breaks: list[dict] = []
    previous_close: float | None = None
    for row in ordered:
        trade_date = str(row["trade_date"])[:10]
        try:
            close = float(row["close"])
        except (TypeError, ValueError):
            close = float("nan")
        if not math.isfinite(close) or close <= 0:
            breaks.append(
                {
                    "date": trade_date,
                    "ratio": None,
                    "reason": f"non-positive or non-finite adjusted close at {trade_date}",
                }
            )
            # A row we cannot trust must not be compared against — it would
            # manufacture a second, spurious break on the following day.
            previous_close = None
            continue
        if previous_close is not None and trade_date not in exempt:
            ratio = max(close / previous_close, previous_close / close)
            if ratio > threshold:
                breaks.append(
                    {
                        "date": trade_date,
                        "ratio": ratio,
                        "reason": f"unexplained {ratio:.2f}x adjusted step at {trade_date} (threshold {threshold:g}x)",
                    }
                )
        previous_close = close
    return breaks
