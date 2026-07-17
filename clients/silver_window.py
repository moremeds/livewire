"""Resolve the silver-grade window of an adjusted daily series.

Silver grade means: every row's basis is correct, every split inside the window is
recorded, and the adjusted series has no discontinuity that is not an evidenced
real market move. Deep history is not a goal — a symbol may publish a short
window; what it publishes must be right.

The window is the longest SUFFIX with no bad discontinuity: start the day of the
last unexplained break and keep everything after it. Derived on every publish and
never persisted, so backfilled history extends the window by itself once the data
supports it, and a bad new bar cannot silently corrupt what is already published.

The suffix rule assumes the NEWER side of a break is the trustworthy one. That
holds for the 2021-06 seed artifact, whose corruption is entirely in the past. It
does NOT hold for a bad *new* bar, where the window would collapse onto the
garbage row — see `rebuild_silver`'s window-regression guard, which fails closed
rather than publishing such a window.
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


def resolve_window(
    rows: list[dict],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    allowlist: frozenset[str] = frozenset(),
    keep_dates: frozenset[str] = frozenset(),
) -> dict:
    """Return the silver-grade window of an adjusted series.

    ``allowlist`` is the operator override (evidence-backed halts/relistings);
    ``keep_dates`` are triage-confirmed real market moves. Both mean "this step is
    real, do not trim". The window starts at the LAST remaining break, so everything
    it serves is downstream of every known problem.
    """
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    if not ordered:
        return {"start": None, "trimmed_at": None, "reason": "empty series", "rows_dropped": 0}
    breaks = find_breaks(ordered, threshold=threshold, exempt=allowlist | keep_dates)
    if not breaks:
        return {
            "start": str(ordered[0]["trade_date"])[:10],
            "trimmed_at": None,
            "reason": "continuous",
            "rows_dropped": 0,
        }
    last = breaks[-1]
    if last["ratio"] is None:
        # The row AT this break is itself unusable (non-positive / non-finite close).
        # A ratio break is different: there the newer basis legitimately begins on the
        # break date, so the window includes it. Here the break date must be excluded,
        # or the window would publish the garbage row as its first bar.
        later = [d for d in (str(row["trade_date"])[:10] for row in ordered) if d > last["date"]]
        start = later[0] if later else None
    else:
        start = last["date"]
    if start is None:
        # Nothing survives — the unusable row is the newest one. Not publishable; the
        # caller's regression guard reports it rather than serving a truncated series.
        return {"start": None, "trimmed_at": last["date"], "reason": last["reason"], "rows_dropped": len(ordered)}
    dropped = sum(1 for row in ordered if str(row["trade_date"])[:10] < start)
    return {"start": start, "trimmed_at": last["date"], "reason": last["reason"], "rows_dropped": dropped}
