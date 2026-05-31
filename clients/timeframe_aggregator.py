"""Lossless OHLCV timeframe aggregation.

Rolls up intraday bar rows from a finer timeframe to a coarser one.
Supported rollups: 1m->5m, 1m->30m, 1m->1h, 30m->1h.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

_MINUTES = {"1m": 1, "5m": 5, "30m": 30, "1h": 60}

VALID_ROLLUPS = {("1m", "5m"), ("1m", "30m"), ("1m", "1h"), ("30m", "1h")}


def _window_start(ts: datetime, target_minutes: int) -> datetime:
    total_minutes = ts.hour * 60 + ts.minute
    window_minute = (total_minutes // target_minutes) * target_minutes
    return ts.replace(
        hour=window_minute // 60,
        minute=window_minute % 60,
        second=0,
        microsecond=0,
    )


def aggregate_bars(
    bars: list[dict[str, Any]],
    *,
    source_tf: str,
    target_tf: str,
) -> list[dict[str, Any]]:
    """Aggregate bars from source_tf to target_tf via lossless OHLCV rollup.

    Input bars must be sorted by bar_timestamp ascending.
    """
    if (source_tf, target_tf) not in VALID_ROLLUPS:
        raise ValueError(
            f"unsupported rollup: {source_tf} -> {target_tf}. "
            f"Valid: {sorted(VALID_ROLLUPS)}"
        )
    if not bars:
        return []

    target_minutes = _MINUTES[target_tf]
    result: list[dict[str, Any]] = []
    window_bars: list[dict[str, Any]] = []
    current_window: datetime | None = None

    for bar in bars:
        ws = _window_start(bar["bar_timestamp"], target_minutes)
        if current_window is not None and ws != current_window:
            result.append(_merge_window(window_bars, current_window))
            window_bars = []
        current_window = ws
        window_bars.append(bar)

    if window_bars and current_window is not None:
        result.append(_merge_window(window_bars, current_window))

    return result


def _merge_window(bars: list[dict[str, Any]], window_ts: datetime) -> dict[str, Any]:
    return {
        "bar_timestamp": window_ts,
        "symbol_id": bars[0]["symbol_id"],
        "open": bars[0]["open"],
        "high": max(b["high"] for b in bars),
        "low": min(b["low"] for b in bars),
        "close": bars[-1]["close"],
        "volume": sum(b["volume"] for b in bars),
    }
