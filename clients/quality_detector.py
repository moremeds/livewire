"""Pure quality-flag detection. No I/O.

See: docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional

_RANGE_SHORTFALL_WARNING_DAYS = 5
_RANGE_SHORTFALL_CRITICAL_DAYS = 30


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class QualityFlag:
    category: str
    severity: str
    detail: dict[str, Any] = field(default_factory=dict)
    ts: str = field(default_factory=_utc_iso)


def detect_range_shortfall(
    expected_start: date,
    actual_start: date,
    ib_head_timestamp: Optional[date],
) -> Optional[QualityFlag]:
    """Flag when actual_start is materially later than expected_start.

    If ib_head_timestamp equals actual_start, treat as "IB has no older data" (clean).
    Otherwise severity follows the shortfall-size thresholds.
    """
    if actual_start <= expected_start:
        return None
    shortfall_days = (actual_start - expected_start).days
    if ib_head_timestamp is not None and ib_head_timestamp >= actual_start:
        return None
    if shortfall_days > _RANGE_SHORTFALL_CRITICAL_DAYS:
        severity = "critical"
    elif shortfall_days > _RANGE_SHORTFALL_WARNING_DAYS:
        severity = "warning"
    else:
        return None
    return QualityFlag(
        category="range_shortfall",
        severity=severity,
        detail={
            "expected_start": expected_start.isoformat(),
            "actual_start": actual_start.isoformat(),
            "shortfall_days": shortfall_days,
            "ib_head_timestamp": ib_head_timestamp.isoformat() if ib_head_timestamp else None,
        },
    )
