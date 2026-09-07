"""Datetime coercions shared across the lake.

Three two-line functions with seven copies between them. They are here rather
than in one of their callers because the alternative homes would each create a
dependency in the wrong direction (an index store on a security master, an
adjustment engine on a price-basis module).
"""

from __future__ import annotations

from datetime import UTC, date, datetime


def utc_iso() -> str:
    """Second-resolution UTC stamp, `Z`-suffixed. The ledger's time format."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def coerce_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def require_aware(value: datetime, label: str) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
