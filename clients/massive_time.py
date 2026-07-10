"""Shared calendar conversion for Massive aggregate timestamps."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


def massive_timestamp_to_trade_date(timestamp: datetime) -> date:
    """Map a timezone-aware Massive event timestamp to its U.S. trade date."""
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Massive timestamp must be timezone-aware")
    return timestamp.astimezone(_ET).date()
