"""Expected coverage = presets x trading calendar x timeframe.

The denominator must never be derived from what is already on disk: a symbol
that never landed has to stay visible. See section 4 of
docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from clients.ingestion_common import load_preset
from clients.trading_calendar import trading_dates_in_range

# run-daily-job's StartCalendarInterval, in UTC. The lane that fills session S
# starts here on S+1.
JOB_START_UTC = time(6, 0)
# The default of MDW_DAILY_JOB_DEADLINE_SECONDS (4h). Read at call time, not at
# import, so a test and a scheduled run can disagree.
DEFAULT_JOB_DEADLINE_SECONDS = 14400


def session_due_at(session: date, lag_days: int = 1) -> datetime:
    """The instant session *session* is due on disk.

    ponytail: reuses MDW_DAILY_JOB_DEADLINE_SECONDS rather than introducing a
    second constant to keep in step. "Closed" is not "delivered" -- the job that
    fills S starts 06:00 UTC on S+1, and a denominator that expects S the moment
    it closes manufactures one tail gap per symbol in the universe.

    lag_days is the number of days after the session that the filling job
    starts. It is 1 for every lane run by run-daily-job, and 2 for rates: FRED
    publishes a session behind, which spec section 8.1 already records as
    T+2. A uniform T+1 there manufactures one phantom gap per series per day.
    """
    seconds = int(os.environ.get("MDW_DAILY_JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS))
    start = datetime.combine(session + timedelta(days=lag_days), JOB_START_UTC, tzinfo=UTC)
    return start + timedelta(seconds=seconds)


# Spec section 8.1. Anything not listed is T+1.
DUE_LAG_DAYS = {"rates": 2}


@dataclass(frozen=True)
class ExpectedSeries:
    symbol: str
    asset_class: str
    timeframe: str
    sessions: tuple[date, ...]


def _contract_expiry(symbol: str) -> date | None:
    """Month-start expiry for a composite futures ticker (ES_202506), else None."""
    _root, _sep, expiry = symbol.partition("_")
    if len(expiry) != 6 or not expiry.isdigit():
        return None
    return date(int(expiry[:4]), int(expiry[4:]), 1)


def build_denominator(
    preset_paths: list[Path],
    asset_class: str,
    timeframe: str,
    start: date,
    end: date,
    as_of: datetime,
    lag_days: int = 1,
) -> list[ExpectedSeries]:
    if as_of.tzinfo is None:
        raise ValueError("as_of must be tz-aware; a naive local datetime silently shifts the due rule")
    # A session is expected only once the job that fills it was due to finish.
    # Guarding on `d < as_of.date()` instead would reintroduce the 497 phantoms:
    # closing is not delivery.
    sessions = tuple(d for d in trading_dates_in_range(start, end) if session_due_at(d, lag_days) <= as_of)
    # Expiry is judged against the WINDOW, not against `as_of`: a contract that
    # was live during the scanned range belongs in that range's denominator even
    # if it has expired by today. Judging against `as_of` made the same window
    # yield a different denominator depending on when you scanned it.
    window_month = end.replace(day=1)
    out: list[ExpectedSeries] = []
    seen: set[str] = set()
    for preset_path in preset_paths:
        _name, tickers, _exchange_map = load_preset(preset_path)
        for ticker in tickers:
            # Presets overlap (sp500 n ndx100 = 87 symbols). Without this the
            # symbol is scanned twice and every gap it has lands in the Tier A
            # manifest twice — two repair instructions for one parquet path.
            if ticker in seen:
                continue
            seen.add(ticker)
            expiry = _contract_expiry(ticker)
            if expiry is not None and expiry < window_month:
                continue
            out.append(ExpectedSeries(ticker, asset_class, timeframe, sessions))
    return out
