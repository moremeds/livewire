"""Intraday update — classify intraday bars for the equity universe.

Iterates over INTRADAY_TIMEFRAMES, classifies each (symbol, timeframe) into
one of 5 session states, and reports a summary. Actual fetching is a future
extension; this script currently does report-only classification.

Usage:
    python scripts/intraday_update.py                 # all symbols, all intraday timeframes
    python scripts/intraday_update.py --dry-run       # report only (same as default for now)
    python scripts/intraday_update.py --force         # run on non-trading day
    python scripts/intraday_update.py --timeframe 5m  # only one timeframe
"""

from __future__ import annotations

import argparse
import enum
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.intraday_bronze_client import (
    INTRADAY_TIMEFRAMES,
    IntradayBronzeClient,
)
from livewire_scripts.daily_update import is_trading_day, session_close_time

log = logging.getLogger("intraday_update")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_UTC = timezone.utc
_ET = ZoneInfo("America/New_York")
_WAREHOUSE_DIR = Path(os.environ.get("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"


class SessionState(enum.Enum):
    COMPLETE = "complete"        # all bars present, session closed
    IN_PROGRESS = "in_progress"  # session closed but stored data is partial
    LIVE = "live"                # session currently open, fetch up to last complete bar
    TAIL_GAP = "tail_gap"        # session not yet started/started today, no today data
    HISTORICAL = "historical"    # multiple trading days behind


def expected_last_bar_utc(trading_day: date, timeframe: str) -> datetime:
    """Return the UTC timestamp of the last bar of *trading_day* at *timeframe*.

    For 1m bars: close - 1min (e.g., 15:59 ET on a normal day).
    For 5m bars: close - 5min (e.g., 15:55 ET on a normal day).
    For 1h bars: close - 1h (e.g., 15:30 ET on a normal day).
    """
    if timeframe not in INTRADAY_TIMEFRAMES:
        raise ValueError(f"unsupported timeframe: {timeframe!r}")

    close_t = session_close_time(trading_day)
    close_dt = datetime(
        trading_day.year, trading_day.month, trading_day.day,
        close_t.hour, close_t.minute, tzinfo=_ET,
    )
    if timeframe == "1m":
        last_et = close_dt - timedelta(minutes=1)
    elif timeframe == "5m":
        last_et = close_dt - timedelta(minutes=5)
    else:  # "1h"
        last_et = close_dt - timedelta(minutes=30)
    return last_et.astimezone(_UTC)


def classify_session_state(
    latest_stored: datetime,
    now: datetime,
    timeframe: str,
) -> SessionState:
    """Classify a (symbol, timeframe) pair into one of 5 session states.

    Args:
        latest_stored: UTC timestamp of the most recent stored bar
        now: current UTC time
        timeframe: '1h' or '5m'
    """
    today_et = now.astimezone(_ET).date()

    # Walk back to find the most recent trading day (today if trading, else previous)
    if is_trading_day(today_et):
        target_day = today_et
    else:
        target_day = today_et - timedelta(days=1)
        while not is_trading_day(target_day):
            target_day -= timedelta(days=1)

    latest_stored_day = latest_stored.astimezone(_ET).date()

    # Determine session boundaries for target_day
    close_t = session_close_time(target_day)
    session_end = datetime(
        target_day.year, target_day.month, target_day.day,
        close_t.hour, close_t.minute, tzinfo=_ET,
    ).astimezone(_UTC)
    # NYSE regular session opens at 09:30 ET
    session_start = datetime(
        target_day.year, target_day.month, target_day.day,
        9, 30, tzinfo=_ET,
    ).astimezone(_UTC)

    if now >= session_end:
        # Session has closed — evaluate against today
        expected_close = expected_last_bar_utc(target_day, timeframe)
        if latest_stored_day < target_day - timedelta(days=1):
            return SessionState.HISTORICAL
        if latest_stored >= expected_close:
            return SessionState.COMPLETE
        return SessionState.IN_PROGRESS

    if now >= session_start:
        # Session is live (open right now)
        if latest_stored_day < target_day:
            return SessionState.TAIL_GAP
        return SessionState.LIVE

    # now < session_start: pre-market — evaluate against the previous completed session
    prev_day = target_day - timedelta(days=1)
    while not is_trading_day(prev_day):
        prev_day -= timedelta(days=1)

    expected_close = expected_last_bar_utc(prev_day, timeframe)
    if latest_stored_day < prev_day - timedelta(days=1):
        return SessionState.HISTORICAL
    if latest_stored >= expected_close:
        return SessionState.COMPLETE
    return SessionState.IN_PROGRESS


def main() -> None:
    parser = argparse.ArgumentParser(description="Intraday update (1h + 5m equity bars)")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no fetches")
    parser.add_argument("--force", action="store_true", help="Run on non-trading day")
    parser.add_argument(
        "--timeframe",
        choices=list(INTRADAY_TIMEFRAMES) + ["all"],
        default="all",
        help="Which timeframe to update (default: all)",
    )
    args = parser.parse_args()

    today = date.today()
    if not args.force and not is_trading_day(today):
        log.info("Not a trading day (%s), skipping. Use --force to override.", today)
        return

    timeframes = INTRADAY_TIMEFRAMES if args.timeframe == "all" else (args.timeframe,)
    bronze_dir = _DATA_LAKE / "bronze" / "asset_class=equity"

    summary: dict[str, dict[str, int]] = {}

    for tf in timeframes:
        log.info("=== Intraday update: %s ===", tf)
        with IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=tf) as client:
            existing = client.get_existing_symbols()
            latest_ts = client.get_latest_timestamps()

            now = datetime.now(_UTC)
            states = {SessionState.COMPLETE: 0, SessionState.IN_PROGRESS: 0,
                      SessionState.LIVE: 0, SessionState.TAIL_GAP: 0,
                      SessionState.HISTORICAL: 0}
            for sym in sorted(existing):
                latest = latest_ts.get(sym)
                if latest is None:
                    states[SessionState.HISTORICAL] += 1
                    continue
                # Ensure tz-aware
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=_UTC)
                state = classify_session_state(latest, now, tf)
                states[state] += 1
            summary[tf] = {s.value: c for s, c in states.items()}
            log.info("Session states for %s: %s", tf, summary[tf])

    if args.dry_run:
        log.info("Dry run — no fetches performed.")
        return

    log.info("Intraday update complete. Summary: %s", summary)


if __name__ == "__main__":
    main()
