"""Full historical intraday backfill orchestrator (1m, 1h, and 5m).

For each ticker:
1. Compute IB request chunks via compute_intraday_chunks
2. Fetch each chunk via IBClient.get_historical_data
3. Convert IB bars → row dicts with tz-aware UTC bar_timestamp
4. Validate via validate_intraday_bar (rejection logged, not fatal)
5. Merge into IntradayBronzeClient
6. On success, mark ticker as completed in the per-timeframe cursor

Per spec § 11. The first script in this repo that actually pulls intraday
bars from IB — daily_update + intraday_update only classify, and
fetch_ib_historical.py is daily-only.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console

from clients.intraday_bronze_client import (
    INTRADAY_IB_BAR_SIZE,
    INTRADAY_TIMEFRAMES,
    IntradayBronzeClient,
)
from clients.massive_client import MassiveClient
from clients.quality_detector import _normalize_bars_for_detection, detect_all
from clients.quality_flags import alert_on_flag, append_audit, write_sidecar
from livewire_scripts.daily_update import (
    _make_contract,
    is_trading_day,
    session_close_time,
    validate_intraday_bar,
)
from livewire_scripts.fetch_ib_historical import compute_intraday_chunks, load_preset

log = logging.getLogger("backfill_intraday")
console = Console()

_WAREHOUSE_DIR = Path(
    os.environ.get("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))
)
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"
_LOG_DIR = _WAREHOUSE_DIR / "logs"
_CURSOR_DIR = _WAREHOUSE_DIR / "cursors"

# IB error codes that mean "skip ticker, do not retry"
_NO_DATA_ERRORS = {162, 200}

_DEFAULT_YEARS = {"1m": 5, "1h": 5, "5m": 5, "30m": 5}
_ET = ZoneInfo("America/New_York")
_UTC = timezone.utc


@dataclass
class TickerOutcome:
    ticker: str
    chunks_fetched: int = 0
    bars_inserted: int = 0
    rejected: int = 0
    skipped_reason: str | None = None
    errors: list[str] = field(default_factory=list)


def _cursor_path(timeframe: str, name: str) -> Path:
    return _CURSOR_DIR / f"cursor_intraday_{timeframe}_{name}.json"


def load_cursor(timeframe: str, name: str) -> set[str]:
    """Return the set of completed tickers for this (timeframe, name) cursor."""
    path = _cursor_path(timeframe, name)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return set()
    return set(data.get("completed", []))


def save_cursor(timeframe: str, name: str, completed: set[str]) -> None:
    path = _cursor_path(timeframe, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timeframe": timeframe,
        "completed": sorted(completed),
        "updated_at": datetime.now(_UTC).isoformat(),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)


class _BarRow:
    """Adapter that exposes a row dict as an object with `bar_timestamp`.

    `validate_intraday_bar` uses ``getattr(bar, 'bar_timestamp')``, so we
    wrap each row dict in a thin proxy before validation.
    """

    __slots__ = ("bar_timestamp",)

    def __init__(self, ts: datetime) -> None:
        self.bar_timestamp = ts


def ib_bar_to_row(bar: Any, symbol_id: int) -> dict[str, Any]:
    """Convert one IB BarData (intraday, formatDate=1) to a bronze row dict.

    IB returns naive datetime in Gateway local time with formatDate=1; we
    attach America/New_York and convert to UTC. Caller validates afterwards.
    """
    raw = bar.date
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            ts_utc = raw.replace(tzinfo=_ET).astimezone(_UTC)
        else:
            ts_utc = raw.astimezone(_UTC)
    else:
        # Date-only — promote to midnight ET (intraday bars should never hit
        # this path in practice; defensive)
        ts_utc = datetime(raw.year, raw.month, raw.day, tzinfo=_ET).astimezone(_UTC)
    return {
        "bar_timestamp": ts_utc,
        "symbol_id": symbol_id,
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": int(bar.volume),
    }


def massive_intraday_bar_to_row(bar: Any, symbol_id: int) -> dict[str, Any]:
    """Convert a normalized Massive intraday bar to a bronze row dict."""
    ts = bar.bar_timestamp
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        raise ValueError("Massive intraday bar_timestamp must be tz-aware")
    return {
        "bar_timestamp": ts.astimezone(_UTC),
        "symbol_id": symbol_id,
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": int(bar.volume),
    }


def _is_regular_trading_timestamp(ts: datetime) -> bool:
    """Return True when a UTC timestamp falls inside the U.S. equity RTH window."""
    if (
        ts.tzinfo is None
        or ts.tzinfo.utcoffset(ts) is None
        or ts.utcoffset() != timedelta(0)
    ):
        return False
    et = ts.astimezone(_ET)
    if not is_trading_day(et.date()):
        return False
    rth_start = et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_t = session_close_time(et.date())
    rth_end = et.replace(
        hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0
    )
    return rth_start <= et < rth_end


def _run_quality_detection(
    *,
    ticker: str,
    timeframe: str,
    bars: list,
    parquet_path: Path,
    outcome: TickerOutcome,
    asset_class: str = "equity",
    source: str = "ib",
) -> None:
    """Run intraday quality detection and emit flags without blocking publish."""
    if not bars:
        return
    errors = [{"code": 0, "count": 1, "message": e} for e in (outcome.errors or [])]
    metadata = {
        "asset_class": asset_class,
        "ticker": ticker,
        "timeframe": timeframe,
        "source": source,
        "bars_received": len(bars),
        "errors_during_fetch": errors,
        "expected_start": None,
        "ib_head_timestamp": None,
    }
    normalized = _normalize_bars_for_detection(bars)
    try:
        flags = detect_all(bars=normalized, metadata=metadata, trading_calendar=None)
    except Exception:  # pragma: no cover - detect_all wraps individual detectors
        return
    if not flags:
        return
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    write_sidecar(parquet_path, flags, metadata)
    for flag in flags:
        append_audit(
            flag,
            source=source,
            ticker=ticker,
            timeframe=timeframe,
            parquet_path=parquet_path,
        )
        if _intraday_backfill_alerts_enabled():
            alert_on_flag(flag, source=source, ticker=ticker)


def _intraday_backfill_alerts_enabled() -> bool:
    """Return True when bulk intraday backfills should send per-flag emails."""
    return os.getenv("MDW_INTRADAY_BACKFILL_ALERTS", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def should_skip_existing(bronze: IntradayBronzeClient, ticker: str, years: int) -> bool:
    """Return True if the bronze parquet already covers ``today - years``."""
    rows = bronze.read_symbol_rows(ticker)
    if not rows:
        return False
    earliest = min(row["bar_timestamp"] for row in rows)
    threshold = datetime.now(_UTC) - timedelta(days=365 * years)
    return earliest <= threshold


def backfill_ticker(
    ticker: str,
    timeframe: str,
    years: int,
    ib: Any,
    bronze: IntradayBronzeClient,
    asset_class: str = "equity",
    lookback_days: int | None = None,
) -> TickerOutcome:
    """Fetch and merge all chunks for one ticker. Returns the outcome."""
    outcome = TickerOutcome(ticker=ticker)
    contract = _make_contract(ticker, asset_class)
    bar_size = INTRADAY_IB_BAR_SIZE[timeframe]
    chunks = (
        compute_intraday_chunks_for_days(timeframe, lookback_days)
        if lookback_days is not None
        else compute_intraday_chunks(timeframe, years)
    )
    symbol_id = bronze.get_symbol_id(ticker)

    all_rows: list[dict[str, Any]] = []
    for duration, end_dt in chunks:
        try:
            bars = ib.get_historical_data(
                contract,
                duration=duration,
                bar_size=bar_size,
                what_to_show="TRADES",
                end_date=end_dt,
            )
        except Exception as exc:
            code = getattr(exc, "code", None) or getattr(exc, "errorCode", None)
            if code in _NO_DATA_ERRORS:
                outcome.skipped_reason = f"IB error {code}"
                return outcome
            outcome.errors.append(f"{end_dt}: {exc}")
            continue

        outcome.chunks_fetched += 1
        if not bars:
            continue

        for bar in bars:
            row = ib_bar_to_row(bar, symbol_id)
            issues = validate_intraday_bar(
                _BarRow(row["bar_timestamp"]),
                ticker,
                timeframe,
                require_rth=asset_class != "volatility",
            )
            if issues:
                outcome.rejected += 1
                for issue in issues:
                    log.debug("rejected %s", issue)
                continue
            all_rows.append(row)

    if all_rows:
        parquet_path = bronze.bronze_dir / f"symbol={ticker}" / f"{timeframe}.parquet"
        _run_quality_detection(
            ticker=ticker,
            timeframe=timeframe,
            bars=all_rows,
            parquet_path=parquet_path,
            outcome=outcome,
            asset_class=asset_class,
        )
        outcome.bars_inserted = bronze.merge_ticker_rows(
            ticker,
            all_rows,
            overwrite_existing=lookback_days is None,
        )
    return outcome


def compute_intraday_date_windows(
    years: int,
    *,
    lookback_days: int | None = None,
    window_days: int = 30,
) -> list[tuple[date, date]]:
    """Return inclusive calendar date windows for Massive intraday aggregate pulls."""
    end_day = datetime.now(_UTC).date()
    days_back = lookback_days if lookback_days is not None else 365 * years
    start_day = end_day - timedelta(days=days_back)
    windows: list[tuple[date, date]] = []
    cursor = start_day
    while cursor <= end_day:
        window_end = min(cursor + timedelta(days=window_days - 1), end_day)
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def backfill_ticker_massive(
    ticker: str,
    timeframe: str,
    years: int,
    massive: MassiveClient,
    bronze: IntradayBronzeClient,
    lookback_days: int | None = None,
) -> TickerOutcome:
    """Fetch and merge Massive intraday aggregates for one equity ticker."""
    outcome = TickerOutcome(ticker=ticker)
    symbol_id = bronze.get_symbol_id(ticker)
    all_rows: list[dict[str, Any]] = []

    for start, end in compute_intraday_date_windows(years, lookback_days=lookback_days):
        try:
            bars = massive.get_intraday_bars(ticker, start, end, timeframe=timeframe)
        except Exception as exc:
            outcome.errors.append(f"{start}:{end}: {exc}")
            continue

        outcome.chunks_fetched += 1
        for bar in bars:
            row = massive_intraday_bar_to_row(bar, symbol_id)
            if not _is_regular_trading_timestamp(row["bar_timestamp"]):
                continue
            issues = validate_intraday_bar(
                _BarRow(row["bar_timestamp"]), ticker, timeframe
            )
            if issues:
                outcome.rejected += 1
                for issue in issues:
                    log.debug("rejected %s", issue)
                continue
            all_rows.append(row)

    if all_rows:
        parquet_path = bronze.bronze_dir / f"symbol={ticker}" / f"{timeframe}.parquet"
        _run_quality_detection(
            ticker=ticker,
            timeframe=timeframe,
            bars=all_rows,
            parquet_path=parquet_path,
            outcome=outcome,
            asset_class="equity",
            source="massive",
        )
        outcome.bars_inserted = bronze.merge_ticker_rows(
            ticker,
            all_rows,
            overwrite_existing=lookback_days is None,
        )
    return outcome


def compute_intraday_chunks_for_days(
    timeframe: str, days_back: int
) -> list[tuple[str, str]]:
    """Generate IB chunks for a recent-day intraday catch-up."""
    if days_back < 1:
        raise ValueError("days_back must be >= 1")
    step_days = {"1m": 1, "5m": 7, "30m": 30, "1h": 30}
    if timeframe not in step_days:
        raise ValueError(f"unsupported intraday timeframe: {timeframe!r}")
    chunks = compute_intraday_chunks(timeframe, 1)
    needed = (days_back + step_days[timeframe] - 1) // step_days[timeframe]
    return chunks[:needed]


def plan_chunks(
    timeframe: str,
    years: int,
    tickers: Sequence[str],
    *,
    source: str = "ib",
    lookback_days: int | None = None,
) -> list[str]:
    """Return human-readable lines describing the planned IB requests."""
    if source == "massive":
        windows = compute_intraday_date_windows(years, lookback_days=lookback_days)
        return [f"{ticker}: {len(windows)} Massive date windows" for ticker in tickers]
    chunks = (
        compute_intraday_chunks_for_days(timeframe, lookback_days)
        if lookback_days is not None
        else compute_intraday_chunks(timeframe, years)
    )
    return [
        f"{ticker}: {len(chunks)} chunks of {INTRADAY_IB_BAR_SIZE[timeframe]}"
        for ticker in tickers
    ]


def _resolve_tickers(args: argparse.Namespace) -> tuple[str, list[str]]:
    if args.preset:
        cursor_name, tickers, _ = load_preset(args.preset)
        return cursor_name, tickers
    if args.tickers:
        return "custom", list(args.tickers)
    raise SystemExit("Must specify --tickers or --preset")


def main() -> None:
    parser = argparse.ArgumentParser(description="Full historical intraday backfill")
    parser.add_argument(
        "--timeframe",
        choices=list(INTRADAY_TIMEFRAMES),
        required=True,
        help="Intraday timeframe (1m, 1h, or 5m)",
    )
    parser.add_argument(
        "--source",
        choices=["ib", "massive"],
        default="ib",
        help="Intraday source (default: ib; Massive supports equity only)",
    )
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures", "cmdty", "fx"],
        default="equity",
        help="Asset class to fetch (default: equity). Non-equity intraday uses IB.",
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--tickers", nargs="+", help="Explicit ticker list")
    grp.add_argument("--preset", type=str, help="Preset JSON path")
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Years of history (default: 2 for 1h, 1 for 5m)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Recent calendar days to fetch instead of a full-year backfill",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print plan only")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tickers whose bronze covers the requested depth",
    )
    parser.add_argument(
        "--existing-only",
        action="store_true",
        help="Only process tickers that already have parquet for this timeframe",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=None,
        help="Cap the number of tickers processed this run",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=1,
        help="Maximum concurrent Massive ticker fetches (default: 1)",
    )
    parser.add_argument("--host", default=os.getenv("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.getenv("MDW_IB_PORT", "4001"))
    )
    args = parser.parse_args()

    years = args.years if args.years is not None else _DEFAULT_YEARS[args.timeframe]
    if args.days is not None and args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.max_concurrent < 1:
        raise SystemExit("--max-concurrent must be >= 1")
    if args.source == "massive" and args.asset_class != "equity":
        raise SystemExit("--source massive is only supported for equity intraday")

    cursor_name, tickers = _resolve_tickers(args)

    # Cursor is only meaningful for preset runs (resumable bulk backfills).
    # When --tickers is passed explicitly, the operator knows what they want;
    # always refetch and skip cursor bookkeeping.
    use_cursor = bool(args.preset)
    completed: set[str] = (
        load_cursor(args.timeframe, cursor_name) if use_cursor else set()
    )

    pending = [t for t in tickers if t not in completed]
    if args.max_tickers is not None:
        pending = pending[: args.max_tickers]

    bronze_dir = _DATA_LAKE / "bronze" / f"asset_class={args.asset_class}"
    bronze = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=args.timeframe)
    if args.existing_only:
        existing_symbols = bronze.get_existing_symbols()
        pending = [ticker for ticker in pending if ticker in existing_symbols]

    console.print(
        f"\n[bold]Backfill intraday[/bold]  tf={args.timeframe}  years={years}  "
        f"days={args.days if args.days is not None else 'full'}  "
        f"source={args.source}  asset_class={args.asset_class}  "
        f"tickers={len(tickers)}  pending={len(pending)}  "
        f"max_concurrent={args.max_concurrent if args.source == 'massive' else 1}  "
        f"cursor={cursor_name if use_cursor else 'disabled'}"
    )

    if args.dry_run:
        for line in plan_chunks(
            args.timeframe,
            years,
            pending,
            source=args.source,
            lookback_days=args.days,
        ):
            console.print(f"  {line}")
        return

    if not pending:
        console.print("[green]All tickers already completed for this cursor.[/green]")
        return

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = (
        _LOG_DIR / f"backfill_intraday_{args.timeframe}_{date.today():%Y-%m-%d}.log"
    )
    log_handler = logging.FileHandler(log_path)
    log_handler.setLevel(logging.INFO)
    log.addHandler(log_handler)
    log.setLevel(logging.INFO)

    t0 = time.monotonic()
    total_inserted = 0
    total_rejected = 0
    skipped: list[str] = []
    failed: list[str] = []

    def record_outcome(ticker: str, outcome: TickerOutcome) -> None:
        nonlocal total_inserted, total_rejected
        if outcome.skipped_reason:
            console.print(f"  [yellow]{ticker}: {outcome.skipped_reason}[/yellow]")
            skipped.append(ticker)
            if use_cursor:
                completed.add(ticker)  # don't retry "no data" tickers
                save_cursor(args.timeframe, cursor_name, completed)
            return

        total_inserted += outcome.bars_inserted
        total_rejected += outcome.rejected
        log.info(
            "%s: chunks=%d inserted=%d rejected=%d errors=%d",
            ticker,
            outcome.chunks_fetched,
            outcome.bars_inserted,
            outcome.rejected,
            len(outcome.errors),
        )
        if outcome.errors:
            failed.append(ticker)
            console.print(
                f"  [red]{ticker}[/red]: provider errors={len(outcome.errors)}; "
                "not marking cursor complete"
            )
            return
        console.print(
            f"  [green]{ticker}[/green]: +{outcome.bars_inserted} bars "
            f"({outcome.rejected} rejected)"
        )
        if use_cursor:
            completed.add(ticker)
            save_cursor(args.timeframe, cursor_name, completed)

    if args.source == "massive" and args.max_concurrent > 1:

        def fetch_massive(ticker: str) -> tuple[str, TickerOutcome]:
            with MassiveClient() as provider:
                outcome = backfill_ticker_massive(
                    ticker,
                    args.timeframe,
                    years,
                    provider,
                    bronze,
                    lookback_days=args.days,
                )
            return ticker, outcome

        with ThreadPoolExecutor(max_workers=args.max_concurrent) as executor:
            futures = {
                executor.submit(fetch_massive, ticker): ticker for ticker in pending
            }
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    ticker, outcome = future.result()
                except Exception as exc:  # noqa: BLE001
                    outcome = TickerOutcome(ticker=ticker, errors=[str(exc)])
                record_outcome(ticker, outcome)
    elif args.source == "massive":
        with MassiveClient() as provider:
            for ticker in pending:
                outcome = backfill_ticker_massive(
                    ticker,
                    args.timeframe,
                    years,
                    provider,
                    bronze,
                    lookback_days=args.days,
                )
                record_outcome(ticker, outcome)
    else:
        # Lazy IB import keeps Massive-only and tests free of the dependency until needed.
        from clients.ib_client import IBClient  # noqa: PLC0415

        provider_context = IBClient()
        with provider_context as provider:
            provider.connect(host=args.host, port=args.port)
            for ticker in pending:
                if args.skip_existing and should_skip_existing(bronze, ticker, years):
                    console.print(
                        f"  [dim]{ticker}: bronze already covers {years}y — skip[/dim]"
                    )
                    if use_cursor:
                        completed.add(ticker)
                        save_cursor(args.timeframe, cursor_name, completed)
                    continue

                outcome = backfill_ticker(
                    ticker,
                    args.timeframe,
                    years,
                    provider,
                    bronze,
                    asset_class=args.asset_class,
                    lookback_days=args.days,
                )
                record_outcome(ticker, outcome)

    elapsed = time.monotonic() - t0
    console.print(
        f"\n[bold]Done.[/bold] inserted={total_inserted} rejected={total_rejected} "
        f"skipped={len(skipped)} failed={len(failed)} elapsed={elapsed:.1f}s"
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
