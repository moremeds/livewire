#!/usr/bin/env python3
"""Daily market data update — append latest bars for tickers in bronze parquet.

Lightweight alternative to fetch_ib_historical.py for daily scheduled runs.
Discovers tickers from bronze parquet, detects gaps, fetches only missing bars,
uses a narrow public fallback chain for unresolved trading dates after IB, and
atomically rewrites per-ticker snapshots.

Requires IB Gateway or TWS running on localhost.

Usage:
    source ~/market-warehouse/.venv/bin/activate

    # Normal daily run (discovers all tickers from bronze parquet):
    python scripts/daily_update.py

    # Dry-run — show gap report without fetching:
    python scripts/daily_update.py --dry-run

    # Force run on a non-trading day (e.g., manual catch-up):
    python scripts/daily_update.py --force

    # Limit to a specific preset:
    python scripts/daily_update.py --preset presets/sp500.json

    # Custom IB port and concurrency:
    python scripts/daily_update.py --port 7497 --max-concurrent 4
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from contextlib import ExitStack
from datetime import date, datetime, time, timedelta
from pathlib import Path

from ib_async import Contract, Forex, Future, Index, Stock
from rich.console import Console
from rich.logging import RichHandler

# Add project root to path so clients module is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.bronze_client import BronzeClient
from clients.daily_bar_fallback import DailyBarFallbackClient
from clients.ib_client import IBClient, IBError
from clients.ingestion_common import (
    ROOT_EXCHANGE_MAP,
    SUPPORTED_IB_FX_PAIRS,
    bars_to_futures_rows,
    bars_to_midpoint_rows,
    bars_to_rows,
    load_preset,
)
from clients.ingestion_common import (
    is_inverted_fx_pair as _is_inverted_fx_pair,
)
from clients.ingestion_common import (
    make_contract as _make_contract,
)
from clients.ingestion_common import (
    resolve_fx_pair as _resolve_fx_pair,
)
from clients.massive_client import MassiveAPIError, MassiveAuthError, MassiveClient
from clients.quality_detector import _normalize_bars_for_detection, detect_all
from clients.quality_flags import alert_on_flag, append_audit, write_sidecar
from clients.trading_calendar import (
    _easter,
    get_nyse_holidays,
    is_trading_day,
    previous_trading_day,
    trading_days_between,
)

_DEFAULT_STORAGE_CLIENT = BronzeClient
StorageClient = BronzeClient


def _storage_client():
    """Return the live storage client, allowing tests to patch the seam."""
    if BronzeClient is not _DEFAULT_STORAGE_CLIENT:
        return BronzeClient
    if StorageClient is not _DEFAULT_STORAGE_CLIENT:
        return StorageClient
    return BronzeClient


_DEFAULT_FALLBACK_CLIENT = DailyBarFallbackClient
FallbackClient = DailyBarFallbackClient


def _fallback_client():
    """Return the live fallback client, allowing tests to patch either name."""
    if DailyBarFallbackClient is not _DEFAULT_FALLBACK_CLIENT:
        return DailyBarFallbackClient()
    if FallbackClient is not _DEFAULT_FALLBACK_CLIENT:
        return FallbackClient()
    return DailyBarFallbackClient()


def _optional_massive_client():
    """Return a Massive client when configured, otherwise disable Massive recovery."""
    try:
        return MassiveClient()
    except MassiveAuthError:
        return None


# ── Config ─────────────────────────────────────────────────────────────

DATA_LAKE = Path(
    os.getenv("MDW_DATA_LAKE", str(Path.home() / "market-warehouse" / "data-lake"))
)
BRONZE_DIR = DATA_LAKE / "bronze" / "asset_class=equity"

console = Console()


# ROOT_EXCHANGE_MAP, SUPPORTED_IB_FX_PAIRS, _resolve_fx_pair,
# _is_inverted_fx_pair, _make_contract → imported from clients.ingestion_common


# ── Trading calendar ───────────────────────────────────────────────────


def get_early_close_days(year: int) -> dict[date, time]:
    """Return ``{trading_date: close_time_ET}`` for half-day trading days.

    Standard early-close days (NYSE close at 13:00 ET):
      - Day after Thanksgiving (4th Thursday of November + 1 day)
      - Christmas Eve (Dec 24, if a trading day)
      - July 3 (if Independence Day on a weekday other than Mon/Sun)
    """
    early: dict[date, time] = {}

    # Day after Thanksgiving — 4th Thursday of November + 1 day
    nov1 = date(year, 11, 1)
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    thanksgiving = first_thu + timedelta(weeks=3)
    day_after = thanksgiving + timedelta(days=1)
    if is_trading_day(day_after):
        early[day_after] = time(13, 0)

    # Christmas Eve — Dec 24, only if trading day
    christmas_eve = date(year, 12, 24)
    if is_trading_day(christmas_eve):
        early[christmas_eve] = time(13, 0)

    # July 3 — early close when Independence Day (Jul 4) is Tue/Wed/Thu/Fri
    july_3 = date(year, 7, 3)
    july_4 = date(year, 7, 4)
    if is_trading_day(july_3) and july_4.weekday() in (1, 2, 3, 4):
        early[july_3] = time(13, 0)

    return early


def session_close_time(d: date) -> time:
    """Return the ET close time for trading day *d*.

    16:00 normally; 13:00 on early-close days.
    """
    early = get_early_close_days(d.year)
    return early.get(d, time(16, 0))


# ── Gap detection ──────────────────────────────────────────────────────


def classify_gaps(
    latest_dates: dict[str, str], target_date: date
) -> tuple[list[str], list[str], list[str]]:
    """Classify tickers into up_to_date, single_day_gap, multi_day_gap.

    Returns (up_to_date, single_day_gap, multi_day_gap).
    """
    up_to_date: list[str] = []
    single_day_gap: list[str] = []
    multi_day_gap: list[str] = []

    for symbol, latest_str in latest_dates.items():
        latest = date.fromisoformat(latest_str)
        gap = trading_days_between(latest, target_date)
        if gap == 0:
            up_to_date.append(symbol)
        elif gap == 1:
            single_day_gap.append(symbol)
        else:
            multi_day_gap.append(symbol)

    return (up_to_date, single_day_gap, multi_day_gap)


def compute_ib_duration(latest_date: date, target_date: date) -> str:
    """Compute the IB duration string to fetch bars from *latest_date* to *target_date*.

    Returns e.g. "5 D", "1 M", "3 M", "1 Y".
    """
    cal_days = (target_date - latest_date).days
    if cal_days <= 0:
        return "1 D"
    # Add a small buffer for safety
    cal_days += 2
    if cal_days <= 180:
        return f"{cal_days} D"
    elif cal_days <= 365:
        return "1 Y"
    else:
        return "2 Y"


def get_missing_trading_dates(
    latest_date: date,
    target_date: date,
    bars: list,
) -> list[date]:
    """Return unresolved trading dates in ``(latest_date, target_date]``."""
    covered = {
        date.fromisoformat(str(bar.date))
        for bar in bars
        if latest_date < date.fromisoformat(str(bar.date)) <= target_date
    }

    missing: list[date] = []
    cursor = latest_date + timedelta(days=1)
    while cursor <= target_date:
        if is_trading_day(cursor) and cursor not in covered:
            missing.append(cursor)
        cursor += timedelta(days=1)
    return missing


# ── Bar validation ─────────────────────────────────────────────────────


def validate_bars(
    bars: list,
    ticker: str,
    asset_class: str = "equity",
) -> tuple[list, list[str]]:
    """Validate bar data quality. Returns (valid_bars, issues).

    Checks: non-null OHLCV, high >= low, high >= open/close,
    low <= open/close, volume >= 0 except midpoint assets, positive open/close,
    valid trading day (skipped for futures), no duplicate dates.
    """
    valid: list = []
    issues: list[str] = []
    seen_dates: set[str] = set()

    for bar in bars:
        bar_date = str(bar.date)
        problems: list[str] = []

        # Duplicate date check
        if bar_date in seen_dates:
            problems.append(f"duplicate date {bar_date}")
        seen_dates.add(bar_date)

        # Null checks
        for field in ("open", "high", "low", "close", "volume"):
            if getattr(bar, field, None) is None:
                problems.append(f"{field} is null")

        if not problems:  # Only check relationships if fields are present
            if bar.high < bar.low:
                problems.append(f"high ({bar.high}) < low ({bar.low})")
            if bar.high < bar.open:
                problems.append(f"high ({bar.high}) < open ({bar.open})")
            if bar.high < bar.close:
                problems.append(f"high ({bar.high}) < close ({bar.close})")
            if bar.low > bar.open:
                problems.append(f"low ({bar.low}) > open ({bar.open})")
            if bar.low > bar.close:
                problems.append(f"low ({bar.low}) > close ({bar.close})")
            if bar.volume < 0 and asset_class not in {"cmdty", "fx"}:
                problems.append(f"negative volume ({bar.volume})")
            if bar.open <= 0:
                problems.append(f"non-positive open ({bar.open})")
            if bar.close <= 0:
                problems.append(f"non-positive close ({bar.close})")

            # Trading day check (skipped for futures — nearly 24h sessions)
            if asset_class != "futures":
                try:
                    bar_d = date.fromisoformat(bar_date)
                    if not is_trading_day(bar_d):
                        problems.append(f"{bar_date} is not a trading day")
                except ValueError:
                    problems.append(f"invalid date format: {bar_date}")

        if problems:
            issues.append(f"{ticker} {bar_date}: {'; '.join(problems)}")
        else:
            valid.append(bar)

    return (valid, issues)


# bars_to_rows, bars_to_futures_rows, bars_to_midpoint_rows
# → imported from clients.ingestion_common


def _run_quality_detection(
    *,
    ticker: str,
    asset_class: str,
    bars: list,
    parquet_path: Path,
    expected_start: date | None = None,
    source: str = "ib",
    reference_source: dict | None = None,
) -> None:
    """Run daily quality detection and emit flags without blocking publish."""
    if not bars:
        return
    normalized = _normalize_bars_for_detection(bars)
    metadata = {
        "asset_class": asset_class,
        "ticker": ticker,
        "timeframe": "1d",
        "source": source,
        "bars_received": len(bars),
        "expected_start": expected_start,
        "ib_head_timestamp": None,
        "errors_during_fetch": [],
    }
    if reference_source is not None:
        metadata["reference_source"] = reference_source
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
            timeframe="1d",
            parquet_path=parquet_path,
        )
        alert_on_flag(flag, source=source, ticker=ticker)


def validate_intraday_bar(
    bar: object,
    ticker: str,
    timeframe: str,
    *,
    require_rth: bool = True,
) -> list[str]:
    """Validate an intraday bar's timestamp against UTC, RTH, and grid alignment.

    Returns list of issue strings; empty if valid. Caller is responsible for
    OHLCV relationship checks (use ``validate_bars`` for those).
    """
    from zoneinfo import ZoneInfo

    issues: list[str] = []

    ts = getattr(bar, "bar_timestamp", None)
    if not isinstance(ts, datetime):
        issues.append(
            f"{ticker}: bar_timestamp must be datetime, got {type(ts).__name__}"
        )
        return issues

    # 1. tz-aware UTC
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        issues.append(f"{ticker} {ts}: bar_timestamp must be tz-aware")
        return issues
    if ts.utcoffset() != timedelta(0):
        issues.append(f"{ticker} {ts}: bar_timestamp must be UTC offset 0")
        return issues

    # 2. Convert to ET for date and session-window checks
    et = ts.astimezone(ZoneInfo("America/New_York"))

    # 3. Trading day
    if not is_trading_day(et.date()):
        issues.append(f"{ticker} {ts}: not a trading day")

    # 4. Within RTH
    rth_start = et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_t = session_close_time(et.date())
    rth_end = et.replace(
        hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0
    )
    if require_rth and not (rth_start <= et < rth_end):
        issues.append(f"{ticker} {ts}: outside RTH ({et.time()} ET)")

    # 5. Grid alignment
    if timeframe == "1m":
        if et.second != 0:
            issues.append(f"{ticker} {ts}: not aligned to 1-min grid")
    elif timeframe == "5m":
        if et.minute % 5 != 0 or et.second != 0:
            issues.append(f"{ticker} {ts}: not aligned to 5-min grid")
    elif timeframe == "1h":
        # IB returns 1h US-equity RTH bars at 9:30 (the opening 30-min bar)
        # then on the hour (10:00, 11:00, ..., 15:00).
        if et.second != 0 or (et.minute != 30 and et.minute != 0):
            issues.append(
                f"{ticker} {ts}: not aligned to 1h grid (expected :30 or :00 ET)"
            )

    return issues


def fetch_fallback_bars(
    ticker: str,
    missing_dates: list[date],
    fallback_client,
) -> tuple[list, list[str]]:
    """Fetch fallback bars for unresolved trading dates."""
    bars: list = []
    sources: list[str] = []

    for trade_date in missing_dates:
        fallback_bar = fallback_client.get_daily_bar(ticker, trade_date)
        if fallback_bar is None:
            continue
        bars.append(fallback_bar)
        sources.append(fallback_bar.source)

    return (bars, sources)


def fetch_massive_bars(
    ticker: str,
    missing_dates: list[date],
    massive_client,
) -> tuple[list, list[str]]:
    """Fetch Massive bars for unresolved daily dates."""
    if massive_client is None or not missing_dates:
        return ([], [])
    wanted = set(missing_dates)
    try:
        bars = massive_client.get_daily_bars(
            ticker, min(missing_dates), max(missing_dates)
        )
    except MassiveAPIError as exc:
        console.print(f"    [yellow]{ticker}: Massive recovery failed — {exc}[/yellow]")
        return ([], [])
    recovered = [bar for bar in bars if date.fromisoformat(str(bar.date)) in wanted]
    return (recovered, ["massive"] * len(recovered))


def _source_comparison(reference_bars: list, actual_bars: list) -> dict | None:
    if not reference_bars:
        return None
    expected_dates = sorted(str(bar.date)[:10] for bar in reference_bars)
    actual_dates = sorted(str(bar.date)[:10] for bar in actual_bars)
    return {
        "source": "massive",
        "expected_count": len(expected_dates),
        "actual_count": len(actual_dates),
        "expected_dates": expected_dates,
        "actual_dates": actual_dates,
    }


# ── Async fetching ─────────────────────────────────────────────────────


async def fetch_ticker_update(
    ticker: str,
    duration: str,
    ib: IBClient,
    semaphore: asyncio.Semaphore,
    asset_class: str = "equity",
) -> tuple[str, list]:
    """Fetch daily bars for *ticker* with the given *duration*.

    Returns ``(ticker, bars)``.
    """
    contract = _make_contract(ticker, asset_class)
    async with semaphore:
        await ib.ib.qualifyContractsAsync(contract)
        bars = await ib.get_historical_data_async(
            contract,
            duration=duration,
            bar_size="1 day",
            what_to_show="MIDPOINT" if asset_class in {"cmdty", "fx"} else "TRADES",
        )
    return (ticker, bars if bars else [])


async def fetch_batch(
    tickers_with_durations: list[tuple[str, str]],
    ib: IBClient,
    max_concurrent: int = 6,
    asset_class: str = "equity",
) -> dict[str, list]:
    """Fetch bars for a batch of tickers. Returns ``{ticker: bars}``."""
    semaphore = asyncio.Semaphore(max_concurrent)
    results: dict[str, list] = {}

    async def _safe_fetch(ticker: str, duration: str) -> tuple[str, list]:
        try:
            return await fetch_ticker_update(
                ticker, duration, ib, semaphore, asset_class=asset_class
            )
        except (IBError, Exception) as exc:
            console.print(f"    [red]{ticker}: {type(exc).__name__} — {exc}[/red]")
            return (ticker, [])

    gathered = await asyncio.gather(
        *[_safe_fetch(t, d) for t, d in tickers_with_durations]
    )
    for ticker, bars in gathered:
        results[ticker] = bars

    return results


# load_preset → imported from clients.ingestion_common


def resolve_target_date(
    today: date, requested_target: str | None, force: bool
) -> date | None:
    """Resolve the trading date this run should recover through."""
    if requested_target is not None:
        target = date.fromisoformat(requested_target)
        if not force and not is_trading_day(target):
            console.print(
                f"[yellow]{target} is not a trading day. Use --force to override.[/yellow]"
            )
            return None
        return target

    if not force and not is_trading_day(today):
        console.print(
            f"[yellow]{today} is not a trading day. Use --force to override.[/yellow]"
        )
        return None

    return today if is_trading_day(today) else previous_trading_day(today)


# ── Main ───────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Daily market data update")
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("MDW_IB_HOST", "127.0.0.1"),
        help="IB Gateway host (default: $MDW_IB_HOST or 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MDW_IB_PORT", "4001")),
        help="IB Gateway port (default: $MDW_IB_PORT or 4001)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=6,
        help="Max concurrent IB requests (default: 6)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Tickers per async batch (default: 50)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report gaps without fetching",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if not a trading day",
    )
    parser.add_argument(
        "--preset",
        type=str,
        default=None,
        help="Limit to tickers in a specific preset file",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=None,
        help="Limit to explicit tickers; missing bronze symbols receive target-date rows only.",
    )
    parser.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="Override the target trading date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures", "cmdty", "fx"],
        default="equity",
        help="Asset class to update (default: equity).",
    )
    parser.add_argument(
        "--source",
        choices=["ib", "massive"],
        default="ib",
        help="Daily source for equity updates (default: ib).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    today = date.today()

    # ── Trading day check ───────────────────────────────────────────
    target = resolve_target_date(today, args.target_date, args.force)
    if target is None:
        return

    asset_class = args.asset_class
    if args.source == "massive" and asset_class != "equity":
        console.print(
            "[red]--source massive is only supported for asset_class=equity.[/red]"
        )
        return 2

    bronze_dir = DATA_LAKE / "bronze" / f"asset_class={asset_class}"

    console.print(
        f"\n[bold]Daily Update[/bold]  target_date={target}  force={args.force}  "
        f"asset_class={asset_class}  source={args.source}  host={args.host}  port={args.port}"
    )

    # ── Load preset filter (if any) ─────────────────────────────────
    preset_tickers: set[str] | None = None
    if args.preset:
        preset_name, preset_list, _ = load_preset(args.preset)
        preset_tickers = set(preset_list)
        console.print(
            f"[bold]Preset:[/bold] {preset_name} ({len(preset_tickers)} tickers)"
        )

    # ── Gap detection ───────────────────────────────────────────────
    with _storage_client()(bronze_dir=bronze_dir, asset_class=asset_class) as bronze:
        latest_dates = bronze.get_latest_dates()

        if not latest_dates and args.tickers is None:
            console.print(
                "[yellow]No tickers found in bronze parquet. Run fetch_ib_historical.py first.[/yellow]"
            )
            return
        if not latest_dates and args.tickers is not None:
            latest_dates = {
                ticker.upper(): previous_trading_day(target).isoformat()
                for ticker in args.tickers
            }

        # Filter to preset tickers if specified
        if preset_tickers is not None:
            latest_dates = {
                k: v for k, v in latest_dates.items() if k in preset_tickers
            }
            if not latest_dates:
                console.print(
                    "[yellow]No preset tickers found in bronze parquet.[/yellow]"
                )
                return
        if args.tickers is not None:
            explicit_tickers = {ticker.upper() for ticker in args.tickers}
            missing_explicit = explicit_tickers - set(latest_dates)
            latest_dates = {
                k: v for k, v in latest_dates.items() if k in explicit_tickers
            }
            latest_dates.update(
                {
                    ticker: previous_trading_day(target).isoformat()
                    for ticker in missing_explicit
                }
            )

        up_to_date, single_gap, multi_gap = classify_gaps(latest_dates, target)
        need_update = single_gap + multi_gap

        console.print(f"\n[bold]Gap Report ({len(latest_dates)} tickers):[/bold]")
        console.print(f"  Up to date: {len(up_to_date)}")
        console.print(f"  Single-day gap: {len(single_gap)}")
        console.print(f"  Multi-day gap:  {len(multi_gap)}")

        if not need_update:
            console.print("\n[green bold]All tickers up to date.[/green bold]\n")
            return

        if args.dry_run:
            console.print("\n[bold]Dry run — tickers needing update:[/bold]")
            for ticker in sorted(need_update):
                latest = latest_dates[ticker]
                gap = trading_days_between(date.fromisoformat(latest), target)
                console.print(f"  {ticker:6s}  latest={latest}  gap={gap} trading days")
            console.print(
                f"\n[yellow]Dry run complete. {len(need_update)} tickers need updating.[/yellow]\n"
            )
            return

        # ── Build fetch plan ────────────────────────────────────────
        tickers_with_durations: list[tuple[str, str]] = []
        for ticker in need_update:
            latest = date.fromisoformat(latest_dates[ticker])
            duration = compute_ib_duration(latest, target)
            tickers_with_durations.append((ticker, duration))

        # ── Fetch and insert ────────────────────────────────────────
        total_inserted = 0
        total_validated = 0
        total_issues: list[str] = []
        tickers_updated = 0
        tickers_failed = 0
        fallback_attempts = 0
        fallback_successes = 0
        fallback_symbols = 0
        source_counts: dict[str, int] = {"ib": 0, "massive": 0}

        if args.source == "massive":
            with MassiveClient() as massive:
                for batch_idx, batch in enumerate(
                    [
                        tickers_with_durations[i : i + args.batch_size]
                        for i in range(0, len(tickers_with_durations), args.batch_size)
                    ]
                ):
                    console.print(
                        f"\n[bold]Massive Batch {batch_idx + 1}"
                        f" ({len(batch)} tickers)[/bold]"
                    )
                    for ticker, _duration in batch:
                        latest = date.fromisoformat(latest_dates[ticker])
                        bars, _sources = fetch_massive_bars(
                            ticker,
                            get_missing_trading_dates(latest, target, []),
                            massive,
                        )
                        valid_bars, issues = validate_bars(
                            bars, ticker, asset_class=asset_class
                        )
                        total_issues.extend(issues)
                        total_validated += len(bars)
                        valid_bars = [
                            b
                            for b in valid_bars
                            if latest < date.fromisoformat(str(b.date)) <= target
                        ]
                        if not valid_bars:
                            console.print(
                                f"  [yellow]{ticker}[/yellow]: no bars from Massive"
                            )
                            tickers_failed += 1
                            continue

                        symbol_id = bronze.get_symbol_id(ticker)
                        rows = bars_to_rows(valid_bars, symbol_id)
                        parquet_path = bronze_dir / f"symbol={ticker}" / "1d.parquet"
                        _run_quality_detection(
                            ticker=ticker,
                            asset_class=asset_class,
                            bars=valid_bars,
                            parquet_path=parquet_path,
                            expected_start=latest + timedelta(days=1)
                            if latest
                            else None,
                            source="massive",
                        )
                        inserted = bronze.merge_ticker_rows(ticker, rows)
                        if hasattr(bronze, "write_ticker_parquet"):
                            bronze.write_ticker_parquet(ticker, symbol_id, bronze_dir)
                        remaining_dates = get_missing_trading_dates(
                            latest, target, valid_bars
                        )
                        total_inserted += inserted
                        source_counts["massive"] += len(valid_bars)
                        if remaining_dates:
                            console.print(
                                f"  [yellow]{ticker}[/yellow]: {inserted} bar"
                                f"{'s' if inserted != 1 else ''} published from Massive, "
                                f"still missing {', '.join(d.isoformat() for d in remaining_dates)}"
                            )
                            tickers_failed += 1
                            continue
                        tickers_updated += 1
                        console.print(
                            f"  [green]{ticker}[/green]: {inserted} bar"
                            f"{'s' if inserted != 1 else ''} published from Massive"
                        )

            console.print(f"\n{'═' * 60}")
            console.print(f"[bold]Daily Update Complete[/bold]")
            console.print(f"  Tickers updated:    {tickers_updated}")
            console.print(f"  Tickers failed:     {tickers_failed}")
            console.print(f"  Fallback attempts:  {fallback_attempts}")
            console.print(f"  Fallback successes: {fallback_successes}")
            console.print(f"  Fallback symbols:   {fallback_symbols}")
            console.print(f"  Source massive:     {source_counts.get('massive', 0)}")
            console.print(f"  Bars inserted:      {total_inserted}")
            console.print(f"  Bars validated:     {total_validated}")
            console.print(f"  Validation issues:  {len(total_issues)}")
            console.print()
            if tickers_failed > 0:
                return 1
            return 0

        with ExitStack() as stack:
            ib = stack.enter_context(IBClient())
            fallback = stack.enter_context(_fallback_client())
            maybe_massive = (
                _optional_massive_client() if asset_class == "equity" else None
            )
            massive = (
                stack.enter_context(maybe_massive)
                if maybe_massive is not None
                else None
            )
            ib.connect(host=args.host, port=args.port)

            batches = [
                tickers_with_durations[i : i + args.batch_size]
                for i in range(0, len(tickers_with_durations), args.batch_size)
            ]

            for batch_idx, batch in enumerate(batches):
                console.print(
                    f"\n[bold]Batch {batch_idx + 1}/{len(batches)}"
                    f" ({len(batch)} tickers)[/bold]"
                )

                ticker_bars = ib.ib.run(
                    fetch_batch(
                        batch,
                        ib,
                        max_concurrent=args.max_concurrent,
                        asset_class=asset_class,
                    )
                )

                for ticker, duration in batch:
                    bars = ticker_bars.get(ticker, [])
                    valid_bars, issues = validate_bars(
                        bars, ticker, asset_class=asset_class
                    )
                    total_issues.extend(issues)
                    total_validated += len(bars)

                    # Filter to only bars after the latest parquet date
                    latest = date.fromisoformat(latest_dates[ticker])
                    valid_bars = [
                        b
                        for b in valid_bars
                        if latest < date.fromisoformat(str(b.date)) <= target
                    ]

                    # Fallback recovery (equity only — Nasdaq/Stooq don't cover indices/futures)
                    reference_bars: list = []
                    if asset_class == "equity":
                        reference_bars, _reference_sources = fetch_massive_bars(
                            ticker,
                            get_missing_trading_dates(latest, target, []),
                            massive,
                        )
                        missing_dates = get_missing_trading_dates(
                            latest, target, valid_bars
                        )
                        fallback_attempts += len(missing_dates)
                        massive_bars, massive_sources = fetch_massive_bars(
                            ticker,
                            missing_dates,
                            massive,
                        )
                        fallback_bars = massive_bars
                        fallback_sources = massive_sources
                        recovered_dates = {
                            date.fromisoformat(str(bar.date)) for bar in massive_bars
                        }
                        public_missing_dates = [
                            missing
                            for missing in missing_dates
                            if missing not in recovered_dates
                        ]
                        public_bars, public_sources = fetch_fallback_bars(
                            ticker,
                            public_missing_dates,
                            fallback,
                        )
                        fallback_bars.extend(public_bars)
                        fallback_sources.extend(public_sources)
                        if fallback_bars:
                            recovered_bars, fallback_issues = validate_bars(
                                fallback_bars, ticker, asset_class=asset_class
                            )
                            total_issues.extend(fallback_issues)
                            total_validated += len(fallback_bars)
                            if recovered_bars:
                                valid_bars.extend(recovered_bars)
                                fallback_successes += len(recovered_bars)
                                fallback_symbols += 1
                                for recovered in recovered_bars:
                                    source_counts[
                                        getattr(recovered, "source", "massive")
                                    ] = (
                                        source_counts.get(
                                            getattr(recovered, "source", "massive"), 0
                                        )
                                        + 1
                                    )
                                console.print(
                                    f"  [cyan]{ticker}[/cyan]: recovered "
                                    f"{len(recovered_bars)} missing trading day"
                                    f"{'s' if len(recovered_bars) != 1 else ''} via "
                                    f"{', '.join(sorted(set(fallback_sources)))}"
                                )

                    if not valid_bars:
                        if bars:
                            console.print(
                                f"  [yellow]{ticker}[/yellow]: no valid target bar from IB or fallback"
                            )
                        else:
                            console.print(
                                f"  [yellow]{ticker}[/yellow]: no bars from IB and no fallback bar"
                            )
                        tickers_failed += 1
                        continue

                    symbol_id = bronze.get_symbol_id(ticker)
                    if asset_class == "futures":
                        root, expiry = ticker.rsplit("_", 1)
                        expiry_date = f"{expiry[:4]}-{expiry[4:6]}-01"
                        rows = bars_to_futures_rows(
                            valid_bars, symbol_id, root, expiry_date
                        )
                    elif asset_class in {"cmdty", "fx"}:
                        rows = bars_to_midpoint_rows(
                            valid_bars,
                            symbol_id,
                            invert=asset_class == "fx" and _is_inverted_fx_pair(ticker),
                        )
                    else:
                        rows = bars_to_rows(valid_bars, symbol_id)
                    parquet_path = bronze_dir / f"symbol={ticker}" / "1d.parquet"
                    _run_quality_detection(
                        ticker=ticker,
                        asset_class=asset_class,
                        bars=valid_bars,
                        parquet_path=parquet_path,
                        expected_start=latest + timedelta(days=1) if latest else None,
                        source="ib",
                        reference_source=_source_comparison(reference_bars, valid_bars),
                    )
                    inserted = bronze.merge_ticker_rows(ticker, rows)
                    if hasattr(bronze, "write_ticker_parquet"):
                        bronze.write_ticker_parquet(ticker, symbol_id, bronze_dir)
                    remaining_dates = get_missing_trading_dates(
                        latest, target, valid_bars
                    )
                    total_inserted += inserted
                    source_counts["ib"] += len(
                        [b for b in valid_bars if getattr(b, "source", "ib") == "ib"]
                    )

                    if remaining_dates:
                        console.print(
                            f"  [yellow]{ticker}[/yellow]: "
                            f"{inserted} bar{'s' if inserted != 1 else ''} published, "
                            f"still missing {', '.join(d.isoformat() for d in remaining_dates)}"
                        )
                        tickers_failed += 1
                        continue

                    tickers_updated += 1
                    console.print(
                        f"  [green]{ticker}[/green]: {inserted} bar{'s' if inserted != 1 else ''} published"
                    )

    # ── Summary ─────────────────────────────────────────────────────
    console.print(f"\n{'═' * 60}")
    console.print(f"[bold]Daily Update Complete[/bold]")
    console.print(f"  Tickers updated:    {tickers_updated}")
    console.print(f"  Tickers failed:     {tickers_failed}")
    console.print(f"  Fallback attempts:  {fallback_attempts}")
    console.print(f"  Fallback successes: {fallback_successes}")
    console.print(f"  Fallback symbols:   {fallback_symbols}")
    for source_name, count in sorted(source_counts.items()):
        console.print(f"  Source {source_name}:     {count}")
    console.print(f"  Bars inserted:      {total_inserted}")
    console.print(f"  Bars validated:     {total_validated}")
    console.print(f"  Validation issues:  {len(total_issues)}")
    if total_issues:
        console.print("\n[bold]Validation issues:[/bold]")
        for issue in total_issues[:20]:
            console.print(f"  [yellow]{issue}[/yellow]")
        if len(total_issues) > 20:  # pragma: no cover
            console.print(f"  ... and {len(total_issues) - 20} more")
    console.print()

    if tickers_failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
