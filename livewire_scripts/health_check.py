"""Core gap detection, backfill engine, and CLI entry point for warehouse health check."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console

from clients import BronzeClient
from clients.intraday_bronze_client import (
    INTRADAY_PARQUET_FILENAME,
    INTRADAY_TIMEFRAMES,
    IntradayBronzeClient,
)
from livewire_scripts.daily_update import (
    _make_contract,
    bars_to_futures_rows,
    bars_to_rows,
    is_trading_day,
    session_close_time,
    validate_bars,
)

_ET = ZoneInfo("America/New_York")
_BAR_SIZE_MINUTES = {"1m": 1, "1h": 60, "5m": 5, "30m": 30}

log = logging.getLogger(__name__)

console = Console()
_WAREHOUSE_DIR = Path(
    os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))
)
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
_INGEST_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ingest.py"
_OPS_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ops.py"


def _resolve_bronze_dir(asset_class: str) -> Path:
    """Return the bronze directory for the given asset class."""
    return _DATA_LAKE / "bronze" / f"asset_class={asset_class}"


def find_interior_gaps(
    actual_dates: list[date], asset_class: str = "equity"
) -> list[date]:
    """Return trading/weekday dates missing between the min and max of *actual_dates*.

    For equity/volatility asset classes the NYSE calendar is used to determine
    which dates are expected.  For futures a simple weekday check is used
    (CME trades some NYSE holidays).

    A single date or empty list always returns ``[]``.
    """
    if len(actual_dates) < 2:
        return []

    actual_set = set(actual_dates)
    start = min(actual_dates)
    end = max(actual_dates)

    gaps: list[date] = []
    current = start + timedelta(days=1)
    while current < end:
        if asset_class == "futures":
            expected = current.weekday() < 5  # Mon-Fri
        else:
            expected = is_trading_day(current)

        if expected and current not in actual_set:
            gaps.append(current)

        current += timedelta(days=1)

    return gaps


def group_contiguous_dates(dates: list[date]) -> list[tuple[date, date]]:
    """Group *dates* into contiguous ``(start, end)`` ranges.

    Contiguous means each successive date is exactly 1 calendar day after
    the previous one.  Non-contiguous dates start a new range.

    Returns ``[]`` for an empty input.
    """
    if not dates:
        return []

    groups: list[tuple[date, date]] = []
    start = dates[0]
    prev = dates[0]

    for current in dates[1:]:
        if (current - prev).days == 1:
            prev = current
        else:
            groups.append((start, prev))
            start = current
            prev = current

    groups.append((start, prev))
    return groups


def compute_range_duration(start_date: date, end_date: date) -> str:
    """Return an IB-style duration string for an arbitrary date range.

    Mirrors the logic in ``compute_ib_duration`` from ``daily_update``:
    calendar days between *start_date* and *end_date* plus a 2-day buffer.

    * ``<= 0`` calendar days → ``"1 D"``
    * ``<= 180`` (after buffer) → ``"{N} D"``
    * ``<= 365`` (after buffer) → ``"1 Y"``
    * else → ``"2 Y"``
    """
    cal_days = (end_date - start_date).days
    if cal_days <= 0:
        return "1 D"
    cal_days += 2
    if cal_days <= 180:
        return f"{cal_days} D"
    elif cal_days <= 365:
        return "1 Y"
    else:
        return "2 Y"


def get_all_trade_dates(bronze: BronzeClient) -> dict[str, list[date]]:
    """Return ``{symbol: [date, ...]}`` for every symbol in bronze, sorted ascending.

    Returns ``{}`` when the bronze directory is empty or has no symbols.
    """
    return bronze.get_trade_dates_by_symbol()


def generate_expected_intraday_timestamps(
    trading_days: list[date], timeframe: str
) -> set[datetime]:
    """Return the set of expected RTH bar timestamps (UTC) for *trading_days*.

    For each trading day:

    * **1m**: bars every minute from 9:30 ET up to ``close - 1min``.
    * **5m**: bars every 5 minutes from 9:30 ET up to ``close - 5min``.
    * **1h**: first bar at 9:30 ET (covers 9:30-10:00), then on the hour
      (10:00, 11:00, …) up to ``close - 1h``. This matches IB's actual
      US-equity RTH 1h grid as verified empirically by
      ``scripts/probe_ib_intraday.py``.
    """
    if timeframe not in _BAR_SIZE_MINUTES:
        raise ValueError(f"unsupported intraday timeframe: {timeframe!r}")

    expected: set[datetime] = set()
    for d in trading_days:
        if not is_trading_day(d):
            continue
        rth_open = datetime.combine(d, time(9, 30), tzinfo=_ET)
        close_t = session_close_time(d)
        rth_close = datetime.combine(d, close_t, tzinfo=_ET)

        if timeframe == "1h":
            # First bar: 9:30 (partial 30-min open)
            expected.add(rth_open.astimezone(timezone.utc))
            # Subsequent bars on the hour, last bar's start strictly before close
            current = datetime.combine(d, time(10, 0), tzinfo=_ET)
            step = timedelta(hours=1)
            while current + step <= rth_close:
                expected.add(current.astimezone(timezone.utc))
                current += step
        else:  # 1m / 5m
            current = rth_open
            step = timedelta(minutes=_BAR_SIZE_MINUTES[timeframe])
            while current + step <= rth_close:
                expected.add(current.astimezone(timezone.utc))
                current += step
    return expected


def find_intraday_gaps(
    actual_timestamps: list[datetime], timeframe: str
) -> tuple[list[datetime], list[tuple[datetime, datetime]]]:
    """Return ``(missing_interior, suspected_halts)`` for an intraday series.

    *actual_timestamps* must be tz-aware UTC datetimes. Interior means dates
    strictly between the first and last actual trading day inclusive — gaps
    outside that envelope (e.g. before IPO or after IB depth roll) are excluded.

    Suspected halts are contiguous runs of missing bars shorter than 30 minutes,
    surrounded on both sides by actual bars (annotation only — no action).
    """
    if len(actual_timestamps) < 2:
        return [], []

    actual = {ts.astimezone(timezone.utc) for ts in actual_timestamps}
    days = sorted({ts.astimezone(_ET).date() for ts in actual})
    expected = generate_expected_intraday_timestamps(days, timeframe)

    missing = sorted(expected - actual)
    if not missing:
        return [], []

    step = timedelta(minutes=_BAR_SIZE_MINUTES[timeframe])
    halt_threshold = timedelta(minutes=30)
    halts: list[tuple[datetime, datetime]] = []

    # Group consecutive missing into runs and flag short ones as halts
    run_start = missing[0]
    prev = missing[0]
    for ts in missing[1:]:
        if ts - prev == step:
            prev = ts
            continue
        if (prev - run_start) + step < halt_threshold:
            halts.append((run_start, prev))
        run_start = ts
        prev = ts
    if (prev - run_start) + step < halt_threshold:
        halts.append((run_start, prev))

    return missing, halts


def report_intraday_health(
    timeframe: str,
    bronze_dir: Path,
    symbol_filter: str | None = None,
) -> dict:
    """Scan intraday parquet for *timeframe* and return a summary dict.

    Report-only — never modifies data. Prints a Rich summary table.
    """
    intraday = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=timeframe)
    symbols = intraday.get_existing_symbols()
    if symbol_filter:
        symbols = symbols & {symbol_filter}

    summary: dict = {
        "timeframe": timeframe,
        "symbols_scanned": 0,
        "symbols_with_gaps": 0,
        "total_missing": 0,
        "total_halts": 0,
        "by_symbol": {},
    }

    for symbol in sorted(symbols):
        rows = intraday.read_symbol_rows(symbol)
        timestamps = [row["bar_timestamp"] for row in rows]
        missing, halts = find_intraday_gaps(timestamps, timeframe)
        summary["symbols_scanned"] += 1
        if missing:
            summary["symbols_with_gaps"] += 1
            summary["total_missing"] += len(missing)
            summary["total_halts"] += len(halts)
            summary["by_symbol"][symbol] = {
                "missing": len(missing),
                "halts": len(halts),
            }
            console.print(
                f"  [red]{symbol}[/red] {timeframe}: "
                f"{len(missing)} missing bars, {len(halts)} suspected halts"
            )

    if summary["symbols_scanned"] == 0:
        console.print(
            f"[yellow]No symbols found at {timeframe} under {bronze_dir}[/yellow]"
        )
    elif summary["symbols_with_gaps"] == 0:
        console.print(
            f"[green]{summary['symbols_scanned']} symbols clean at {timeframe}[/green]"
        )
    else:
        console.print(
            f"\n[bold]{summary['symbols_with_gaps']}/{summary['symbols_scanned']} symbols "
            f"have interior gaps at {timeframe} "
            f"({summary['total_missing']} missing bars, {summary['total_halts']} halts)[/bold]"
        )

    return summary


def repair_intraday_window(
    symbol: str,
    timeframe: str,
    since: date,
    host: str,
    port: int,
) -> int:
    """Shell out to backfill_intraday.py to repair a single symbol/timeframe.

    The narrow scope (one symbol, one timeframe) is enforced by the caller
    in main(). The ``since`` argument is converted to a ``--years`` window
    rounded up to whole years, since the chunking helper works in year-back
    increments.
    """
    today = date.today()
    days = max((today - since).days, 1)
    years = max(1, (days + 364) // 365)
    cmd = [
        sys.executable,
        str(_INGEST_SCRIPT),
        "intraday-backfill",
        "--tickers",
        symbol,
        "--timeframe",
        timeframe,
        "--years",
        str(years),
        "--host",
        host,
        "--port",
        str(port),
    ]
    console.print(
        f"[cyan]Repairing {symbol} {timeframe} since {since} ({years}y)[/cyan]"
    )
    result = subprocess.run(cmd, check=False)
    return result.returncode


def _send_alert(
    run_date: str,
    asset_class: str,
    total_gaps: int,
    repaired: int,
    log_path: Path,
) -> None:
    """Send an email alert via the Node.js alert script."""
    error_summary = (
        f"health_check ({asset_class}): {total_gaps} interior gaps detected, "
        f"{repaired} repaired."
    )
    cmd = [
        sys.executable,
        str(_OPS_SCRIPT),
        "send-alert",
        "--run-date",
        run_date,
        "--log-file",
        str(log_path),
        "--error-summary",
        error_summary,
        "--repo-root",
        str(_REPO_ROOT),
        "--job-name",
        "health_check",
    ]
    subprocess.run(cmd, check=False)


def main() -> None:
    """CLI entry point for the warehouse health check."""
    parser = argparse.ArgumentParser(description="Livewire health check")
    parser.add_argument(
        "--dry-run", action="store_true", help="Report gaps without backfilling"
    )
    parser.add_argument(
        "--force", action="store_true", help="Run even if not a trading day"
    )
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures"],
        default="equity",
        help="Asset class to check (default: equity)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("MDW_IB_HOST", "127.0.0.1"),
        help="IB Gateway host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MDW_IB_PORT", "4001")),
        help="IB Gateway port",
    )
    parser.add_argument(
        "--alert-threshold",
        type=int,
        default=10,
        help="Number of repaired gaps that triggers an email alert (default: 10)",
    )
    parser.add_argument(
        "--intraday",
        action="store_true",
        help="Run intraday (1m/1h/5m) health check instead of daily",
    )
    parser.add_argument(
        "--timeframe",
        choices=list(INTRADAY_TIMEFRAMES),
        help="Intraday timeframe to scan (required with --intraday)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Restrict intraday scan to a single symbol (and enable repair when "
        "combined with --since)",
    )
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        help="Start date for targeted intraday repair (YYYY-MM-DD). Repair fires "
        "only when --symbol, --since, and --timeframe are all set.",
    )
    args = parser.parse_args()

    today = date.today()

    # ── Intraday branch ──────────────────────────────────────────────
    if args.intraday:
        if not args.timeframe:
            parser.error("--intraday requires --timeframe {1h,5m}")
        bronze_dir = _resolve_bronze_dir("equity")
        console.print(
            f"\n[bold]Intraday Health Check[/bold]  date={today}  "
            f"timeframe={args.timeframe}  symbol={args.symbol or '*'}"
        )
        report_intraday_health(
            timeframe=args.timeframe,
            bronze_dir=bronze_dir,
            symbol_filter=args.symbol,
        )
        # Implicit repair when fully scoped
        if args.symbol and args.since and not args.dry_run:
            rc = repair_intraday_window(
                symbol=args.symbol,
                timeframe=args.timeframe,
                since=args.since,
                host=args.host,
                port=args.port,
            )
            if rc != 0:
                console.print(f"[red]Repair subprocess exited with code {rc}[/red]")
        elif args.symbol and not args.since:
            console.print(
                "[dim]Report-only: pass --since YYYY-MM-DD to enable repair[/dim]"
            )
        return

    # ── Trading day check ─────────────────────────────────────────────
    if not args.force and not is_trading_day(today):
        console.print(
            f"[yellow]{today} is not a trading day. Use --force to override.[/yellow]"
        )
        return

    asset_class = args.asset_class
    bronze_dir = _resolve_bronze_dir(asset_class)

    console.print(
        f"\n[bold]Health Check[/bold]  date={today}  asset_class={asset_class}"
        f"  dry_run={args.dry_run}  force={args.force}"
    )

    # ── Gap detection ──────────────────────────────────────────────────
    with BronzeClient(bronze_dir=bronze_dir, asset_class=asset_class) as bronze:
        all_dates = get_all_trade_dates(bronze)

        if not all_dates:
            console.print(
                "[yellow]No tickers found in bronze parquet. Run fetch_ib_historical.py first.[/yellow]"
            )
            return

        gaps_by_symbol: dict[str, list[date]] = {}
        for symbol, dates in all_dates.items():
            gaps = find_interior_gaps(dates, asset_class=asset_class)
            if gaps:
                gaps_by_symbol[symbol] = gaps

        total_gaps = sum(len(g) for g in gaps_by_symbol.values())
        console.print(f"\n[bold]Gap Report ({len(all_dates)} symbols):[/bold]")
        if gaps_by_symbol:
            console.print(
                f"  [red]Interior gaps found in {len(gaps_by_symbol)} symbols ({total_gaps} gap-days total)[/red]"
            )
            for sym, gaps in sorted(gaps_by_symbol.items()):
                ranges = group_contiguous_dates(gaps)
                for start, end in ranges:
                    dur = compute_range_duration(start, end)
                    console.print(f"    {sym}: {start} – {end} ({dur})")
        else:
            console.print("  [green]No interior gaps detected[/green]")

        if args.dry_run:
            console.print("\n[bold]Dry-run complete — no backfill performed.[/bold]")
            return

        if not gaps_by_symbol:
            return

        # ── Backfill ───────────────────────────────────────────────────
        if asset_class == "volatility":
            console.print(
                "[yellow]Backfill for volatility not yet implemented. "
                "Use fetch_cboe_volatility.py instead.[/yellow]"
            )
            return

        # Lazy import — only needed when actually connecting to IB
        from clients.ib_client import IBClient  # noqa: PLC0415
        from livewire_scripts.daily_update import fetch_fallback_bars  # noqa: PLC0415

        log_dir = _WAREHOUSE_DIR / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"health_check_{today:%Y-%m-%d}.log"

        repaired = 0
        fallback_repaired = 0

        console.print(
            f"\n[bold]Backfilling {len(gaps_by_symbol)} symbols via IB...[/bold]"
        )

        with IBClient(host=args.host, port=args.port) as ib:
            ib.ib.run(ib.connect())

            for symbol, gaps in sorted(gaps_by_symbol.items()):
                ranges = group_contiguous_dates(gaps)
                symbol_rows: list[dict] = []

                for start, end in ranges:
                    duration = compute_range_duration(start, end)
                    contract = _make_contract(symbol, asset_class)
                    try:
                        ib.ib.run(ib.ib.qualifyContractsAsync(contract))
                        bars = ib.ib.run(
                            ib.get_historical_data_async(
                                contract,
                                duration=duration,
                                bar_size="1 day",
                                what_to_show="TRADES",
                            )
                        )
                    except Exception as exc:
                        console.print(f"  [red]{symbol}: IB error — {exc}[/red]")
                        bars = []

                    if not bars:
                        continue

                    valid_bars, issues = validate_bars(bars, symbol, asset_class)
                    for issue in issues:
                        log.warning(issue)

                    if asset_class == "futures":
                        root_symbol = symbol.rsplit("_", 1)[0]
                        expiry_code = symbol.rsplit("_", 1)[1]
                        expiry_date = f"{expiry_code[:4]}-{expiry_code[4:6]}-01"
                        contract_id = bronze.get_symbol_id(symbol)
                        rows = bars_to_futures_rows(
                            valid_bars, contract_id, root_symbol, expiry_date
                        )
                    else:
                        symbol_id = bronze.get_symbol_id(symbol)
                        rows = bars_to_rows(valid_bars, symbol_id)

                    symbol_rows.extend(rows)

                if symbol_rows:
                    inserted = bronze.merge_ticker_rows(symbol, symbol_rows)
                    repaired += inserted
                    console.print(
                        f"  [green]{symbol}: +{inserted} bars repaired via IB[/green]"
                    )

                # Equity fallback for remaining gaps
                if asset_class == "equity":
                    # Re-check which gaps remain after IB merge
                    updated_dates = [
                        date.fromisoformat(r["trade_date"])
                        for r in bronze.read_symbol_rows(symbol)
                    ]
                    remaining = find_interior_gaps(
                        updated_dates, asset_class=asset_class
                    )
                    remaining_set = set(remaining) & set(gaps)

                    if remaining_set:
                        from clients.daily_bar_fallback import (
                            DailyBarFallbackClient,  # noqa: PLC0415
                        )

                        fallback = DailyBarFallbackClient()
                        fb_bars, _ = fetch_fallback_bars(
                            symbol, sorted(remaining_set), fallback
                        )
                        if fb_bars:
                            symbol_id = bronze.get_symbol_id(symbol)
                            fb_rows = bars_to_rows(fb_bars, symbol_id)
                            fb_inserted = bronze.merge_ticker_rows(symbol, fb_rows)
                            fallback_repaired += fb_inserted
                            console.print(
                                f"  [cyan]{symbol}: +{fb_inserted} bars repaired via fallback[/cyan]"
                            )

        total_repaired = repaired + fallback_repaired
        console.print(
            f"\n[bold]Summary:[/bold] {total_gaps} gap-days detected, "
            f"{total_repaired} repaired ({repaired} IB, {fallback_repaired} fallback)"
        )

        # Log to file
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(
                f"{today} health_check: asset_class={asset_class} "
                f"total_gaps={total_gaps} repaired={total_repaired}\n"
            )

        # Alert if threshold exceeded
        if total_repaired >= args.alert_threshold:
            _send_alert(str(today), asset_class, total_gaps, total_repaired, log_path)


if __name__ == "__main__":
    main()
