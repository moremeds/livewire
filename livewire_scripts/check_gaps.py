"""Gap detection — compare expected trading days vs bronze parquet."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients import BronzeClient
from clients.tag_registry import TagRegistry
from clients.trading_calendar import is_trading_day

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))


@dataclass
class GapReport:
    ticker: str
    earliest_available: str | None
    bronze_start: str | None
    bronze_end: str | None
    bronze_count: int
    expected_count: int
    gap_count: int
    missing_dates: list[date] = field(default_factory=list)
    complete: bool = False


def _trading_days_in_range(start: date, end: date) -> set[date]:
    days: set[date] = set()
    d = start
    while d <= end:
        if is_trading_day(d):
            days.add(d)
        d += timedelta(days=1)
    return days


def compute_gaps(
    ticker: str,
    earliest_available: str | None,
    bronze_dates: set[date],
    as_of: date | None = None,
) -> GapReport:
    today = as_of or date.today()

    if not earliest_available:
        return GapReport(
            ticker=ticker,
            earliest_available=None,
            bronze_start=min(bronze_dates).isoformat() if bronze_dates else None,
            bronze_end=max(bronze_dates).isoformat() if bronze_dates else None,
            bronze_count=len(bronze_dates),
            expected_count=0,
            gap_count=0,
            complete=False,
        )

    start = date.fromisoformat(earliest_available)
    expected = _trading_days_in_range(start, today)
    missing = sorted(expected - bronze_dates)

    return GapReport(
        ticker=ticker,
        earliest_available=earliest_available,
        bronze_start=min(bronze_dates).isoformat() if bronze_dates else None,
        bronze_end=max(bronze_dates).isoformat() if bronze_dates else None,
        bronze_count=len(bronze_dates),
        expected_count=len(expected),
        gap_count=len(missing),
        missing_dates=missing,
        complete=len(missing) == 0,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Gap detection for bronze parquet")
    parser.add_argument("--preset", type=str, default=None, help="Limit to preset tickers")
    parser.add_argument("--show-gaps", action="store_true", help="Show individual missing dates")
    parser.add_argument(
        "--incomplete-only",
        action="store_true",
        help="Only show tickers with gaps",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    registry = TagRegistry(_WAREHOUSE_DIR / "registry.json")
    bronze_dir = _WAREHOUSE_DIR / "data-lake" / "bronze" / "asset_class=equity"
    bronze = BronzeClient(bronze_dir=bronze_dir)
    all_dates = bronze.get_trade_dates_by_symbol()

    tickers = sorted(all_dates.keys())
    if args.preset:
        with open(args.preset) as f:
            preset_tickers = set(json.load(f).get("tickers", []))
        tickers = [t for t in tickers if t in preset_tickers]

    table = Table(title="Gap Report")
    table.add_column("Ticker", style="bold")
    table.add_column("Earliest", justify="right")
    table.add_column("Bronze", justify="right")
    table.add_column("Expected", justify="right")
    table.add_column("Gaps", justify="right")
    table.add_column("Status")

    n_complete, n_gaps, n_unknown = 0, 0, 0
    for ticker in tickers:
        entry = registry.get(ticker)
        earliest = entry.earliest_available if entry else None
        report = compute_gaps(ticker, earliest, set(all_dates.get(ticker, [])))

        if args.incomplete_only and report.complete:
            continue

        if report.complete:
            n_complete += 1
            status = "[green]complete[/green]"
        elif earliest is None:
            n_unknown += 1
            status = "[dim]no bounds[/dim]"
        else:
            n_gaps += 1
            status = f"[yellow]{report.gap_count} gaps[/yellow]"

        table.add_row(
            ticker,
            earliest or "?",
            f"{report.bronze_count:,}",
            f"{report.expected_count:,}",
            str(report.gap_count),
            status,
        )

        if args.show_gaps and report.missing_dates:
            for d in report.missing_dates[:20]:
                table.add_row("", "", "", "", d.isoformat(), "")
            if len(report.missing_dates) > 20:
                table.add_row(
                    "",
                    "",
                    "",
                    "",
                    f"... +{len(report.missing_dates) - 20} more",
                    "",
                )

    console.print(table)
    console.print(f"\n[bold]{n_complete} complete, {n_gaps} with gaps, {n_unknown} no bounds[/bold]")


if __name__ == "__main__":  # pragma: no cover
    main()
