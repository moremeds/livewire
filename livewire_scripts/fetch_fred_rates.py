#!/usr/bin/env python3
"""Fetch U.S. Treasury constant-maturity yields from FRED into bronze parquet."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from rich.console import Console

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients.bronze_client import BronzeClient
from clients.fred_client import FRED_FREQUENCIES, FredClient, FredObservation
from clients.symbol_ids import stable_symbol_id


DEFAULT_WAREHOUSE = Path.home() / "market-warehouse"
ASSET_CLASS = "rates"
DEFAULT_SERIES = {
    "DGS3": 3.0,
    "DGS5": 5.0,
    "DGS10": 10.0,
    "DGS30": 30.0,
}
DEFAULT_AGGREGATION_METHOD = "eop"

console = Console()


def observations_to_rate_rows(
    series_id: str,
    tenor_years: float,
    observations: list[FredObservation],
) -> list[dict]:
    """Convert FRED observations to the rates bronze row schema."""
    symbol_id = stable_symbol_id(series_id)
    return [
        {
            "trade_date": observation.date,
            "symbol_id": symbol_id,
            "tenor_years": float(tenor_years),
            "yield_pct": float(observation.value),
            "source": "fred",
        }
        for observation in observations
    ]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--series",
        nargs="+",
        choices=sorted(DEFAULT_SERIES),
        default=list(DEFAULT_SERIES),
        help="FRED Treasury yield series to fetch (default: DGS3 DGS5 DGS10 DGS30)",
    )
    parser.add_argument(
        "--start",
        dest="observation_start",
        help="Observation start date, YYYY-MM-DD",
    )
    parser.add_argument(
        "--end",
        dest="observation_end",
        help="Observation end date, YYYY-MM-DD",
    )
    parser.add_argument(
        "--frequency",
        choices=sorted(FRED_FREQUENCIES),
        default="d",
        help="FRED frequency aggregation (default: d). Use m/q/a/etc. for lower-frequency aggregates.",
    )
    parser.add_argument(
        "--aggregation-method",
        choices=["avg", "sum", "eop"],
        default=DEFAULT_AGGREGATION_METHOD,
        help="FRED aggregation method when frequency rolls up data (default: eop)",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=DEFAULT_WAREHOUSE,
        help=f"Warehouse directory (default: {DEFAULT_WAREHOUSE})",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def run(argv: Sequence[str] | None = None, *, client: FredClient | None = None) -> int:
    args = parse_args(argv)
    fred = client or FredClient()
    bronze_dir = args.warehouse / "data-lake" / "bronze" / f"asset_class={ASSET_CLASS}"

    console.print(f"\n[bold]Fetching FRED Treasury rates: {args.series}[/bold]\n")
    with BronzeClient(bronze_dir=bronze_dir, asset_class=ASSET_CLASS) as bronze:
        for series_id in args.series:
            observations = fred.fetch_observations(
                series_id,
                observation_start=args.observation_start,
                observation_end=args.observation_end,
                frequency=args.frequency,
                aggregation_method=args.aggregation_method,
            )
            if not observations:
                console.print(f"  [yellow]{series_id}: no observations returned[/yellow]")
                continue

            rows = observations_to_rate_rows(series_id, DEFAULT_SERIES[series_id], observations)
            inserted = bronze.merge_ticker_rows(series_id, rows)
            console.print(
                f"  {series_id}: fetched {len(rows)} rows, inserted {inserted}, "
                f"{rows[0]['trade_date']} -> {rows[-1]['trade_date']}"
            )

    console.print("[bold green]Done.[/bold green]")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
