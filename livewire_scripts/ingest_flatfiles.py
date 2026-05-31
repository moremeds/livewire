"""Flat file ingestion — bulk download Polygon S3 minute aggregates.

Downloads per-day gzipped CSVs, writes 1m bronze parquet per ticker,
then derives 5m/30m/1h via lossless aggregation. Temp CSV files are
deleted after each day is parsed.

Usage:
    python scripts/livewire_ingest.py flatfile-ingest --preset presets/sp500.json --years 5
    python scripts/livewire_ingest.py flatfile-ingest --tickers AAPL MSFT --years 2
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Sequence

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients.ingestion_common import load_preset
from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_client import MassiveFlatfileClient, trading_dates_between
from clients.timeframe_aggregator import aggregate_bars

log = logging.getLogger("livewire.ingest_flatfiles")

DERIVED_TIMEFRAMES = ("5m", "30m", "1h")

_WAREHOUSE_DIR = Path(
    os.environ.get("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))
)
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"


def ingest_date(
    client: MassiveFlatfileClient,
    d: date,
    *,
    target_tickers: set[str],
    bronze_dir: Path,
) -> dict[str, Any]:
    """Download one day's flat file, write 1m, derive 5m/30m/1h."""
    data = client.download_date(d, target_tickers=target_tickers)
    stats: dict[str, Any] = {"date": d.isoformat(), "tickers_written": 0, "bars_1m": 0}

    if not data:
        return stats

    for ticker, rows in data.items():
        if not rows:
            continue

        bronze_1m = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="1m")
        symbol_id = bronze_1m.get_symbol_id(ticker)
        for row in rows:
            row["symbol_id"] = symbol_id

        bronze_1m.merge_ticker_rows(ticker, rows, overwrite_existing=False)
        stats["tickers_written"] += 1
        stats["bars_1m"] += len(rows)

        for target_tf in DERIVED_TIMEFRAMES:
            agg_rows = aggregate_bars(rows, source_tf="1m", target_tf=target_tf)
            if agg_rows:
                bronze_tf = IntradayBronzeClient(
                    bronze_dir=bronze_dir, timeframe=target_tf
                )
                bronze_tf.merge_ticker_rows(ticker, agg_rows, overwrite_existing=False)

    return stats


def ingest_range(
    client: MassiveFlatfileClient,
    *,
    start: date,
    end: date,
    target_tickers: set[str],
    bronze_dir: Path,
    skip_existing: bool = False,
) -> dict[str, Any]:
    """Download and ingest a date range of flat files."""
    dates = trading_dates_between(start, end)
    stats: dict[str, Any] = {
        "dates_total": len(dates),
        "dates_processed": 0,
        "dates_skipped": 0,
        "total_tickers": 0,
        "total_bars_1m": 0,
    }

    for d in dates:
        log.info(
            "Processing %s (%d/%d)",
            d.isoformat(),
            stats["dates_processed"] + 1,
            len(dates),
        )
        day_stats = ingest_date(
            client, d, target_tickers=target_tickers, bronze_dir=bronze_dir
        )
        stats["dates_processed"] += 1
        stats["total_tickers"] += day_stats["tickers_written"]
        stats["total_bars_1m"] += day_stats["bars_1m"]

    return stats


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Bulk ingest Polygon S3 minute flat files"
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--tickers", nargs="+", help="Explicit ticker list")
    grp.add_argument("--preset", type=str, help="Preset JSON path")
    parser.add_argument(
        "--years", type=int, default=5, help="Years of history (default: 5)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show plan without downloading"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.preset:
        _, tickers, _ = load_preset(args.preset)
    else:
        tickers = [t.upper() for t in args.tickers]

    end = date.today()
    start = end - timedelta(days=365 * args.years)
    dates = trading_dates_between(start, end)

    if args.dry_run:
        log.info(
            "DRY RUN: %d tickers, %d trading days (%s to %s)",
            len(tickers),
            len(dates),
            start,
            end,
        )
        log.info("Estimated S3 downloads: %d files", len(dates))
        return 0

    bronze_dir = _DATA_LAKE / "bronze" / "asset_class=equity"
    with MassiveFlatfileClient() as client:
        stats = ingest_range(
            client,
            start=start,
            end=end,
            target_tickers=set(tickers),
            bronze_dir=bronze_dir,
        )
    log.info(
        "Done: %d dates, %d ticker-days, %d 1m bars ingested",
        stats["dates_processed"],
        stats["total_tickers"],
        stats["total_bars_1m"],
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
