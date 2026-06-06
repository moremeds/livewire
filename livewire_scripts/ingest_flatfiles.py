"""Full-market Massive flat-file ingestion CLI."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

from clients.massive_flatfile_client import MassiveFlatfileClient, require_flatfile_credentials
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from clients.trading_calendar import trading_dates_in_range
from livewire_scripts.flatfile_downloader import download_dates
from livewire_scripts.flatfile_planner import discover_plan, require_capacity
from livewire_scripts.flatfile_publisher import publish_dates

log = logging.getLogger("livewire.ingest_flatfiles")


def _require_credentials() -> None:
    try:
        require_flatfile_credentials()
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc


def _parse_dates(args: argparse.Namespace, plan_dates: tuple[date, ...]) -> list[date]:
    if args.mode == "backfill":
        return list(plan_dates)
    if args.mode == "catch-up":
        end = plan_dates[-1]
        return [d for d in plan_dates if d >= end - timedelta(days=args.days)]
    if args.dates:
        return sorted(date.fromisoformat(value) for value in args.dates)
    if args.start and args.end:
        return trading_dates_in_range(date.fromisoformat(args.start), date.fromisoformat(args.end))
    raise SystemExit("repair requires --dates or --start and --end")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Full-market Massive flat-file ingestion")
    parser.add_argument("mode", choices=["discover", "backfill", "catch-up", "repair"])
    parser.add_argument("--days", type=int, default=int(os.getenv("MDW_FLATFILE_LOOKBACK_DAYS", "7")))
    parser.add_argument("--dates", nargs="+")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    _require_credentials()

    warehouse = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
    store = MassiveFlatfileStore(warehouse, bucket_count=int(os.getenv("MDW_FLATFILE_BUCKETS", "256")))
    state = MassiveFlatfileState(Path(os.getenv("MDW_CURSOR_DIR", str(warehouse / "cursors"))))
    with MassiveFlatfileClient() as client:
        plan = discover_plan(client, warehouse)
        log.info(
            "Massive flat files: %s to %s, %d days, %.2f GiB compressed, %.2f GiB projected, %.2f GiB free",
            plan.earliest,
            plan.latest,
            len(plan.dates),
            plan.compressed_bytes / 1024**3,
            plan.projected_bytes / 1024**3,
            plan.free_bytes / 1024**3,
        )
        state.set_discovery(
            earliest=plan.earliest,
            latest=plan.latest,
            object_count=len(plan.dates),
            compressed_bytes=plan.compressed_bytes,
        )
        if args.mode == "discover" or args.dry_run:
            return 0
        if args.mode == "backfill":
            require_capacity(plan)
        dates = _parse_dates(args, plan.dates)
        download_stats = download_dates(client, store, state, dates, replace=args.mode == "repair")
    bronze_dir = warehouse / "data-lake" / "bronze" / "asset_class=equity"
    scope = f"{args.mode}_{dates[0].isoformat()}_{dates[-1].isoformat()}_{len(dates)}"
    if args.mode == "repair":
        state.reset_publish_scope(scope)
    publish_stats = publish_dates(
        store,
        state,
        dates,
        bronze_dir,
        replace_complete=args.mode == "backfill",
        scope=scope,
    )
    log.info(
        "Downloaded=%d skipped=%d published_tickers=%d",
        download_stats.downloaded,
        download_stats.skipped,
        publish_stats["tickers"],
    )
    return 0
