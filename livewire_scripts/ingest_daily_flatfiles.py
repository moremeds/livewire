"""Full-market Massive day_aggs flat-file ingestion CLI.

Mirrors livewire_scripts.ingest_flatfiles but for daily 1d bars instead of
1m intraday. The daily flat files at s3://flatfiles/us_stocks_sip/day_aggs_v1/
publish one row per ticker per trading day across the full SIP universe
(~20K tickers), so this widens the daily ingest universe well beyond the
~2.5K preset-driven `daily` command.
"""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from pathlib import Path

from clients.massive_daily_flatfile_store import MassiveDailyFlatfileStore
from clients.massive_flatfile_client import S3_PREFIX_DAILY, MassiveFlatfileClient
from clients.massive_flatfile_state import MassiveFlatfileState
from livewire_scripts.daily_flatfile_publisher import publish_daily_dates
from livewire_scripts.flatfile_downloader import download_dates
from livewire_scripts.flatfile_planner import discover_plan, require_capacity
from livewire_scripts.ingest_flatfiles import _parse_dates, _require_credentials
from livewire_scripts.paths import cursor_dir, warehouse_dir
from livewire_scripts.sync_runner import EQUITY_PRESETS, ticker_union

log = logging.getLogger("livewire.ingest_daily_flatfiles")

REPO_ROOT = Path(__file__).resolve().parent.parent


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Full-market Massive day_aggs flat-file ingestion")
    parser.add_argument("mode", choices=["discover", "backfill", "catch-up", "repair"])
    parser.add_argument("--days", type=int, default=int(os.getenv("MDW_FLATFILE_DAILY_LOOKBACK_DAYS", "14")))
    parser.add_argument("--dates", nargs="+")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("MDW_FLATFILE_DAILY_WORKERS", "4")),
        help="Parallel worker count for download and per-bucket publish (default 4; env MDW_FLATFILE_DAILY_WORKERS).",
    )
    parser.add_argument(
        "--buckets",
        type=int,
        default=int(os.getenv("MDW_FLATFILE_DAILY_BUCKETS", "32")),
        help="Raw ticker buckets per trading day (default 32; env MDW_FLATFILE_DAILY_BUCKETS).",
    )
    parser.add_argument(
        "--protect-preset",
        nargs="+",
        default=[str(REPO_ROOT / p) for p in EQUITY_PRESETS],
        help=(
            "Preset files whose tickers the `daily` command owns; this lane skips them. "
            "Everything else in the SIP universe is merged and kept current."
        ),
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("clients.bronze_client").setLevel(logging.WARNING)
    _require_credentials()

    warehouse = warehouse_dir()
    store = MassiveDailyFlatfileStore(warehouse, bucket_count=args.buckets)
    state = MassiveFlatfileState(
        cursor_dir(),
        name="massive_daily_flatfile",
    )
    with MassiveFlatfileClient(prefix=S3_PREFIX_DAILY) as client:
        plan = discover_plan(client, warehouse)
        log.info(
            "Massive day_aggs flat files: %s to %s, %d days, %.3f GiB compressed, %.3f GiB projected, %.2f GiB free",
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
            try:
                require_capacity(plan)
            except RuntimeError as exc:
                raise SystemExit(str(exc)) from exc
        dates = _parse_dates(args, plan.dates)
        log.info("Workers: %d (download and per-bucket publish)", args.workers)
        download_stats = download_dates(
            client, store, state, dates, replace=args.mode == "repair", workers=args.workers
        )
    bronze_dir = warehouse / "data-lake" / "bronze" / "asset_class=equity"
    scope = f"daily_{args.mode}_{dates[0].isoformat()}_{dates[-1].isoformat()}_{len(dates)}"
    if args.mode == "repair":
        state.reset_publish_scope(scope)
    publish_stats = publish_daily_dates(
        store,
        state,
        dates,
        bronze_dir,
        scope=scope,
        workers=args.workers,
        protected_symbols=frozenset(ticker_union(args.protect_preset)),
    )
    log.info(
        "Downloaded=%d skipped=%d published_tickers=%d rows=%d skipped_existing=%d",
        download_stats.downloaded,
        download_stats.skipped,
        publish_stats["tickers"],
        publish_stats["rows_1d"],
        publish_stats["skipped_existing"],
    )
    return 0
