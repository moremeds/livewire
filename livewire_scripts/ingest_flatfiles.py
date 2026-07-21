"""Full-market Massive flat-file ingestion CLI."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from datetime import date, timedelta

from clients.massive_flatfile_client import MassiveFlatfileClient, require_flatfile_credentials
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from clients.trading_calendar import trading_dates_in_range
from livewire_scripts.flatfile_downloader import download_dates
from livewire_scripts.flatfile_planner import discover_plan, require_capacity
from livewire_scripts.flatfile_publisher import PublishStats, publish_dates
from livewire_scripts.paths import cursor_dir, warehouse_dir

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
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("MDW_FLATFILE_WORKERS", "1")),
        help="Parallel worker count for download+stage and per-bucket publish (default 1; env MDW_FLATFILE_WORKERS).",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("clients.intraday_bronze_client").setLevel(logging.WARNING)
    _require_credentials()

    warehouse = warehouse_dir()
    store = MassiveFlatfileStore(warehouse, bucket_count=int(os.getenv("MDW_FLATFILE_BUCKETS", "256")))
    state = MassiveFlatfileState(cursor_dir())
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
            try:
                require_capacity(plan)
            except RuntimeError as exc:
                raise SystemExit(str(exc)) from exc
        dates = _parse_dates(args, plan.dates)
        log.info("Workers: %d (download+stage and per-bucket publish)", args.workers)
        download_stats = download_dates(
            client, store, state, dates, replace=args.mode == "repair", workers=args.workers
        )
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
        workers=args.workers,
    )
    log.info(
        "Downloaded=%d skipped=%d published_tickers=%d",
        download_stats.downloaded,
        download_stats.skipped,
        publish_stats["tickers"],
    )
    quarantined = publish_stats.get("quarantined") or []
    if quarantined:
        log.error(
            "%d symbol(s) had unreadable parquet and were quarantined; each needs a targeted backfill: %s",
            len(quarantined),
            ", ".join(sorted(quarantined)),
        )
    return verify_publish_coverage(store, dates, publish_stats)


def verify_publish_coverage(
    store: MassiveFlatfileStore,
    dates: list[date],
    publish_stats: PublishStats,
    min_ratio: float | None = None,
) -> int:
    """Fail the run when publish covered far fewer tickers than the raw files hold.

    Nothing checked this before: the raw file could hold 12,000 symbols, publish
    could write 40, and the phase exited 0 — indistinguishable from a full run
    in the log, the exit code, SUMMARY_JSON and the digest.

    An unmeasurable window fails rather than passes, and a partially-resumed run
    scales the floor instead of switching the check off.
    """
    total_buckets = publish_stats.get("buckets", 0)
    resumed_buckets = publish_stats.get("resumed_buckets", 0)
    if total_buckets and resumed_buckets >= total_buckets:
        log.info("Publish coverage check skipped: all %d buckets were already complete", total_buckets)
        return 0

    expected = set()
    for day in dates:
        expected |= store.symbols_for_date(day)
    if not expected:
        # Fail, do not pass. `symbols_for_date` returns an empty set for a missing
        # or unreadable `_symbols.parquet`, so "I cannot measure coverage" used to
        # be indistinguishable from "coverage is fine" — the exact blindness this
        # function exists to remove, reproduced inside it.
        log.error(
            "Publish coverage cannot be verified: no raw symbol set for %s — "
            "the _symbols.parquet for these dates is missing or unreadable.",
            ", ".join(d.isoformat() for d in dates),
        )
        return 1

    ratio_floor = min_ratio if min_ratio is not None else float(os.getenv("MDW_FLATFILE_MIN_PUBLISH_RATIO", "0.9"))
    # Tickers skipped as already complete were published by an earlier run of this
    # scope, so they count as covered. Whole buckets skipped were never enumerated,
    # so scale the floor by the share of the window this run could actually see
    # rather than abandoning the check — one resumed bucket out of 256 used to
    # disable it outright, which is nearly every nightly catch-up.
    published = publish_stats.get("tickers", 0) + publish_stats.get("resumed", 0)
    if total_buckets:
        ratio_floor *= (total_buckets - resumed_buckets) / total_buckets
    ratio = published / len(expected)
    if ratio < ratio_floor:
        log.error(
            "Publish coverage %.1f%% (%d of %d raw tickers) is below the %.1f%% floor; "
            "the run wrote far less than the raw files hold.",
            ratio * 100,
            published,
            len(expected),
            ratio_floor * 100,
        )
        return 1
    log.info("Publish coverage %.1f%% (%d of %d raw tickers)", ratio * 100, published, len(expected))
    return 0
