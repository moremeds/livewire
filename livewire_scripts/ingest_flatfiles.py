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
from livewire_scripts.flatfile_publisher import publish_dates
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
    publish_stats: dict[str, int],
    min_ratio: float | None = None,
) -> int:
    """Fail the run when publish covered far fewer tickers than the raw files hold.

    Nothing checked this before: the raw file could hold 12,000 symbols, publish
    could write 40, and the phase exited 0 — indistinguishable from a full run
    in the log, the exit code, SUMMARY_JSON and the digest.

    Skipped on a resumed run, where the published count legitimately undercounts
    the window because earlier buckets are already complete.
    """
    if publish_stats.get("resumed"):
        log.info("Publish coverage check skipped: resumed run (%d already complete)", publish_stats["resumed"])
        return 0
    expected = set()
    for day in dates:
        expected |= store.symbols_for_date(day)
    if not expected:
        return 0
    ratio_floor = min_ratio if min_ratio is not None else float(os.getenv("MDW_FLATFILE_MIN_PUBLISH_RATIO", "0.9"))
    published = publish_stats.get("tickers", 0)
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
