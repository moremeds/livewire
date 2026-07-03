"""Archive OTC/pinksheet equity symbols to bronze-delisted.

Identifies equity tickers present in the bronze data lake but absent from
the Massive SIP day_aggs universe, then moves their entire symbol directories
to bronze-delisted so they are excluded from future syncs, backfills, and
coverage tracking.

Usage:
    python scripts/livewire_store.py archive-otc            # Archive all non-SIP tickers
    python scripts/livewire_store.py archive-otc --dry-run  # Preview without moving
    python scripts/livewire_store.py archive-otc --sip-date 2026-06-11  # Specific SIP date
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pyarrow.parquet as pq

_DEFAULT_WAREHOUSE = Path.home() / "market-warehouse"


def load_sip_universe(raw_base: Path, date: str | None = None) -> tuple[set[str], str]:
    """Return (tickers, date_used) from Massive SIP day_aggs raw parquet files.

    Picks the latest available date when *date* is None.
    """
    date_dirs = sorted(raw_base.glob("date=*/"))
    if not date_dirs:
        raise FileNotFoundError(f"No day_aggs raw data found under {raw_base}")

    if date is not None:
        target = raw_base / f"date={date}"
        if not target.is_dir():
            raise FileNotFoundError(f"SIP date not found: {target}")
        date_used = date
    else:
        target = date_dirs[-1]
        date_used = target.name.removeprefix("date=")

    bucket_files = sorted(f for f in target.glob("*.parquet") if not f.name.startswith("."))
    if not bucket_files:
        raise FileNotFoundError(f"No parquet bucket files in {target}")

    tickers: set[str] = set()
    for f in bucket_files:
        tbl = pq.read_table(f, columns=["ticker"])
        tickers.update(tbl.column("ticker").to_pylist())
    return tickers, date_used


def find_non_sip_symbols(bronze_equity: Path, sip_universe: set[str]) -> list[str]:
    """Return symbol names in bronze_equity absent from sip_universe, sorted."""
    return sorted(
        sym_dir.name.removeprefix("symbol=")
        for sym_dir in bronze_equity.glob("symbol=*/")
        if sym_dir.name.removeprefix("symbol=") not in sip_universe
    )


def archive_symbol(
    sym: str,
    bronze_equity: Path,
    delisted_equity: Path,
    dry_run: bool,
) -> str:
    """Move symbol=X/ from bronze to bronze-delisted.

    Returns one of: ``"archived"``, ``"skipped_exists"``, ``"dry_run"``.
    """
    dst = delisted_equity / f"symbol={sym}"
    if dst.exists():
        return "skipped_exists"
    if dry_run:
        return "dry_run"
    delisted_equity.mkdir(parents=True, exist_ok=True)
    shutil.move(str(bronze_equity / f"symbol={sym}"), str(dst))
    return "archived"


def run_archive(
    bronze_equity: Path,
    delisted_equity: Path,
    raw_base: Path,
    sip_date: str | None,
    dry_run: bool,
) -> dict[str, int]:
    """Identify and archive non-SIP tickers. Returns stats dict."""
    sip_universe, date_used = load_sip_universe(raw_base, sip_date)
    print(f"SIP universe: {len(sip_universe)} tickers (date={date_used})")

    candidates = find_non_sip_symbols(bronze_equity, sip_universe)
    print(f"Non-SIP tickers in bronze: {len(candidates)}")

    if not candidates:
        print("Nothing to archive.")
        return {"archived": 0, "skipped_exists": 0, "dry_run": 0}

    stats: dict[str, int] = {"archived": 0, "skipped_exists": 0, "dry_run": 0}
    for sym in candidates:
        result = archive_symbol(sym, bronze_equity, delisted_equity, dry_run)
        stats[result] += 1
        if result == "skipped_exists":
            print(f"  SKIP (already in delisted): {sym}")

    verb = "Would archive" if dry_run else "Archived"
    n = stats["dry_run"] if dry_run else stats["archived"]
    print(f"\n{verb} {n} tickers, skipped {stats['skipped_exists']} already in bronze-delisted")
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Archive OTC/pinksheet equity symbols to bronze-delisted"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without moving files")
    parser.add_argument(
        "--sip-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="SIP day_aggs date to use as active universe (default: latest available)",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=_DEFAULT_WAREHOUSE,
        help="Path to market-warehouse root (default: ~/market-warehouse)",
    )
    args = parser.parse_args(argv)

    warehouse = args.warehouse
    raw_base = warehouse / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "day_aggs_v1"
    bronze_equity = warehouse / "data-lake" / "bronze" / "asset_class=equity"
    delisted_equity = warehouse / "data-lake" / "bronze-delisted" / "asset_class=equity"

    if not bronze_equity.is_dir():
        print(f"ERROR: bronze equity dir not found: {bronze_equity}")
        return 1

    run_archive(
        bronze_equity=bronze_equity,
        delisted_equity=delisted_equity,
        raw_base=raw_base,
        sip_date=args.sip_date,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
