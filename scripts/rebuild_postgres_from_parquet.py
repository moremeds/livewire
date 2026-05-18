#!/usr/bin/env python3
"""Rebuild Postgres analytical tables from canonical bronze parquet."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.postgres_client import PostgresClient

DATA_LAKE = Path.home() / "market-warehouse" / "data-lake"
DEFAULT_TELEMETRY_PATH = Path.home() / "market-warehouse" / "logs" / "telemetry.jsonl"
DEFAULT_QUALITY_AUDIT_PATH = Path.home() / "market-warehouse" / "logs" / "quality_audit.jsonl"
VENUE_MAP = {"equity": "SMART", "volatility": "CBOE", "futures": "CME"}

console = Console()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="Postgres DSN; defaults to MDW_POSTGRES_DSN")
    parser.add_argument("--schema", default=None, help="Postgres schema; defaults to MDW_POSTGRES_SCHEMA or md")
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures"],
        default="equity",
        help="Asset class to rebuild",
    )
    parser.add_argument(
        "--timeframe",
        choices=["1d", "1h", "5m", "all"],
        default="all",
        help="Timeframe to rebuild",
    )
    parser.add_argument("--bronze-dir", type=Path, default=None, help="Bronze parquet root")
    parser.add_argument("--include-reliability", action="store_true", help="Import telemetry and quality JSONL")
    parser.add_argument("--telemetry-path", type=Path, default=DEFAULT_TELEMETRY_PATH)
    parser.add_argument("--quality-audit-path", type=Path, default=DEFAULT_QUALITY_AUDIT_PATH)
    args = parser.parse_args(argv)

    bronze_dir = args.bronze_dir or DATA_LAKE / "bronze" / f"asset_class={args.asset_class}"
    _validate_bronze_inputs(bronze_dir, args.asset_class, args.timeframe)

    venue = VENUE_MAP[args.asset_class]
    with PostgresClient(dsn=args.dsn, schema=args.schema) as db:
        db.ensure_schema()
        if args.asset_class == "futures":
            counts = db.replace_futures_from_parquet(bronze_dir)
            console.print(f"[green]Rebuilt[/green] md.futures_daily with {counts['rows']:,} rows")
        elif args.asset_class == "volatility":
            counts = db.replace_equities_from_parquet(bronze_dir, asset_class="volatility", venue=venue)
            console.print(
                f"[green]Rebuilt[/green] volatility daily with "
                f"{counts['symbols']:,} symbols and {counts['rows']:,} rows"
            )
        else:
            _rebuild_equity_timeframes(db, bronze_dir, args.timeframe)

        if args.include_reliability:
            telemetry = db.replace_telemetry_from_jsonl(args.telemetry_path)
            quality = db.replace_quality_flags_from_jsonl(args.quality_audit_path)
            console.print(
                f"Imported reliability JSONL: telemetry={telemetry['rows']:,} "
                f"quality={quality['rows']:,} skipped={telemetry['skipped'] + quality['skipped']:,}"
            )
    return 0


def _rebuild_equity_timeframes(db: PostgresClient, bronze_dir: Path, timeframe: str) -> None:
    if timeframe in ("1d", "all"):
        if _has_parquet(bronze_dir, "1d.parquet"):
            counts = db.replace_equities_from_parquet(bronze_dir, asset_class="equity", venue="SMART")
            console.print(
                f"[green]Rebuilt[/green] equity daily with "
                f"{counts['symbols']:,} symbols and {counts['rows']:,} rows"
            )
        else:
            console.print("Skipping 1d: no parquet snapshots found")
    for intraday_tf in ("1h", "5m"):
        if timeframe not in (intraday_tf, "all"):
            continue
        if not _has_parquet(bronze_dir, f"{intraday_tf}.parquet"):
            console.print(f"Skipping {intraday_tf}: no parquet snapshots found")
            continue
        counts = db.replace_equities_intraday_from_parquet(bronze_dir, intraday_tf)
        console.print(f"[green]Rebuilt[/green] md.equities_{intraday_tf} with {counts['rows']:,} rows")


def _validate_bronze_inputs(bronze_dir: Path, asset_class: str, timeframe: str) -> None:
    if not bronze_dir.exists():
        raise FileNotFoundError(f"bronze directory does not exist: {bronze_dir}")
    filenames = _expected_filenames(asset_class, timeframe)
    if not any(_has_parquet(bronze_dir, filename) for filename in filenames):
        if timeframe in ("1h", "5m"):
            raise FileNotFoundError(f"no {timeframe} parquet snapshots found under: {bronze_dir}")
        raise FileNotFoundError(f"no bronze parquet snapshots found under: {bronze_dir}")


def _expected_filenames(asset_class: str, timeframe: str) -> list[str]:
    if asset_class in ("volatility", "futures"):
        return ["1d.parquet"]
    if timeframe == "all":
        return ["1d.parquet", "1h.parquet", "5m.parquet"]
    if timeframe == "1d":
        return ["1d.parquet"]
    return [f"{timeframe}.parquet"]


def _has_parquet(bronze_dir: Path, filename: str) -> bool:
    return any(bronze_dir.glob(f"symbol=*/{filename}"))


if __name__ == "__main__":
    raise SystemExit(main())
