#!/usr/bin/env python3
"""Atomically add legacy equity price-basis metadata to Bronze snapshots."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from pathlib import Path

import pyarrow.parquet as pq

from clients.bronze_client import BronzeClient
from clients.source_evidence import sha256_file
from clients.symbol_paths import encode_symbol
from livewire_scripts.paths import data_lake_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+")
    scope.add_argument("--full", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cursor", type=Path)
    return parser.parse_args(list(argv) if argv is not None else None)


def _write_cursor(path: Path, completed: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps({"completed": sorted(completed)}, sort_keys=True), encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def run(
    argv: Sequence[str] | None = None,
    *,
    bronze_root: Path | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(bronze_root) if bronze_root is not None else data_lake_dir() / "bronze/asset_class=equity"
    client = BronzeClient(root, "equity")
    symbols = (
        sorted(client.get_existing_symbols()) if args.full else list(dict.fromkeys(s.upper() for s in args.tickers))
    )
    cursor_path = args.cursor or (root / ".price_basis_migration_cursor.json" if args.full else None)
    completed: set[str] = set()
    if cursor_path is not None and cursor_path.exists():
        completed = set(json.loads(cursor_path.read_text(encoding="utf-8")).get("completed", []))
    artifacts: list[dict] = []
    migrated = 0
    resumed = 0
    unchanged = 0
    for symbol in symbols:
        path = root / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        names = set(pq.ParquetFile(path).schema_arrow.names)
        if {"source", "price_basis"} <= names:
            unchanged += 1
            if symbol in completed:
                resumed += 1
            elif not args.dry_run and cursor_path is not None:
                completed.add(symbol)
                _write_cursor(cursor_path, completed)
            continue
        source_hash = sha256_file(path)
        artifact = {"symbol": symbol, "path": str(path), "source_sha256": source_hash}
        if not args.dry_run:
            rows = client.read_symbol_rows(symbol)
            client.replace_ticker_rows(symbol, rows)
            artifact["target_sha256"] = sha256_file(path)
            if cursor_path is not None:
                completed.add(symbol)
                _write_cursor(cursor_path, completed)
        artifacts.append(artifact)
        migrated += 1
    print(
        json.dumps(
            {
                "artifacts": artifacts,
                "dry_run": bool(args.dry_run),
                "migrated": migrated,
                "resumed": resumed,
                "unchanged": unchanged,
            },
            sort_keys=True,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
