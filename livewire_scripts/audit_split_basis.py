#!/usr/bin/env python3
"""Audit equity Bronze split basis and write a read-only repair manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.price_basis import classify_split_events, normalize_ib_rows
from clients.symbol_paths import encode_symbol
from livewire_scripts.paths import data_lake_dir

NEW_YORK = ZoneInfo("America/New_York")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+")
    scope.add_argument("--full", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _classification_payload(item) -> dict:
    payload = asdict(item)
    payload["ex_date"] = item.ex_date.isoformat()
    payload["split_factor"] = str(item.split_factor)
    return payload


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    as_of_date: date | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else args.data_lake_root or data_lake_dir()
    root = root.resolve()
    bronze_root = root / "bronze/asset_class=equity"
    bronze = BronzeClient(bronze_root, "equity")
    actions = CorporateActionStore(root)
    effective_as_of = as_of_date or datetime.now(NEW_YORK).date()
    symbols = (
        sorted(bronze.get_existing_symbols()) if args.full else list(dict.fromkeys(s.upper() for s in args.tickers))
    )
    manifest_symbols = []
    failed = 0
    for symbol in symbols:
        path = bronze_root / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
        rows = bronze.read_symbol_rows(symbol)
        staged = [{**row, "source": "ib", "price_basis": "unknown"} for row in rows]
        classifications = classify_split_events(staged, actions.latest_active(symbol), effective_as_of)
        eligible = all(item.treatment != "ambiguous" for item in classifications)
        replacements = []
        if eligible:
            proposed = normalize_ib_rows(staged, classifications)
            for old, new in zip(rows, proposed, strict=True):
                new["source"] = old["source"]
            replacements = [
                {"trade_date": old["trade_date"], "original": old, "proposed": new}
                for old, new in zip(rows, proposed, strict=True)
                if old != new
            ]
        else:
            failed += 1
        manifest_symbols.append(
            {
                "approved": False,
                "classifications": [_classification_payload(item) for item in classifications],
                "eligible": eligible,
                "path": str(path),
                "replacements": replacements,
                "source_sha256": _sha256(path),
                "symbol": symbol,
            }
        )
    payload = {
        "as_of_date": effective_as_of.isoformat(),
        "data_lake_root": str(root),
        "schema_version": 1,
        "symbols": manifest_symbols,
    }
    _write_atomic(args.output, payload)
    print(json.dumps({"failed": failed, "output": str(args.output), "symbols": len(symbols)}, sort_keys=True))
    return 1 if failed else 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
