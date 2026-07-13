#!/usr/bin/env python3
"""Write a read-only per-split daily price-basis calibration report."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.price_basis import classify_split_events
from livewire_scripts.paths import data_lake_dir

NEW_YORK = ZoneInfo("America/New_York")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+")
    scope.add_argument("--full", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--as-of", type=date.fromisoformat)
    return parser.parse_args(list(argv) if argv is not None else None)


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def run(argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else args.data_lake_root or data_lake_dir()
    root = root.resolve()
    as_of = args.as_of or datetime.now(NEW_YORK).date()
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    action_store = CorporateActionStore(root)
    symbols = (
        sorted(bronze.get_existing_symbols())
        if args.full
        else list(dict.fromkeys(symbol.upper() for symbol in args.tickers))
    )
    reports = []
    passed = True
    for symbol in symbols:
        rows = [{**row, "source": "ib", "price_basis": "unknown"} for row in bronze.read_symbol_rows(symbol)]
        events = []
        for item in classify_split_events(rows, action_store.latest_active(symbol), as_of):
            event = asdict(item)
            event["ex_date"] = item.ex_date.isoformat()
            event["split_factor"] = str(item.split_factor)
            event["passed"] = item.treatment != "ambiguous"
            events.append(event)
        symbol_passed = all(event["passed"] for event in events)
        passed = passed and symbol_passed
        reports.append({"symbol": symbol, "passed": symbol_passed, "events": events})
    payload = {
        "as_of_date": as_of.isoformat(),
        "data_lake_root": str(root),
        "passed": passed,
        "symbols": reports,
    }
    _write_atomic(args.output, payload)
    print(json.dumps({"output": str(args.output), "passed": passed, "symbols": len(symbols)}, sort_keys=True))
    return 0 if passed else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
