#!/usr/bin/env python3
"""Reconcile Massive split and dividend events into canonical bronze Parquet."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from clients.corporate_action_store import CorporateActionStore
from clients.ingestion_common import load_preset
from clients.massive_client import MassiveClient
from clients.symbol_paths import decode_symbol
from livewire_scripts.paths import data_lake_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--tickers", nargs="+", help="Explicit ticker list")
    scope.add_argument("--preset", type=Path, help="Preset JSON containing a tickers array")
    parser.add_argument(
        "--full-reconcile",
        action="store_true",
        help="Treat absent provider events as cancellations (requires complete symbol fetches)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compare provider state without publishing")
    return parser.parse_args(list(argv) if argv is not None else None)


def _discover_symbols(root: Path) -> list[str]:
    equity_root = root / "bronze" / "asset_class=equity"
    return sorted(
        decode_symbol(path.name.removeprefix("symbol=")) for path in equity_root.glob("symbol=*") if path.is_dir()
    )


def _resolve_tickers(args: argparse.Namespace, root: Path) -> list[str]:
    if args.tickers:
        tickers = args.tickers
    elif args.preset:
        _, tickers, _ = load_preset(args.preset)
    else:
        tickers = _discover_symbols(root)
    normalized = list(dict.fromkeys(str(ticker).upper() for ticker in tickers))
    if not normalized:
        raise SystemExit("no tickers found for corporate-action reconciliation")
    return normalized


def run(
    argv: Sequence[str] | None = None,
    *,
    client: MassiveClient | None = None,
    store: CorporateActionStore | None = None,
    data_lake_root: Path | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else data_lake_dir()
    tickers = _resolve_tickers(args, root)
    massive = client or MassiveClient()
    action_store = store or CorporateActionStore(root)
    counters = {"inserted": 0, "revised": 0, "cancelled": 0, "unchanged": 0, "failed": 0}

    try:
        for ticker in tickers:
            try:
                events = [*massive.get_splits(ticker), *massive.get_dividends(ticker)]
                result = action_store.reconcile(
                    ticker,
                    events,
                    datetime.now(UTC),
                    full_reconcile=args.full_reconcile,
                    dry_run=args.dry_run,
                )
            except Exception as exc:
                counters["failed"] += 1
                print(f"{ticker}: {exc}", file=sys.stderr)
                continue
            for key in ("inserted", "revised", "cancelled", "unchanged"):
                counters[key] += int(getattr(result, key))
    finally:
        if client is None:
            massive.close()

    print(json.dumps(counters, sort_keys=True))
    return 1 if counters["failed"] else 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
