"""Offline basis-consistency audit over the legacy equity population.

Classifies each symbol ``clean`` or ``mixed`` by building its adjusted daily
series and running the same continuity invariant used at publish time. A ``mixed``
symbol is one whose adjusted series still has a >threshold adjacent-day jump —
the signature of already-adjusted rows mislabeled ``price_basis='raw'``. Read-only:
computes and reports, never mutates bronze. The IB re-derivation (repair) consumes
this manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ingestion_common import load_preset
from clients.silver_continuity import ContinuityBreak, check_adjusted_continuity
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 1
CONTINUITY_THRESHOLD = 6.0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+")
    scope.add_argument("--full", action="store_true")
    scope.add_argument("--preset", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--continuity-threshold", type=float, default=CONTINUITY_THRESHOLD)
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _classify(bronze: BronzeClient, store: CorporateActionStore, symbol: str, as_of: date, threshold: float) -> dict:
    rows = bronze.read_symbol_rows(symbol)
    path = bronze.symbol_path(symbol)
    entry: dict = {
        "symbol": symbol,
        "path": str(path),
        "source_sha256": _sha256(path),
        "klass": "clean",
        "max_ratio": None,
        "break_date": None,
    }
    if not rows:
        return entry
    # Isolate ALL per-symbol failures so a single bad symbol never aborts --full.
    try:
        actions = store.latest_active(symbol)
        intervals = build_factor_intervals(rows, actions, as_of)
        adjusted = adjust_daily_rows(rows, intervals, revision=1)
        check_adjusted_continuity(adjusted, threshold=threshold)
    except ContinuityBreak as exc:
        entry["klass"] = "mixed"
        entry["break_date"] = exc.date
        entry["max_ratio"] = exc.ratio
    except Exception as exc:
        # build/adjust errors (e.g. `unknown price_basis` rows → WS3's 593, not a
        # legacy-basis mix) or a non-positive-close ValueError. NOT fed to repair.
        entry["klass"] = "error"
        entry["error"] = str(exc)
    return entry


def _resolve_symbols(args, bronze: BronzeClient) -> list[str]:
    if args.full:
        return sorted(bronze.get_existing_symbols())
    if args.preset:
        _, tickers, _ = load_preset(args.preset)
        return [t.upper() for t in tickers]
    return [t.upper() for t in args.tickers]


def run(
    argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None, as_of_date: date | None = None
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    as_of = as_of_date or datetime.now(UTC).date()
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    store = CorporateActionStore(root)

    symbols = _resolve_symbols(args, bronze)
    existing = bronze.get_existing_symbols()
    entries = [_classify(bronze, store, s, as_of, args.continuity_threshold) for s in symbols if s in existing]
    counts = {k: sum(e["klass"] == k for e in entries) for k in ("clean", "mixed", "error")}
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "data_lake_root": str(root.resolve()),
        "as_of_date": as_of.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "threshold": args.continuity_threshold,
        "symbols": sorted(entries, key=lambda e: e["symbol"]),
        "counts": counts,
    }
    _write_atomic(args.output, manifest)
    print(json.dumps({**counts, "output": str(args.output)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
