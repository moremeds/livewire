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
from clients.seed_boundary import classify_seed_boundary
from clients.silver_continuity import ContinuityBreak, check_adjusted_continuity
from clients.silver_window import find_breaks
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
    path = bronze.symbol_path(symbol)
    entry: dict = {
        "symbol": symbol,
        "path": str(path),
        "source_sha256": _sha256(path) if path.is_file() else None,
        "klass": "clean",
        "max_ratio": None,
        "break_date": None,
        "breaks": [],
        "detector": None,
        "seed_boundary": None,
    }
    if not path.is_file():
        entry["klass"] = "error"
        entry["error"] = f"symbol not in bronze: {symbol}"
        return entry
    rows = bronze.read_symbol_rows(symbol)
    if not rows:
        # An empty parquet is a broken symbol, not a clean one — never silently pass.
        entry["klass"] = "error"
        entry["error"] = "no bronze rows"
        return entry
    # Isolate ALL per-symbol failures so a single bad symbol never aborts --full.
    try:
        actions = store.latest_active(symbol)
    except Exception as exc:
        entry["klass"] = "error"
        entry["error"] = str(exc)
        return entry
    # Seed-boundary first: deterministic (known location, predicted fold), so it
    # resolves the sub-threshold population the continuity heuristic cannot see.
    seed = classify_seed_boundary(rows, actions)
    entry["seed_boundary"] = seed
    if seed["verdict"] == "corrupt":
        entry["klass"] = "mixed"
        entry["detector"] = "seed_boundary"
        entry["break_date"] = seed["date"]
        entry["max_ratio"] = seed["observed"]
        return entry
    try:
        intervals = build_factor_intervals(rows, actions, as_of)
        adjusted = adjust_daily_rows(rows, intervals, revision=1)
        # Enumerate EVERY break, not just the first: each one is a triage candidate,
        # and a break the audit never reports is a break that never gets triaged and
        # whose real history the window then trims away permanently.
        entry["breaks"] = find_breaks(adjusted, threshold=threshold)
        # Not a redundant second scan of what find_breaks just computed — do NOT
        # "simplify" it into `if entry["breaks"]`. The two disagree on purpose about a
        # non-positive close: find_breaks records it as a break (ratio None), this
        # raises a plain ValueError, which the handler below routes to `error`. That
        # routing matters — `mixed` is fed to IB repair, `error` is not, and a symbol
        # with a zero close is not a basis problem repair can fix.
        check_adjusted_continuity(adjusted, threshold=threshold)
    except ContinuityBreak as exc:
        entry["klass"] = "mixed"
        entry["detector"] = "continuity"
        entry["break_date"] = exc.date
        entry["max_ratio"] = exc.ratio
    except Exception as exc:
        # build/adjust errors (e.g. `unknown price_basis` rows → WS3's backlog) or a
        # non-positive-close ValueError. NOT fed to repair.
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
    # Do NOT filter against get_existing_symbols(): a requested symbol that is
    # missing must surface as an `error` entry, not vanish from the manifest.
    entries = [_classify(bronze, store, s, as_of, args.continuity_threshold) for s in symbols]
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
