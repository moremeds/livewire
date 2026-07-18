"""Evidence-gated Yahoo -> action-store split repair.

For each symbol whose store splits do not reconcile with Yahoo's:

* ADD every split Yahoo records that the store lacks (Yahoo is the penny-validated deep
  split authority; our store carries the documented pre-2003 gap).
* CANCEL a spurious store split (Yahoo lacks it) only when we can show it is not real:
  - ratio in [0.95, 1.05]  -> a stock/scrip dividend mis-recorded as a split; safe.
  - otherwise -> only if Yahoo's own raw series is SMOOTH across the ex-date. Yahoo folds
    every real split, so a smooth boundary means no split happened there (a true phantom).
    A visible fold (or a date outside Yahoo's coverage) is KEPT and reported, never
    cancelled -- it could be a genuine split Yahoo missed.

Dry-run (default) writes a plan manifest and never mutates. ``--apply`` backs the store
parquet up verbatim first (undoable with ``--rollback --output-dir``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from clients.corporate_action_store import CorporateActionStore, SplitAddition
from clients.symbol_paths import canonical_symbol, encode_symbol
from clients.yahoo_basis import reconcile_splits, reconstruct_raw_closes
from clients.yahoo_client import YahooClient, YahooError, YahooNotFound
from livewire_scripts.paths import data_lake_dir

_IB_EARLIEST = date(1962, 1, 1)
# A store "split" whose ratio sits in this band is a stock/scrip dividend, not a split.
_STOCK_DIV_BAND = (0.95, 1.05)
# Cancel a real-ratio spurious split only when the observed Yahoo-raw boundary step is
# closer to no-fold (1.0) than to a full fold (1/ratio) — i.e. within this fraction of
# the way. Erring small keeps genuine folds (possible Yahoo gaps) rather than cancelling.
_PHANTOM_FRAC = 0.5


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--symbols-file", type=Path, help="JSON list, or {key: [...]}, of symbols")
    parser.add_argument("--symbols-key", default="split_mismatch", help="key to read when --symbols-file is a dict")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--apply", action="store_true", help="mutate the store (default: dry-run plan only)")
    parser.add_argument("--rollback", action="store_true", help="restore store parquet from --output-dir backups")
    parser.add_argument(
        "--output-dir", type=Path, help="backups + rollback sidecars (required with --apply/--rollback)"
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _symbols(args: argparse.Namespace) -> list[str]:
    if args.tickers:
        return [t.upper() for t in args.tickers]
    if args.symbols_file:
        payload = json.loads(args.symbols_file.read_text())
        raw = payload[args.symbols_key] if isinstance(payload, dict) else payload
        return [str(t).upper() for t in raw]
    raise ValueError("provide --tickers or --symbols-file")


def _store_split_ratios(actions) -> list[tuple[date, float]]:
    return [
        (a.ex_date, float(a.split_to) / float(a.split_from))
        for a in actions
        if a.action_type == "split" and a.status == "active" and a.split_from and a.split_to
    ]


def _phantom_verdict(ex_date: date, ratio: float, yahoo_raw: dict[date, float], raw_dates: list[date]) -> dict:
    """Is a spurious real-ratio store split at ``ex_date`` a phantom (no real event)?

    Yahoo folds every real split, so if Yahoo's raw close is smooth across the boundary
    there was no split. A visible ~1/ratio fold, or a boundary outside Yahoo's coverage,
    is inconclusive -> keep."""
    prior = [d for d in raw_dates if d < ex_date]
    on_after = [d for d in raw_dates if d >= ex_date]
    if not prior or not on_after:
        return {"phantom": False, "evidence": "no_yahoo_boundary"}
    before = yahoo_raw[prior[-1]]
    after = yahoo_raw[on_after[0]]
    if before <= 0 or after <= 0:
        return {"phantom": False, "evidence": "nonpositive_raw"}
    step = after / before
    fold_target = 1.0 / ratio
    observed = abs(math.log(step))
    full_fold = abs(math.log(fold_target))
    phantom = full_fold > 0 and observed < full_fold * _PHANTOM_FRAC
    return {"phantom": phantom, "evidence": f"step={step:.4f} fold_target={fold_target:.4f}"}


def plan_symbol(symbol: str, *, store: CorporateActionStore, yahoo, as_of: date) -> dict:
    try:
        ybars, ysplits = yahoo.get_daily(symbol, _IB_EARLIEST, as_of)
    except YahooNotFound:
        return {"symbol": symbol, "status": "yahoo_missing"}
    except YahooError as exc:
        return {"symbol": symbol, "status": "yahoo_error", "detail": str(exc)[:80]}
    if not ybars:
        return {"symbol": symbol, "status": "yahoo_empty"}
    actions = store.latest_active(symbol)
    reconciliation = reconcile_splits(ysplits, _store_split_ratios(actions))
    if reconciliation.reconciled:
        return {"symbol": symbol, "status": "already_reconciled"}
    yahoo_raw = reconstruct_raw_closes(ybars, ysplits)
    raw_dates = sorted(yahoo_raw)
    ysplit_by_exdate = {s.ex_date: s for s in ysplits}

    adds = []
    for ex_date, mult in reconciliation.yahoo_only:
        ys = ysplit_by_exdate.get(ex_date)
        if ys is None:  # pragma: no cover - reconcile only emits ex-dates it saw
            continue
        adds.append(
            {
                "ex_date": ex_date.isoformat(),
                "split_from": float(ys.denominator),
                "split_to": float(ys.numerator),
                "ratio": round(mult, 6),
            }
        )

    cancel_safe, cancel_phantom, kept = [], [], []
    for ex_date, ratio in reconciliation.store_only:
        if _STOCK_DIV_BAND[0] <= ratio <= _STOCK_DIV_BAND[1]:
            cancel_safe.append({"ex_date": ex_date.isoformat(), "ratio": round(ratio, 6), "reason": "stock_dividend"})
            continue
        verdict = _phantom_verdict(ex_date, ratio, yahoo_raw, raw_dates)
        entry = {"ex_date": ex_date.isoformat(), "ratio": round(ratio, 6), "evidence": verdict["evidence"]}
        (cancel_phantom if verdict["phantom"] else kept).append(entry)

    # Reconciles after repair only if nothing spurious is left un-cancelled.
    would = not kept
    return {
        "symbol": symbol,
        "status": "would_reconcile" if would else "partial",
        "adds": adds,
        "cancel_safe": cancel_safe,
        "cancel_phantom": cancel_phantom,
        "kept_ambiguous": kept,
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2))
    os.replace(tmp, path)


def _backup_store(symbol: str, *, store: CorporateActionStore, output_dir: Path) -> tuple[Path, str]:
    source = store.path_for(symbol)
    original = source.read_bytes()
    sha = hashlib.sha256(original).hexdigest()
    backup_path = output_dir / "backup" / f"{encode_symbol(canonical_symbol(symbol))}.events.parquet"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = backup_path.with_name(f".{backup_path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_bytes(original)
        os.replace(tmp, backup_path)
    finally:
        tmp.unlink(missing_ok=True)
    return backup_path, sha


def apply_symbol(symbol: str, *, store: CorporateActionStore, yahoo, as_of: date, output_dir: Path) -> dict:
    plan = plan_symbol(symbol, store=store, yahoo=yahoo, as_of=as_of)
    if plan["status"] not in ("would_reconcile", "partial"):
        return {**plan, "applied": "skipped"}
    backup_path, sha = _backup_store(symbol, store=store, output_dir=output_dir)
    sidecar_path = output_dir / "symbols" / f"{encode_symbol(canonical_symbol(symbol))}.json"
    _write_json(
        sidecar_path, {"symbol": symbol, "status": "in_progress", "backup_path": str(backup_path), "backup_sha256": sha}
    )
    add_splits = [SplitAddition(date.fromisoformat(a["ex_date"]), a["split_from"], a["split_to"]) for a in plan["adds"]]
    cancel_ex = [date.fromisoformat(c["ex_date"]) for c in (*plan["cancel_safe"], *plan["cancel_phantom"])]
    result = store.apply_repairs(symbol, add_splits=add_splits, cancel_ex_dates=cancel_ex, fetched_at=datetime.now(UTC))
    _write_json(
        sidecar_path,
        {
            "symbol": symbol,
            "status": "done",
            "backup_path": str(backup_path),
            "backup_sha256": sha,
            "added": result.added,
            "cancelled": result.cancelled,
        },
    )
    return {**plan, "applied": {"added": result.added, "cancelled": result.cancelled}}


def rollback(output_dir: Path, *, store: CorporateActionStore, tickers: list[str] | None = None) -> int:
    restored = 0
    for sidecar in sorted((output_dir / "symbols").glob("*.json")):
        entry = json.loads(sidecar.read_text())
        symbol = entry["symbol"]
        if tickers and symbol.upper() not in {t.upper() for t in tickers}:
            continue
        backup = Path(entry["backup_path"])
        if not backup.is_file():
            print(f"{symbol}: backup missing, skipped", file=sys.stderr)
            continue
        target = store.path_for(symbol)
        tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        tmp.write_bytes(backup.read_bytes())
        os.replace(tmp, target)
        restored += 1
        print(f"{symbol}: restored", file=sys.stderr)
    print(json.dumps({"restored": restored}, sort_keys=True))
    return 0


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    yahoo_factory: Callable[[], object] = YahooClient,
    as_of_date: date | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    as_of = as_of_date or datetime.now(UTC).date()
    store = CorporateActionStore(root)
    if args.apply and args.output_dir is None:
        raise ValueError("--apply requires --output-dir")
    if args.rollback:
        if args.output_dir is None:
            raise ValueError("--rollback requires --output-dir")
        return rollback(args.output_dir, store=store, tickers=args.tickers)

    yahoo = yahoo_factory()
    counts: dict[str, int] = {}
    totals = {"adds": 0, "cancel_safe": 0, "cancel_phantom": 0, "kept_ambiguous": 0}
    results = []
    for symbol in _symbols(args):
        try:
            if args.apply:
                entry = apply_symbol(symbol, store=store, yahoo=yahoo, as_of=as_of, output_dir=args.output_dir)
            else:
                entry = plan_symbol(symbol, store=store, yahoo=yahoo, as_of=as_of)
        except Exception as exc:  # one bad symbol never aborts the sweep
            entry = {"symbol": symbol, "status": "error", "detail": str(exc)[:120]}
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
        for key in totals:
            totals[key] += len(entry.get(key, []))
        results.append(entry)
        applied = entry.get("applied")
        print(f"{symbol}: {entry['status']}{f' [{applied}]' if applied else ''}", file=sys.stderr)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "data_lake_root": str(root.resolve()),
                "as_of": as_of.isoformat(),
                "counts": counts,
                "totals": totals,
                "symbols": results,
            },
            indent=2,
            sort_keys=True,
        )
    )
    print(json.dumps({"counts": counts, "totals": totals}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
