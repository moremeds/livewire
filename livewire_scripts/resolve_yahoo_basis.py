"""Read-only Yahoo basis resolver (dry-run).

For each requested symbol: fetch the Yahoo split-adjusted series + splits, reconcile
Yahoo's splits against our action store, reconstruct the true raw close per date, and
classify every existing bronze row as already-raw (relabel), adjusted (rewrite), or
neither (mismatch). Then confirm the corrected series would stage in Silver
(``build_factor_intervals`` no longer raises ``unknown price_basis``).

This NEVER writes bronze — it produces the manifest an operator reviews before the
apply step. Symbols whose splits do not reconcile, or that carry mismatch rows, fail
closed and are reported, not written.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from clients.adjustment_engine import build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.yahoo_basis import classify_existing_basis, reconcile_splits, reconstruct_raw_closes
from clients.yahoo_client import YahooClient, YahooError, YahooNotFound
from livewire_scripts.paths import data_lake_dir

_IB_EARLIEST = date(1962, 1, 1)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--symbols-file", type=Path, help="JSON list, or {list_name: [...]}, of symbols")
    parser.add_argument("--symbols-key", default="RESOLVED_validated", help="key to read when --symbols-file is a dict")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
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


def resolve_symbol(symbol: str, *, bronze: BronzeClient, store: CorporateActionStore, yahoo, as_of: date) -> dict:
    existing = bronze.read_symbol_rows(symbol)
    if not existing:
        return {"symbol": symbol, "status": "no_bronze_rows"}
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
    yahoo_raw = reconstruct_raw_closes(ybars, ysplits)
    yahoo_adjusted = {bar.trade_date: bar.close for bar in ybars}
    classification = classify_existing_basis(existing, yahoo_raw, yahoo_adjusted)
    result = {
        "symbol": symbol,
        "rows": len(existing),
        "relabel": len(classification.relabel),
        "rewrite": len(classification.rewrite),
        "mismatch": len(classification.mismatch),
        "unmatched": len(classification.unmatched),
        "store_missing_splits": [[d.isoformat(), r] for d, r in reconciliation.yahoo_only],
        "store_spurious_splits": [[d.isoformat(), r] for d, r in reconciliation.store_only],
    }
    if not reconciliation.reconciled:
        result["status"] = "split_mismatch"
        return result
    if not classification.clean:
        result["status"] = "row_mismatch"
        result["mismatch_sample"] = [
            [d.isoformat(), round(c, 4), round(r, 4), round(a, 4)] for d, c, r, a in classification.mismatch[:5]
        ]
        return result
    # Would the corrected series stage? Rewrite adjusted rows to raw, stamp basis, and
    # confirm build_factor_intervals no longer raises `unknown price_basis`.
    rewrite = set(classification.rewrite)
    corrected = [
        {**row, "price_basis": "raw", "close": yahoo_raw.get(_as_date(row["trade_date"]), row["close"])}
        if _as_date(row["trade_date"]) in rewrite
        else {**row, "price_basis": "raw"}
        for row in existing
    ]
    try:
        build_factor_intervals(corrected, actions, as_of)
        result["status"] = "would_resolve"
    except Exception as exc:
        result["status"] = "stage_fail"
        result["detail"] = str(exc)[:120]
    return result


def _as_date(value) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


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
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    store = CorporateActionStore(root)
    yahoo = yahoo_factory()
    counts: dict[str, int] = {}
    results = []
    for symbol in _symbols(args):
        try:
            entry = resolve_symbol(symbol, bronze=bronze, store=store, yahoo=yahoo, as_of=as_of)
        except Exception as exc:  # one bad symbol never aborts the sweep
            entry = {"symbol": symbol, "status": "error", "detail": str(exc)[:120]}
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
        results.append(entry)
        print(f"{symbol}: {entry['status']}", file=sys.stderr)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {"data_lake_root": str(root.resolve()), "as_of": as_of.isoformat(), "counts": counts, "symbols": results},
            indent=2,
            sort_keys=True,
        )
    )
    print(json.dumps({"counts": counts, "symbols": len(results)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
