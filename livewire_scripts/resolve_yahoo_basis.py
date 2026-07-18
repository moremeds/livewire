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
import hashlib
import json
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from clients.adjustment_engine import build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.symbol_paths import encode_symbol
from clients.yahoo_basis import classify_existing_basis, reconcile_splits, reconstruct_raw_closes
from clients.yahoo_client import YahooClient, YahooError, YahooNotFound
from livewire_scripts.paths import data_lake_dir

_IB_EARLIEST = date(1962, 1, 1)
# Above this share of rows disagreeing with Yahoo, the ticker is a different series
# (reuse / wrong listing), not an isolated bad bar → fail closed.
_MAX_MISMATCH_FRACTION = 0.05


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--symbols-file", type=Path, help="JSON list, or {list_name: [...]}, of symbols")
    parser.add_argument("--symbols-key", default="RESOLVED_validated", help="key to read when --symbols-file is a dict")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--apply", action="store_true", help="write bronze (default: dry-run, no writes)")
    parser.add_argument("--output-dir", type=Path, help="backups + rollback sidecars (required with --apply)")
    parser.add_argument(
        "--relabel-only",
        action="store_true",
        help="apply only zero-value-change symbols (rewrite==0): flip price_basis to raw, never touch prices",
    )
    parser.add_argument(
        "--allow-rewrite",
        action="store_true",
        help="also apply symbols with adjusted deep rows (rewrite>0): rewrite full OHLCV to true raw",
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


@dataclass(frozen=True)
class _Resolution:
    """Outcome of resolving one symbol. ``corrected`` (the full-OHLCV true-raw series)
    is populated only when ``result['status'] == 'would_resolve'`` — the apply step
    writes exactly these rows, so it can never diverge from what staging validated."""

    result: dict
    corrected: list[dict] | None
    actions: list


def _resolve(symbol: str, *, bronze: BronzeClient, store: CorporateActionStore, yahoo, as_of: date) -> _Resolution:
    existing = bronze.read_symbol_rows(symbol)
    if not existing:
        return _Resolution({"symbol": symbol, "status": "no_bronze_rows"}, None, [])
    try:
        ybars, ysplits = yahoo.get_daily(symbol, _IB_EARLIEST, as_of)
    except YahooNotFound:
        return _Resolution({"symbol": symbol, "status": "yahoo_missing"}, None, [])
    except YahooError as exc:
        return _Resolution({"symbol": symbol, "status": "yahoo_error", "detail": str(exc)[:80]}, None, [])
    if not ybars:
        return _Resolution({"symbol": symbol, "status": "yahoo_empty"}, None, [])
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
        return _Resolution(result, None, actions)
    # An isolated row that matches neither raw nor adjusted is kept at its bronze value
    # and flagged (operator decision: never overwrite bronze on Yahoo's word alone). But
    # a LARGE mismatch fraction is not isolated noise — it is a different series behind the
    # ticker (reuse / wrong listing), so fail closed rather than publish a chimera.
    result["mismatch_fraction"] = round(len(classification.mismatch) / len(existing), 4)
    if result["mismatch_fraction"] > _MAX_MISMATCH_FRACTION:
        result["status"] = "high_mismatch"
        result["mismatch_sample"] = [
            [d.isoformat(), round(c, 4), round(r, 4), round(a, 4)] for d, c, r, a in classification.mismatch[:5]
        ]
        return _Resolution(result, None, actions)
    # Corrected series: rewrite adjusted rows to true raw (full OHLCV, not just close —
    # a split scales all price fields uniformly and volume inversely); keep relabel and
    # flagged-mismatch rows at their existing value; stamp every row price_basis='raw'.
    # Confirm build_factor_intervals no longer raises `unknown price_basis`. A flagged row
    # that is genuinely bad and >threshold off is caught by the window continuity scan.
    corrected = _corrected_rows(existing, yahoo_raw, yahoo_adjusted, classification.rewrite)
    try:
        build_factor_intervals(corrected, actions, as_of)
        result["status"] = "would_resolve"
        if classification.mismatch:
            result["flagged"] = [
                [d.isoformat(), round(c, 4), round(r, 4)] for d, c, r, a in classification.mismatch[:20]
            ]
    except Exception as exc:
        result["status"] = "stage_fail"
        result["detail"] = str(exc)[:120]
        return _Resolution(result, None, actions)
    return _Resolution(result, corrected, actions)


def resolve_symbol(symbol: str, *, bronze: BronzeClient, store: CorporateActionStore, yahoo, as_of: date) -> dict:
    return _resolve(symbol, bronze=bronze, store=store, yahoo=yahoo, as_of=as_of).result


def _as_date(value) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _scale_row(row: dict, factor: float) -> dict:
    """Rescale one adjusted row to raw. A split scales every price field by ``factor``
    and volume inversely, so OHLC internal ratios (the bar's shape) are preserved."""
    scaled = {
        **row,
        "open": float(row["open"]) * factor,
        "high": float(row["high"]) * factor,
        "low": float(row["low"]) * factor,
        "close": float(row["close"]) * factor,
        "adj_close": float(row["adj_close"]) * factor,
        "price_basis": "raw",
    }
    if factor:
        scaled["volume"] = int(round(float(row["volume"]) / factor))
    return scaled


def _corrected_rows(existing: list[dict], yahoo_raw: dict, yahoo_adjusted: dict, rewrite_dates) -> list[dict]:
    """Full-OHLCV true-raw series. Rewrite dates are scaled adjusted->raw by that date's
    split fold (``yahoo_raw / yahoo_adjusted`` — pure split ratio, independent of the close
    value); every other row keeps its bronze value. All rows are stamped price_basis='raw'."""
    rewrite = set(rewrite_dates)
    corrected: list[dict] = []
    for row in existing:
        day = _as_date(row["trade_date"])
        if day in rewrite:
            adjusted = yahoo_adjusted.get(day)
            raw = yahoo_raw.get(day)
            factor = (raw / adjusted) if adjusted else 1.0
            corrected.append(_scale_row(row, factor))
        else:
            corrected.append({**row, "price_basis": "raw"})
    return corrected


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2))
    os.replace(tmp, path)


def _backup_and_write(symbol: str, *, bronze: BronzeClient, output_dir: Path, new_rows: list[dict], mode: str) -> dict:
    """Back the parquet up verbatim, record a write-ahead intent sidecar, then replace the
    rows. A crash mid-write is still undoable by ``rollback-legacy-basis --output-dir``
    because the backup and its sha256 are durable before any bronze mutation."""
    source = bronze.symbol_path(symbol)
    original = source.read_bytes()
    sha = hashlib.sha256(original).hexdigest()
    backup_path = output_dir / "backup" / f"{encode_symbol(symbol)}.1d.parquet"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = backup_path.with_name(f".{backup_path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_bytes(original)
        os.replace(tmp, backup_path)
    finally:
        tmp.unlink(missing_ok=True)
    sidecar_path = output_dir / "symbols" / f"{encode_symbol(symbol)}.json"
    _write_json(
        sidecar_path,
        {
            "symbol": symbol,
            "status": "in_progress",
            "mode": mode,
            "backup_path": str(backup_path),
            "backup_sha256": sha,
        },
    )
    written = bronze.replace_ticker_rows(symbol, new_rows)
    sidecar = {
        "symbol": symbol,
        "status": "done",
        "mode": mode,
        "backup_path": str(backup_path),
        "backup_sha256": sha,
        "rows_written": written,
    }
    _write_json(sidecar_path, sidecar)
    return sidecar


def apply_relabel_only(symbol: str, *, bronze: BronzeClient, output_dir: Path) -> dict:
    """Flip every existing row's price_basis to 'raw' — no price/volume change at all.
    Only valid for symbols the resolver classified as pure relabel (rewrite == 0)."""
    rows = bronze.read_symbol_rows(symbol)
    return _backup_and_write(
        symbol,
        bronze=bronze,
        output_dir=output_dir,
        new_rows=[{**row, "price_basis": "raw"} for row in rows],
        mode="relabel",
    )


def apply_rewrite(
    symbol: str, *, bronze: BronzeClient, store: CorporateActionStore, yahoo, as_of: date, output_dir: Path
) -> dict:
    """Rewrite adjusted deep rows to true raw (full OHLCV) and stamp price_basis='raw'.

    Re-resolves the symbol so the bytes written are exactly the series that just passed
    ``build_factor_intervals``; refuses any symbol that does not resolve cleanly."""
    resolution = _resolve(symbol, bronze=bronze, store=store, yahoo=yahoo, as_of=as_of)
    if resolution.result["status"] != "would_resolve" or resolution.corrected is None:
        raise ValueError(f"refusing rewrite: {symbol} status={resolution.result['status']}")
    return _backup_and_write(
        symbol, bronze=bronze, output_dir=output_dir, new_rows=resolution.corrected, mode="rewrite"
    )


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
    if args.apply:
        if args.output_dir is None:
            raise ValueError("--apply requires --output-dir")
        if not (args.relabel_only or args.allow_rewrite):
            # Refuse to run apply without a flag that names what it will do: --relabel-only
            # (never touches prices) or --allow-rewrite (rewrites adjusted deep rows to raw).
            raise ValueError("--apply requires --relabel-only or --allow-rewrite")
    cursor = {"identity": {"data_lake_root": str(root.resolve())}, "completed": {}}
    counts: dict[str, int] = {}
    results = []
    for symbol in _symbols(args):
        try:
            entry = resolve_symbol(symbol, bronze=bronze, store=store, yahoo=yahoo, as_of=as_of)
        except Exception as exc:  # one bad symbol never aborts the sweep
            entry = {"symbol": symbol, "status": "error", "detail": str(exc)[:120]}
        if args.apply and entry["status"] == "would_resolve":
            try:
                if args.allow_rewrite:
                    apply_rewrite(
                        symbol, bronze=bronze, store=store, yahoo=yahoo, as_of=as_of, output_dir=args.output_dir
                    )
                    entry["applied"] = "rewritten" if entry.get("rewrite", 0) else "relabeled"
                elif entry.get("rewrite", 0) == 0:
                    apply_relabel_only(symbol, bronze=bronze, output_dir=args.output_dir)
                    entry["applied"] = "relabeled"
                else:
                    entry["applied"] = "skipped_rewrite"  # --relabel-only defers value rewrites
                if entry["applied"] in ("relabeled", "rewritten"):
                    cursor["completed"][symbol] = {"status": "done"}
                    _write_json(args.output_dir / "cursor.json", cursor)
            except Exception as exc:
                entry["applied"] = f"apply_failed: {exc}"
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
        results.append(entry)
        print(
            f"{symbol}: {entry['status']}{' [' + entry['applied'] + ']' if entry.get('applied') else ''}",
            file=sys.stderr,
        )
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
