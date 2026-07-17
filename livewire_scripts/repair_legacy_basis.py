"""Audit-driven IB re-derivation of mixed-basis legacy equity symbols.

Consumes the legacy-basis audit manifest, and for each ``mixed`` symbol (ordered
sp500 → ndx100 → r2k → remainder) re-fetches deep IB history, normalizes it to
canonical true-raw, self-checks that the resulting adjusted series is continuous,
and merges the corrected rows back to bronze. Resumable via a per-symbol cursor.
Never writes an unconfirmable symbol — ambiguity fails closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ib_client import IBClient
from clients.ingestion_common import load_preset
from clients.price_basis import prepare_ib_rows_for_publish
from clients.silver_continuity import check_adjusted_continuity
from clients.symbol_paths import encode_symbol
from livewire_scripts.adjusted_history_sources import IBHistoryFetcher
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 1
_PRIORITY_PRESETS = ("sp500", "ndx100", "r2k")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--presets-dir", type=Path, default=Path("presets"))
    parser.add_argument("--continuity-threshold", type=float, default=6.0)
    parser.add_argument("--host", default=os.environ.get("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MDW_IB_PORT", "4001")))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--priority-only",
        action="store_true",
        help="repair only sp500/ndx100/r2k members; defer the tail to a later full run",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _priority_rank(presets_dir: Path) -> dict[str, int]:
    rank: dict[str, int] = {}
    for tier, name in enumerate(_PRIORITY_PRESETS):
        preset_path = presets_dir / f"{name}.json"
        if not preset_path.is_file():
            continue
        _, tickers, _ = load_preset(preset_path)
        for ticker in tickers:
            rank.setdefault(ticker.upper(), tier)
    return rank


def _order_symbols(symbols: list[str], rank: dict[str, int]) -> list[str]:
    return sorted(symbols, key=lambda s: (rank.get(s, len(_PRIORITY_PRESETS)), s))


def _repair_one(
    symbol: str,
    *,
    bronze: BronzeClient,
    store: CorporateActionStore,
    fetcher: Callable[[str, date, date], list[dict]],
    as_of: date,
    threshold: float,
) -> tuple[str, dict]:
    """Return (status, sidecar). status in {'done','ambiguous','failed'}."""
    existing = bronze.read_symbol_rows(symbol)
    if not existing:
        return "failed", {"symbol": symbol, "reason": "no_bronze_rows"}
    actions = store.latest_active(symbol)
    # Re-fetch only the range bronze already covers — we're correcting the basis of
    # existing rows, not extending history. Fetching from an absolute 1980 floor
    # would issue ~46 empty yearly IB requests per symbol and hammer the gateway.
    start = min(date.fromisoformat(str(r["trade_date"])) for r in existing)
    ib_rows = fetcher(symbol, start, as_of)
    if not ib_rows:
        return "failed", {"symbol": symbol, "reason": "ib_no_data"}
    try:
        canonical = prepare_ib_rows_for_publish(ib_rows, existing_rows=existing, actions=actions, as_of_date=as_of)
    except ValueError as exc:
        return "ambiguous", {"symbol": symbol, "reason": f"classification: {exc}"}
    ib_only = [r for r in canonical if r.get("source") == "ib"]
    if not ib_only:
        return "failed", {"symbol": symbol, "reason": "no_ib_rows_after_normalize"}
    # Self-check on the POST-MERGE series (existing rows overwritten by IB per date),
    # NOT the IB rows alone — partial IB coverage could otherwise pass the check yet
    # leave un-replaced corrupt legacy dates in bronze. (codex F2)
    merged_by_date = {str(r["trade_date"]): r for r in existing}
    for r in ib_only:
        merged_by_date[str(r["trade_date"])] = r
    merged = [merged_by_date[d] for d in sorted(merged_by_date)]
    try:
        intervals = build_factor_intervals(merged, actions, as_of)
        adjusted = adjust_daily_rows(merged, intervals, revision=1)
        check_adjusted_continuity(adjusted, threshold=threshold)
    except ValueError as exc:
        return "ambiguous", {"symbol": symbol, "reason": f"post_merge_discontinuous: {exc}"}
    inserted = bronze.merge_ticker_rows(symbol, ib_only)
    return "done", {"symbol": symbol, "rows_written": len(ib_only), "inserted": inserted}


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    ib_factory: Callable[[], Any] = IBClient,
    ib_fetcher_factory: Callable[[Any], Callable[[str, date, date], list[dict]]] = IBHistoryFetcher,
    as_of_date: date | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    as_of = as_of_date or datetime.now(UTC).date()
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    store = CorporateActionStore(root)

    audit = json.loads(args.audit_manifest.read_text())
    audit_sha256 = _sha256(args.audit_manifest)
    mixed = [item["symbol"] for item in audit["symbols"] if item.get("klass") == "mixed"]
    rank = _priority_rank(args.presets_dir)
    ordered = _order_symbols(mixed, rank)
    if args.priority_only:
        ordered = [s for s in ordered if s in rank]  # rank holds only preset members

    identity = {"schema_version": SCHEMA_VERSION, "audit_sha256": audit_sha256, "data_lake_root": str(root.resolve())}
    cursor_path = args.output_dir / "cursor.json"
    cursor = {"identity": identity, "completed": {}}
    if args.resume and cursor_path.is_file():
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("identity") != identity:
            raise ValueError("resume cursor does not match the active audit manifest")
        cursor = loaded

    ib_client: Any = None
    fetcher: Callable[[str, date, date], list[dict]] | None = None
    counts: dict[str, int] = {"done": 0, "ambiguous": 0, "failed": 0}
    try:
        for symbol in ordered:
            checkpoint = cursor["completed"].get(symbol)
            if args.resume and checkpoint and checkpoint.get("status") == "done":
                counts["done"] += 1
                continue
            if fetcher is None:
                # Lazy-connect once. A connection failure ABORTS the whole run —
                # per CLAUDE.md, livewire never auto-retries IB connection failures
                # (they mean 2FA / maintenance / session conflict, not something to
                # retry). Re-entering the loop must NOT reconnect per symbol.
                try:
                    ib_client = ib_factory()
                    connect = getattr(ib_client, "connect", None)
                    if callable(connect):
                        connect(host=args.host, port=args.port)  # IBClient handles error-326 clientId retry only
                    fetcher = ib_fetcher_factory(ib_client)
                except Exception as exc:
                    print(f"IB connection failed, aborting run: {exc}", file=sys.stderr)
                    cursor["completed"][symbol] = {
                        "source_sha256": next(
                            (i["source_sha256"] for i in audit["symbols"] if i["symbol"] == symbol), None
                        ),
                        "status": "failed",
                    }
                    _write_atomic(cursor_path, cursor)
                    counts["failed"] += 1
                    break  # remaining symbols stay unprocessed; --resume continues later
            try:
                status, sidecar = _repair_one(
                    symbol,
                    bronze=bronze,
                    store=store,
                    fetcher=fetcher,
                    as_of=as_of,
                    threshold=args.continuity_threshold,
                )
            except Exception as exc:  # per-symbol fetch/derive failure — mark, continue
                status, sidecar = "failed", {"symbol": symbol, "reason": f"exception: {exc}"}
            sidecar_path = args.output_dir / "symbols" / f"{encode_symbol(symbol)}.json"
            _write_atomic(
                sidecar_path,
                {
                    **sidecar,
                    "status": status,
                    "data_lake_root": str(root.resolve()),
                    "repaired_at": datetime.now(UTC).isoformat(),
                },
            )
            cursor["completed"][symbol] = {
                "source_sha256": next((i["source_sha256"] for i in audit["symbols"] if i["symbol"] == symbol), None),
                "status": status,
            }
            _write_atomic(cursor_path, cursor)
            counts[status] = counts.get(status, 0) + 1
    finally:
        if ib_client is not None:
            disconnect = getattr(ib_client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    _write_atomic(
        args.output_dir / "summary.json", {"audit_sha256": audit_sha256, "counts": counts, "symbols": len(ordered)}
    )
    print(json.dumps({"counts": counts, "symbols": len(ordered)}, sort_keys=True))
    return 0 if counts["failed"] == 0 else 1


def summarize_progress(audit_manifest: dict, batch_summary: dict) -> dict:
    """Quantify remaining tail work from a full audit + a first (priority-only) batch.

    The audit is full-universe, so ``tail_mixed_exact`` is exact, not projected.
    Only the tail's un-repairable share is estimated, using the batch's observed
    ambiguous rate as the sample (each tail mixed symbol = one deep IB fetch).
    """
    ac = audit_manifest["counts"]
    total = ac["clean"] + ac["mixed"] + ac["error"]
    mixed_total = ac["mixed"]
    bc = batch_summary["counts"]
    attempted = bc["done"] + bc["ambiguous"] + bc["failed"]
    tail_mixed = max(0, mixed_total - attempted)
    amb_rate = (bc["ambiguous"] / attempted) if attempted else 0.0
    return {
        "audit_total": total,
        "audit_mixed": mixed_total,
        "audit_mixed_rate": round(mixed_total / total, 4) if total else 0.0,
        "batch_attempted": attempted,
        "batch_done": bc["done"],
        "batch_ambiguous": bc["ambiguous"],
        "batch_ambiguous_rate": round(amb_rate, 4),
        "tail_mixed_exact": tail_mixed,
        "tail_estimated_unrepairable": round(tail_mixed * amb_rate),
    }


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
