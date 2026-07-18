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
from clients.ib_client import IBClient, IBConnectionError
from clients.ingestion_common import load_preset
from clients.price_basis import prepare_ib_rows_for_publish
from clients.seed_boundary import check_seed_boundary
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
    parser.add_argument("--dry-run", action="store_true", help="fetch, classify and self-check, but never write bronze")
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def backup_symbol(bronze: BronzeClient, symbol: str, backup_dir: Path) -> dict:
    """Copy a symbol's bronze parquet verbatim before any mutation.

    Bronze is the system of record and merge_ticker_rows overwrites rows in place,
    so the pre-repair bytes are otherwise unrecoverable. The sibling split-basis
    repair family ships rollback; this one must too.
    """
    source = bronze.symbol_path(symbol)
    backup_dir.mkdir(parents=True, exist_ok=True)
    destination = backup_dir / f"{encode_symbol(symbol)}.1d.parquet"
    payload = source.read_bytes()
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return {"symbol": symbol, "backup_path": str(destination), "sha256": hashlib.sha256(payload).hexdigest()}


def _priority_rank(presets_dir: Path) -> dict[str, int]:
    rank: dict[str, int] = {}
    found = 0
    for tier, name in enumerate(_PRIORITY_PRESETS):
        preset_path = presets_dir / f"{name}.json"
        if not preset_path.is_file():
            continue
        found += 1
        _, tickers, _ = load_preset(preset_path)
        for ticker in tickers:
            rank.setdefault(ticker.upper(), tier)
    if not found:
        # --presets-dir defaults to a cwd-relative Path("presets"); from
        # ~/market-warehouse this silently repaired zero symbols and exited 0.
        raise ValueError(f"no priority preset found in {presets_dir.resolve()} (expected {_PRIORITY_PRESETS})")
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
    backup_dir: Path | None,
    audit_sha256: str | None,
) -> tuple[str, dict]:
    """Return (status, sidecar). status in {'done','would-repair','ambiguous','failed'}."""
    path = bronze.symbol_path(symbol)
    if audit_sha256 is not None and path.is_file():
        if hashlib.sha256(path.read_bytes()).hexdigest() != audit_sha256:
            # The audit's mixed/clean verdict describes bytes that no longer exist.
            return "failed", {"symbol": symbol, "reason": "bronze changed since the audit"}
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
        # The heuristic above cannot see a 2x-5x residual. SeedBoundaryBreak
        # subclasses ValueError, so a partial re-fetch fails closed as `ambiguous`
        # rather than being recorded as a successful repair.
        check_seed_boundary(merged, actions)
    except ValueError as exc:
        return "ambiguous", {"symbol": symbol, "reason": f"post_merge_discontinuous: {exc}"}
    if backup_dir is None:
        return "would-repair", {"symbol": symbol, "rows_would_write": len(ib_only)}
    saved = backup_symbol(bronze, symbol, backup_dir)
    # Write-ahead intent, NOT a redundant sidecar: the next line mutates bronze, the
    # system of record, and the caller does not write this symbol's sidecar until
    # _repair_one returns. A crash in that window (OOM kill, power cut) would leave
    # mutated bronze plus a backup that nothing points at, and rollback restores only
    # symbols a sidecar names — i.e. a mutation that cannot be undone by the supplied
    # command. Record where the undo lives BEFORE taking the action that needs undoing.
    # The caller atomically replaces this with the terminal sidecar.
    _write_atomic(
        backup_dir.parent / "symbols" / f"{encode_symbol(symbol)}.json",
        {
            "symbol": symbol,
            "status": "in_progress",
            "backup_path": saved["backup_path"],
            "backup_sha256": saved["sha256"],
        },
    )
    inserted = bronze.merge_ticker_rows(symbol, ib_only)
    return "done", {
        "symbol": symbol,
        "rows_written": len(ib_only),
        "inserted": inserted,
        "backup_path": saved["backup_path"],
        "backup_sha256": saved["sha256"],
    }


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
    manifest_root = audit.get("data_lake_root")
    # CLAUDE.md repair contract: reject a different active data-lake root before
    # mutation. A manifest with no root recorded cannot be checked → refuse it.
    if manifest_root is None:
        raise ValueError("audit manifest has no data_lake_root: refusing to mutate bronze")
    if manifest_root != str(root.resolve()):
        raise ValueError(f"audit manifest data_lake_root {manifest_root} does not match active root {root.resolve()}")
    mixed = [item["symbol"] for item in audit["symbols"] if item.get("klass") == "mixed"]
    rank = _priority_rank(args.presets_dir) if args.priority_only else {}
    ordered = _order_symbols(mixed, rank) if rank else sorted(mixed)
    if args.priority_only:
        ordered = [s for s in ordered if s in rank]  # rank holds only preset members

    identity = {"schema_version": SCHEMA_VERSION, "audit_sha256": audit_sha256, "data_lake_root": str(root.resolve())}
    cursor_path = args.output_dir / "cursor.json"
    cursor = {"identity": identity, "completed": {}}
    if cursor_path.is_file():
        if not args.resume:
            raise ValueError(f"cursor already exists in {args.output_dir}: pass --resume to continue it")
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("identity") != identity:
            raise ValueError("resume cursor does not match the active audit manifest")
        cursor = loaded

    ib_client: Any = None
    fetcher: Callable[[str, date, date], list[dict]] | None = None
    counts: dict[str, int] = {"done": 0, "ambiguous": 0, "failed": 0}
    # An abort is not a per-symbol failure, but it is never a successful run either:
    # tracked separately so a dead gateway cannot exit 0 with an empty cursor.
    aborted = False
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
                    # Never attempted: do NOT record a cursor entry or count it as
                    # failed — --resume must pick this symbol up cleanly.
                    print(f"IB connection failed, aborting run: {exc}", file=sys.stderr)
                    aborted = True
                    break
            aborting = False
            try:
                status, sidecar = _repair_one(
                    symbol,
                    bronze=bronze,
                    store=store,
                    fetcher=fetcher,
                    as_of=as_of,
                    threshold=args.continuity_threshold,
                    backup_dir=None if args.dry_run else args.output_dir / "backup",
                    audit_sha256=next((i["source_sha256"] for i in audit["symbols"] if i["symbol"] == symbol), None),
                )
            except (IBConnectionError, ConnectionError, OSError, TimeoutError) as exc:
                # IB session dropped mid-run. Aborting mirrors the initial-connect
                # abort: every remaining symbol would fail through the dead socket,
                # so mark this one failed and leave the rest for --resume.
                print(f"IB session lost mid-run, aborting run: {exc}", file=sys.stderr)
                status, sidecar = "failed", {"symbol": symbol, "reason": f"connection_lost: {exc}"}
                aborting = True
                aborted = True
            except Exception as exc:  # non-connection per-symbol failure — mark, continue
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
            if aborting:
                break  # remaining symbols stay unprocessed; --resume continues later
    finally:
        if ib_client is not None:
            disconnect = getattr(ib_client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    _write_atomic(
        args.output_dir / "summary.json",
        {
            "audit_sha256": audit_sha256,
            "counts": counts,
            "symbols": len(ordered),
            "complete": not aborted and len(cursor["completed"]) >= len(ordered),
        },
    )
    print(json.dumps({"counts": counts, "symbols": len(ordered), "aborted": aborted}, sort_keys=True))
    return 0 if counts["failed"] == 0 and not aborted else 1


def summarize_progress(audit_manifest: dict, batch_summary: dict, *, cursor: dict | None = None) -> dict:
    """Quantify remaining tail work from a full audit + a first (priority-only) batch.

    Outcome counts equal coverage only when the batch ran to completion. An aborted
    batch leaves priority symbols unprocessed; counting them as tail work would
    understate the remaining priority run, so the tail is a lower bound instead.
    Pass ``cursor`` (the batch's ``cursor.json``) to measure coverage exactly.
    """
    ac = audit_manifest["counts"]
    total = ac["clean"] + ac["mixed"] + ac["error"]
    mixed_total = ac["mixed"]
    bc = batch_summary["counts"]
    attempted = len(cursor["completed"]) if cursor else bc["done"] + bc["ambiguous"] + bc["failed"]
    unprocessed = max(0, mixed_total - attempted)
    amb_rate = (bc["ambiguous"] / attempted) if attempted else 0.0
    result = {
        "audit_total": total,
        "audit_mixed": mixed_total,
        "audit_mixed_rate": round(mixed_total / total, 4) if total else 0.0,
        "batch_attempted": attempted,
        "batch_unprocessed": unprocessed,
        "batch_done": bc["done"],
        "batch_ambiguous": bc["ambiguous"],
        "batch_ambiguous_rate": round(amb_rate, 4),
        "tail_estimated_unrepairable": round(unprocessed * amb_rate),
    }
    key = "tail_mixed_exact" if batch_summary.get("complete") else "tail_mixed_lower_bound"
    result[key] = unprocessed
    return result


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
