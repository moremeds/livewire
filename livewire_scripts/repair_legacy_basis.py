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
from contextlib import ExitStack
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ib_client import IBClient, IBConnectionError
from clients.ingestion_common import load_preset
from clients.parquet_io import publish_parquet, restore_parquet_exact, symbol_lock, write_json_atomic
from clients.price_basis import prepare_ib_rows_for_publish
from clients.seed_boundary import check_seed_boundary
from clients.silver_continuity import check_adjusted_continuity
from clients.source_evidence import sha256_file
from clients.symbol_paths import canonical_symbol, encode_symbol
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
    checksum = hashlib.sha256(payload).hexdigest()
    if destination.exists():
        if sha256_file(destination) != checksum:
            raise ValueError(f"{symbol}: existing backup does not match the current source")
    else:
        restore_parquet_exact(source, destination, checksum)
    return {"symbol": symbol, "backup_path": str(destination), "sha256": checksum}


def _publish_with_rollback(
    symbol: str,
    *,
    bronze: BronzeClient,
    output_dir: Path,
    rows: list[dict],
    sidecar_fields: dict,
) -> dict:
    """Stage exact replacement bytes and record both CAS hashes before publish."""
    saved = backup_symbol(bronze, symbol, output_dir / "backup")
    normalized = bronze._normalize_rows(rows, symbol)
    candidate = output_dir / "candidates" / f"{encode_symbol(symbol)}.1d.parquet"
    publish_parquet(candidate, bronze._table_from_rows(normalized), "trade_date")
    applied_sha256 = sha256_file(candidate)
    sidecar_path = output_dir / "symbols" / f"{encode_symbol(symbol)}.json"
    intent = {
        **sidecar_fields,
        "symbol": symbol,
        "status": "in_progress",
        "backup_path": saved["backup_path"],
        "backup_sha256": saved["sha256"],
        "candidate_path": str(candidate),
        "applied_sha256": applied_sha256,
        "rows_written": len(normalized),
    }
    write_json_atomic(sidecar_path, intent)
    restore_parquet_exact(candidate, bronze.symbol_path(symbol), applied_sha256)
    done = {**intent, "status": "done"}
    write_json_atomic(sidecar_path, done)
    return done


def _resume_repair_sidecar(bronze: BronzeClient, symbol: str, output_dir: Path) -> str | None:
    """Finish or classify a durable repair intent without refetching providers."""
    sidecar_path = output_dir / "symbols" / f"{encode_symbol(symbol)}.json"
    if not sidecar_path.is_file():
        return None
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if sidecar.get("status") not in {"in_progress", "done", "rollback_in_progress", "rolled_back"}:
        return None
    if sidecar.get("symbol") != symbol:
        raise ValueError(f"{symbol}: repair sidecar symbol mismatch")
    backup = Path(sidecar.get("backup_path", ""))
    backup_sha256 = sidecar.get("backup_sha256")
    if not backup.is_file() or not backup_sha256 or sha256_file(backup) != backup_sha256:
        raise ValueError(f"{symbol}: repair backup is missing or corrupt")
    target = bronze.symbol_path(symbol)
    action_path_value = sidecar.get("action_path")
    action_path = Path(action_path_value) if action_path_value else None
    with ExitStack() as locks:
        if action_path is not None:
            locks.enter_context(symbol_lock(action_path))
        locks.enter_context(symbol_lock(target))
        current_sha256 = sha256_file(target) if target.is_file() else None
        applied_sha256 = sidecar.get("applied_sha256")
        if current_sha256 == backup_sha256:
            if sidecar["status"] in {"done", "rollback_in_progress", "rolled_back"} or not applied_sha256:
                write_json_atomic(sidecar_path, {**sidecar, "status": "rolled_back"})
                return "rolled_back"
            candidate = Path(sidecar.get("candidate_path", ""))
            if not candidate.is_file() or sha256_file(candidate) != applied_sha256:
                raise ValueError(f"{symbol}: staged repair candidate is missing or corrupt")
            if "action_sha256" not in sidecar or action_path is None:
                raise ValueError(f"{symbol}: repair intent has no corporate-action identity")
            current_action_sha256 = sha256_file(action_path) if action_path.is_file() else None
            if current_action_sha256 != sidecar["action_sha256"]:
                raise ValueError(f"{symbol}: corporate actions changed before repair publication")
            restore_parquet_exact(candidate, target, applied_sha256)
            current_sha256 = applied_sha256
        if not applied_sha256 or current_sha256 != applied_sha256:
            raise ValueError(f"{symbol}: current Bronze does not match repair source or applied bytes")
        if sidecar["status"] in {"rollback_in_progress", "rolled_back"}:
            raise ValueError(f"{symbol}: rollback is incomplete; finish rollback before repair resume")
        write_json_atomic(sidecar_path, {**sidecar, "status": "done"})
        return "done"


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
            rank.setdefault(canonical_symbol(ticker), tier)
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
    action_path = store.path_for(symbol)
    # Match snapshot lock ordering: corporate actions before equity.
    with symbol_lock(action_path), symbol_lock(path):
        source_hash = sha256_file(path) if path.is_file() else None
        if audit_sha256 is not None and source_hash != audit_sha256:
            return "failed", {"symbol": symbol, "reason": "bronze changed since the audit"}
        existing = bronze.read_symbol_rows(symbol)
        if not existing:
            return "failed", {"symbol": symbol, "reason": "no_bronze_rows"}
        action_hash = sha256_file(action_path) if action_path.is_file() else None
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
    with symbol_lock(action_path), symbol_lock(path):
        current_source = sha256_file(path) if path.is_file() else None
        current_actions = sha256_file(action_path) if action_path.is_file() else None
        if (current_source, current_actions) != (source_hash, action_hash):
            return "failed", {"symbol": symbol, "reason": "inputs changed during repair; rerun audit"}
        inserted = len({r["trade_date"] for r in ib_only} - {r["trade_date"] for r in existing})
        sidecar = _publish_with_rollback(
            symbol,
            bronze=bronze,
            output_dir=backup_dir.parent,
            rows=merged,
            sidecar_fields={
                "inserted": inserted,
                "repaired_rows": len(ib_only),
                "action_path": str(action_path.resolve()),
                "action_sha256": action_hash,
            },
        )
    return "done", sidecar


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
    audit_sha256 = sha256_file(args.audit_manifest)
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
    if not args.dry_run and not cursor_path.is_file():
        write_json_atomic(cursor_path, cursor)

    ib_client: Any = None
    fetcher: Callable[[str, date, date], list[dict]] | None = None
    counts: dict[str, int] = {"done": 0, "ambiguous": 0, "failed": 0}
    # An abort is not a per-symbol failure, but it is never a successful run either:
    # tracked separately so a dead gateway cannot exit 0 with an empty cursor.
    aborted = False
    try:
        for symbol in ordered:
            checkpoint = cursor["completed"].get(symbol)
            if args.resume:
                if checkpoint and checkpoint.get("status") in {"done", "rolled_back"}:
                    counts[checkpoint["status"]] = counts.get(checkpoint["status"], 0) + 1
                    continue
                try:
                    recovered = None if args.dry_run else _resume_repair_sidecar(bronze, symbol, args.output_dir)
                except ValueError as exc:
                    counts["failed"] += 1
                    cursor["completed"][symbol] = {"status": "failed", "reason": str(exc)}
                    write_json_atomic(cursor_path, cursor)
                    continue
                if recovered is not None:
                    cursor["completed"][symbol] = {"status": recovered}
                    write_json_atomic(cursor_path, cursor)
                    counts[recovered] = counts.get(recovered, 0) + 1
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
            existing_sidecar = None
            if status == "failed" and sidecar_path.is_file():
                try:
                    existing_sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    # A mutation may already have happened. Preserve even an unreadable
                    # receipt for forensic recovery instead of replacing it with a
                    # generic failure that rollback would skip.
                    existing_sidecar = {"status": "unreadable"}
            if not (
                existing_sidecar
                and existing_sidecar.get("status")
                in {"in_progress", "done", "rollback_in_progress", "rolled_back", "unreadable"}
            ):
                write_json_atomic(
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
            write_json_atomic(cursor_path, cursor)
            counts[status] = counts.get(status, 0) + 1
            if aborting:
                break  # remaining symbols stay unprocessed; --resume continues later
    finally:
        if ib_client is not None:
            disconnect = getattr(ib_client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    write_json_atomic(
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
