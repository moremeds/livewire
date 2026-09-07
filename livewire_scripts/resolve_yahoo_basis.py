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
from clients.bronze_client import EQUITY_SOURCES, BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ib_client import IBClient, IBConnectionError
from clients.parquet_io import write_json_atomic
from clients.symbol_paths import encode_symbol
from clients.yahoo_basis import (
    AnchorVerdict,
    anchor_window,
    classify_existing_basis,
    ib_anchor_verdict,
    last_split_ex_date,
    reconcile_splits,
    reconstruct_raw_closes,
)
from clients.yahoo_client import YahooClient, YahooError, YahooNotFound
from livewire_scripts.adjusted_history_sources import IBHistoryFetcher
from livewire_scripts.paths import data_lake_dir
from livewire_scripts.repair_legacy_basis import _order_symbols, _priority_rank

# Reason string Silver raises when a split lands on an unknown-basis row (the batch-1 target).
_SPLIT_UNKNOWN_REASON = "unknown price_basis for split-affected row"

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
    parser.add_argument(
        "--failure-manifest",
        type=Path,
        help="rebuild-silver --failure-output JSON; uses its split-affected unknown-basis failures as the symbol source",
    )
    parser.add_argument("--resume", action="store_true", help="skip symbols already recorded done in cursor.json")
    parser.add_argument("--limit", type=int, help="process at most N not-yet-completed symbols this session")
    parser.add_argument("--priority-order", action="store_true", help="order sp500 -> ndx100 -> r2k -> tail")
    parser.add_argument("--presets-dir", type=Path, default=Path("presets"), help="preset dir for --priority-order")
    parser.add_argument(
        "--ib-verify",
        action="store_true",
        help="confirm each reconstruction against IB on the post-last-split window before writing",
    )
    parser.add_argument("--ib-host", default=os.environ.get("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--ib-port", type=int, default=int(os.environ.get("MDW_IB_PORT", "4001")))
    parser.add_argument("--ib-tolerance", type=float, default=0.02)
    parser.add_argument("--ib-window-cap", type=int, default=250)
    parser.add_argument("--ib-min-overlap", type=int, default=5)
    return parser.parse_args(list(argv) if argv is not None else None)


def _symbols(args: argparse.Namespace, *, root: Path) -> list[str]:
    if args.tickers:
        return [t.upper() for t in args.tickers]
    if getattr(args, "failure_manifest", None):
        # Schema is rebuild-silver --failure-output: {"failures": [{"symbol", "error", ...}],
        # "data_lake_root": ...}. Verified against a real rev manifest — do not guess it.
        payload = json.loads(args.failure_manifest.read_text())
        recorded = payload.get("data_lake_root")
        if not recorded:
            raise ValueError("failure manifest records no data_lake_root; refusing to fail open")
        if Path(recorded).resolve() != root.resolve():
            raise ValueError(f"failure manifest is for {recorded}, active root is {root}")
        symbols = [
            str(f["symbol"]).upper()
            for f in payload.get("failures", [])
            if _SPLIT_UNKNOWN_REASON in str(f.get("error", ""))
        ]
        if not symbols:
            # A schema drift or an already-clean manifest must not read as "nothing to do".
            raise ValueError(f"no {_SPLIT_UNKNOWN_REASON!r} failures in {args.failure_manifest}")
        return symbols
    if args.symbols_file:
        payload = json.loads(args.symbols_file.read_text())
        raw = payload[args.symbols_key] if isinstance(payload, dict) else payload
        return [str(t).upper() for t in raw]
    raise ValueError("provide --tickers, --symbols-file, or --failure-manifest")


def _ordered_symbols(args: argparse.Namespace, symbols: list[str]) -> list[str]:
    if not args.priority_order:
        return symbols
    rank = _priority_rank(args.presets_dir)  # raises if no preset found (never a silent zero-symbol run)
    return _order_symbols(symbols, rank)


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
    split_ratios = _store_split_ratios(actions)
    # Bound reconciliation to in-history splits: a split on/before the first stored row
    # affects no stored row, so a Yahoo/store disagreement there is a false block.
    first_bronze = min(_as_date(r["trade_date"]) for r in existing)
    reconciliation = reconcile_splits(ysplits, split_ratios, min_date=first_bronze)
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
    # A row Yahoo has NO bar for carries no evidence at all, yet _corrected_rows would
    # still stamp it price_basis='raw'. Where a split lies ahead of such a row that stamp
    # is a claim we cannot support: Silver would then split-adjust a row that may already
    # be adjusted. Rows after the last split are harmless (raw == adjusted there), so gate
    # exactly the split-affected ones. Yahoo history that starts later than bronze — common
    # for legacy/delisted tickers — lands here instead of publishing unverified deep rows.
    last_ex = last_split_ex_date(split_ratios)
    unverified = [d for d in classification.unmatched if last_ex is not None and d <= last_ex]
    if unverified:
        result["status"] = "unmatched_split_affected"
        result["unverified_sample"] = [d.isoformat() for d in sorted(unverified)[:5]]
        return _Resolution(result, None, actions)
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


def _anchor_ok(
    resolution: _Resolution, symbol: str, *, fetcher, tol: float, window_cap: int, min_overlap: int
) -> AnchorVerdict:
    """Confirm the reconstructed true-raw series against IB on the post-last-split window.
    IB is a gate, never written into bronze."""
    corrected = resolution.corrected or []
    last_ex = last_split_ex_date(_store_split_ratios(resolution.actions))
    # Fetch ONLY the comparison window — both ends. IBHistoryFetcher chunks the requested
    # calendar range into roughly one request per year, so anchoring the request to the
    # series start (or running it out to as_of for a symbol delisted in 2015) would spend
    # a decade of IB requests on a 250-day comparison and hit pacing across the batch.
    window = anchor_window(corrected, last_split_ex=last_ex, window_cap=window_cap)
    if not window:  # bronze ends at/before the last split
        return AnchorVerdict(False, "ib_insufficient_overlap", 0, None)
    ib_rows = fetcher(symbol, _as_date(window[0]["trade_date"]), _as_date(window[-1]["trade_date"]))
    return ib_anchor_verdict(
        corrected, ib_rows, last_split_ex=last_ex, tol=tol, min_overlap=min_overlap, window_cap=window_cap
    )


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


def _normalize_source(row: dict) -> dict:
    """Coerce a stray non-canonical source to ``legacy``. A handful of unknown-basis
    symbols carry one leftover ``source='yahoo'`` row from an earlier experiment;
    BronzeClient rejects it on write, so a single such row would otherwise block the
    whole symbol. These rows ARE the legacy population — relabel them accordingly."""
    return row if row["source"] in EQUITY_SOURCES else {**row, "source": "legacy"}


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
            corrected.append(_normalize_source(_scale_row(row, factor)))
        else:
            corrected.append(_normalize_source({**row, "price_basis": "raw"}))
    return corrected


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
    write_json_atomic(
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
    write_json_atomic(sidecar_path, sidecar)
    return sidecar


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    yahoo_factory: Callable[[], object] = YahooClient,
    ib_factory: Callable[[], object] = IBClient,
    ib_fetcher_factory: Callable[[object], Callable[[str, date, date], list[dict]]] = IBHistoryFetcher,
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
        if not args.ib_verify:
            # No publish without IB confirmation — the reconstruction is only trusted once
            # its post-last-split window is confirmed against IB.
            raise ValueError("--apply requires --ib-verify (no publish without IB confirmation)")
    cursor = {"identity": {"data_lake_root": str(root.resolve())}, "completed": {}}
    cursor_path = (args.output_dir / "cursor.json") if args.output_dir else None
    if args.apply and not args.resume and cursor_path and cursor_path.is_file():
        # Re-running a batch would re-back-up each symbol from its ALREADY-MUTATED parquet,
        # overwriting the pristine backup and silently turning rollback into a no-op.
        raise ValueError(f"cursor already exists in {args.output_dir}: pass --resume to continue it")
    if args.resume and cursor_path and cursor_path.is_file():
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("identity") != cursor["identity"]:
            raise ValueError("resume cursor does not match the active data-lake root")
        cursor = loaded
    counts: dict[str, int] = {}
    results = []
    processed = 0
    ib_client = None
    fetcher: Callable[[str, date, date], list[dict]] | None = None
    aborted = False
    try:
        for symbol in _ordered_symbols(args, _symbols(args, root=root)):
            if args.resume and cursor["completed"].get(symbol, {}).get("status") == "done":
                continue
            if args.limit is not None and processed >= args.limit:
                break
            processed += 1
            try:
                resolution = _resolve(symbol, bronze=bronze, store=store, yahoo=yahoo, as_of=as_of)
                entry = resolution.result
            except Exception as exc:  # one bad symbol never aborts the sweep
                resolution = None
                entry = {"symbol": symbol, "status": "error", "detail": str(exc)[:120]}
            if args.apply and entry["status"] == "would_resolve":
                if args.ib_verify:
                    if fetcher is None:
                        # Lazy-connect once. A connection failure ABORTS the run and is never
                        # a per-symbol verdict — livewire never auto-retries an IB connection
                        # failure (2FA / maintenance / session conflict). --resume re-asks it.
                        try:
                            ib_client = ib_factory()
                            ib_client.connect(host=args.ib_host, port=args.ib_port)
                            fetcher = ib_fetcher_factory(ib_client)
                        except Exception as exc:
                            print(f"IB connection failed, aborting run: {exc}", file=sys.stderr)
                            aborted = True
                            break
                    try:
                        verdict = _anchor_ok(
                            resolution,
                            symbol,
                            fetcher=fetcher,
                            tol=args.ib_tolerance,
                            window_cap=args.ib_window_cap,
                            min_overlap=args.ib_min_overlap,
                        )
                    except (IBConnectionError, ConnectionError, OSError, TimeoutError) as exc:
                        # Session dropped mid-run: every remaining symbol would fail through the
                        # dead socket. Record this one as an IB error (not a verdict) and abort;
                        # it stays uncheckpointed so --resume re-asks it.
                        print(f"IB session lost mid-run, aborting run: {exc}", file=sys.stderr)
                        entry["ib_verdict"] = f"ib_error: {exc}"
                        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
                        results.append(entry)
                        aborted = True
                        break
                    except Exception as exc:
                        # Any other per-symbol IB failure is a VERDICT, never a run abort —
                        # this population is full of delisted / reused tickers and one of
                        # them must not kill the batch (and lose the whole manifest, which
                        # is only written after the loop). Catching broadly is deliberate:
                        # the fetcher runs ib_async directly, so its exception surface is
                        # not enumerable from IBClient. Not checkpointed → --resume re-asks.
                        verdict = AnchorVerdict(False, f"ib_error: {str(exc)[:120]}", 0, None)
                    entry["ib_verdict"] = verdict.reason
                    if not verdict.verified:
                        # Not confirmed → withheld to the review queue, bronze untouched. The
                        # queue is only triageable if it says WHY, so carry the numbers out.
                        entry["applied"] = "withheld_ib"
                        entry["ib_overlap"] = verdict.overlap
                        entry["ib_window_start"] = verdict.window_start.isoformat() if verdict.window_start else None
                        if verdict.mismatches:
                            entry["ib_mismatch_sample"] = [
                                [d.isoformat(), round(c, 4), round(i, 4)] for d, c, i in verdict.mismatches[:5]
                            ]
                        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
                        results.append(entry)
                        continue
                try:
                    # Publish EXACTLY the rows the IB anchor just verified. Re-resolving here
                    # would refetch Yahoo and reread bronze/actions, so a change between the
                    # two calls would publish a candidate no anchor ever approved.
                    if args.allow_rewrite or entry.get("rewrite", 0) == 0:
                        rewritten = bool(entry.get("rewrite", 0))
                        _backup_and_write(
                            symbol,
                            bronze=bronze,
                            output_dir=args.output_dir,
                            new_rows=resolution.corrected,
                            mode="rewrite" if rewritten else "relabel",
                        )
                        entry["applied"] = "rewritten" if rewritten else "relabeled"
                    else:
                        entry["applied"] = "skipped_rewrite"  # --relabel-only defers value rewrites
                    if entry["applied"] in ("relabeled", "rewritten"):
                        cursor["completed"][symbol] = {"status": "done"}
                        write_json_atomic(args.output_dir / "cursor.json", cursor)
                except Exception as exc:
                    entry["applied"] = f"apply_failed: {exc}"
            counts[entry["status"]] = counts.get(entry["status"], 0) + 1
            results.append(entry)
            print(
                f"{symbol}: {entry['status']}{' [' + entry['applied'] + ']' if entry.get('applied') else ''}",
                file=sys.stderr,
            )
    finally:
        if ib_client is not None:
            disconnect = getattr(ib_client, "disconnect", None)
            if callable(disconnect):
                disconnect()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {"data_lake_root": str(root.resolve()), "as_of": as_of.isoformat(), "counts": counts, "symbols": results},
            indent=2,
            sort_keys=True,
        )
    )
    print(json.dumps({"counts": counts, "symbols": len(results), "aborted": aborted}, sort_keys=True))
    return 1 if aborted else 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
