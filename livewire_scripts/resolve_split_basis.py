#!/usr/bin/env python3
"""Resolve ambiguous split-basis boundaries from repeated read-only IB requests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ib_client import IBClient
from clients.massive_client import MassiveClient
from clients.split_basis_evidence import classify_split_from_reference, correct_invalid_ohlc_from_reference
from clients.symbol_paths import encode_symbol
from livewire_scripts.adjusted_history_sources import IBHistoryFetcher
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 3
WINDOW_DAYS = (20, 60)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--host", default=os.environ.get("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MDW_IB_PORT", "4001")))
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _classification_payload(classification) -> dict:
    payload = asdict(classification)
    for key in ("pre_date", "post_date"):
        if payload[key] is not None:
            payload[key] = payload[key].isoformat()
    return payload


def _rows_payload(rows: list[dict]) -> list[dict]:
    return [{**row, "trade_date": str(row["trade_date"])} for row in rows]


def _massive_rows(client: Any, symbol: str, start: date, end: date) -> list[dict]:
    return [
        {
            "trade_date": bar.trade_date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "adj_close": bar.close,
            "volume": bar.volume,
            "source": "massive",
            "price_basis": "split_adjusted",
        }
        for bar in client.get_daily_bars(symbol, start, end, adjusted=True)
    ]


def run(
    argv: Sequence[str] | None = None,
    *,
    ib_factory: Callable[[], Any] = IBClient,
    ib_fetcher_factory: Callable[[Any], Callable[[str, date, date], list[dict]]] = IBHistoryFetcher,
    massive_factory: Callable[[], Any] = MassiveClient,
) -> int:
    args = parse_args(argv)
    root = (args.data_lake_root or data_lake_dir()).expanduser().resolve()
    audit_path = args.audit_manifest.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    audit_sha256 = _sha256(audit_path)
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("schema_version") != 1:
        raise ValueError("unsupported audit manifest schema")
    if Path(audit.get("data_lake_root", "")).resolve() != root:
        raise ValueError("audit manifest data-lake root does not match active root")
    identity = {
        "schema_version": SCHEMA_VERSION,
        "audit_sha256": audit_sha256,
        "data_lake_root": str(root),
        "window_days": list(WINDOW_DAYS),
    }
    cursor_path = output / "cursor.json"
    cursor = {"identity": identity, "completed": {}}
    if args.resume and cursor_path.is_file():
        cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        if cursor.get("identity") != identity:
            raise ValueError("resume cursor does not match the active audit manifest")

    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    action_store = CorporateActionStore(root)
    targets = [
        item
        for item in audit["symbols"]
        if item.get("error") or any(event["treatment"] == "ambiguous" for event in item["classifications"])
    ]
    results: list[dict] = []
    ib_client = None
    fetcher = None
    massive_client = None
    try:
        for item in targets:
            symbol = item["symbol"]
            detail_path = output / "symbols" / f"{encode_symbol(symbol)}.json"
            checkpoint = cursor["completed"].get(symbol)
            if (
                args.resume
                and checkpoint
                and checkpoint.get("status") in {"resolved", "pending"}
                and checkpoint.get("source_sha256") == item["source_sha256"]
                and detail_path.is_file()
                and checkpoint.get("detail_sha256") == _sha256(detail_path)
            ):
                results.append(json.loads(detail_path.read_text(encoding="utf-8")))
                continue
            source_path = Path(item["path"]).resolve()
            bronze_root = (root / "bronze/asset_class=equity").resolve()
            if not source_path.is_relative_to(bronze_root):
                raise ValueError("audit target is outside equity Bronze")
            if not source_path.is_file() or _sha256(source_path) != item["source_sha256"]:
                detail = {
                    "audit_sha256": audit_sha256,
                    "data_lake_root": str(root),
                    "events": [],
                    "reason": "stale_bronze_source",
                    "source_sha256": item["source_sha256"],
                    "status": "error",
                    "symbol": symbol,
                }
                _write_atomic(detail_path, detail)
                cursor["completed"][symbol] = {
                    "detail_sha256": _sha256(detail_path),
                    "source_sha256": item["source_sha256"],
                    "status": "error",
                }
                _write_atomic(cursor_path, cursor)
                results.append(detail)
                continue
            rows = bronze.read_symbol_rows(symbol)
            latest = max(date.fromisoformat(str(row["trade_date"])) for row in rows)
            actions = {action.action_id: action for action in action_store.latest_active(symbol)}
            ohlc_corrections = []
            for row in rows:
                if all(float(row[column]) > 0 for column in ("open", "high", "low", "close", "adj_close")):
                    continue
                trade_date = date.fromisoformat(str(row["trade_date"]))
                try:
                    if fetcher is None:
                        ib_client = ib_factory()
                        ib_client.connect(host=args.host, port=args.port)
                        fetcher = ib_fetcher_factory(ib_client)
                    provider_runs = [
                        fetcher(symbol, trade_date - timedelta(days=days), trade_date + timedelta(days=days))
                        for days in WINDOW_DAYS
                    ]
                    correction = correct_invalid_ohlc_from_reference(row, provider_runs)
                    provider = "ib"
                    fallback_error = None
                    if correction.status == "ambiguous":
                        try:
                            if massive_client is None:
                                massive_client = massive_factory()
                            massive_runs = [
                                _massive_rows(
                                    massive_client,
                                    symbol,
                                    trade_date - timedelta(days=days),
                                    trade_date + timedelta(days=days),
                                )
                                for days in WINDOW_DAYS
                            ]
                            massive_correction = correct_invalid_ohlc_from_reference(row, massive_runs)
                        except Exception as exc:
                            fallback_error = str(exc)
                        else:
                            if massive_correction.status == "resolved":
                                correction = massive_correction
                                provider_runs = massive_runs
                                provider = "massive"
                except Exception as exc:
                    ohlc_corrections.append(
                        {
                            "provider_runs": [],
                            "reason": str(exc),
                            "status": "error",
                            "trade_date": trade_date.isoformat(),
                        }
                    )
                    continue
                correction_payload = asdict(correction)
                correction_payload["trade_date"] = correction.trade_date.isoformat()
                ohlc_corrections.append(
                    {
                        "correction": correction_payload,
                        "fallback_error": fallback_error,
                        "provider": provider,
                        "provider_runs": [_rows_payload(run_rows) for run_rows in provider_runs],
                        "reason": correction.reason,
                        "status": correction.status,
                        "trade_date": trade_date.isoformat(),
                    }
                )
            events = []
            for event in item["classifications"]:
                if event["treatment"] != "ambiguous":
                    continue
                ex_date = date.fromisoformat(event["ex_date"])
                if ex_date > latest:
                    events.append(
                        {
                            "action_id": event["action_id"],
                            "ex_date": event["ex_date"],
                            "reason": "awaiting_post_split_session",
                            "status": "pending",
                            "provider_runs": [],
                        }
                    )
                    continue
                action = actions.get(event["action_id"])
                if action is None or action.ex_date != ex_date:
                    events.append(
                        {
                            "action_id": event["action_id"],
                            "ex_date": event["ex_date"],
                            "reason": "action_inventory_mismatch",
                            "status": "error",
                            "provider_runs": [],
                        }
                    )
                    continue
                try:
                    if fetcher is None:
                        ib_client = ib_factory()
                        ib_client.connect(host=args.host, port=args.port)
                        fetcher = ib_fetcher_factory(ib_client)
                    provider_runs = [
                        fetcher(symbol, ex_date - timedelta(days=days), ex_date + timedelta(days=days))
                        for days in WINDOW_DAYS
                    ]
                    classification = classify_split_from_reference(rows, provider_runs, action)
                    provider = "ib"
                    fallback_error = None
                    if classification.reason == "missing_reference_boundary":
                        bronze_dates = sorted(date.fromisoformat(str(row["trade_date"])) for row in rows)
                        previous = [day for day in bronze_dates if day < ex_date]
                        following = [day for day in bronze_dates if day >= ex_date]
                        if previous and following:
                            wide_runs = [
                                fetcher(
                                    symbol,
                                    previous[-1] - timedelta(days=padding),
                                    following[0] + timedelta(days=padding),
                                )
                                for padding in (90, 180)
                            ]
                            wide_classification = classify_split_from_reference(rows, wide_runs, action)
                            provider_runs = wide_runs
                            classification = wide_classification
                    if classification.treatment == "ambiguous":
                        try:
                            if massive_client is None:
                                massive_client = massive_factory()
                            massive_runs = [
                                _massive_rows(
                                    massive_client,
                                    symbol,
                                    ex_date - timedelta(days=days),
                                    ex_date + timedelta(days=days),
                                )
                                for days in WINDOW_DAYS
                            ]
                            massive_classification = classify_split_from_reference(rows, massive_runs, action)
                        except Exception as exc:
                            fallback_error = str(exc)
                        else:
                            if massive_classification.treatment != "ambiguous":
                                classification = massive_classification
                                provider_runs = massive_runs
                                provider = "massive"
                except Exception as exc:
                    events.append(
                        {
                            "action_id": event["action_id"],
                            "ex_date": event["ex_date"],
                            "reason": str(exc),
                            "status": "error",
                            "provider_runs": [],
                        }
                    )
                    continue
                events.append(
                    {
                        "action_id": event["action_id"],
                        "ex_date": event["ex_date"],
                        "classification": _classification_payload(classification),
                        "fallback_error": fallback_error,
                        "provider": provider,
                        "provider_runs": [_rows_payload(run_rows) for run_rows in provider_runs],
                        "reason": classification.reason,
                        "status": "resolved" if classification.treatment != "ambiguous" else "ambiguous",
                    }
                )
            statuses = {event["status"] for event in [*events, *ohlc_corrections]}
            if statuses == {"resolved"}:
                status = "resolved"
            elif "error" in statuses:
                status = "error"
            elif "ambiguous" in statuses:
                status = "ambiguous"
            else:
                status = "pending"
            detail = {
                "audit_sha256": audit_sha256,
                "data_lake_root": str(root),
                "events": events,
                "ohlc_corrections": ohlc_corrections,
                "source_sha256": item["source_sha256"],
                "status": status,
                "symbol": symbol,
            }
            _write_atomic(detail_path, detail)
            cursor["completed"][symbol] = {
                "detail_sha256": _sha256(detail_path),
                "source_sha256": item["source_sha256"],
                "status": status,
            }
            _write_atomic(cursor_path, cursor)
            results.append(detail)
    finally:
        if ib_client is not None:
            ib_client.disconnect()
        if massive_client is not None:
            massive_client.close()
    counts: dict[str, int] = {}
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1
    summary = {"audit_sha256": audit_sha256, "counts": counts, "symbols": len(results)}
    _write_atomic(output / "summary.json", summary)
    print(json.dumps(summary, sort_keys=True))
    return 0 if results and all(result["status"] == "resolved" for result in results) else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
