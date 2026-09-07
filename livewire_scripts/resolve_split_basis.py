#!/usr/bin/env python3
"""Resolve ambiguous split-basis boundaries from repeated read-only IB requests."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Sequence
from dataclasses import asdict, replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ib_client import IBClient
from clients.massive_client import MassiveClient
from clients.parquet_io import write_json_atomic
from clients.source_evidence import sha256_file
from clients.split_basis_evidence import (
    classify_reference_basis,
    classify_split_from_reference,
    correct_invalid_ohlc_from_reference,
)
from clients.symbol_paths import encode_symbol
from livewire_scripts.adjusted_history_sources import IBHistoryFetcher
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 3
EVIDENCE_VERSION = 7
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


def _classify_provider_reference(rows: list[dict], provider_runs: list[list[dict]], action, provider: str):
    reference_basis = "adjusted" if provider == "massive" else classify_reference_basis(provider_runs, action)
    if reference_basis == "ambiguous":
        classification = classify_split_from_reference(rows, provider_runs, action)
        if classification.reason != "missing_reference_boundary":
            classification = replace(classification, treatment="ambiguous", reason="reference_basis_ambiguous")
        return classification, reference_basis
    return (
        classify_split_from_reference(rows, provider_runs, action, reference_basis=reference_basis),
        reference_basis,
    )


def _replay_resolved_detail(
    detail: dict,
    item: dict,
    rows: list[dict],
    actions: dict[str, Any],
) -> dict | None:
    """Upgrade saved provider rows only when current code reproduces every resolution."""
    replayed = json.loads(json.dumps(detail))
    events = {event.get("action_id"): event for event in replayed.get("events", [])}
    for target in item.get("classifications", []):
        if target.get("treatment") != "ambiguous":
            continue
        event = events.get(target.get("action_id"))
        action = actions.get(target.get("action_id"))
        if event is None or action is None or event.get("provider") not in {"ib", "massive"}:
            return None
        classification, reference_basis = _classify_provider_reference(
            rows,
            event.get("provider_runs", []),
            action,
            event["provider"],
        )
        if classification.treatment == "ambiguous":
            return None
        event["classification"] = _classification_payload(classification)
        event["reason"] = classification.reason
        event["reference_basis"] = reference_basis
        event["status"] = "resolved"

    corrections = {item.get("trade_date"): item for item in replayed.get("ohlc_corrections", [])}
    for row in rows:
        if all(float(row[column]) > 0 for column in ("open", "high", "low", "close", "adj_close")):
            continue
        trade_date = str(row["trade_date"])
        saved = corrections.get(trade_date)
        if saved is None:
            return None
        correction = correct_invalid_ohlc_from_reference(row, saved.get("provider_runs", []))
        if correction.status != "resolved":
            return None
        payload = asdict(correction)
        payload["trade_date"] = correction.trade_date.isoformat()
        saved["correction"] = payload
        saved["reason"] = correction.reason
        saved["status"] = "resolved"

    replayed["evidence_version"] = EVIDENCE_VERSION
    replayed["status"] = "resolved"
    return replayed


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
    audit_sha256 = sha256_file(audit_path)
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
            checkpoint_artifact_valid = (
                args.resume
                and checkpoint
                and checkpoint.get("source_sha256") == item["source_sha256"]
                and detail_path.is_file()
                and checkpoint.get("detail_sha256") == sha256_file(detail_path)
            )
            checkpoint_valid = checkpoint_artifact_valid and checkpoint.get("status") == "resolved"
            source_path = Path(item["path"]).resolve()
            bronze_root = (root / "bronze/asset_class=equity").resolve()
            if not source_path.is_relative_to(bronze_root):
                raise ValueError("audit target is outside equity Bronze")
            if not source_path.is_file() or sha256_file(source_path) != item["source_sha256"]:
                detail = {
                    "audit_sha256": audit_sha256,
                    "data_lake_root": str(root),
                    "events": [],
                    "reason": "stale_bronze_source",
                    "source_sha256": item["source_sha256"],
                    "status": "error",
                    "symbol": symbol,
                }
                write_json_atomic(detail_path, detail)
                cursor["completed"][symbol] = {
                    "detail_sha256": sha256_file(detail_path),
                    "source_sha256": item["source_sha256"],
                    "status": "error",
                }
                write_json_atomic(cursor_path, cursor)
                results.append(detail)
                continue
            rows = bronze.read_symbol_rows(symbol)
            latest = max(date.fromisoformat(str(row["trade_date"])) for row in rows)
            actions = {action.action_id: action for action in action_store.latest_active(symbol)}
            saved_detail = json.loads(detail_path.read_text(encoding="utf-8")) if checkpoint_artifact_valid else None
            if checkpoint_valid:
                assert saved_detail is not None
                if saved_detail.get("evidence_version") == EVIDENCE_VERSION:
                    results.append(saved_detail)
                    continue
                replayed = _replay_resolved_detail(saved_detail, item, rows, actions)
                if replayed is not None:
                    write_json_atomic(detail_path, replayed)
                    cursor["completed"][symbol] = {
                        "detail_sha256": sha256_file(detail_path),
                        "source_sha256": item["source_sha256"],
                        "status": "resolved",
                    }
                    write_json_atomic(cursor_path, cursor)
                    results.append(replayed)
                    continue
            ohlc_corrections = []
            saved_corrections = {
                correction.get("trade_date"): correction
                for correction in (saved_detail or {}).get("ohlc_corrections", [])
            }
            for row in rows:
                if all(float(row[column]) > 0 for column in ("open", "high", "low", "close", "adj_close")):
                    continue
                trade_date = date.fromisoformat(str(row["trade_date"]))
                try:
                    saved_correction = saved_corrections.get(trade_date.isoformat())
                    if saved_correction and saved_correction.get("provider") == "ib":
                        provider_runs = saved_correction.get("provider_runs", [])
                    else:
                        if fetcher is None:
                            ib_client = ib_factory()
                            ib_client.connect(host=args.host, port=args.port)
                            fetcher = ib_fetcher_factory(ib_client)
                        provider_runs = [
                            fetcher(
                                symbol,
                                trade_date - timedelta(days=days),
                                trade_date + timedelta(days=days),
                            )
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
            saved_events = {
                saved_event.get("action_id"): saved_event for saved_event in (saved_detail or {}).get("events", [])
            }
            for event in item["classifications"]:
                if event["treatment"] != "ambiguous":
                    continue
                ex_date = date.fromisoformat(event["ex_date"])
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
                    saved_event = saved_events.get(event["action_id"])
                    if saved_event and saved_event.get("provider") == "ib":
                        provider_runs = saved_event.get("provider_runs", [])
                    else:
                        if fetcher is None:
                            ib_client = ib_factory()
                            ib_client.connect(host=args.host, port=args.port)
                            fetcher = ib_fetcher_factory(ib_client)
                        provider_runs = [
                            fetcher(symbol, ex_date - timedelta(days=days), ex_date + timedelta(days=days))
                            for days in WINDOW_DAYS
                        ]
                    classification, reference_basis = _classify_provider_reference(rows, provider_runs, action, "ib")
                    provider = "ib"
                    fallback_error = None
                    if classification.reason == "missing_reference_boundary":
                        bronze_dates = sorted(date.fromisoformat(str(row["trade_date"])) for row in rows)
                        previous = [day for day in bronze_dates if day < ex_date]
                        following = [day for day in bronze_dates if day >= ex_date]
                        if previous and following and any(provider_runs):
                            if fetcher is None:
                                ib_client = ib_factory()
                                ib_client.connect(host=args.host, port=args.port)
                                fetcher = ib_fetcher_factory(ib_client)
                            wide_runs = [
                                fetcher(
                                    symbol,
                                    previous[-1] - timedelta(days=padding),
                                    following[0] + timedelta(days=padding),
                                )
                                for padding in (90, 180)
                            ]
                            wide_classification, wide_reference_basis = _classify_provider_reference(
                                rows, wide_runs, action, "ib"
                            )
                            provider_runs = wide_runs
                            classification = wide_classification
                            reference_basis = wide_reference_basis
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
                            massive_classification, massive_reference_basis = _classify_provider_reference(
                                rows, massive_runs, action, "massive"
                            )
                        except Exception as exc:
                            fallback_error = str(exc)
                        else:
                            if massive_classification.treatment != "ambiguous":
                                classification = massive_classification
                                provider_runs = massive_runs
                                provider = "massive"
                                reference_basis = massive_reference_basis
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
                event_status = "resolved" if classification.treatment != "ambiguous" else "ambiguous"
                event_reason = classification.reason
                if (
                    event_status == "ambiguous"
                    and ex_date > latest
                    and classification.reason == "missing_reference_boundary"
                ):
                    event_status = "pending"
                    event_reason = "awaiting_post_split_reference"
                events.append(
                    {
                        "action_id": event["action_id"],
                        "ex_date": event["ex_date"],
                        "classification": _classification_payload(classification),
                        "fallback_error": fallback_error,
                        "provider": provider,
                        "provider_runs": [_rows_payload(run_rows) for run_rows in provider_runs],
                        "reference_basis": reference_basis,
                        "reason": event_reason,
                        "status": event_status,
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
                "evidence_version": EVIDENCE_VERSION,
                "events": events,
                "ohlc_corrections": ohlc_corrections,
                "source_sha256": item["source_sha256"],
                "status": status,
                "symbol": symbol,
            }
            write_json_atomic(detail_path, detail)
            cursor["completed"][symbol] = {
                "detail_sha256": sha256_file(detail_path),
                "source_sha256": item["source_sha256"],
                "status": status,
            }
            write_json_atomic(cursor_path, cursor)
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
    write_json_atomic(output / "summary.json", summary)
    print(json.dumps(summary, sort_keys=True))
    return 0 if results and all(result["status"] == "resolved" for result in results) else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
