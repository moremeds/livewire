#!/usr/bin/env python3
"""Audit equity Bronze split basis and write a read-only repair manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, replace
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.parquet_io import write_json_atomic
from clients.price_basis import classify_split_events, normalize_ib_rows
from clients.split_basis_evidence import (
    classify_reference_basis,
    classify_split_from_reference,
    correct_invalid_ohlc_from_reference,
)
from clients.symbol_paths import canonical_symbol, encode_symbol
from livewire_scripts.paths import data_lake_dir

NEW_YORK = ZoneInfo("America/New_York")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+")
    scope.add_argument("--full", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _classification_payload(item) -> dict:
    payload = asdict(item)
    payload["ex_date"] = item.ex_date.isoformat()
    payload["split_factor"] = str(item.split_factor)
    return payload


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    as_of_date: date | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else args.data_lake_root or data_lake_dir()
    root = root.resolve()
    bronze_root = root / "bronze/asset_class=equity"
    bronze = BronzeClient(bronze_root, "equity")
    actions = CorporateActionStore(root)
    effective_as_of = as_of_date or datetime.now(NEW_YORK).date()
    symbols = (
        sorted(bronze.get_existing_symbols())
        if args.full
        else list(dict.fromkeys(canonical_symbol(symbol) for symbol in args.tickers))
    )
    manifest_symbols = []
    failed = 0
    for symbol in symbols:
        path = bronze_root / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
        rows = bronze.read_symbol_rows(symbol)
        staged = [{**row, "source": "ib", "price_basis": "unknown"} for row in rows]
        symbol_actions = actions.latest_active(symbol)
        evidence = None
        evidence_path = None
        if args.evidence_dir:
            candidate = args.evidence_dir.resolve() / "symbols" / f"{encode_symbol(symbol)}.json"
            if candidate.is_file():
                candidate_evidence = json.loads(candidate.read_text(encoding="utf-8"))
                if (
                    candidate_evidence.get("status") == "resolved"
                    and candidate_evidence.get("symbol") == symbol
                    and Path(candidate_evidence.get("data_lake_root", "")).resolve() == root
                    and candidate_evidence.get("source_sha256") == _sha256(path)
                ):
                    evidence = candidate_evidence
                    evidence_path = candidate
        corrections_replayed = True
        if evidence:
            by_date = {str(row["trade_date"]): row for row in staged}
            for item in evidence.get("ohlc_corrections", []):
                row = by_date.get(item["trade_date"])
                if row is None:
                    corrections_replayed = False
                    continue
                correction = correct_invalid_ohlc_from_reference(row, item.get("provider_runs", []))
                if correction.status == "resolved":
                    row.update(correction.proposed_values)
                else:
                    corrections_replayed = False
        classifications = classify_split_events(staged, symbol_actions, effective_as_of)
        resolution_evidence_sha256 = None
        if evidence and any(item.treatment == "ambiguous" for item in classifications):
            action_by_id = {item.action_id: item for item in symbol_actions}
            event_by_id = {item["action_id"]: item for item in evidence.get("events", [])}
            resolved = []
            for classification in classifications:
                event = event_by_id.get(classification.action_id)
                action = action_by_id.get(classification.action_id)
                if classification.treatment != "ambiguous" or event is None or action is None:
                    resolved.append(classification)
                    continue
                provider_runs = event.get("provider_runs", [])
                provider = event.get("provider")
                reference_basis = (
                    "adjusted" if provider == "massive" else classify_reference_basis(provider_runs, action)
                )
                if reference_basis == "ambiguous":
                    resolved.append(classification)
                    continue
                reference = classify_split_from_reference(
                    staged,
                    provider_runs,
                    action,
                    reference_basis=reference_basis,
                )
                if reference.treatment == "ambiguous":
                    resolved.append(classification)
                    continue
                resolved.append(
                    replace(
                        classification,
                        treatment=reference.treatment,
                        confidence=reference.confidence,
                    )
                )
            classifications = resolved
        if evidence_path and corrections_replayed and all(item.treatment != "ambiguous" for item in classifications):
            resolution_evidence_sha256 = _sha256(evidence_path)
        eligible = all(item.treatment != "ambiguous" for item in classifications)
        replacements = []
        error = None
        if eligible and classifications:
            try:
                proposed = normalize_ib_rows(staged, classifications)
            except ValueError as exc:
                eligible = False
                error = str(exc)
                failed += 1
            else:
                for old, new in zip(rows, proposed, strict=True):
                    new["source"] = old["source"]
                replacements = [
                    {"trade_date": old["trade_date"], "original": old, "proposed": new}
                    for old, new in zip(rows, proposed, strict=True)
                    if old != new
                ]
        elif not eligible:
            failed += 1
        manifest_symbols.append(
            {
                "approved": False,
                "classifications": [_classification_payload(item) for item in classifications],
                "eligible": eligible,
                "error": error,
                "path": str(path),
                "replacements": replacements,
                "resolution_evidence_sha256": resolution_evidence_sha256,
                "source_sha256": _sha256(path),
                "symbol": symbol,
            }
        )
    payload = {
        "as_of_date": effective_as_of.isoformat(),
        "data_lake_root": str(root),
        "schema_version": 1,
        "symbols": manifest_symbols,
    }
    write_json_atomic(args.output, payload)
    print(json.dumps({"failed": failed, "output": str(args.output), "symbols": len(symbols)}, sort_keys=True))
    return 1 if failed else 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
