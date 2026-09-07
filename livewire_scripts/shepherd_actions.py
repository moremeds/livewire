#!/usr/bin/env python3
"""Export replayable point-in-time corporate-action evidence by symbol."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clients.corporate_action_store import CorporateAction, CorporateActionFetch, CorporateActionStore
from clients.source_evidence import SourceEvidenceStore, canonical_bytes
from clients.symbol_paths import canonical_symbol
from livewire_scripts.paths import data_lake_dir


def _hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _verified_ref(store: SourceEvidenceStore, ref: str, digest: str) -> bool:
    try:
        return _hash(store.read(ref)) == digest
    except (OSError, ValueError):
        return False


def _raw_event_matches(
    store: SourceEvidenceStore,
    ref: str,
    provider_event_id: str,
    payload_hash: str,
) -> bool:
    try:
        payload = json.loads(store.read(ref))
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    rows = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return False
    matches = [row for row in rows if isinstance(row, dict) and str(row.get("id", "")) == provider_event_id]
    if len(matches) != 1:
        return False
    return _hash(json.dumps(matches[0], sort_keys=True, separators=(",", ":"), default=str).encode()) == payload_hash


def _fetch_payload(fetch: CorporateActionFetch) -> dict[str, Any]:
    return {
        "fetchId": fetch.fetch_id,
        "fetchedAt": fetch.fetched_at.isoformat(),
        "fullReconcile": fetch.full_reconcile,
        "resources": list(fetch.resources),
        "sourceRefs": list(fetch.source_refs),
        "sourceHashes": list(fetch.source_hashes),
        "cursorIdentities": list(fetch.cursor_identities),
    }


def _action_payload(row: CorporateAction, status_at_as_of: str) -> dict[str, Any]:
    return {
        "actionId": row.action_id,
        "provider": row.provider,
        "providerEventId": row.provider_event_id,
        "eventRevision": row.event_revision,
        "supersedesActionId": row.supersedes_action_id,
        "symbol": row.symbol,
        "actionType": row.action_type,
        "exDate": row.ex_date.isoformat(),
        "splitFrom": row.split_from,
        "splitTo": row.split_to,
        "cashAmount": row.cash_amount,
        "currency": row.currency,
        "declarationDate": None if row.declaration_date is None else row.declaration_date.isoformat(),
        "recordDate": None if row.record_date is None else row.record_date.isoformat(),
        "payDate": None if row.pay_date is None else row.pay_date.isoformat(),
        "storageStatus": row.status,
        "statusAtAsOf": status_at_as_of,
        "fetchedAt": row.fetched_at.isoformat(),
        "payloadHash": row.payload_hash,
        "sourceRef": row.source_ref,
        "sourceHash": row.source_hash,
        "sourceFetchedAt": None if row.source_fetched_at is None else row.source_fetched_at.isoformat(),
        "sourceCursorIdentity": row.source_cursor_identity,
    }


def _export_symbol(
    symbol: str,
    as_of: datetime,
    *,
    action_store: CorporateActionStore,
    evidence_store: SourceEvidenceStore,
) -> dict[str, Any]:
    issues: list[str] = []
    fetches = [fetch for fetch in action_store.fetch_history(symbol) if fetch.fetched_at <= as_of]
    bound_pages: set[tuple[str, str, str]] = set()
    for fetch in fetches:
        if len(fetch.source_refs) == len(fetch.source_hashes) == len(fetch.cursor_identities):
            bound_pages.update(zip(fetch.source_refs, fetch.source_hashes, fetch.cursor_identities, strict=True))
    latest_fetch = fetches[-1] if fetches else None
    if latest_fetch is None:
        issues.append("missing-provider-fetch-receipt")
    else:
        lengths = {
            len(latest_fetch.resources),
            len(latest_fetch.source_refs),
            len(latest_fetch.source_hashes),
            len(latest_fetch.cursor_identities),
        }
        if lengths != {len(latest_fetch.resources)} or not {"splits", "dividends"}.issubset(latest_fetch.resources):
            issues.append("incomplete-provider-fetch-receipt")
        if len(latest_fetch.source_refs) == len(latest_fetch.source_hashes):
            for ref, digest in zip(latest_fetch.source_refs, latest_fetch.source_hashes, strict=True):
                if not _verified_ref(evidence_store, ref, digest):
                    issues.append("missing-or-corrupt-fetch-evidence")

    rows = [row for row in action_store.history(symbol) if row.fetched_at <= as_of]
    by_event: dict[str, list[CorporateAction]] = defaultdict(list)
    for row in rows:
        by_event[row.provider_event_id].append(row)
        if (
            row.source_ref is None
            or row.source_hash is None
            or row.source_fetched_at is None
            or row.source_cursor_identity is None
        ):
            issues.append(f"missing-event-evidence:{row.provider_event_id}:{row.event_revision}")
        elif row.source_fetched_at > as_of:
            issues.append(f"future-event-evidence:{row.provider_event_id}:{row.event_revision}")
        elif not _verified_ref(evidence_store, row.source_ref, row.source_hash):
            issues.append(f"missing-or-corrupt-event-evidence:{row.provider_event_id}:{row.event_revision}")
        elif (row.source_ref, row.source_hash, row.source_cursor_identity) not in bound_pages:
            issues.append(f"unbound-event-evidence:{row.provider_event_id}:{row.event_revision}")
        elif not _raw_event_matches(evidence_store, row.source_ref, row.provider_event_id, row.payload_hash):
            issues.append(f"event-payload-not-in-raw-evidence:{row.provider_event_id}:{row.event_revision}")

    payloads: list[dict[str, Any]] = []
    for event_id in sorted(by_event):
        revisions = sorted(by_event[event_id], key=lambda row: (row.event_revision, row.action_id))
        latest = revisions[-1]
        for row in revisions:
            if row is not latest:
                status = "superseded"
            elif row.status == "cancelled":
                status = "cancelled"
            else:
                status = "active"
            payloads.append(_action_payload(row, status))

    return {
        "symbol": symbol,
        "state": "VERIFIED" if not issues else "UNRESOLVED",
        "fetch": None if latest_fetch is None else _fetch_payload(latest_fetch),
        "actions": payloads,
        "issues": sorted(set(issues)),
    }


def export_actions(symbols: list[str], as_of: datetime, *, data_lake_root: Path) -> dict[str, Any]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as-of must be timezone-aware")
    root = Path(data_lake_root).expanduser()
    action_store = CorporateActionStore(root)
    evidence_store = SourceEvidenceStore(root)
    normalized = sorted(set(canonical_symbol(symbol) for symbol in symbols))
    if not normalized:
        raise ValueError("at least one symbol is required")
    items = [
        _export_symbol(symbol, as_of, action_store=action_store, evidence_store=evidence_store) for symbol in normalized
    ]
    verified = sum(item["state"] == "VERIFIED" for item in items)
    receipt: dict[str, Any] = {
        "version": 1,
        "operation": "shepherd-actions-export",
        "asOf": as_of.astimezone(UTC).isoformat(),
        "symbols": items,
        "summary": {
            "requested": len(items),
            "verified": verified,
            "unresolved": len(items) - verified,
        },
        "mutated": False,
    }
    receipt["receiptHash"] = f"sha256:{_hash(canonical_bytes(receipt))}"
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-root", type=Path, default=data_lake_dir())
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export")
    export.add_argument("--symbols", nargs="+", required=True)
    export.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = export_actions(args.symbols, args.as_of, data_lake_root=args.data_lake_root)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
