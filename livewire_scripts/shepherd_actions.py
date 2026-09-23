#!/usr/bin/env python3
"""Export replayable point-in-time corporate-action evidence by symbol."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clients.corporate_action_store import (
    RECONCILE_PROVIDER,
    CorporateAction,
    CorporateActionFetch,
    CorporateActionStore,
)
from clients.source_evidence import SourceEvidenceStore, canonical_bytes
from clients.symbol_paths import canonical_symbol
from livewire_scripts.paths import data_lake_dir

# v1 proved every stored row and copied the store's mutable status column, so no v1 receipt
# replays once a later revision lands; v2 proves each event's head row at as-of.
RECEIPT_VERSION = 2


def _hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _verified_ref(store: SourceEvidenceStore, ref: str, digest: str) -> bool:
    try:
        return _hash(store.read(ref)) == digest
    except (OSError, ValueError):
        return False


@dataclass(frozen=True)
class _Page:
    """What one fetch's verified pages carry: payload hashes by provider id, and split content."""

    by_id: dict[str, set[str]]
    splits: set[tuple[str, float, float]]


def _page_events(store: SourceEvidenceStore, fetch: CorporateActionFetch) -> _Page | None:
    """Index every provider event in one fetch's pages; None if any page fails to verify."""
    if not len(fetch.resources) == len(fetch.source_refs) == len(fetch.source_hashes):
        return None
    page = _Page(defaultdict(set), set())
    for resource, ref, digest in zip(fetch.resources, fetch.source_refs, fetch.source_hashes, strict=True):
        try:
            payload_bytes = store.read(ref)
            payload = json.loads(payload_bytes)
        except (OSError, ValueError):  # JSONDecodeError is a ValueError
            return None
        rows = payload.get("results") if isinstance(payload, dict) else None
        if _hash(payload_bytes) != digest or not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                continue
            page.by_id[str(row.get("id", ""))].add(
                _hash(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode())
            )
            if resource == "splits":
                try:
                    page.splits.add((str(row["execution_date"]), float(row["split_from"]), float(row["split_to"])))
                except (KeyError, TypeError, ValueError):
                    pass  # a malformed split row can still match by id, never by content
    return page


def _listed(head: CorporateAction, page: _Page) -> bool:
    """The page carries this head: by provider id and payload, or, for a split, by content.

    Massive hands the same split two ids on alternate days (CSX 2006-08-16 flipped between
    two ids six times between 2026-09-16 and 09-23), so an id-only match left 5 ndx100 and
    28 sp500 splits unresolved that the as-of page listed at the same date and ratio.
    """
    if page.by_id.get(head.provider_event_id) == {head.payload_hash}:
        return True
    # ponytail: splits only — no dividend id flapping measured on 2026-09-23; add a dividend key when one is.
    return (
        head.action_type == "split"
        and head.split_from is not None
        and head.split_to is not None
        and (head.ex_date.isoformat(), float(head.split_from), float(head.split_to)) in page.splits
    )


def _status_at_as_of(head: CorporateAction, page: _Page | None) -> str:
    """The head's status at as-of, never the store's live column.

    ``reconcile`` rewrites a superseded row's ``status`` to ``corrected`` in place, so a head
    whose successor arrived after as-of reads ``corrected`` today (AXON 2004-02-11, revised
    2026-09-23T06:01Z, broke replay of both PIT manifests published at 04:25Z). The row
    alone cannot say whether it was active or cancelled; the as-of page can.
    """
    if head.status != "corrected":
        return head.status
    if head.provider == RECONCILE_PROVIDER and page is not None:
        return "active" if _listed(head, page) else "cancelled"
    return "active"


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
    # Only each event's head row at as-of must be proven, not the lineage behind it. Rows
    # ingested before per-event provenance existed (2026-08-31) carry no source_ref and
    # reconcile never back-fills an unchanged event, so proving every row left 413 of 458
    # sp500 symbols unresolved whose every head sat, byte for byte, in the latest page.
    page = None if latest_fetch is None else _page_events(evidence_store, latest_fetch)
    if latest_fetch is not None and page is None:
        issues.append("unreadable-latest-fetch-evidence")
    payloads: list[dict[str, Any]] = []
    for event_id in sorted(by_event):
        revisions = sorted(by_event[event_id], key=lambda row: (row.event_revision, row.action_id))
        head = revisions[-1]
        status = _status_at_as_of(head, page)
        tag = f"{event_id}:{head.event_revision}"
        if head.provider != RECONCILE_PROVIDER:
            # Nothing in a Massive page speaks for a yahoo/eod_fx/legacy row, and none of
            # them carries a provider page of its own: no proof path exists yet.
            issues.append(f"non-massive-head-without-proof:{head.provider}:{tag}")
        elif page is None:
            pass  # the fetch receipt itself is already an issue
        elif status == "cancelled":
            # A full page set that no longer lists the event is the cancellation's proof.
            if event_id in page.by_id:
                issues.append(f"cancelled-event-in-latest-fetch:{tag}")
        elif status != "active":
            issues.append(f"unexpected-head-status:{tag}")
        elif not _listed(head, page):
            issues.append(f"event-not-in-latest-fetch:{tag}")
        payloads.extend(_action_payload(row, "superseded") for row in revisions[:-1])
        payloads.append(_action_payload(head, status))

    return {
        "symbol": symbol,
        "state": "VERIFIED" if not issues else "UNRESOLVED",
        "fetch": None if latest_fetch is None else _fetch_payload(latest_fetch),
        "actions": payloads,
        "issues": sorted(set(issues)),
    }


def export_actions(
    symbols: list[str],
    as_of: datetime,
    *,
    data_lake_root: Path,
) -> dict[str, Any]:
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
        "version": RECEIPT_VERSION,
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
