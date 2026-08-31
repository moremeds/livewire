from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveClient, MassivePageEvidence, MassiveSplit
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from livewire_scripts.shepherd_actions import export_actions

AT = datetime(2026, 8, 31, 1, 0, tzinfo=UTC)


def _page(root: Path, resource: str, payload: bytes, cursor: str) -> MassivePageEvidence:
    store = SourceEvidenceStore(root)
    artifact = store.persist_raw(payload)
    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=f"massive-response://sha256/{artifact.sha256}",
            retrieved_at=AT,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="application/json",
        )
    )
    return MassivePageEvidence(resource, artifact.ref, artifact.sha256, AT, cursor)


def _verified_empty_fetch(root: Path, symbol: str) -> list[MassivePageEvidence]:
    pages = [
        _page(root, "splits", f'{{"ticker":"{symbol}","results":[]}}'.encode(), "sha256:" + "1" * 64),
        _page(root, "dividends", f'{{"ticker":"{symbol}","results":[]}}'.encode(), "sha256:" + "2" * 64),
    ]
    CorporateActionStore(root).record_fetch(symbol, pages, AT, full_reconcile=True)
    return pages


def test_export_proves_zero_actions_from_both_exact_provider_responses(tmp_path: Path) -> None:
    _verified_empty_fetch(tmp_path, "AAPL")

    receipt = export_actions(["AAPL"], AT + timedelta(minutes=1), data_lake_root=tmp_path)

    item = receipt["symbols"][0]
    assert item["symbol"] == "AAPL"
    assert item["state"] == "VERIFIED"
    assert item["actions"] == []
    assert set(item["fetch"]["resources"]) == {"splits", "dividends"}
    assert receipt["mutated"] is False
    assert receipt["receiptHash"].startswith("sha256:")


def test_export_keeps_raw_and_revision_lineage_and_reconstructs_as_of_status(tmp_path: Path) -> None:
    store = CorporateActionStore(tmp_path)
    first_payload = {
        "id": "split-1",
        "ticker": "NVDA",
        "execution_date": "2024-06-10",
        "split_from": 1,
        "split_to": 10,
    }
    first_page = _page(
        tmp_path,
        "splits",
        json.dumps({"status": "OK", "results": [first_payload]}, separators=(",", ":")).encode(),
        "sha256:" + "1" * 64,
    )
    first_dividends = _page(tmp_path, "dividends", b'{"status":"OK","results":[]}', "sha256:" + "2" * 64)
    store.record_fetch("NVDA", [first_page, first_dividends], AT, full_reconcile=True)
    first = MassiveSplit(
        provider_event_id="split-1",
        ticker="NVDA",
        execution_date=date(2024, 6, 10),
        split_from=Decimal("1"),
        split_to=Decimal("10"),
        payload_hash=MassiveClient._payload_hash(first_payload),
        source_ref=first_page.ref,
        source_hash=first_page.sha256,
        source_fetched_at=AT,
        source_cursor_identity=first_page.cursor_identity,
    )
    store.reconcile("NVDA", [first], AT)
    corrected_at = AT + timedelta(days=1)
    corrected_payload = {**first_payload, "split_to": 4}
    corrected_page = _page(
        tmp_path,
        "splits",
        json.dumps({"status": "OK", "results": [corrected_payload]}, separators=(",", ":")).encode(),
        "sha256:" + "3" * 64,
    )
    corrected_dividends = _page(tmp_path, "dividends", b'{"status":"OK","results":[]}', "sha256:" + "4" * 64)
    second = MassiveSplit(
        **{
            **first.__dict__,
            "split_to": Decimal("4"),
            "payload_hash": MassiveClient._payload_hash(corrected_payload),
            "source_ref": corrected_page.ref,
            "source_hash": corrected_page.sha256,
            "source_fetched_at": corrected_at,
            "source_cursor_identity": corrected_page.cursor_identity,
        }
    )
    store.reconcile("NVDA", [second], corrected_at)
    store.record_fetch("NVDA", [corrected_page, corrected_dividends], corrected_at, full_reconcile=True)

    before = export_actions(["NVDA"], AT + timedelta(hours=1), data_lake_root=tmp_path)
    after = export_actions(["NVDA"], corrected_at + timedelta(hours=1), data_lake_root=tmp_path)

    assert [(row["eventRevision"], row["statusAtAsOf"]) for row in before["symbols"][0]["actions"]] == [(1, "active")]
    assert [(row["eventRevision"], row["statusAtAsOf"]) for row in after["symbols"][0]["actions"]] == [
        (1, "superseded"),
        (2, "active"),
    ]


def test_one_tampered_symbol_is_local_unresolved_and_does_not_hide_verified_peer(tmp_path: Path) -> None:
    bad_pages = _verified_empty_fetch(tmp_path, "AAPL")
    _verified_empty_fetch(tmp_path, "MSFT")
    SourceEvidenceStore(tmp_path).raw_path(bad_pages[0].sha256).write_bytes(b"tampered")

    receipt = export_actions(["AAPL", "MSFT"], AT + timedelta(minutes=1), data_lake_root=tmp_path)

    states = {item["symbol"]: item["state"] for item in receipt["symbols"]}
    assert states == {"AAPL": "UNRESOLVED", "MSFT": "VERIFIED"}
    assert receipt["summary"] == {"requested": 2, "verified": 1, "unresolved": 1}
