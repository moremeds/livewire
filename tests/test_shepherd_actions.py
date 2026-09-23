from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.corporate_action_store import CorporateActionStore, SplitAddition
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


JULY = datetime(2026, 7, 13, 8, 42, tzinfo=UTC)  # before per-event provenance existed (2026-08-31)


def _split(payload: dict) -> MassiveSplit:
    """A split as ingested in July: no source_ref, source_hash, fetched-at or cursor."""
    return MassiveSplit(
        provider_event_id=payload["id"],
        ticker=payload["ticker"],
        execution_date=date.fromisoformat(payload["execution_date"]),
        split_from=Decimal(str(payload["split_from"])),
        split_to=Decimal(str(payload["split_to"])),
        payload_hash=MassiveClient._payload_hash(payload),
    )


def _fetch(root: Path, symbol: str, splits: list[dict]) -> None:
    body = json.dumps({"status": "OK", "results": splits}, separators=(",", ":")).encode()
    pages = [
        _page(root, "splits", body, "sha256:" + "5" * 64),
        _page(root, "dividends", b'{"status":"OK","results":[]}', "sha256:" + "6" * 64),
    ]
    CorporateActionStore(root).record_fetch(symbol, pages, AT, full_reconcile=True)


PAYLOAD = {"id": "split-1", "ticker": "NVDA", "execution_date": "2024-06-10", "split_from": 1, "split_to": 10}


def test_a_null_provenance_july_lineage_row_does_not_block_a_head_proven_by_the_latest_page(tmp_path: Path) -> None:
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split({**PAYLOAD, "split_to": 4})], JULY)
    store.reconcile("NVDA", [_split(PAYLOAD)], JULY + timedelta(days=1))  # rev 2 corrects rev 1
    _fetch(tmp_path, "NVDA", [PAYLOAD])

    receipt = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)

    item = receipt["symbols"][0]
    assert (receipt["version"], item["state"], item["issues"]) == (2, "VERIFIED", [])
    assert [(row["eventRevision"], row["statusAtAsOf"]) for row in item["actions"]] == [
        (1, "superseded"),
        (2, "active"),
    ]


def test_a_head_whose_payload_the_latest_page_does_not_carry_is_unresolved(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).reconcile("NVDA", [_split(PAYLOAD)], JULY)
    _fetch(tmp_path, "NVDA", [{**PAYLOAD, "split_to": 4}])

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert (item["state"], item["issues"]) == ("UNRESOLVED", ["event-not-in-latest-fetch:split-1:1"])


def test_a_cancelled_head_is_proven_by_its_absence_from_the_latest_page(tmp_path: Path) -> None:
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split(PAYLOAD)], JULY)
    store.reconcile("NVDA", [], JULY + timedelta(days=1), full_reconcile=True)
    _fetch(tmp_path, "NVDA", [])

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert (item["state"], item["issues"]) == ("VERIFIED", [])
    assert [row["statusAtAsOf"] for row in item["actions"]] == ["superseded", "cancelled"]


def test_a_cancelled_head_the_provider_still_lists_is_unresolved(tmp_path: Path) -> None:
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split(PAYLOAD)], JULY)
    store.reconcile("NVDA", [], JULY + timedelta(days=1), full_reconcile=True)
    _fetch(tmp_path, "NVDA", [PAYLOAD])

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert (item["state"], item["issues"]) == ("UNRESOLVED", ["cancelled-event-in-latest-fetch:split-1:2"])


def test_a_non_massive_head_is_never_proven_by_a_massive_page(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).apply_repairs(
        "NVDA", add_splits=[SplitAddition(date(1999, 6, 1), 1.0, 2.0)], cancel_ex_dates=[], fetched_at=JULY
    )
    _fetch(tmp_path, "NVDA", [])

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert item["state"] == "UNRESOLVED"
    assert [issue.split(":")[:2] for issue in item["issues"]] == [["non-massive-head-without-proof", "yahoo"]]


def test_an_unreadable_latest_page_blocks_every_massive_head(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).reconcile("NVDA", [_split(PAYLOAD)], JULY)
    _fetch(tmp_path, "NVDA", [PAYLOAD])
    fetch = CorporateActionStore(tmp_path).fetch_history("NVDA")[-1]
    SourceEvidenceStore(tmp_path).raw_path(fetch.source_hashes[0]).write_bytes(b"tampered")

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert item["issues"] == ["missing-or-corrupt-fetch-evidence", "unreadable-latest-fetch-evidence"]


def test_a_latest_page_without_a_results_list_is_unreadable(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).reconcile("NVDA", [_split(PAYLOAD)], JULY)
    pages = [
        _page(tmp_path, "splits", b'{"status":"OK"}', "sha256:" + "5" * 64),
        _page(tmp_path, "dividends", b'{"status":"OK","results":[]}', "sha256:" + "6" * 64),
    ]
    CorporateActionStore(tmp_path).record_fetch("NVDA", pages, AT, full_reconcile=True)

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert item["issues"] == ["unreadable-latest-fetch-evidence"]


def test_the_export_command_prints_a_v2_receipt(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from livewire_scripts.shepherd_actions import main

    _verified_empty_fetch(tmp_path, "AAPL")
    as_of = (AT + timedelta(minutes=1)).isoformat()

    assert main(["--data-lake-root", str(tmp_path), "export", "--symbols", "AAPL", "--as-of", as_of]) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert (receipt["version"], receipt["summary"]["verified"]) == (2, 1)


def test_a_provider_revision_after_as_of_does_not_change_the_replay(tmp_path: Path) -> None:
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split(PAYLOAD)], JULY)
    _fetch(tmp_path, "NVDA", [PAYLOAD])
    as_of = AT + timedelta(minutes=1)
    before = export_actions(["NVDA"], as_of, data_lake_root=tmp_path)

    july = store.history("NVDA")
    # AXON 2004-02-11, 2026-09-23T06:01Z: the provider revised a July split after as-of.
    store.reconcile("NVDA", [_split({**PAYLOAD, "split_to": 4})], as_of + timedelta(hours=1))

    assert [row for row in store.history("NVDA") if row.event_revision == 1] == july  # append only
    assert export_actions(["NVDA"], as_of, data_lake_root=tmp_path) == before
    assert [row["statusAtAsOf"] for row in before["symbols"][0]["actions"]] == ["active"]


def test_a_cancellation_later_revived_still_replays_as_cancelled(tmp_path: Path) -> None:
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split(PAYLOAD)], JULY)
    store.reconcile("NVDA", [], JULY + timedelta(days=1), full_reconcile=True)
    _fetch(tmp_path, "NVDA", [])
    as_of = AT + timedelta(minutes=1)
    before = export_actions(["NVDA"], as_of, data_lake_root=tmp_path)

    store.reconcile("NVDA", [_split(PAYLOAD)], as_of + timedelta(hours=1))  # the provider lists it again

    assert export_actions(["NVDA"], as_of, data_lake_root=tmp_path) == before
    assert before["symbols"][0]["state"] == "VERIFIED"


def test_a_split_the_provider_lists_under_another_id_is_proven_by_its_content(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).reconcile("NVDA", [_split(PAYLOAD)], JULY)
    _fetch(tmp_path, "NVDA", [{**PAYLOAD, "id": "split-1-alias"}])  # CSX 2006-08-16: two ids, alternate days

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert (item["state"], item["issues"]) == ("VERIFIED", [])


def test_a_split_under_another_id_at_another_ratio_is_not_the_same_split(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).reconcile("NVDA", [_split(PAYLOAD)], JULY)
    _fetch(tmp_path, "NVDA", [{**PAYLOAD, "id": "split-1-alias", "split_to": 4}])

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert (item["state"], item["issues"]) == ("UNRESOLVED", ["event-not-in-latest-fetch:split-1:1"])


def _rewrite_status_in_place(root: Path, symbol: str, action_id: str, status: str) -> None:
    """What reconcile did to a superseded row until 2026-09-23."""
    store = CorporateActionStore(root)
    path = store.path_for(symbol)
    rows = pq.ParquetFile(path).read().to_pylist()
    for row in rows:
        if row["action_id"] == action_id:
            row["status"] = status
    pq.write_table(pa.Table.from_pylist(rows, schema=store.schema), path)


@pytest.mark.parametrize(
    ("revived_payload", "status_at_as_of"),
    [(PAYLOAD, "cancelled"), ({**PAYLOAD, "split_to": 4}, "active")],
)
def test_a_head_rewritten_in_place_before_the_store_went_append_only_reads_its_as_of_status(
    tmp_path: Path, revived_payload: dict, status_at_as_of: str
) -> None:
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split(PAYLOAD)], JULY)
    if status_at_as_of == "cancelled":
        store.reconcile("NVDA", [], JULY + timedelta(days=1), full_reconcile=True)
    _fetch(tmp_path, "NVDA", [] if status_at_as_of == "cancelled" else [PAYLOAD])
    as_of = AT + timedelta(minutes=1)
    before = export_actions(["NVDA"], as_of, data_lake_root=tmp_path)
    head = max(store.history("NVDA"), key=lambda row: row.event_revision)

    store.reconcile("NVDA", [_split(revived_payload)], as_of + timedelta(hours=1))
    _rewrite_status_in_place(tmp_path, "NVDA", head.action_id, "corrected")

    after = export_actions(["NVDA"], as_of, data_lake_root=tmp_path)
    assert after == before
    assert before["symbols"][0]["actions"][-1]["statusAtAsOf"] == status_at_as_of
