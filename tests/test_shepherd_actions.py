from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.corporate_action_store import CorporateAction, CorporateActionStore, SplitAddition
from clients.massive_client import MassiveClient, MassivePageEvidence, MassiveSplit
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from clients.yahoo_client import YahooSplit
from livewire_scripts import sync_corporate_actions
from livewire_scripts.shepherd_actions import (
    _restoration_grade,
    _verify_restoration,
    _yahoo_bytes_list_the_split,
    export_actions,
)

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


# --- restore-yahoo-splits proof path -----------------------------------------

# IBM 2-for-1 split, 1968-04-23 (a yahoo row in the macmini store, checked 2026-09-23) -- pre-IB-floor (1993-01-29), so its
# restoration needs no IB evidence, only Yahoo's listing.
_IBM_EX = date(1968, 4, 23)


def _yahoo_chart_bytes(splits: list[YahooSplit]) -> bytes:
    events = {
        str(index): {
            "date": int(datetime.combine(split.ex_date, datetime.min.time(), tzinfo=UTC).timestamp()),
            "numerator": split.numerator,
            "denominator": split.denominator,
        }
        for index, split in enumerate(splits)
    }
    return json.dumps({"chart": {"result": [{"events": {"splits": events}}]}}).encode()


class _FakeYahooSplits:
    def __init__(self, splits: list[YahooSplit]):
        self._splits = splits

    def get_split_events(self, symbol: str) -> tuple[bytes, list[YahooSplit]]:
        return _yahoo_chart_bytes(self._splits), self._splits


def _seed_cancelled_ibm(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).apply_repairs(
        "IBM", add_splits=[SplitAddition(_IBM_EX, 1.0, 2.0)], cancel_ex_dates=[], fetched_at=JULY
    )
    CorporateActionStore(tmp_path).apply_repairs("IBM", add_splits=[], cancel_ex_dates=[_IBM_EX], fetched_at=JULY)


def _restore_ibm(tmp_path: Path, *, now: datetime, seed: bool = True) -> dict:
    if seed:
        _seed_cancelled_ibm(tmp_path)
    return sync_corporate_actions.restore_yahoo_splits(
        tickers=["IBM"],
        apply=True,
        ib_verify=True,
        output_dir=None,
        lake_root=tmp_path,
        now=now,
        yahoo_factory=lambda: _FakeYahooSplits([YahooSplit(_IBM_EX, 2.0, 1.0)]),
    )


def test_a_yahoo_only_restoration_is_proven_and_carries_its_grade(tmp_path: Path) -> None:
    restored_at = JULY + timedelta(days=60)
    result = _restore_ibm(tmp_path, now=restored_at)
    _fetch(tmp_path, "IBM", [])
    assert result["restored"] == 1

    item = export_actions(["IBM"], restored_at + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert item["state"] == "VERIFIED"
    head = max(item["actions"], key=lambda row: row["eventRevision"])
    assert head["evidenceGrade"] == "yahoo_only"
    assert head["statusAtAsOf"] == "active"


def test_a_tampered_restoration_envelope_is_restoration_evidence_invalid(tmp_path: Path) -> None:
    restored_at = JULY + timedelta(days=60)
    _restore_ibm(tmp_path, now=restored_at)
    _fetch(tmp_path, "IBM", [])
    head = CorporateActionStore(tmp_path).latest_active("IBM")[0]
    SourceEvidenceStore(tmp_path).raw_path(head.source_hash).write_bytes(b"tampered")

    item = export_actions(["IBM"], restored_at + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert item["state"] == "UNRESOLVED"
    tag = f"{head.provider_event_id}:{head.event_revision}"
    assert item["issues"] == [f"restoration-evidence-invalid:{tag}"]


def test_an_old_cancelled_yahoo_head_without_an_envelope_is_still_unproven(tmp_path: Path) -> None:
    """The pre-fix shape: a cancelled yahoo head with no `source_cursor_identity`
    at all -- `restore-yahoo-splits` never ran, so there is no envelope to check."""
    CorporateActionStore(tmp_path).apply_repairs(
        "NVDA", add_splits=[SplitAddition(date(2007, 9, 11), 1.0, 1.5)], cancel_ex_dates=[], fetched_at=JULY
    )
    CorporateActionStore(tmp_path).apply_repairs(
        "NVDA", add_splits=[], cancel_ex_dates=[date(2007, 9, 11)], fetched_at=JULY
    )
    _fetch(tmp_path, "NVDA", [])

    item = export_actions(["NVDA"], AT + timedelta(minutes=1), data_lake_root=tmp_path)["symbols"][0]

    assert item["state"] == "UNRESOLVED"
    assert [issue.split(":")[:2] for issue in item["issues"]] == [["non-massive-head-without-proof", "yahoo"]]


def test_a_receipt_before_the_restoration_is_unaffected_by_it(tmp_path: Path) -> None:
    """The revival's `fetched_at` is after this as-of, so it must never enter the
    replay -- other rows' payloads (and the whole receipt) stay byte-identical."""
    _seed_cancelled_ibm(tmp_path)
    as_of = JULY + timedelta(days=1)
    before = export_actions(["IBM"], as_of, data_lake_root=tmp_path)

    _restore_ibm(tmp_path, now=JULY + timedelta(days=60), seed=False)

    after = export_actions(["IBM"], as_of, data_lake_root=tmp_path)
    assert after == before


# --- direct unit tests for the proof helpers ---------------------------------


def test_restoration_grade_is_none_without_a_matching_cursor_identity(tmp_path: Path) -> None:
    CorporateActionStore(tmp_path).apply_repairs(
        "IBM",
        add_splits=[SplitAddition(_IBM_EX, 1.0, 2.0, source_cursor_identity="some-other-repair")],
        cancel_ex_dates=[],
        fetched_at=JULY,
    )
    head = CorporateActionStore(tmp_path).latest_active("IBM")[0]
    assert _restoration_grade(head) is None


def test_yahoo_bytes_list_the_split_true_when_it_matches() -> None:
    raw = _yahoo_chart_bytes([YahooSplit(_IBM_EX, 2.0, 1.0)])
    assert _yahoo_bytes_list_the_split(raw, _IBM_EX, 2.0) is True


def test_yahoo_bytes_list_the_split_false_on_malformed_json() -> None:
    assert _yahoo_bytes_list_the_split(b"not json", _IBM_EX, 2.0) is False


def test_yahoo_bytes_list_the_split_false_when_the_result_block_is_not_a_dict() -> None:
    raw = json.dumps({"chart": {"result": []}}).encode()
    assert _yahoo_bytes_list_the_split(raw, _IBM_EX, 2.0) is False


def _envelope(symbol: str, ex_date: date, split_from: float, split_to: float, grade: str, yahoo: dict, ib=None) -> dict:
    return {
        "kind": "yahoo-split-restoration",
        "version": 1,
        "symbol": symbol,
        "exDate": ex_date.isoformat(),
        "splitFrom": split_from,
        "splitTo": split_to,
        "grade": grade,
        "yahoo": yahoo,
        "ib": ib,
        "restoredAt": JULY.isoformat(),
    }


def _persist(store: SourceEvidenceStore, payload: bytes) -> tuple[str, str]:
    artifact = store.persist_raw(payload)
    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url="test://fixture",
            retrieved_at=JULY,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="application/json",
        )
    )
    return artifact.ref, artifact.sha256


def _head_row(tmp_path: Path, *, source_ref, source_hash, cursor_identity: str, grade_fields=None) -> CorporateAction:
    fields = grade_fields or {"ex_date": _IBM_EX, "split_from": 1.0, "split_to": 2.0}
    CorporateActionStore(tmp_path).apply_repairs(
        "IBM",
        add_splits=[
            SplitAddition(
                fields["ex_date"],
                fields["split_from"],
                fields["split_to"],
                source_ref=source_ref,
                source_hash=source_hash,
                source_fetched_at=JULY,
                source_cursor_identity=cursor_identity,
            )
        ],
        cancel_ex_dates=[],
        fetched_at=JULY,
    )
    return CorporateActionStore(tmp_path).latest_active("IBM")[0]


def test_verify_restoration_false_when_the_row_carries_no_envelope_reference(tmp_path: Path) -> None:
    head = _head_row(tmp_path, source_ref=None, source_hash=None, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", SourceEvidenceStore(tmp_path)) is False


def test_verify_restoration_false_on_malformed_envelope_json(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    ref, sha = _persist(evidence, b"not json")
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", evidence) is False


def test_verify_restoration_false_on_a_non_dict_envelope(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    ref, sha = _persist(evidence, json.dumps([1, 2, 3]).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", evidence) is False


def test_verify_restoration_false_when_the_envelope_ex_date_disagrees_with_the_row(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    yref, ysha = _persist(evidence, _yahoo_chart_bytes([YahooSplit(_IBM_EX, 2.0, 1.0)]))
    bad = _envelope("IBM", date(1999, 1, 1), 1.0, 2.0, "yahoo_only", {"ref": yref, "sha256": ysha})
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", evidence) is False


def test_verify_restoration_false_when_the_yahoo_block_is_not_a_dict(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    bad = _envelope("IBM", _IBM_EX, 1.0, 2.0, "yahoo_only", "not-a-dict")
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", evidence) is False


def test_verify_restoration_false_when_the_yahoo_artifact_is_missing(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    bad = _envelope(
        "IBM", _IBM_EX, 1.0, 2.0, "yahoo_only", {"ref": "artifact://sha256/" + "0" * 64, "sha256": "0" * 64}
    )
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", evidence) is False


def test_verify_restoration_false_when_the_yahoo_artifact_hash_disagrees(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    yref, _ = _persist(evidence, _yahoo_chart_bytes([YahooSplit(_IBM_EX, 2.0, 1.0)]))
    bad = _envelope("IBM", _IBM_EX, 1.0, 2.0, "yahoo_only", {"ref": yref, "sha256": "f" * 64})
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", evidence) is False


def test_verify_restoration_false_when_the_yahoo_response_does_not_list_the_split(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    yref, ysha = _persist(evidence, _yahoo_chart_bytes([]))  # no splits at all
    bad = _envelope("IBM", _IBM_EX, 1.0, 2.0, "yahoo_only", {"ref": yref, "sha256": ysha})
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:yahoo_only")
    assert _verify_restoration(head, "yahoo_only", evidence) is False


def test_verify_restoration_false_when_the_ib_block_is_missing_for_ib_verified(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    yref, ysha = _persist(evidence, _yahoo_chart_bytes([YahooSplit(_IBM_EX, 2.0, 1.0)]))
    bad = _envelope("IBM", _IBM_EX, 1.0, 2.0, "ib_verified", {"ref": yref, "sha256": ysha}, ib=None)
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:ib_verified")
    assert _verify_restoration(head, "ib_verified", evidence) is False


def test_verify_restoration_false_when_the_ib_artifact_is_missing(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    yref, ysha = _persist(evidence, _yahoo_chart_bytes([YahooSplit(_IBM_EX, 2.0, 1.0)]))
    bad = _envelope(
        "IBM",
        _IBM_EX,
        1.0,
        2.0,
        "ib_verified",
        {"ref": yref, "sha256": ysha},
        ib={"ref": "artifact://sha256/" + "1" * 64, "sha256": "1" * 64, "step": 0.5, "overlap": 8},
    )
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:ib_verified")
    assert _verify_restoration(head, "ib_verified", evidence) is False


def test_verify_restoration_false_when_the_ib_artifact_hash_disagrees(tmp_path: Path) -> None:
    evidence = SourceEvidenceStore(tmp_path)
    yref, ysha = _persist(evidence, _yahoo_chart_bytes([YahooSplit(_IBM_EX, 2.0, 1.0)]))
    iref, _ = _persist(evidence, b'{"real":"ib-bytes"}')  # readable, but metadata claims the wrong digest
    bad = _envelope(
        "IBM",
        _IBM_EX,
        1.0,
        2.0,
        "ib_verified",
        {"ref": yref, "sha256": ysha},
        ib={"ref": iref, "sha256": "f" * 64, "step": 0.5, "overlap": 8},
    )
    ref, sha = _persist(evidence, json.dumps(bad, sort_keys=True).encode())
    head = _head_row(tmp_path, source_ref=ref, source_hash=sha, cursor_identity="restore-yahoo-splits:ib_verified")
    assert _verify_restoration(head, "ib_verified", evidence) is False


def _valid_restoration(tmp_path: Path, ex_date: date, grade: str) -> CorporateAction:
    """A restoration whose envelope and Yahoo listing are both intact: only the grade policy can fail it."""
    evidence = SourceEvidenceStore(tmp_path)
    yref, ysha = _persist(evidence, _yahoo_chart_bytes([YahooSplit(ex_date, 2.0, 1.0)]))
    ib = None
    if grade == "ib_verified":
        iref, isha = _persist(evidence, b"[]")
        ib = {"ref": iref, "sha256": isha, "step": 2.0, "overlap": 10}
    ref, sha = _persist(
        evidence, json.dumps(_envelope("IBM", ex_date, 1.0, 2.0, grade, {"ref": yref, "sha256": ysha}, ib)).encode()
    )
    return _head_row(
        tmp_path,
        source_ref=ref,
        source_hash=sha,
        cursor_identity=f"restore-yahoo-splits:{grade}",
        grade_fields={"ex_date": ex_date, "split_from": 1.0, "split_to": 2.0},
    )


@pytest.mark.parametrize(
    ("ex_date", "grade", "proven"),
    [
        (_IBM_EX, "yahoo_only", True),  # below IB's floor Yahoo alone is the accepted evidence
        (date(1997, 5, 28), "yahoo_only", False),  # at/after the floor Yahoo alone proves nothing
        (_IBM_EX, "ib_verified", False),  # IB has no history below its floor to verify with
        (date(1997, 5, 28), "self_asserted", False),  # an unknown grade is never a proof
    ],
)
def test_the_restoration_grade_must_fit_the_ex_date(tmp_path: Path, ex_date: date, grade: str, proven: bool) -> None:
    head = _valid_restoration(tmp_path, ex_date, grade)

    assert _verify_restoration(head, grade, SourceEvidenceStore(tmp_path)) is proven


def test_the_yahoo_only_floor_is_ib_s_history_floor() -> None:
    from livewire_scripts.fetch_ib_historical import IB_EARLIEST_DATE
    from livewire_scripts.shepherd_actions import YAHOO_ONLY_BEFORE

    assert IB_EARLIEST_DATE.date() == YAHOO_ONLY_BEFORE
