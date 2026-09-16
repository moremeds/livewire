"""Tests for livewire_scripts/import_researched_identities.py."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients.security_master import SecurityMaster
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from livewire_scripts import import_researched_identities as imp

NOW = datetime(2026, 9, 17, 0, 0, tzinfo=UTC)

ROUND1_FIELDS = [
    "index_id",
    "ticker",
    "action",
    "effective_at",
    "company_name",
    "candidate_cik",
    "date_range_evidence",
    "resolution",
    "confidence",
    "source_url",
    "notes",
]
ROUND2_FIELDS = [
    "index_id",
    "ticker",
    "action",
    "effective_at",
    "company_name",
    "candidate_cik",
    "exchange_mic",
    "exchange_confidence",
    "exchange_source_url",
    "exchange_notes",
]


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
    return path


# Two djia add/remove events for one company (shares a CIK -> one security_id),
# one not_found row (excluded by resolution), one row with no CIK (excluded),
# one CIK-backed row with no confirmed exchange (excluded by round 2).
ROUND1_ROWS = [
    {
        "index_id": "djia",
        "ticker": "AA",
        "action": "add",
        "effective_at": "1991-05-06",
        "company_name": "Alcoa Inc.",
        "candidate_cik": "0000004281",
        "date_range_evidence": "1959-2016",
        "resolution": "resolved",
        "confidence": "high",
        "source_url": "https://en.wikipedia.org/wiki/Alcoa",
        "notes": "",
    },
    {
        "index_id": "djia",
        "ticker": "AA",
        "action": "remove",
        "effective_at": "2013-09-23",
        "company_name": "Alcoa Inc.",
        "candidate_cik": "0000004281",
        "date_range_evidence": "1959-2016",
        "resolution": "resolved",
        "confidence": "high",
        "source_url": "https://en.wikipedia.org/wiki/Alcoa",
        "notes": "",
    },
    {
        "index_id": "sp500",
        "ticker": "AFS.A",
        "action": "add",
        "effective_at": "1998-04-08",
        "company_name": "",
        "candidate_cik": "",
        "date_range_evidence": "",
        "resolution": "not_found",
        "confidence": "low",
        "source_url": "",
        "notes": "",
    },
    {
        "index_id": "ndx100",
        "ticker": "AEOS",
        "action": "add",
        "effective_at": "1998-01-01",
        "company_name": "American Eagle Outfitters",
        "candidate_cik": "",
        "date_range_evidence": "current",
        "resolution": "resolved",
        "confidence": "medium",
        "source_url": "https://en.wikipedia.org/wiki/American_Eagle_Outfitters",
        "notes": "no CIK found",
    },
]

ROUND2_ROWS = [
    {
        "index_id": "djia",
        "ticker": "AA",
        "action": "add",
        "effective_at": "1991-05-06",
        "company_name": "Alcoa Inc.",
        "candidate_cik": "0000004281",
        "exchange_mic": "XNYS",
        "exchange_confidence": "high",
        "exchange_source_url": "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281",
        "exchange_notes": "",
    },
    {
        "index_id": "djia",
        "ticker": "AA",
        "action": "remove",
        "effective_at": "2013-09-23",
        "company_name": "Alcoa Inc.",
        "candidate_cik": "0000004281",
        "exchange_mic": "XNYS",
        "exchange_confidence": "high",
        "exchange_source_url": "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281",
        "exchange_notes": "",
    },
]


@pytest.fixture
def csvs(tmp_path):
    round1 = _write_csv(tmp_path / "round1.csv", ROUND1_FIELDS, ROUND1_ROWS)
    round2 = _write_csv(tmp_path / "round2.csv", ROUND2_FIELDS, ROUND2_ROWS)
    return round1, round2


def test_load_rows_keeps_only_resolved_cik_backed_mic_backed_rows(csvs):
    round1, round2 = csvs
    rows = imp.load_rows(round1, round2)
    assert len(rows) == 2
    assert {row.ticker for row in rows} == {"AA"}
    assert all(row.cik == "0000004281" for row in rows)
    assert all(row.exchange_mic == "XNYS" for row in rows)


def _seed_evidence(store: SourceEvidenceStore, url: str) -> None:
    artifact = store.persist_raw(f"evidence for {url}".encode())
    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=url,
            retrieved_at=NOW,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="text/html",
        )
    )


def test_derive_events_groups_by_cik_under_one_security_id(csvs):
    round1, round2 = csvs
    rows = imp.load_rows(round1, round2)
    evidence = {
        "https://en.wikipedia.org/wiki/Alcoa": SourceEvidence(
            ref="artifact://sha256/" + "a" * 64,
            sha256="a" * 64,
            source_url="https://en.wikipedia.org/wiki/Alcoa",
            retrieved_at=NOW,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="text/html",
        ),
        "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281": SourceEvidence(
            ref="artifact://sha256/" + "b" * 64,
            sha256="b" * 64,
            source_url="https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281",
            retrieved_at=NOW,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="text/html",
        ),
    }
    events, skipped = imp.derive_events(rows, existing=[], evidence=evidence, now=NOW)
    assert skipped == []
    assert len(events) == 2
    assert len({event.security_id for event in events}) == 1
    assert sorted(event.revision for event in events) == [1, 2]
    assert all(event.provider == imp.PROVIDER for event in events)
    assert all(event.status == "verified" for event in events)
    assert all(event.continuity_basis == "regulator_filing" for event in events)
    add = next(event for event in events if event.effective_from.date().isoformat() == "1991-05-05")
    assert add.effective_to.date().isoformat() == "1991-05-07"


def test_derive_events_skips_rows_with_no_committed_evidence(csvs):
    round1, round2 = csvs
    rows = imp.load_rows(round1, round2)
    events, skipped = imp.derive_events(rows, existing=[], evidence={}, now=NOW)
    assert events == []
    assert len(skipped) == 2


def test_derive_events_reuses_the_security_id_of_an_earlier_import(csvs):
    """A rerun for the same CIK must not open a second identity."""
    round1, round2 = csvs
    rows = imp.load_rows(round1, round2)[:1]  # just the "add" row
    evidence = {
        "https://en.wikipedia.org/wiki/Alcoa": SourceEvidence(
            ref="artifact://sha256/" + "a" * 64,
            sha256="a" * 64,
            source_url="https://en.wikipedia.org/wiki/Alcoa",
            retrieved_at=NOW,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="text/html",
        ),
    }
    first_events, _ = imp.derive_events(rows, existing=[], evidence=evidence, now=NOW)
    assert len(first_events) == 1
    second_events, second_skipped = imp.derive_events(rows, existing=first_events, evidence=evidence, now=NOW)
    assert second_events == []
    assert second_skipped == []


def test_run_appends_verified_events_that_membership_sync_can_then_resolve(csvs, tmp_path):
    round1, round2 = csvs
    lake = tmp_path / "lake"
    store = SourceEvidenceStore(lake)
    _seed_evidence(store, "https://en.wikipedia.org/wiki/Alcoa")
    _seed_evidence(store, "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281")

    exit_code = imp.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)
    assert exit_code == 0

    from livewire_scripts.membership_sync import _resolve

    master = SecurityMaster(lake, evidence_verifier=None)
    resolved_add = _resolve(master, "AA", datetime(1991, 5, 6, tzinfo=UTC), NOW)
    resolved_remove = _resolve(master, "AA", datetime(2013, 9, 23, tzinfo=UTC), NOW)
    assert resolved_add is not None
    assert resolved_add == resolved_remove


def test_run_is_idempotent(csvs, tmp_path):
    round1, round2 = csvs
    lake = tmp_path / "lake"
    store = SourceEvidenceStore(lake)
    _seed_evidence(store, "https://en.wikipedia.org/wiki/Alcoa")
    _seed_evidence(store, "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281")

    imp.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)
    master = SecurityMaster(lake, evidence_verifier=None)
    before = len(master.events())

    imp.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)
    master = SecurityMaster(lake, evidence_verifier=None)
    assert len(master.events()) == before


def test_event_id_is_deterministic_across_runs():
    row = imp.ResearchedRow(
        index_id="djia",
        ticker="AA",
        action="add",
        effective_at=datetime(1991, 5, 6, tzinfo=UTC),
        company_name="Alcoa Inc.",
        cik="0000004281",
        exchange_mic="XNYS",
        source_url="https://en.wikipedia.org/wiki/Alcoa",
        exchange_source_url="https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281",
    )
    refs = ("artifact://sha256/" + "a" * 64,)
    first = imp._event_id("sec_x", 1, row, refs)
    second = imp._event_id("sec_x", 1, row, refs)
    assert first == second
    assert len(first) == 64 and all(c in "0123456789abcdef" for c in first)
