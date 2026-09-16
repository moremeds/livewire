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
    """Seed evidence under the key it would *really* be stored under.

    `MediaWikiClient` records `source_url` as the REST endpoint it fetched,
    never the `/wiki/` page a row cites (`imp._evidence_key` is the
    translation) — seeding under the raw `url` would hide exactly the bug
    this file exists to catch.
    """
    stored_url = imp._evidence_key(url)
    artifact = store.persist_raw(f"evidence for {url}".encode())
    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=stored_url,
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
    wiki_key = imp._evidence_key("https://en.wikipedia.org/wiki/Alcoa")
    evidence = {
        wiki_key: SourceEvidence(
            ref="artifact://sha256/" + "a" * 64,
            sha256="a" * 64,
            source_url=wiki_key,
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
    wiki_key = imp._evidence_key("https://en.wikipedia.org/wiki/Alcoa")
    evidence = {
        wiki_key: SourceEvidence(
            ref="artifact://sha256/" + "a" * 64,
            sha256="a" * 64,
            source_url=wiki_key,
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


def test_evidence_key_translates_a_wiki_url_to_the_mediawiki_rest_endpoint():
    assert imp._evidence_key("https://en.wikipedia.org/wiki/Alcoa") == (
        "https://en.wikipedia.org/w/rest.php/v1/page/Alcoa/html"
    )
    # A space in the title becomes an underscore, then percent-encoded punctuation.
    assert imp._evidence_key("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") == (
        "https://en.wikipedia.org/w/rest.php/v1/page/List_of_S%26P_500_companies/html"
    )


def test_evidence_key_leaves_non_wikipedia_urls_untouched():
    edgar_url = "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281"
    assert imp._evidence_key(edgar_url) == edgar_url


def test_evidence_key_matches_what_mediawikiclient_actually_fetches():
    """Fix the twin: this must track MediaWikiClient's own URL construction,
    not just agree with a second copy of the same hardcoded string."""
    from clients import mediawiki_client

    title = "Historical_components_of_the_S&P_500"
    encoded_title = mediawiki_client.quote(title.replace(" ", "_"), safe="")
    expected = f"{mediawiki_client._REST_ROOT}/{encoded_title}/html"
    assert imp._evidence_key(f"https://en.wikipedia.org/wiki/{title}") == expected


def test_derive_events_matches_wiki_evidence_seeded_under_the_rest_endpoint_key(csvs, tmp_path):
    """Reproduces the real bug: evidence committed by the real fetch script
    (keyed by the REST endpoint) must still resolve a row citing the plain
    /wiki/ page — this is the exact mismatch found on 2026-09-17 production
    data (312/759 rows silently unmatched despite evidence existing)."""
    round1, round2 = csvs
    rows = imp.load_rows(round1, round2)
    lake = tmp_path / "lake"
    store = SourceEvidenceStore(lake)
    _seed_evidence(store, "https://en.wikipedia.org/wiki/Alcoa")
    _seed_evidence(store, "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281")

    evidence = imp.evidence_by_url(store)
    # Confirm the store really did key this under the REST endpoint, not the
    # plain wiki URL — otherwise this test would pass for the wrong reason.
    assert "https://en.wikipedia.org/wiki/Alcoa" not in evidence
    assert imp._evidence_key("https://en.wikipedia.org/wiki/Alcoa") in evidence

    events, skipped = imp.derive_events(rows, existing=[], evidence=evidence, now=NOW)
    assert skipped == []
    assert len(events) == 2
