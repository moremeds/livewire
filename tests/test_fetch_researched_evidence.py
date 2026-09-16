"""Tests for livewire_scripts/fetch_researched_evidence.py."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from clients.source_evidence import SourceEvidenceStore
from livewire_scripts import fetch_researched_evidence as fetch

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
]


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
    return path


@pytest.fixture
def csvs(tmp_path):
    round1 = _write_csv(tmp_path / "round1.csv", ROUND1_FIELDS, ROUND1_ROWS)
    round2 = _write_csv(tmp_path / "round2.csv", ROUND2_FIELDS, ROUND2_ROWS)
    return round1, round2


def test_wikipedia_title_extracts_from_wiki_url():
    assert fetch._wikipedia_title("https://en.wikipedia.org/wiki/Alcoa") == "Alcoa"
    assert fetch._wikipedia_title("https://www.sec.gov/cgi-bin/browse-edgar?CIK=1") is None


class _FakeResponse:
    def __init__(self, content: bytes, content_type: str = "text/html") -> None:
        self.content = content
        self.headers = {"Content-Type": content_type}
        self.url = "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281"

    def raise_for_status(self) -> None:
        pass


class _FakeSession:
    def get(self, url, headers=None, timeout=None):
        return _FakeResponse(f"page for {url}".encode())


def test_run_fetches_wikipedia_via_mediawiki_and_edgar_via_generic_get(csvs, tmp_path, monkeypatch):
    round1, round2 = csvs
    lake = tmp_path / "lake"

    def fake_snapshot(self, title):
        from clients.source_evidence import SourceEvidence

        artifact = self.store.persist_raw(f"wiki page for {title}".encode())
        evidence = SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=f"https://en.wikipedia.org/wiki/{title}",
            retrieved_at=NOW,
            publication_time=NOW,
            mediawiki_revision_id=1,
            mediawiki_revision_time=NOW,
            content_type="text/html",
        )
        self.store.record(evidence)
        return evidence

    from clients.mediawiki_client import MediaWikiClient

    monkeypatch.setattr(MediaWikiClient, "snapshot", fake_snapshot)
    monkeypatch.setattr(requests, "Session", lambda: _FakeSession())

    exit_code = fetch.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)
    assert exit_code == 0

    store = SourceEvidenceStore(lake)
    committed = {item.source_url for item in store.list_verified()}
    assert committed == {
        "https://en.wikipedia.org/wiki/Alcoa",
        "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281",
    }


def test_run_skips_already_committed_urls(csvs, tmp_path, monkeypatch):
    round1, round2 = csvs
    lake = tmp_path / "lake"
    store = SourceEvidenceStore(lake)
    artifact = store.persist_raw(b"already fetched")
    from clients.source_evidence import SourceEvidence

    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url="https://en.wikipedia.org/wiki/Alcoa",
            retrieved_at=NOW,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="text/html",
        )
    )

    calls = []

    def fail_snapshot(self, title):
        calls.append(title)
        raise AssertionError("should not refetch an already-committed URL")

    from clients.mediawiki_client import MediaWikiClient

    monkeypatch.setattr(MediaWikiClient, "snapshot", fail_snapshot)
    monkeypatch.setattr(requests, "Session", lambda: _FakeSession())

    exit_code = fetch.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)
    assert exit_code == 0
    assert calls == []
    store = SourceEvidenceStore(lake)
    committed = {item.source_url for item in store.list_verified()}
    assert "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281" in committed
