"""Tests for livewire_scripts/fetch_researched_evidence.py."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from clients.mediawiki_client import MediaWikiClient
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
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

_EDGAR_URL = "https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000004281"
_REQUEST = httpx.Request("GET", _EDGAR_URL)


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
    assert fetch._wikipedia_title(_EDGAR_URL) is None


def _edgar_response(status_code: int = 200, content_type: str = "text/html") -> httpx.Response:
    return httpx.Response(
        status_code,
        request=_REQUEST,
        content=b"edgar company page",
        headers={"Content-Type": content_type},
    )


def _fake_snapshot(self, title):
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


def test_run_fetches_wikipedia_via_mediawiki_and_edgar_via_generic_get(csvs, tmp_path, monkeypatch):
    round1, round2 = csvs
    lake = tmp_path / "lake"

    monkeypatch.setattr(MediaWikiClient, "snapshot", _fake_snapshot)
    with patch("livewire_scripts.fetch_researched_evidence.httpx.get", return_value=_edgar_response()):
        exit_code = fetch.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)
    assert exit_code == 0

    store = SourceEvidenceStore(lake)
    committed = {item.source_url for item in store.list_verified()}
    assert committed == {"https://en.wikipedia.org/wiki/Alcoa", _EDGAR_URL}


def test_run_skips_already_committed_urls(csvs, tmp_path, monkeypatch):
    round1, round2 = csvs
    lake = tmp_path / "lake"
    store = SourceEvidenceStore(lake)
    artifact = store.persist_raw(b"already fetched")
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

    monkeypatch.setattr(MediaWikiClient, "snapshot", fail_snapshot)
    with patch("livewire_scripts.fetch_researched_evidence.httpx.get", return_value=_edgar_response()):
        exit_code = fetch.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)
    assert exit_code == 0
    assert calls == []
    store = SourceEvidenceStore(lake)
    committed = {item.source_url for item in store.list_verified()}
    assert _EDGAR_URL in committed


def test_run_retries_a_transient_503_and_commits_on_the_successful_attempt(csvs, tmp_path, monkeypatch):
    """SEC EDGAR 503s transiently under load; the retry lives in
    `clients.http_retry`, not a private loop in this script."""
    round1, round2 = csvs
    lake = tmp_path / "lake"

    monkeypatch.setattr("clients.http_retry.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(MediaWikiClient, "snapshot", _fake_snapshot)
    mock_get = MagicMock(side_effect=[_edgar_response(503), _edgar_response(200)])

    with patch("livewire_scripts.fetch_researched_evidence.httpx.get", mock_get):
        exit_code = fetch.run(round1_path=round1, round2_path=round2, data_lake_root=lake, now=NOW)

    assert exit_code == 0
    assert mock_get.call_count == 2
    store = SourceEvidenceStore(lake)
    committed = {item.source_url for item in store.list_verified()}
    assert _EDGAR_URL in committed
