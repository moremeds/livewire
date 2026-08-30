from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import quote

import pytest
import responses

from clients.mediawiki_client import MediaWikiClient, MediaWikiFetchError
from clients.source_evidence import SourceEvidenceStore

REST_ROOT = "https://en.wikipedia.org/w/rest.php/v1/page"


def rest_url(title: str) -> str:
    return f"{REST_ROOT}/{quote(title.replace(' ', '_'), safe='')}/html"


def payload(content: str = '<table id="constituents"><tr><td>AAPL</td></tr></table>') -> bytes:
    return f"""<!DOCTYPE html>
    <html about="//en.wikipedia.org/wiki/Special:Redirect/revision/123456">
      <head>
        <meta property="dc:modified" content="2026-08-30T12:00:00Z" />
        <link rel="dc:isVersionOf" href="//en.wikipedia.org/wiki/List_of_S%26P_500_companies" />
        <title>List of S&amp;P 500 companies</title>
      </head>
      <body>{content}</body>
    </html>""".encode()


@responses.activate
def test_snapshot_binds_one_revision_and_persists_raw_response(tmp_path):
    raw = payload()
    responses.add(
        responses.GET,
        rest_url("List of S&P 500 companies"),
        body=raw,
        status=200,
        content_type="text/html",
    )
    client = MediaWikiClient(
        SourceEvidenceStore(tmp_path),
        now=lambda: datetime(2026, 8, 31, 1, 0, tzinfo=UTC),
    )
    snapshot = client.snapshot("List of S&P 500 companies")

    assert snapshot.revision_id == 123456
    assert snapshot.revision_time == datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    assert snapshot.canonical_url.endswith("List_of_S%26P_500_companies")
    assert "AAPL" in snapshot.content
    assert client.store.read(snapshot.evidence.ref) == raw
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == rest_url("List of S&P 500 companies")


@responses.activate
def test_raw_bytes_survive_a_parse_failure(tmp_path):
    responses.add(responses.GET, rest_url("Broken"), body=b"not html metadata", status=200, content_type="text/html")
    store = SourceEvidenceStore(tmp_path)
    with pytest.raises(MediaWikiFetchError, match="revision payload"):
        MediaWikiClient(store).snapshot("Broken")
    raw_files = [path for path in store.raw_root.iterdir() if path.name.endswith(".lock") is False]
    assert len(raw_files) == 1
    assert raw_files[0].read_bytes() == b"not html metadata"
    assert store.manifest_path.exists() is False


@responses.activate
def test_rejects_missing_or_ambiguous_revision_payloads(tmp_path):
    bodies = [
        b"<html><head><title>Missing revision</title></head></html>",
        payload().replace(b"dc:modified", b"dc:unknown"),
        payload().replace(b"dc:isVersionOf", b"dc:unknown"),
    ]
    for body in bodies:
        responses.add(responses.GET, rest_url("Broken"), body=body, status=200)
    client = MediaWikiClient(SourceEvidenceStore(tmp_path))
    for _ in bodies:
        with pytest.raises(MediaWikiFetchError):
            client.snapshot("Broken")


@responses.activate
def test_http_error_is_bounded_and_does_not_create_evidence(tmp_path):
    responses.add(responses.GET, rest_url("List of S&P 500 companies"), status=503)
    store = SourceEvidenceStore(tmp_path)
    with pytest.raises(MediaWikiFetchError, match="503"):
        MediaWikiClient(store, timeout=7).snapshot("List of S&P 500 companies")
    assert responses.calls[0].request.url == rest_url("List of S&P 500 companies")
    assert list(store.raw_root.glob("[0-9a-f]*")) == []
