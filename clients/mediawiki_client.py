"""Revision-bound MediaWiki source client for index-universe evidence."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote, urljoin

import requests
from lxml import html

from clients.source_evidence import SourceEvidence, SourceEvidenceStore

_REST_ROOT = "https://en.wikipedia.org/w/rest.php/v1/page"
_USER_AGENT = "livewire/1.0 (market-data-warehouse)"
_REVISION_PATH = re.compile(r"/Special:Redirect/revision/(\d+)$")


class MediaWikiFetchError(Exception):
    """A bounded MediaWiki revision fetch or validation failed."""


@dataclass(frozen=True)
class MediaWikiSnapshot:
    """Rendered content and immutable evidence from one exact revision."""

    title: str
    canonical_url: str
    revision_id: int
    revision_time: datetime
    content: str
    evidence: SourceEvidence


class MediaWikiClient:
    """Fetch exactly one rendered revision in one bounded request."""

    def __init__(
        self,
        store: SourceEvidenceStore,
        *,
        timeout: float = 30,
        now: Callable[[], datetime] | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.store = store
        self.timeout = timeout
        self._now = now or (lambda: datetime.now(UTC))
        self._session = session or requests.Session()

    def snapshot(self, title: str) -> MediaWikiSnapshot:
        encoded_title = quote(title.replace(" ", "_"), safe="")
        url = f"{_REST_ROOT}/{encoded_title}/html"
        try:
            response = self._session.get(
                url,
                headers={"User-Agent": _USER_AGENT},
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            status = getattr(exc.response, "status_code", None)
            detail = f"HTTP {status}" if status is not None else str(exc)
            raise MediaWikiFetchError(f"MediaWiki fetch failed for {title}: {detail}") from exc

        raw = bytes(response.content)
        artifact = self.store.persist_raw(raw)
        try:
            document = html.fromstring(raw)
            about = _one(document.xpath("/html/@about"), "revision identity")
            revision_match = _REVISION_PATH.search(about)
            if revision_match is None:
                raise ValueError("invalid revision identity")
            revision_id = int(revision_match.group(1))
            revision_time = _parse_time(
                _one(document.xpath("/html/head/meta[@property='dc:modified']/@content"), "revision time")
            )
            canonical_href = _one(
                document.xpath("/html/head/link[@rel='dc:isVersionOf']/@href"),
                "canonical URL",
            )
            canonical_url = urljoin("https://en.wikipedia.org", canonical_href)
            resolved_title = _one(document.xpath("/html/head/title/text()"), "page title")
            content = raw.decode(response.encoding or "utf-8")
        except (TypeError, UnicodeDecodeError, ValueError) as exc:
            raise MediaWikiFetchError(f"Invalid MediaWiki revision payload for {title}: {exc}") from exc

        retrieved_at = self._now()
        if retrieved_at.tzinfo is None:
            raise MediaWikiFetchError("retrieval clock must return a timezone-aware datetime")
        source = SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=response.url,
            retrieved_at=retrieved_at.astimezone(UTC),
            publication_time=revision_time,
            mediawiki_revision_id=revision_id,
            mediawiki_revision_time=revision_time,
            content_type=response.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0],
        )
        self.store.record(source)
        return MediaWikiSnapshot(
            title=resolved_title,
            canonical_url=canonical_url,
            revision_id=revision_id,
            revision_time=revision_time,
            content=content,
            evidence=source,
        )


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("revision timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("revision timestamp must include a timezone")
    return parsed.astimezone(UTC)


def _one(values: list[str], label: str) -> str:
    if len(values) != 1 or not values[0]:
        raise ValueError(f"expected exactly one {label}")
    return values[0]
