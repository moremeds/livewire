"""One-off fetch: commit source evidence for the researched identity rows.

`import_researched_identities` never fetches evidence itself — every row's
`source_url` (Wikipedia) and `exchange_source_url` (SEC EDGAR) must already be
committed to `SourceEvidenceStore`, or the row is skipped, not guessed. This
script does that fetch, once, for the same 759 CIK+MIC-backed rows the
importer will consume.

Wikipedia pages go through `MediaWikiClient.snapshot()` (revision-bound,
existing client, used by `universe_client`). Everything else (SEC EDGAR) is a
plain HTTP GET, persisted and recorded with no MediaWiki-specific fields.
Idempotent: a URL already in `SourceEvidenceStore.list_verified()` is skipped.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

from clients import ledger
from clients.mediawiki_client import MediaWikiClient, MediaWikiFetchError
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from livewire_scripts.import_researched_identities import evidence_by_url, load_rows
from livewire_scripts.paths import data_lake_dir

_TIMEOUT = 30
# SEC EDGAR's Fair Access policy rejects any User-Agent with no contact
# email (403, verified against https://www.sec.gov/cgi-bin/browse-edgar on
# 2026-09-16) — the exact format it documents at
# https://www.sec.gov/os/webmaster-faq#developers.
_USER_AGENT = "livewire-research lcxxcllcx@gmail.com"


def _wikipedia_title(url: str) -> str | None:
    parsed = urlparse(url)
    if not parsed.netloc.endswith("wikipedia.org") or "/wiki/" not in parsed.path:
        return None
    return unquote(parsed.path.split("/wiki/", 1)[1])


def _fetch_generic(url: str, store: SourceEvidenceStore, now: datetime, session: requests.Session) -> None:
    response = session.get(url, headers={"User-Agent": _USER_AGENT}, timeout=_TIMEOUT)
    response.raise_for_status()
    artifact = store.persist_raw(bytes(response.content))
    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=url,
            retrieved_at=now,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type=response.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0],
        )
    )


def run(*, round1_path: Path, round2_path: Path, data_lake_root: Path, now: datetime, dry_run: bool = False) -> int:
    root = Path(data_lake_root)
    store = SourceEvidenceStore(root)
    session = requests.Session()
    mediawiki = MediaWikiClient(store, timeout=_TIMEOUT, now=lambda: now, session=session)

    rows = load_rows(round1_path, round2_path)
    urls = sorted(
        {row.source_url for row in rows} | {row.exchange_source_url for row in rows if row.exchange_source_url}
    )
    already_committed = set(evidence_by_url(store))
    to_fetch = [url for url in urls if url not in already_committed]

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("fetch-researched-evidence")
    run_row = {
        "run_id": run_id,
        "job": "fetch-researched-evidence",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    abandoned = ledger.open_run(run_row)
    if abandoned:
        print(f"abandoned {len(abandoned)} open run(s) of fetch-researched-evidence: {', '.join(abandoned)}")

    def close(exit_code: int) -> int:
        ledger.emit(
            "runs",
            [
                run_row
                | {"ended": datetime.now(UTC), "exit_code": exit_code, "verdict": "OK" if exit_code == 0 else "FAILED"}
            ],
            run_id=run_id,
        )
        return exit_code

    try:
        fetched = failed = 0
        if dry_run:
            print(
                f"dry-run: would fetch {len(to_fetch)} of {len(urls)} URLs ({len(urls) - len(to_fetch)} already committed)"
            )
        else:
            for url in to_fetch:
                try:
                    title = _wikipedia_title(url)
                    if title is not None:
                        mediawiki.snapshot(title)
                    else:
                        _fetch_generic(url, store, now, session)
                    fetched += 1
                except (MediaWikiFetchError, requests.RequestException, OSError) as exc:
                    failed += 1
                    print(f"failed: {url}: {exc}", file=sys.stderr)

        counts = {
            "evidence_urls_considered": len(urls),
            "evidence_urls_already_committed": len(urls) - len(to_fetch),
            "evidence_urls_fetched": fetched,
            "evidence_urls_failed": failed,
        }
        if not dry_run:
            ledger.emit(
                "measurements",
                [
                    {
                        "name": name,
                        "scope": "all",
                        "measured_at": datetime.now(UTC),
                        "value": float(value),
                        "unit": "count",
                        "source": "measured",
                        "run_id": run_id,
                    }
                    for name, value in counts.items()
                ],
                run_id=run_id,
            )
        print(counts)
    except BaseException:
        # BaseException, not Exception: an operator watching hundreds of HTTP
        # fetches is exactly who interrupts one (pm:2026-09-16-interrupted-runs-never-closed).
        close(1)
        raise
    return close(0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--round1", type=Path, required=True, help="resolved_ticker_identities.csv")
    parser.add_argument("--round2", type=Path, required=True, help="round2_exchange_mics.csv")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return run(
        round1_path=args.round1,
        round2_path=args.round2,
        data_lake_root=data_lake_dir(),
        now=datetime.now(UTC),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
