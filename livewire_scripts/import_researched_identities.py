"""One-off import: Wikipedia/EDGAR-researched security identities.

`security-master-sync` only knows Massive; ~1216 index-membership placeholders
stayed `unresolved:<ticker>` because Massive has no record of them (mostly
pre-2003 delistings, plus a handful of ticker-reuse conflicts it refuses to
guess). A separate research pass (an external agent, ticker by ticker against
Wikipedia and SEC EDGAR) produced two CSVs: round 1 resolves ticker+date to a
company and, where verifiable, a CIK; round 2 adds the primary exchange for
the CIK-backed subset.

This script writes ONE narrow verified `SecurityIdentityEvent` per importable
row: `[effective_at-1d, effective_at+1d)`. That is the only claim actually
verified — "on this exact historical date, this ticker under this CIK was
this company, on this exchange" — never an inferred listing history. Rows for
the same CIK share one `security_id` so add/remove pairs resolve to the same
identity.

Provider is `wikipedia_sec_research`, never `massive` — provider scopes
cancellation inference (pm:2026-07-19), so mislabeling these would expose them
to being "reconciled" by a future Massive-only pass that never asserted them.

Evidence is never fetched here. Every row's `source_url` (and, when present,
`exchange_source_url`) must already be committed to `SourceEvidenceStore` by a
separate fetch step; a row whose evidence is missing is skipped, not guessed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import socket
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

from clients import ledger
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.source_evidence import SourceEvidence, SourceEvidenceStore, digest_bytes
from livewire_scripts.paths import data_lake_dir

PROVIDER = "wikipedia_sec_research"
_CONTINUITY_BASIS = "regulator_filing"
_WINDOW = timedelta(days=1)
_MEDIAWIKI_REST_PAGE = "https://en.wikipedia.org/w/rest.php/v1/page"
MEASURE_NAMES = ("wikipedia_rows_considered", "wikipedia_events_appended", "wikipedia_rows_missing_evidence")


def _evidence_key(url: str) -> str:
    """The key a URL's evidence is actually stored under.

    `MediaWikiClient.snapshot()` records `SourceEvidence.source_url` as the
    REST endpoint it fetched (`.../rest.php/v1/page/{title}/html`), never the
    plain `/wiki/{title}` page a row cites — the two are different strings for
    the same page, so a raw `evidence.get(row.source_url)` never matches a
    wiki citation. Translate a `/wiki/` URL the same way `MediaWikiClient`
    would have encoded the title; leave every other URL (EDGAR, press
    releases, a citation that already is a REST/oldid link) untouched, since
    `_fetch_generic` records those verbatim.
    """
    parsed = urlparse(url)
    if not parsed.netloc.endswith("wikipedia.org") or "/wiki/" not in parsed.path:
        return url
    title = unquote(parsed.path.split("/wiki/", 1)[1])
    return f"{_MEDIAWIKI_REST_PAGE}/{quote(title.replace(' ', '_'), safe='')}/html"


@dataclass(frozen=True)
class ResearchedRow:
    index_id: str
    ticker: str
    action: str
    effective_at: datetime
    company_name: str
    cik: str
    exchange_mic: str
    source_url: str
    exchange_source_url: str


def _key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (row["index_id"], row["ticker"], row["action"], row["effective_at"])


def _parse_date(value: str) -> datetime:
    return datetime.combine(datetime.fromisoformat(value.strip()).date(), datetime.min.time(), tzinfo=UTC)


def _iso_or_blank(value: datetime | None) -> str:
    return "" if value is None else value.isoformat()


def load_rows(round1_path: Path, round2_path: Path) -> list[ResearchedRow]:
    """Join round 1 (identity) and round 2 (exchange) on the shared 4-column
    key; keep only rows that are resolved, CIK-backed, and have a usable MIC.
    """
    with round1_path.open(newline="", encoding="utf-8") as handle:
        round1 = {_key(row): row for row in csv.DictReader(handle)}
    rows: list[ResearchedRow] = []
    with round2_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            mic = row["exchange_mic"].strip()
            cik = row["candidate_cik"].strip()
            if not mic or not cik or not cik.isdigit():
                continue
            base = round1.get(_key(row))
            if base is None or base["resolution"] != "resolved":
                continue
            rows.append(
                ResearchedRow(
                    index_id=row["index_id"],
                    ticker=row["ticker"],
                    action=row["action"],
                    effective_at=_parse_date(row["effective_at"]),
                    company_name=row["company_name"],
                    cik=cik.zfill(10),
                    exchange_mic=mic,
                    source_url=base["source_url"].strip(),
                    exchange_source_url=row.get("exchange_source_url", "").strip(),
                )
            )
    return rows


def evidence_by_url(store: SourceEvidenceStore) -> dict[str, SourceEvidence]:
    """The most-recently-retrieved committed evidence for each source URL."""
    by_url: dict[str, SourceEvidence] = {}
    for item in store.list_verified():
        current = by_url.get(item.source_url)
        if current is None or item.retrieved_at > current.retrieved_at:
            by_url[item.source_url] = item
    return by_url


def _event_id(security_id: str, revision: int, row: ResearchedRow, refs: tuple[str, ...]) -> str:
    """A content address over everything the row asserts; a rerun that finds
    the same evidence for the same row produces the same id, so `append` is a
    no-op rather than a duplicate (`AtomicParquetLog.append` dedupes by key).
    """
    payload = "\x00".join(
        [
            security_id,
            str(revision),
            PROVIDER,
            row.ticker,
            row.exchange_mic,
            row.cik,
            (row.effective_at - _WINDOW).date().isoformat(),
            (row.effective_at + _WINDOW).date().isoformat(),
            "verified",
            *refs,
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def derive_events(
    rows: list[ResearchedRow],
    existing: list[SecurityIdentityEvent],
    evidence: dict[str, SourceEvidence],
    now: datetime,
    *,
    new_security_id=None,
) -> tuple[list[SecurityIdentityEvent], list[ResearchedRow]]:
    """One verified event per row, grouped under one `security_id` per CIK.

    A row whose `source_url` has no matching committed evidence is returned in
    the second list, never appended on a guess.
    """
    new_id = new_security_id or SecurityMaster.new_security_id
    security_id_by_cik: dict[str, str] = {}
    next_revision: dict[str, int] = {}
    # A row already imported is identified by content, not by revision number:
    # a rerun must find its own prior row here and skip it, or every rerun
    # would open a new revision for the same fact (revision is part of the
    # event_id, so "just recompute the id" alone is not idempotent).
    covered: set[tuple[str, str, str, str, str]] = set()
    for item in existing:
        next_revision[item.security_id] = max(next_revision.get(item.security_id, 0), item.revision)
        if item.provider != PROVIDER:
            continue
        if item.cik:
            security_id_by_cik.setdefault(item.cik, item.security_id)
        covered.add(
            (
                item.symbol,
                item.exchange_mic,
                item.cik or "",
                item.effective_from.isoformat(),
                _iso_or_blank(item.effective_to),
            )
        )

    events: list[SecurityIdentityEvent] = []
    skipped: list[ResearchedRow] = []
    for row in sorted(rows, key=lambda item: (item.cik, item.effective_at)):
        start = row.effective_at - _WINDOW
        end = row.effective_at + _WINDOW
        if (row.ticker, row.exchange_mic, row.cik, start.isoformat(), end.isoformat()) in covered:
            continue  # this exact fact is already in the master; nothing to do
        source = evidence.get(_evidence_key(row.source_url))
        if source is None:
            skipped.append(row)
            continue
        refs = [(source.ref, source.sha256)]
        exchange_source = evidence.get(_evidence_key(row.exchange_source_url))
        if exchange_source is not None and exchange_source.ref != source.ref:
            refs.append((exchange_source.ref, exchange_source.sha256))

        if row.cik not in security_id_by_cik:
            security_id_by_cik[row.cik] = new_id()
        security_id = security_id_by_cik[row.cik]
        revision = next_revision.get(security_id, 0) + 1
        next_revision[security_id] = revision
        covered.add((row.ticker, row.exchange_mic, row.cik, start.isoformat(), end.isoformat()))

        events.append(
            SecurityIdentityEvent(
                event_id=_event_id(security_id, revision, row, tuple(ref for ref, _ in refs)),
                security_id=security_id,
                revision=revision,
                symbol=row.ticker,
                provider=PROVIDER,
                exchange_mic=row.exchange_mic,
                currency="USD",
                effective_from=start,
                effective_to=end,
                known_at=now,
                issuer_name=row.company_name,
                cik=row.cik,
                composite_figi=None,
                share_class_figi=None,
                continuity_basis=_CONTINUITY_BASIS,
                relationship_type=None,
                related_security_id=None,
                source_refs=tuple(ref for ref, _ in refs),
                source_hashes=tuple(digest for _, digest in refs),
                status="verified",
                supersedes=None,
            )
        )
    return events, skipped


def _append_all(master: SecurityMaster, events: list[SecurityIdentityEvent]) -> tuple[int, int]:
    """Append derived rows; a collision the master raises is counted, never swallowed."""
    appended = collisions = 0
    for event in events:
        try:
            if master.append(event):
                appended += 1
        except ValueError as exc:
            collisions += 1
            print(f"skipped {event.symbol} ({event.security_id}): {exc}", file=sys.stderr)
    return appended, collisions


def run(
    *,
    round1_path: Path,
    round2_path: Path,
    data_lake_root: Path,
    now: datetime,
    dry_run: bool = False,
) -> int:
    root = Path(data_lake_root)
    evidence_store = SourceEvidenceStore(root)

    def verifier(ref: str, digest: str) -> bool:
        try:
            return digest_bytes(evidence_store.read(ref)) == digest
        except (OSError, ValueError):
            return False

    reader = SecurityMaster(root, evidence_verifier=None)
    writer = reader if dry_run else SecurityMaster(root, evidence_verifier=verifier)

    rows = load_rows(round1_path, round2_path)
    events, skipped = derive_events(rows, reader.events(), evidence_by_url(evidence_store), now)

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("import-researched-identities")
    run_row = {
        "run_id": run_id,
        "job": "import-researched-identities",
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
        print(f"abandoned {len(abandoned)} open run(s) of import-researched-identities: {', '.join(abandoned)}")

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
        appended = collisions = 0
        if not dry_run:
            appended, collisions = _append_all(writer, events)
        else:
            print(f"dry-run: would append {len(events)} events, skip {len(skipped)} rows missing evidence")

        for row in skipped:
            print(
                f"missing evidence: {row.index_id} {row.ticker} {row.action} {row.effective_at.date()} {row.source_url}"
            )

        counts = {
            "wikipedia_rows_considered": len(rows),
            "wikipedia_events_appended": appended,
            "wikipedia_rows_missing_evidence": len(skipped),
            "wikipedia_identity_collisions": collisions,
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
        # BaseException, not Exception: Ctrl-C is a KeyboardInterrupt, and an
        # operator watching 759 evidence lookups is exactly who interrupts one
        # (pm:2026-09-16-interrupted-runs-never-closed).
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
