"""Backfill security identities from Massive reference data.

`membership_sync` writes `unresolved:<ticker>` placeholders for every index
member with no identity; the master held one verified row, so every PIT
membership query answered the empty set. This module turns Massive's
`/v3/reference/tickers` listings into evidence-backed `SecurityMaster`
intervals so those placeholders can be reresolved.

`derive_identity_events` is pure: it reads no lake and writes nothing. The
identity rules it encodes are the table in
`docs/superpowers/specs/2026-09-15-security-master-backfill-design.md` §4.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import time as time_module
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from pathlib import Path

from clients import constants, ledger
from clients.index_membership_store import IndexMembershipStore
from clients.security_master import SecurityIdentityEvent, SecurityMaster, _overlaps
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from clients.universe_client import IdentityRecord, UniverseFetchError, fetch_ticker_identity
from livewire_scripts.membership_sync import DEFAULT_INDEXES, _evidence_verifier, _measure, _resolve
from livewire_scripts.paths import data_lake_dir

_PROVIDER = "massive"

COUNT_NAMES = (
    "identity_candidate",
    "identity_no_start",
    "identity_conflict",
    "identity_unknown_to_provider",
)


@dataclass(frozen=True)
class DerivedIdentities:
    events: list[SecurityIdentityEvent]
    counts: dict[str, int] = field(default_factory=dict)


def _midnight(value: str) -> datetime:
    """`2010-01-04` or `2017-06-19T00:00:00Z` -> an aware UTC timestamp."""
    return datetime.combine(date.fromisoformat(value[:10]), time.min, tzinfo=UTC)


def _start_of(record: IdentityRecord) -> datetime | None:
    """`list_date`, else the date the `date=` probe proved it existed, else None.

    An empty probe proves nothing about the past, and `known_at` would be an
    invented date — for a delisted record, one that ends before it starts.
    """
    if record.list_date:
        return _midnight(record.list_date)
    if record.existed_at:
        return _midnight(record.existed_at)
    return None


def _end_of(record: IdentityRecord) -> datetime | None:
    return _midnight(record.delisted_utc) if record.delisted_utc else None


def _event_id(
    security_id: str,
    revision: int,
    record: IdentityRecord,
    start: datetime,
    end: datetime | None,
    status: str,
) -> str:
    """A content address over everything the row asserts.

    `revision` and `end` are in it because a widened row supersedes one that
    differs from it only in those two fields, and the master's log rejects a
    repeated `event_id` carrying different content.
    """
    payload = "\x00".join(
        [
            security_id,
            str(revision),
            _PROVIDER,
            record.ticker,
            record.mic or "",
            record.composite_figi or "",
            record.share_class_figi or "",
            start.date().isoformat(),
            "" if end is None else end.date().isoformat(),
            status,
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _build(
    *,
    security_id: str,
    revision: int,
    record: IdentityRecord,
    start: datetime,
    end: datetime | None,
    status: str,
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    supersedes: str | None = None,
) -> SecurityIdentityEvent:
    cik = record.cik.zfill(10) if record.cik and record.cik.isdigit() else None
    basis = "provider_figi" if (record.composite_figi or record.share_class_figi) else "provider_reference"
    return SecurityIdentityEvent(
        event_id=_event_id(security_id, revision, record, start, end, status),
        security_id=security_id,
        revision=revision,
        symbol=record.ticker,
        provider=_PROVIDER,
        exchange_mic=record.mic,
        # Massive omits currency_name on some rows; every MIC this backfill
        # touches (XNAS/XNYS/ARCX/XASE) is a US venue, so USD is the listing
        # currency, not a guess about the instrument.
        currency=record.currency or "USD",
        effective_from=start,
        effective_to=end,
        known_at=now,
        # Massive omits `name` on some historical rows (spec §2); the ticker is
        # the only issuer label the provider gave, and the field is non-nullable.
        issuer_name=record.name or record.ticker,
        cik=cik,
        composite_figi=record.composite_figi,
        share_class_figi=record.share_class_figi,
        continuity_basis=basis,
        relationship_type=None,
        related_security_id=None,
        source_refs=tuple(ref for ref, _ in refs),
        source_hashes=tuple(digest for _, digest in refs),
        status=status,
        supersedes=supersedes,
    )


def _next_revision(existing: list[SecurityIdentityEvent], security_id: str) -> int:
    """The master requires `max(revision of the id) + 1`, over every row of the
    id, superseded ones included. A renamed id carries one row per symbol, so
    `matched_row.revision + 1` would collide as soon as the id has two."""
    return max((item.revision for item in existing if item.security_id == security_id), default=0) + 1


def _current(existing: list[SecurityIdentityEvent]) -> list[SecurityIdentityEvent]:
    superseded = {item.supersedes for item in existing if item.supersedes is not None}
    return [item for item in existing if item.event_id not in superseded]


def _existing_match(
    record: IdentityRecord, existing: list[SecurityIdentityEvent], status: str
) -> SecurityIdentityEvent | None:
    """The master's current row of `status` for the same listing and the same FIGIs.

    A second `security_id` for this listing would be rejected by the master's
    FIGI collision check, so the only correct move is a new revision on the
    existing id (spec §4).
    """
    return next(
        (
            item
            for item in _current(existing)
            if item.status == status
            and (item.provider, item.symbol, item.exchange_mic) == (_PROVIDER, record.ticker, record.mic)
            and item.composite_figi == record.composite_figi
            and item.share_class_figi == record.share_class_figi
        ),
        None,
    )


def _rename_match(record: IdentityRecord, existing: list[SecurityIdentityEvent]) -> SecurityIdentityEvent | None:
    """The master's current verified row carrying this record's FIGIs under
    another symbol: a rename seen one ticker at a time (spec §4 rename row).

    `sync` fetches each ticker on its own, so the two halves of a rename never
    reach `derive_identity_events` together; without this the second half
    would open a second `security_id` for one issuer, which the master allows
    because its FIGI collision check ignores disjoint intervals.
    """
    if not record.composite_figi or not record.share_class_figi:
        return None
    return next(
        (
            item
            for item in _current(existing)
            if item.status == "verified"
            and item.provider == _PROVIDER
            and item.symbol != record.ticker
            and (item.composite_figi, item.share_class_figi) == (record.composite_figi, record.share_class_figi)
        ),
        None,
    )


def _overlaps_sibling(
    security_id: str,
    start: datetime,
    end: datetime | None,
    existing: list[SecurityIdentityEvent],
    *,
    exclude: str | None,
) -> bool:
    """Whether [start, end) overlaps another current verified row of this id."""
    return any(
        _overlaps(start, end, item.effective_from, item.effective_to)
        for item in _current(existing)
        if item.security_id == security_id and item.status == "verified" and item.event_id != exclude
    )


def _conflict_row(
    record: IdentityRecord,
    start: datetime,
    end: datetime | None,
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    new_id,
) -> SecurityIdentityEvent:
    """This half of a FIGI conflict: `unresolved` on its own id, nothing inferred."""
    return _build(
        security_id=new_id(), revision=1, record=record, start=start, end=end, status="unresolved", now=now, refs=refs
    )


def _widen(
    match: SecurityIdentityEvent,
    existing: list[SecurityIdentityEvent],
    record: IdentityRecord,
    start: datetime,
    end: datetime | None,
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    status: str = "verified",
) -> SecurityIdentityEvent | None:
    covers_start = match.effective_from <= start
    covers_end = match.effective_to is None or (end is not None and end <= match.effective_to)
    if covers_start and covers_end:
        return None
    widened_end = None if match.effective_to is None or end is None else max(match.effective_to, end)
    return _build(
        security_id=match.security_id,
        revision=_next_revision(existing, match.security_id),
        record=record,
        start=min(match.effective_from, start),
        end=widened_end,
        status=status,
        now=now,
        refs=refs,
        supersedes=match.event_id,
    )


def _one_record(
    record: IdentityRecord,
    start: datetime,
    existing: list[SecurityIdentityEvent],
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    counts: dict[str, int],
    new_id,
) -> list[SecurityIdentityEvent]:
    end = _end_of(record)
    if not (record.composite_figi or record.share_class_figi):
        counts["identity_candidate"] += 1
        # The master's collision check covers verified rows only, so a re-fetch
        # of a FIGI-less listing must find its own candidate row here or it
        # would append a duplicate every run.
        match = _existing_match(record, existing, "candidate")
        if match is not None:
            widened = _widen(match, existing, record, start, end, now, refs, status="candidate")
            return [widened] if widened is not None else []
        return [
            _build(
                security_id=new_id(),
                revision=1,
                record=record,
                start=start,
                end=end,
                status="candidate",
                now=now,
                refs=refs,
            )
        ]
    match = _existing_match(record, existing, "verified")
    if match is not None:
        widened = _widen(match, existing, record, start, end, now, refs)
        if widened is not None and _overlaps_sibling(
            widened.security_id, widened.effective_from, widened.effective_to, existing, exclude=match.event_id
        ):
            # A renamed id holds one row per symbol; widening one across the
            # other is the spec §4 conflict, and the master's collision check
            # skips rows sharing an id, so it is refused here.
            counts["identity_conflict"] += 1
            return [_conflict_row(record, start, end, now, refs, new_id)]
        return [widened] if widened is not None else []
    renamed = _rename_match(record, existing)
    if renamed is not None and _overlaps_sibling(renamed.security_id, start, end, existing, exclude=None):
        # The master's collision check skips rows sharing an id, so an overlap
        # must be refused here: same FIGIs, overlapping dates is the spec §4
        # conflict row, and this half stays unresolved on its own id.
        counts["identity_conflict"] += 1
        return [_conflict_row(record, start, end, now, refs, new_id)]
    if renamed is not None:
        # Same issuer under a new symbol: a new revision on its id.
        return [
            _build(
                security_id=renamed.security_id,
                revision=_next_revision(existing, renamed.security_id),
                record=record,
                start=start,
                end=end,
                status="verified",
                now=now,
                refs=refs,
            )
        ]
    return [
        _build(
            security_id=new_id(),
            revision=1,
            record=record,
            start=start,
            end=end,
            status="verified",
            now=now,
            refs=refs,
        )
    ]


def _split_ids(
    group: list[tuple[IdentityRecord, datetime]],
    status: str,
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    new_id,
) -> list[SecurityIdentityEvent]:
    """One `security_id` per record. Same-id rows would bypass the master's
    collision checks, so a conflicted group never shares an id."""
    return [
        _build(
            security_id=new_id(),
            revision=1,
            record=record,
            start=start,
            end=_end_of(record),
            status=status,
            now=now,
            refs=refs,
        )
        for record, start in group
    ]


def derive_identity_events(
    records: list[IdentityRecord],
    existing_master_rows: list[SecurityIdentityEvent],
    now: datetime,
    evidence_refs: tuple[tuple[str, str], ...],
    *,
    new_security_id=None,
) -> DerivedIdentities:
    """Turn one ticker's Massive listings into master rows, per spec §4.

    `new_security_id` defaults to `SecurityMaster.new_security_id` — opaque ids,
    never derived from FIGI or ticker (contract lines 12-15). It is a parameter
    only so a test can make the ids deterministic.
    """
    new_id = new_security_id or SecurityMaster.new_security_id
    counts = dict.fromkeys(COUNT_NAMES, 0)
    if not records:
        counts["identity_unknown_to_provider"] = 1
        return DerivedIdentities([], counts)

    usable: list[tuple[IdentityRecord, datetime]] = []
    for record in records:
        start = _start_of(record)
        if start is None:
            counts["identity_no_start"] += 1
            continue
        if not record.mic:
            # No venue, so no listing any of the four resolve MICs can name.
            counts["identity_unknown_to_provider"] += 1
            continue
        usable.append((record, start))
    if not usable:
        return DerivedIdentities([], counts)

    groups: dict[str, list[tuple[IdentityRecord, datetime]]] = {}
    for index, (record, start) in enumerate(usable):
        # A record with no composite FIGI joins nothing; key it uniquely.
        groups.setdefault(record.composite_figi or f"\x00{index}", []).append((record, start))

    events: list[SecurityIdentityEvent] = []
    for group in groups.values():
        if len(group) == 1:
            record, start = group[0]
            events.extend(_one_record(record, start, existing_master_rows, now, evidence_refs, counts, new_id))
            continue
        share_classes = {record.share_class_figi for record, _ in group}
        if len(group) > 2 or (len(share_classes) > 1 and None not in share_classes):
            # Differing share classes under one composite FIGI (or more listings
            # than this rule describes) is a material FIGI conflict, contract
            # rule 7: two ids, both unresolved, nothing inferred.
            counts["identity_conflict"] += 1
            events.extend(_split_ids(group, "unresolved", now, evidence_refs, new_id))
            continue
        if None in share_classes:
            # Incomplete rather than contradictory: a composite FIGI alone does
            # not prove continuity (contract priority 3).
            counts["identity_conflict"] += 1
            events.extend(_split_ids(group, "candidate", now, evidence_refs, new_id))
            continue
        ordered = sorted(group, key=lambda item: item[1])
        (first, first_start), (second, second_start) = ordered
        first_end = _end_of(first)
        if first_end is None or first_end > second_start:
            counts["identity_conflict"] += 1
            events.extend(_split_ids(ordered, "unresolved", now, evidence_refs, new_id))
            continue
        security_id = new_id()
        events.append(
            _build(
                security_id=security_id,
                revision=1,
                record=first,
                start=first_start,
                end=first_end,
                status="verified",
                now=now,
                refs=evidence_refs,
            )
        )
        events.append(
            _build(
                security_id=security_id,
                revision=2,
                record=second,
                start=second_start,
                end=_end_of(second),
                status="verified",
                now=now,
                refs=evidence_refs,
            )
        )
    return DerivedIdentities(events, counts)


# The list form of Massive's reference endpoint; `fetch_ticker_identity` calls
# it with `ticker`, `active` and `date` parameters.
_SOURCE_URL = "https://api.polygon.io/v3/reference/tickers"

# Tickers fetched between two evidence commits. At 5 req/min a chunk is
# ~30 min of requests, so a crash loses at most that much refetching.
FLUSH_EVERY_TICKERS = 50


class _CommitFailed(Exception):
    """`record_many` raised; the chunk's identities were not appended."""


MEASURE_NAMES = (
    "identity_tickers_requested",
    "identity_events_appended",
    *COUNT_NAMES,
    "identity_collisions",
    "identity_fetch_failed",
)


def needed_dates(store: IndexMembershipStore, indexes: list[str]) -> dict[str, set[datetime]]:
    """ticker -> the effective dates its current unresolved events need.

    Current means non-superseded; a rejected placeholder chain is skipped, so a
    reresolved index stops asking for identities it already has.
    """
    wanted: dict[str, set[datetime]] = {}
    for index_id in indexes:
        events = store.events(index_id)
        superseded = {item.supersedes for item in events if item.supersedes is not None}
        for event in events:
            if (
                event.event_id in superseded
                or event.status != "unresolved"
                or not event.security_id.startswith("unresolved:")
            ):
                continue
            wanted.setdefault(event.security_id.removeprefix("unresolved:"), set()).add(event.effective_at)
    return wanted


def _covered(
    master: SecurityMaster, ticker: str, dates: set[datetime], now: datetime, empty_probes: set[tuple[str, str]]
) -> bool:
    """Every needed date sits inside a verified interval for this symbol, or
    was probed on an earlier run and answered nothing.

    An earlier run's empty probe is a persisted fact (`identity_probe_empty`
    measurement, scope `<ticker>:<date>`), not an inference from an interval's
    start: an interval beginning in 2015 says nothing about whether 2010 was
    ever asked. Without the second clause a ticker with one pre-coverage date
    (the S&P 500 shape) would be refetched on every run and after every restart.
    """
    return all(
        _resolve(master, ticker, effective_at, now) is not None
        or (ticker, effective_at.date().isoformat()) in empty_probes
        for effective_at in dates
    )


def _append_all(master: SecurityMaster, events: list[SecurityIdentityEvent]) -> tuple[int, int]:
    """Append derived rows; a collision the master raises is counted, never swallowed."""
    appended = collisions = 0
    for event in events:
        try:
            if master.append(event):
                appended += 1
        except ValueError as exc:
            collisions += 1
            print(json.dumps({"skipped": event.symbol, "reason": str(exc)}, sort_keys=True))
    return appended, collisions


def sync(
    *,
    indexes: list[str],
    data_lake_root: Path,
    now: datetime,
    tickers: list[str] | None = None,
    fetch_fn=None,
    sleep_fn=None,
    clock_fn=None,
    dry_run: bool = False,
) -> int:
    """Fetch Massive identities for every unresolved membership ticker.

    Idempotent: a ticker whose verified intervals already cover every needed
    membership date is skipped without a fetch. Evidence is committed once per
    run, before any identity row is appended — the verifier the master runs
    checks raw bytes only, so this ordering is what keeps a manifest-less
    identity row out of the store (spec §6).
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    reader = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=reader, evidence_verifier=None)
    sleep = sleep_fn or time_module.sleep
    # `known_at` and `retrieved_at` are the fetch time, not the run's start
    # (spec §4): a paced run spans hours, and an as_of inside it must not see
    # identities fetched after it.
    clock = clock_fn or (lambda: datetime.now(UTC))
    pace_s = 60.0 / constants.declared("massive_requests_per_minute/reference")
    # Paced per request inside the fetch (a ticker is 2-3 requests), including
    # the run's first one: one idle pace_s beats a rate computed per ticker.
    fetch = fetch_fn or (
        lambda ticker, *, probe_dates=(): fetch_ticker_identity(
            ticker,
            os.environ.get("MASSIVE_API_KEY"),
            probe_dates=probe_dates,
            pace_fn=lambda: sleep(pace_s),
        )
    )
    backoff_s = constants.declared("massive_backoff_s/reference")
    scope = "subset" if tickers else "all"

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("security-master-sync")
    run_row = {
        "run_id": run_id,
        "job": "security-master-sync",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.emit("runs", [run_row], run_id=run_id)

    def close(exit_code: int) -> int:
        ledger.emit(
            "runs",
            [
                run_row
                | {
                    "ended": datetime.now(UTC),
                    "exit_code": exit_code,
                    "verdict": "OK" if exit_code == 0 else "FAILED",
                }
            ],
            run_id=run_id,
        )
        return exit_code

    counts = dict.fromkeys(MEASURE_NAMES, 0)
    try:
        wanted = needed_dates(store, indexes)
        empty_probes = {
            tuple(row["scope"].split(":", 1))
            for row in ledger.query("select scope from measurements where name = 'identity_probe_empty'")
        }
        if tickers:
            selected = {ticker.upper() for ticker in tickers}
            wanted = {ticker: dates for ticker, dates in wanted.items() if ticker in selected}

        pending: list[tuple[list[IdentityRecord], tuple[tuple[str, str], ...], datetime]] = []
        manifest: list[SourceEvidence] = []
        probe_rows: list[dict] = []
        writer = reader if dry_run else SecurityMaster(root, evidence_verifier=_evidence_verifier(evidence))

        def flush() -> None:
            """Commit this chunk's manifest, then append its identities.

            One commit per chunk, never per response (41 min/night,
            pm:2026-08-31-source-evidence-per-response-cost) and never one per
            run: a full backlog is a day of paced requests, and a single commit
            at the end made a crash at ticker 2000 throw all of it away. A
            failed commit appends nothing from its chunk: the master's verifier
            checks raw bytes only, so an identity row could otherwise outlive
            its manifest entry (spec §6).
            """
            if not dry_run and manifest:
                try:
                    evidence.record_many(manifest)
                except Exception as exc:  # noqa: BLE001 — any commit failure is terminal
                    print(json.dumps({"evidence_commit_failed": str(exc)}, sort_keys=True))
                    raise _CommitFailed from exc
            for records, ticker_refs, fetched_at in pending:
                derived = derive_identity_events(records, writer.events(), fetched_at, ticker_refs)
                for name, value in derived.counts.items():
                    counts[name] += value
                if dry_run:
                    continue
                appended, collisions = _append_all(writer, derived.events)
                counts["identity_events_appended"] += appended
                counts["identity_collisions"] += collisions
            if probe_rows and not dry_run:
                # Recorded only once the chunk's evidence is committed: the
                # fact "this date was asked" must never outlive its bodies.
                ledger.emit("measurements", list(probe_rows), run_id=run_id)
            pending.clear()
            manifest.clear()
            probe_rows.clear()

        for ticker in sorted(wanted):
            dates = wanted[ticker]
            if _covered(reader, ticker, dates, now, empty_probes):
                continue
            counts["identity_tickers_requested"] += 1
            # Ascending: the earliest date the provider answers is the start.
            probe_dates = tuple(sorted({day.date().isoformat() for day in dates}))
            try:
                result = fetch(ticker, probe_dates=probe_dates)
            except UniverseFetchError as exc:
                if exc.status_code != 429:
                    counts["identity_fetch_failed"] += 1
                    continue
                sleep(backoff_s)
                try:
                    result = fetch(ticker, probe_dates=probe_dates)
                except UniverseFetchError:
                    counts["identity_fetch_failed"] += 1
                    continue
            fetched_at = clock()
            refs: list[tuple[str, str]] = []
            for body in result.responses:
                artifact = evidence.persist_raw(body)
                manifest.append(
                    SourceEvidence(
                        ref=artifact.ref,
                        sha256=artifact.sha256,
                        source_url=f"{_SOURCE_URL}?ticker={ticker}",
                        retrieved_at=fetched_at,
                        publication_time=None,
                        mediawiki_revision_id=None,
                        mediawiki_revision_time=None,
                        content_type="application/json",
                    )
                )
                refs.append((artifact.ref, artifact.sha256))
            # each identity row cites its own ticker's bodies, not the run's
            pending.append((result.records, tuple(refs), fetched_at))
            probe_rows.extend(
                {
                    "name": "identity_probe_empty",
                    "scope": f"{ticker}:{probe_date}",
                    "measured_at": fetched_at,
                    "value": 1.0,
                    "unit": "count",
                    "source": "measured",
                    "run_id": run_id,
                }
                for probe_date in result.empty_probes
            )
            if len(pending) >= FLUSH_EVERY_TICKERS:
                flush()
        flush()

        _measure(run_id, scope, now, counts)
        print(json.dumps({"scope": scope, "run_id": run_id, "dry_run": dry_run} | counts, sort_keys=True))
    except _CommitFailed:
        return close(1)
    except Exception:
        close(1)
        raise
    return close(1 if counts["identity_fetch_failed"] else 0)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py security-master",
        description="Backfill security identities from Massive reference data",
    )
    parser.add_argument("subcommand", choices=["sync"])
    parser.add_argument(
        "--index",
        action="extend",
        nargs="+",
        choices=sorted(DEFAULT_INDEXES),
        help="Index stores to drain (space-separated and/or repeatable; default: all four)",
    )
    parser.add_argument("--tickers", action="extend", nargs="+", help="Explicit ticker subset, for repairs")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and derive without appending")
    args = parser.parse_args(argv)
    return sync(
        indexes=args.index or list(DEFAULT_INDEXES),
        tickers=args.tickers,
        data_lake_root=data_lake_dir(),
        now=datetime.now(UTC),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
