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

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time

from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.universe_client import IdentityRecord

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


def _widen(
    match: SecurityIdentityEvent,
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
        revision=match.revision + 1,
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
            widened = _widen(match, record, start, end, now, refs, status="candidate")
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
        widened = _widen(match, record, start, end, now, refs)
        return [widened] if widened is not None else []
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
