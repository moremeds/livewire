"""PIT index membership — import grok events.jsonl panels into the store.

The grok layer kept ~30 years of add/remove events keyed by ticker with no
`security_id`, no evidence hash, and no `known_at` — and a README claiming a
weekday auto-update no scheduler runs. `import_events` maps each jsonl line to
a `MembershipEvent` (spec §2.2): resolved through `SecurityMaster`, committed
source files as `HashedRef` evidence, `known_at` = import time because PIT is
honest — an `as_of` before the import returns nothing, because we did not
know it then.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal

from clients import ledger
from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.mediawiki_client import MediaWikiClient, MediaWikiFetchError
from clients.security_master import SecurityMaster
from clients.shepherd_repair import HashedRef
from clients.source_evidence import SourceEvidence, SourceEvidenceStore, canonical_bytes, digest_bytes
from clients.universe_client import (
    DJIA_SLICKCHARTS_URL,
    NDX100_WIKIPEDIA_TITLE,
    R2K_SLICKCHARTS_URL,
    UniverseFetchError,
    fetch_djia,
    fetch_r2k,
    parse_constituent_table,
)
from livewire_scripts import notify
from livewire_scripts.paths import data_lake_dir

Confidence = Literal["B", "C", "D"]

# The security master records equity identities under provider `massive`; US
# index constituents list on NASDAQ, NYSE, NYSE Arca or NYSE American — resolve
# across all four rather than assume one venue. The Russell list carries 44
# NYSE American names (measured from the TradingView scanner 2026-09-15) and
# the master holds them under XASE.
_RESOLVE_PROVIDER = "massive"
_RESOLVE_MICS = ("XNAS", "XNYS", "ARCX", "XASE")

_CONFIDENCE_STATUS = {"B": "verified", "C": "candidate", "D": "candidate"}

DEFAULT_INDEXES = ("sp500", "ndx100", "djia", "r2k-proxy")

_WIKIPEDIA_SOURCES = {
    "sp500": ("List of S&P 500 companies", "S&P 500"),
    "ndx100": (NDX100_WIKIPEDIA_TITLE, "Nasdaq-100"),
}

# grok's own guardrail in r2k_proxy_live/update_from_live.py: a fetched R2K
# set under 1500 members means the page parse broke, and diffing it would
# emit ~1500 false removes. Below the floor is a fetch failure, not a diff.
_R2K_PROXY_MIN_MEMBERS = 1500


def _evidence_verifier(store: SourceEvidenceStore):
    def verify(ref: str, digest: str) -> bool:
        try:
            return digest_bytes(store.read(ref)) == digest
        except (OSError, ValueError):
            return False

    return verify


def _resolve(master: SecurityMaster, ticker: str, effective_at: datetime, as_of: datetime) -> str | None:
    matches = {
        security_id
        for mic in _RESOLVE_MICS
        if (security_id := master.resolve_symbol(_RESOLVE_PROVIDER, ticker, mic, effective_at, as_of)) is not None
    }
    if len(matches) == 1:
        return next(iter(matches))
    return None  # no identity, or the same ticker verified on two venues


def _event_id(index_id: str, key: str, action: str, effective_date: str, source_hashes: tuple[str, ...]) -> str:
    payload = "\x00".join([index_id, key, action, effective_date, *source_hashes])
    return hashlib.sha256(payload.encode()).hexdigest()


def unresolved_backlog(events: list[MembershipEvent]) -> set[str]:
    """Distinct `security_id`s among current rows still carrying no identity.

    One helper for `import`, `sync` and `reresolve`. Counting superseded rows
    too — which import and sync used to do — would let a reresolve drain the
    number to zero while the rejected placeholder chain sat in the store, and
    the next nightly sync would restore the WARN.
    """
    superseded = {item.supersedes for item in events if item.supersedes is not None}
    return {item.security_id for item in events if item.event_id not in superseded and item.status == "unresolved"}


def import_events(
    *,
    index_id: str,
    events_path: Path,
    sources: list[Path],
    data_lake_root: Path,
    now: datetime,
    confidence: Confidence,
) -> dict:
    """Import one grok `events.jsonl` panel into `IndexMembershipStore`.

    Each line (`effective_date`, `action`, `ticker`, `kind`, optional `year` /
    `confidence`) becomes one `MembershipEvent`. Unresolvable tickers are kept
    as `status="unresolved"` events under a deterministic `unresolved:<ticker>`
    security_id so a later identity backfill can revise them; `members_effective_at`
    never counts them. Re-running the same import appends nothing — `event_id`
    is a content hash, and the store dedups on it.
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    verifier = _evidence_verifier(evidence)
    master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=verifier)

    artifacts = [evidence.persist_raw(Path(source).read_bytes()) for source in sources]
    source_refs = tuple(artifact.ref for artifact in artifacts)
    source_hashes = tuple(artifact.sha256 for artifact in artifacts)
    for source, artifact in zip(sources, artifacts, strict=True):
        evidence.record(
            SourceEvidence(
                ref=artifact.ref,
                sha256=artifact.sha256,
                source_url=Path(source).resolve().as_uri(),
                retrieved_at=now,
                publication_time=None,
                mediawiki_revision_id=None,
                mediawiki_revision_time=None,
                content_type="application/jsonl" if Path(source).suffix == ".jsonl" else "application/octet-stream",
            )
        )

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("membership-sync")
    run_row = {
        "run_id": run_id,
        "job": "membership-sync",
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
    try:
        resolved_status = "candidate" if index_id == "r2k-proxy" else _CONFIDENCE_STATUS[confidence]
        existing = store.events(index_id)
        seen_ids = {event.event_id for event in existing}
        revisions: dict[str, int] = {}
        for event in existing:
            revisions[event.security_id] = max(revisions.get(event.security_id, 0), event.revision)

        added = removed = skipped = 0
        for line in events_path.read_text().splitlines():
            if not (line := line.strip()):
                continue
            row = json.loads(line)
            ticker, action = row["ticker"], row["action"]
            effective_at = datetime.combine(date.fromisoformat(row["effective_date"]), time.min, tzinfo=UTC)
            resolved = _resolve(master, ticker, effective_at, now)
            event_id = _event_id(index_id, resolved or ticker, action, row["effective_date"], source_hashes)
            if event_id in seen_ids:
                skipped += 1
                continue
            security_id = resolved or f"unresolved:{ticker}"
            revision = revisions.get(security_id, 0) + 1
            appended = store.append(
                MembershipEvent(
                    event_id=event_id,
                    index_id=index_id,
                    security_id=security_id,
                    action=action,
                    announced_at=None,
                    effective_at=effective_at,
                    known_at=now,
                    source_refs=source_refs,
                    source_hashes=source_hashes,
                    revision=revision,
                    supersedes=None,
                    status=resolved_status if resolved else "unresolved",
                )
            )
            if appended:
                seen_ids.add(event_id)
                revisions[security_id] = revision
                if action == "add":
                    added += 1
                else:
                    removed += 1

        # The measurement is the store's current unresolved backlog, not this
        # run's appends — a re-import that adds nothing must still report it,
        # or a standing WARN in status would silently clear.
        unresolved_ids = unresolved_backlog(store.events(index_id))
        ledger.emit(
            "measurements",
            [
                {
                    "name": name,
                    "scope": index_id,
                    "measured_at": now,
                    "value": float(value),
                    "unit": "count",
                    "source": "measured",
                    "run_id": run_id,
                }
                for name, value in (
                    ("membership_events_added", added),
                    ("membership_events_removed", removed),
                    ("membership_unresolved", len(unresolved_ids)),
                )
            ],
            run_id=run_id,
        )
    except Exception:
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}],
            run_id=run_id,
        )
        raise
    ledger.emit(
        "runs",
        [run_row | {"ended": datetime.now(UTC), "exit_code": 0, "verdict": "OK"}],
        run_id=run_id,
    )
    return {
        "index": index_id,
        "added": added,
        "removed": removed,
        "unresolved": len(unresolved_ids),
        "skipped": skipped,
        "run_id": run_id,
    }


def _current_members(items: list[MembershipEvent]) -> set[str]:
    """Replay non-superseded events to the live member set.

    Every non-`rejected` status counts: an `unresolved:` placeholder is a
    member (else a still-unresolved ticker would be re-added on every sync),
    and a `candidate` is a member pending review. `rejected` never enters.
    """
    superseded = {item.supersedes for item in items if item.supersedes is not None}
    applicable = [item for item in items if item.event_id not in superseded and item.status != "rejected"]
    applicable.sort(key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id))
    members: set[str] = set()
    for item in applicable:
        if item.action == "add":
            members.add(item.security_id)
        else:
            members.discard(item.security_id)
    return members


def members_at(
    *,
    index_id: str,
    effective_at: datetime,
    as_of: datetime,
    data_lake_root: Path,
) -> list[str]:
    """Return the index's member symbols at `effective_at` as known at `as_of`.

    Read-only: replays non-superseded, non-`rejected` events effective by the
    cutoff — the same replay `sync` diffs, so an `unresolved:` placeholder or a
    `candidate` still counts as a member — then maps each `security_id` back to
    a symbol through `SecurityMaster`. A security_id with no verified identity
    renders as `?<security_id>` so unresolved members stay visible next to the
    resolved ones.
    """
    root = Path(data_lake_root)
    master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=None)
    events = [event for event in store.events(index_id, as_of=as_of) if event.effective_at <= effective_at]
    members = _current_members(events)

    identities = master.events(as_of=as_of)
    superseded = {item.supersedes for item in identities if item.supersedes is not None}
    verified = [item for item in identities if item.event_id not in superseded and item.status == "verified"]

    def symbol_for(security_id: str) -> str:
        candidates = [item for item in verified if item.security_id == security_id]
        containing = [
            item
            for item in candidates
            if item.effective_from <= effective_at and (item.effective_to is None or effective_at < item.effective_to)
        ]
        pick = containing or candidates
        if not pick:
            return f"?{security_id}"
        return max(pick, key=lambda item: (item.effective_from, item.known_at, item.event_id)).symbol

    return sorted({symbol_for(security_id) for security_id in members})


def _slickcharts_ref(store: SourceEvidenceStore, members: set[str], source_url: str, now: datetime) -> HashedRef:
    """Commit the fetched set as the evidence artifact for a Slickcharts source."""
    artifact = store.persist_raw(canonical_bytes(sorted(members)))
    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=source_url,
            retrieved_at=now,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="application/json",
        )
    )
    return HashedRef(artifact.ref, artifact.sha256)


def _default_fetch(index_id: str, store: SourceEvidenceStore, now: datetime) -> tuple[set[str], HashedRef]:
    """Fetch one index's live constituent set with content-addressed evidence."""
    if index_id in _WIKIPEDIA_SOURCES:
        title, label = _WIKIPEDIA_SOURCES[index_id]
        try:
            snapshot = MediaWikiClient(store, now=lambda: now).snapshot(title)
            members = parse_constituent_table(snapshot.content, label)
        except MediaWikiFetchError as exc:
            raise UniverseFetchError(f"{label}: {exc}") from exc
        return members, HashedRef(snapshot.evidence.ref, snapshot.evidence.sha256)
    if index_id == "djia":
        members = fetch_djia()  # floors at 30 constituents inside the fetcher
        return members, _slickcharts_ref(store, members, DJIA_SLICKCHARTS_URL, now)
    if index_id == "r2k-proxy":
        members = fetch_r2k()
        if len(members) < _R2K_PROXY_MIN_MEMBERS:
            raise UniverseFetchError(
                f"r2k-proxy: fetched {len(members)} members, below the {_R2K_PROXY_MIN_MEMBERS} floor"
            )
        return members, _slickcharts_ref(store, members, R2K_SLICKCHARTS_URL, now)
    raise UniverseFetchError(f"membership-sync: no live source for index {index_id!r}")


def _status_for(index_id: str, resolved: bool) -> str:
    if not resolved:
        return "unresolved"
    return "candidate" if index_id == "r2k-proxy" else "verified"


def _measure(run_id: str, index_id: str, measured_at: datetime, pairs: dict[str, float]) -> None:
    ledger.emit(
        "measurements",
        [
            {
                "name": name,
                "scope": index_id,
                "measured_at": measured_at,
                "value": float(value),
                "unit": "count",
                "source": "measured",
                "run_id": run_id,
            }
            for name, value in pairs.items()
        ],
        run_id=run_id,
    )


def sync(
    *,
    indexes: list[str],
    data_lake_root: Path,
    now: datetime,
    fetch_fn=None,
    runner=None,
    dry_run: bool = False,
) -> int:
    """Diff each index's live source against the store and append add/remove.

    A fetch failure pages once per run and exits 3 — never a silent swap to a
    secondary source. `dry_run` computes and prints the diff without appending;
    measurements then report the actual appended counts (zero).
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=_evidence_verifier(evidence))
    fetch = fetch_fn or (lambda index_id: _default_fetch(index_id, evidence, now))

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("membership-sync")
    run_row = {
        "run_id": run_id,
        "job": "membership-sync",
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
    failures: dict[str, Exception] = {}
    try:
        for index_id in indexes:
            existing = store.events(index_id)
            revisions: dict[str, int] = {}
            for event in existing:
                revisions[event.security_id] = max(revisions.get(event.security_id, 0), event.revision)
            try:
                tickers, source = fetch(index_id)
            except Exception as exc:
                failures[index_id] = exc
                unresolved_ids = unresolved_backlog(store.events(index_id))
                _measure(
                    run_id,
                    index_id,
                    now,
                    {"membership_source_fetch_ok": 0, "membership_unresolved": len(unresolved_ids)},
                )
                print(json.dumps({"index": index_id, "fetch_ok": 0, "error": str(exc)}, sort_keys=True))
                continue

            members = _current_members(existing)
            fetched: dict[str, str | None] = {}
            for ticker in sorted(tickers):
                resolved = _resolve(master, ticker, now, now)
                fetched[resolved or f"unresolved:{ticker}"] = resolved
            adds = sorted(set(fetched) - members)
            removes = sorted(members - set(fetched))
            added = removed = 0
            if not dry_run:
                pending = [(key, "add") for key in adds] + [(key, "remove") for key in removes]
                for security_id, action in pending:
                    resolved = fetched.get(security_id) if action == "add" else None
                    event = MembershipEvent(
                        event_id=_event_id(index_id, security_id, action, now.date().isoformat(), (source.sha256,)),
                        index_id=index_id,
                        security_id=security_id,
                        action=action,
                        announced_at=None,
                        effective_at=now,
                        known_at=now,
                        source_refs=(source.ref,),
                        source_hashes=(source.sha256,),
                        revision=revisions.get(security_id, 0) + 1,
                        supersedes=None,
                        status=_status_for(
                            index_id,
                            resolved is not None if action == "add" else not security_id.startswith("unresolved:"),
                        ),
                    )
                    if store.append(event):
                        revisions[security_id] = event.revision
                        if action == "add":
                            added += 1
                        else:
                            removed += 1
            unresolved_ids = unresolved_backlog(store.events(index_id))
            _measure(
                run_id,
                index_id,
                now,
                {
                    "membership_events_added": added,
                    "membership_events_removed": removed,
                    "membership_unresolved": len(unresolved_ids),
                    "membership_source_fetch_ok": 1,
                },
            )
            print(
                json.dumps(
                    {
                        "index": index_id,
                        "applied": not dry_run,
                        "adds": adds,
                        "removes": removes,
                        "unresolved": len(unresolved_ids),
                    },
                    sort_keys=True,
                )
            )
    except Exception:
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}],
            run_id=run_id,
        )
        raise
    exit_code = 3 if failures else 0
    ledger.emit(
        "runs",
        [run_row | {"ended": datetime.now(UTC), "exit_code": exit_code, "verdict": "FAILED" if failures else "OK"}],
        run_id=run_id,
    )
    if failures:
        summary = "; ".join(f"{index_id}: {exc}" for index_id, exc in failures.items())
        notice = notify.page_for_lane(now.date(), "membership-sync", exit_code, summary, summary)
        notify.send(notice, runner=runner)
    return exit_code


def _replay_is_balanced(events: list[MembershipEvent], proposed: MembershipEvent) -> bool:
    """Would inserting `proposed` leave a balanced add/remove replay?

    The same rule `pit_silver_revision` enforces ("duplicate open membership
    interval", "membership removal has no open interval"), filtered to the
    status the proposed event will carry: PIT Silver replays verified events
    only, so a guard over mixed statuses could pass an add-then-remove whose
    add is `candidate` and leave the verified replay with a remove and no open
    add. A nightly `sync` add that landed *later* than the historical add being
    inserted is caught here and nowhere else.
    """
    superseded = {item.supersedes for item in events if item.supersedes is not None}
    sequence = [
        item
        for item in events
        if item.event_id not in superseded
        and item.status == proposed.status
        and item.security_id == proposed.security_id
    ]
    sequence.append(proposed)
    sequence.sort(key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id))
    is_open = False
    for item in sequence:
        if item.action == "add":
            if is_open:
                return False
            is_open = True
        else:
            if not is_open:
                return False
            is_open = False
    return True


def _open_resolved_adds(master: SecurityMaster, events: list[MembershipEvent], as_of: datetime) -> dict[str, str]:
    """Ticker -> security_id of every resolved add still open in the index.

    Seeds `last_add` with adds resolved by an earlier pass (spec §3.3 rule 1):
    a retry that lands after an add's pair but before its delisting-date
    remove must still resolve that remove through the add, not the clock. The
    ticker comes from the master row of that id containing the add's date.
    """
    symbols = [
        (item.security_id, item.symbol, item.effective_from, item.effective_to) for item in master.events(as_of=as_of)
    ]
    superseded = {item.supersedes for item in events if item.supersedes is not None}
    current = [
        item
        for item in events
        if item.event_id not in superseded
        and item.status not in ("unresolved", "rejected")
        and not item.security_id.startswith("unresolved:")
    ]
    current.sort(key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id))
    open_adds: dict[str, str] = {}
    for item in current:
        tickers = [
            symbol
            for security_id, symbol, start, end in symbols
            if security_id == item.security_id
            and start <= item.effective_at
            and (end is None or item.effective_at < end)
        ]
        for ticker in tickers:
            if item.action == "add":
                open_adds[ticker] = item.security_id
            else:
                open_adds.pop(ticker, None)
    return open_adds


def _resolved_event_id(placeholder_id: str, security_id: str) -> str:
    return hashlib.sha256(f"reresolve\x00{placeholder_id}\x00{security_id}".encode()).hexdigest()


def _rejection_event_id(placeholder_id: str) -> str:
    return hashlib.sha256(f"reresolve-reject\x00{placeholder_id}".encode()).hexdigest()


def reresolve(
    *,
    index_id: str,
    data_lake_root: Path,
    now: datetime,
    confidence: Confidence,
) -> dict:
    """Rewrite each resolvable `unresolved:` placeholder onto its security_id.

    Two appends per placeholder because `IndexMembershipStore` only lets an
    event supersede one with the same `(index_id, security_id)`: the resolved
    event first, then the `rejected` revision of the placeholder. That order is
    restart-safe — a crash between them leaves the placeholder current and the
    retry recovers the stored replacement by its deterministic `event_id`; the
    reverse order would lose the membership on retry.

    `known_at = now` keeps PIT honest: an `as_of` before this pass still sees
    the placeholder (spec 2026-09-13-dividend-fx-and-pit-membership, rule 8 of
    docs/contracts/shepherd-security-identity.md).
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    verifier = _evidence_verifier(evidence)
    master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=verifier)

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("membership-reresolve")
    run_row = {
        "run_id": run_id,
        "job": "membership-reresolve",
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
    try:
        resolved_status = "candidate" if index_id == "r2k-proxy" else _CONFIDENCE_STATUS[confidence]
        events = store.events(index_id)
        superseded = {item.supersedes for item in events if item.supersedes is not None}
        placeholders = sorted(
            (
                item
                for item in events
                if item.event_id not in superseded
                and item.status == "unresolved"
                and item.security_id.startswith("unresolved:")
            ),
            key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id),
        )
        by_id = {item.event_id: item for item in events}
        revisions: dict[str, int] = {}
        for item in events:
            revisions[item.security_id] = max(revisions.get(item.security_id, 0), item.revision)

        last_add = _open_resolved_adds(master, list(by_id.values()), now)
        resolved_count = conflicts = 0
        for placeholder in placeholders:
            ticker = placeholder.security_id.removeprefix("unresolved:")
            if placeholder.action == "add":
                security_id = _resolve(master, ticker, placeholder.effective_at, now)
            else:
                # Master intervals are end-exclusive, so a remove effective on
                # the delisting date resolves through its own add, not the clock.
                security_id = last_add.get(ticker) or _resolve(master, ticker, placeholder.effective_at, now)
            if security_id is None:
                continue

            replacement_id = _resolved_event_id(placeholder.event_id, security_id)
            if replacement_id not in by_id:
                # Fail closed on an unbalanced replay; no conflict check runs on
                # a recovery, because the replacement is already in the store.
                proposed = MembershipEvent(
                    event_id=replacement_id,
                    index_id=index_id,
                    security_id=security_id,
                    action=placeholder.action,
                    announced_at=placeholder.announced_at,
                    effective_at=placeholder.effective_at,
                    known_at=now,
                    source_refs=placeholder.source_refs,
                    source_hashes=placeholder.source_hashes,
                    revision=revisions.get(security_id, 0) + 1,
                    supersedes=None,
                    status=resolved_status,
                )
                if not _replay_is_balanced(list(by_id.values()), proposed):
                    conflicts += 1
                    continue
                store.append(proposed)
                revisions[security_id] = proposed.revision
                by_id[proposed.event_id] = proposed
                resolved_count += 1

            rejection_id = _rejection_event_id(placeholder.event_id)
            if rejection_id not in by_id:
                revisions[placeholder.security_id] = revisions.get(placeholder.security_id, 0) + 1
                rejection = MembershipEvent(
                    event_id=rejection_id,
                    index_id=index_id,
                    security_id=placeholder.security_id,
                    action=placeholder.action,
                    announced_at=placeholder.announced_at,
                    effective_at=placeholder.effective_at,
                    known_at=now,
                    source_refs=placeholder.source_refs,
                    source_hashes=placeholder.source_hashes,
                    revision=revisions[placeholder.security_id],
                    supersedes=placeholder.event_id,
                    status="rejected",
                )
                store.append(rejection)
                by_id[rejection.event_id] = rejection
            if placeholder.action == "add":
                last_add[ticker] = security_id
            else:
                last_add.pop(ticker, None)

        backlog = len(unresolved_backlog(store.events(index_id)))
        _measure(
            run_id,
            index_id,
            now,
            {"membership_reresolve_conflict": conflicts, "membership_unresolved": backlog},
        )
    except Exception:
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}],
            run_id=run_id,
        )
        raise
    ledger.emit(
        "runs",
        [run_row | {"ended": datetime.now(UTC), "exit_code": 0, "verdict": "OK"}],
        run_id=run_id,
    )
    return {
        "index": index_id,
        "resolved": resolved_count,
        "conflicts": conflicts,
        "unresolved": backlog,
        "run_id": run_id,
    }


def _reresolve_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py membership-sync reresolve",
        description="Rewrite resolvable unresolved: placeholders onto their security_id",
    )
    parser.add_argument("--index", required=True, choices=sorted(DEFAULT_INDEXES))
    parser.add_argument(
        "--confidence",
        required=True,
        choices=["B", "C", "D"],
        help="A placeholder does not record the confidence its history was imported under",
    )
    args = parser.parse_args(argv)
    print(
        json.dumps(
            reresolve(
                index_id=args.index,
                data_lake_root=data_lake_dir(),
                now=datetime.now(UTC),
                confidence=args.confidence,
            ),
            sort_keys=True,
        )
    )
    return 0


def _import_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py membership-sync import",
        description="One-time import of a grok events.jsonl panel",
    )
    parser.add_argument("--index", required=True, help="Index id (sp500, ndx100, djia, r2k-proxy)")
    parser.add_argument("--events", type=Path, required=True, help="Path to the grok events.jsonl")
    parser.add_argument(
        "--source",
        type=Path,
        action="append",
        required=True,
        help="Provenance file committed to the evidence CAS (repeatable)",
    )
    parser.add_argument("--confidence", choices=["B", "C", "D"], default="B")
    args = parser.parse_args(argv)
    print(
        json.dumps(
            import_events(
                index_id=args.index,
                events_path=args.events,
                sources=args.source,
                data_lake_root=data_lake_dir(),
                now=datetime.now(UTC),
                confidence=args.confidence,
            ),
            sort_keys=True,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    if argv[:1] == ["import"]:
        return _import_main(argv[1:])
    if argv[:1] == ["reresolve"]:
        return _reresolve_main(argv[1:])
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py membership-sync",
        description="Diff each index's live source against the membership store",
    )
    parser.add_argument(
        "--index",
        action="extend",
        nargs="+",
        choices=sorted(DEFAULT_INDEXES),
        help="Indexes to sync (space-separated and/or repeatable; default: all four)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the diff without appending events")
    args = parser.parse_args(argv)
    return sync(
        indexes=args.index or list(DEFAULT_INDEXES),
        data_lake_root=data_lake_dir(),
        now=datetime.now(UTC),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
