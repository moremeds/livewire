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
import shutil
import socket
import sys
import tempfile
from dataclasses import replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal

from clients import ledger
from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.mediawiki_client import MediaWikiClient, MediaWikiFetchError
from clients.security_master import SecurityIdentityEvent, SecurityMaster
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
#
# `wikipedia_sec_research` is the 2026-09-16 one-off backfill of pre-2003
# delistings Massive has no record of at all; it is a second, distinct
# provider (never `massive`, so a Massive full-reconcile never touches it —
# pm:2026-07-19-cancellation-inference-provider-scoped) that reresolve must
# also check, or the whole import would sit unused in the master.
_RESOLVE_PROVIDERS = ("massive", "wikipedia_sec_research")
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
        for provider in _RESOLVE_PROVIDERS
        for mic in _RESOLVE_MICS
        if (security_id := master.resolve_symbol(provider, ticker, mic, effective_at, as_of)) is not None
    }
    if len(matches) == 1:
        return next(iter(matches))
    return None  # no identity, the same ticker verified on two venues, or two providers disagree


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
    abandoned = ledger.open_run(run_row)
    if abandoned:
        print(f"abandoned {len(abandoned)} open run(s) of membership-sync: {', '.join(abandoned)}")
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
    except BaseException:  # noqa: BLE001 - pm:2026-09-16-interrupted-runs-never-closed: still close the run on SystemExit/KeyboardInterrupt
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


def _replay_membership(items: list[MembershipEvent]) -> list[MembershipEvent]:
    """Non-superseded, non-`rejected` events in replay order.

    Shared by `_current_members` and the last-add tracking R3 needs: every
    non-`rejected` status counts (an `unresolved:` placeholder is a member,
    else a still-unresolved ticker would be re-added on every sync; a
    `candidate` is a member pending review), `rejected` never enters.
    """
    superseded = {item.supersedes for item in items if item.supersedes is not None}
    applicable = [item for item in items if item.event_id not in superseded and item.status != "rejected"]
    applicable.sort(key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id))
    return applicable


def _current_members(items: list[MembershipEvent]) -> set[str]:
    """Replay to the live member set (security_ids)."""
    members: set[str] = set()
    for item in _replay_membership(items):
        if item.action == "add":
            members.add(item.security_id)
        else:
            members.discard(item.security_id)
    return members


def _current_members_last_add(items: list[MembershipEvent]) -> dict[str, datetime]:
    """Live security_ids -> `effective_at` of the add that put them in the index.

    R3: the ticker a currently-held member maps to is read off the identity
    claim covering *this* date, not `now` — a security whose ticker changed
    since the add would otherwise resolve to the wrong symbol.
    """
    members: dict[str, datetime] = {}
    for item in _replay_membership(items):
        if item.action == "add":
            members[item.security_id] = item.effective_at
        else:
            members.pop(item.security_id, None)
    return members


def _active_identities(items: list[SecurityIdentityEvent]) -> list[SecurityIdentityEvent]:
    superseded = {item.supersedes for item in items if item.supersedes is not None}
    return [item for item in items if item.event_id not in superseded and item.status == "verified"]


def _identity_ticker(security_id: str, at: datetime, identities: list[SecurityIdentityEvent]) -> str | None:
    """The symbol of the verified identity claim covering `at`.

    A placeholder carries its own ticker (R3 rule 1). Otherwise this mirrors
    `members_at.symbol_for`: the claim containing `at`, or (no claim contains
    it) the most recently known claim, so a member never silently drops out
    of the diff for want of a covering window.
    """
    if security_id.startswith("unresolved:"):
        return security_id.removeprefix("unresolved:")
    candidates = [item for item in identities if item.security_id == security_id]
    containing = [
        item
        for item in candidates
        if item.effective_from <= at and (item.effective_to is None or at < item.effective_to)
    ]
    pick = containing or candidates
    if not pick:
        return None
    return max(pick, key=lambda item: (item.effective_from, item.known_at, item.event_id)).symbol


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
    abandoned = ledger.open_run(run_row)
    if abandoned:
        print(f"abandoned {len(abandoned)} open run(s) of membership-sync: {', '.join(abandoned)}")
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

            # R3: diff by ticker, not security_id. A member's ticker is its
            # placeholder's own ticker, or the symbol of the identity claim
            # covering its last add — never the ticker `_resolve` would find
            # at `now`, which a narrow researched window does not cover
            # (pm:2026-09-23-narrow-identity-window-churned-membership).
            identities = _active_identities(master.events(as_of=now))
            member_ticker: dict[str, str] = {}
            for security_id, add_at in _current_members_last_add(existing).items():
                ticker = _identity_ticker(security_id, add_at, identities)
                if ticker is not None:
                    member_ticker[ticker] = security_id
            add_tickers = sorted(set(tickers) - set(member_ticker))
            remove_tickers = sorted(set(member_ticker) - set(tickers))

            fetched: dict[str, str | None] = {}
            for ticker in add_tickers:
                resolved = _resolve(master, ticker, now, now)
                fetched[resolved or f"unresolved:{ticker}"] = resolved
            adds = sorted(fetched)
            removes = [member_ticker[ticker] for ticker in remove_tickers]
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
    except BaseException:  # noqa: BLE001 - pm:2026-09-16-interrupted-runs-never-closed: still close the run on SystemExit/KeyboardInterrupt
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
    abandoned = ledger.open_run(run_row)
    if abandoned:
        print(f"abandoned {len(abandoned)} open run(s) of membership-reresolve: {', '.join(abandoned)}")
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
    except BaseException:  # noqa: BLE001 - pm:2026-09-16-interrupted-runs-never-closed: still close the run on SystemExit/KeyboardInterrupt
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


# --- repair-identity: R1 (CIK merge), R2 (window extension), R4 (churn reject) ---
# docs/superpowers/specs/2026-09-23-membership-identity-continuity-design.md

REPAIR_INDEXES = ("sp500", "ndx100", "djia")

# Acceptance §4: the three sp500 as-of dates the before/after member counts are
# checked against. Fixed, not a CLI flag — the design names them, not a range.
_MEMBER_COUNT_DATES = (
    datetime(2015, 1, 2, tzinfo=UTC),
    datetime(2025, 1, 2, tzinfo=UTC),
    datetime(2026, 9, 1, tzinfo=UTC),
)


def _r1_reject_event_id(claim_event_id: str) -> str:
    return hashlib.sha256(f"repair-identity-r1-reject\x00{claim_event_id}".encode()).hexdigest()


def _r1_merge_event_id(claim_event_id: str, canonical_id: str) -> str:
    return hashlib.sha256(f"repair-identity-r1-merge\x00{claim_event_id}\x00{canonical_id}".encode()).hexdigest()


def _r2_extend_event_id(claim_event_id: str) -> str:
    return hashlib.sha256(f"repair-identity-r2-extend\x00{claim_event_id}".encode()).hexdigest()


def _r4_reject_event_id(rejected_event_id: str) -> str:
    return hashlib.sha256(f"repair-identity-r4-reject\x00{rejected_event_id}".encode()).hexdigest()


def _plan_r1(
    identities: list[SecurityIdentityEvent],
    revisions: dict[str, int],
    now: datetime,
) -> tuple[list[dict], list[dict], list[SecurityIdentityEvent], dict[str, str]]:
    """R1: a `massive` identity sharing CIK and symbol with a researched one is
    a duplicate — its active claim is rejected and re-appended under the
    researched (canonical) `security_id`. Different or missing CIKs merge
    nothing and are reported as a conflict, never guessed.

    `revisions` is threaded through so R2's later appends for the same
    security continue the sequence this function starts.
    """
    active = _active_identities(identities)
    researched_by_id: dict[str, list[SecurityIdentityEvent]] = {}
    for item in active:
        if item.provider == "wikipedia_sec_research":
            researched_by_id.setdefault(item.security_id, []).append(item)
    massive = [item for item in active if item.provider == "massive"]

    merges: list[dict] = []
    conflicts: list[dict] = []
    events: list[SecurityIdentityEvent] = []
    merged_massive_ids: dict[str, str] = {}

    for canonical_id, claims in sorted(researched_by_id.items()):
        ciks = {c.cik for c in claims if c.cik}
        cik_r = next(iter(ciks)) if len(ciks) == 1 else None
        symbols = {c.symbol for c in claims}
        for symbol in sorted(symbols):
            for candidate in massive:
                if candidate.symbol != symbol or candidate.security_id == canonical_id:
                    continue
                if cik_r is None or candidate.cik is None or candidate.cik != cik_r:
                    conflicts.append(
                        {
                            "ticker": symbol,
                            "researched_security_id": canonical_id,
                            "massive_security_id": candidate.security_id,
                            "researched_cik": cik_r,
                            "massive_cik": candidate.cik,
                            "reason": "missing_cik" if cik_r is None or candidate.cik is None else "cik_mismatch",
                        }
                    )
                    continue
                reject_id = _r1_reject_event_id(candidate.event_id)
                merge_id = _r1_merge_event_id(candidate.event_id, canonical_id)
                revisions[candidate.security_id] = revisions.get(candidate.security_id, 0) + 1
                events.append(
                    replace(
                        candidate,
                        event_id=reject_id,
                        revision=revisions[candidate.security_id],
                        known_at=now,
                        status="rejected",
                        supersedes=candidate.event_id,
                    )
                )
                revisions[canonical_id] = revisions.get(canonical_id, 0) + 1
                events.append(
                    replace(
                        candidate,
                        event_id=merge_id,
                        security_id=canonical_id,
                        revision=revisions[canonical_id],
                        known_at=now,
                        status="verified",
                        supersedes=None,
                    )
                )
                merged_massive_ids[candidate.security_id] = canonical_id
                merges.append(
                    {
                        "cik": cik_r,
                        "ticker": symbol,
                        "researched_security_id": canonical_id,
                        "massive_security_id": candidate.security_id,
                        "rejected_event_id": reject_id,
                        "merged_event_id": merge_id,
                    }
                )
    return merges, conflicts, events, merged_massive_ids


def _detect_churn(
    events_by_index: dict[str, list[MembershipEvent]],
    identities: list[SecurityIdentityEvent],
) -> dict[str, set[str]]:
    """R4: index_id -> event_ids of same-timestamp remove+add pairs for one
    ticker. `_identity_ticker` already resolves a placeholder to its own
    ticker and a resolved id to the symbol its identity claim covering that
    moment carries, so an add that only *looks* like a different security
    (old id removed, new massive id or placeholder added) still pairs.
    """
    churn: dict[str, set[str]] = {}
    for index_id, events in events_by_index.items():
        active = _replay_membership(events)
        by_time: dict[datetime, list[MembershipEvent]] = {}
        for item in active:
            by_time.setdefault(item.effective_at, []).append(item)
        ids: set[str] = set()
        for group in by_time.values():
            removes = [item for item in group if item.action == "remove"]
            adds = [item for item in group if item.action == "add"]
            for rem in removes:
                ticker_r = _identity_ticker(rem.security_id, rem.effective_at, identities)
                if ticker_r is None:
                    continue
                for add in adds:
                    if _identity_ticker(add.security_id, add.effective_at, identities) == ticker_r:
                        ids.add(rem.event_id)
                        ids.add(add.event_id)
        churn[index_id] = ids
    return churn


def _plan_r2(
    identities: list[SecurityIdentityEvent],
    events_by_index: dict[str, list[MembershipEvent]],
    churn_ids_by_index: dict[str, set[str]],
    revisions: dict[str, int],
    now: datetime,
) -> tuple[list[dict], list[dict], list[SecurityIdentityEvent]]:
    """R2: extend each researched narrow claim to the next non-churn remove
    of its security in any processed index, capped (never forced) at the
    start of a claim it would otherwise collide with under
    `SecurityMaster._validate_append`.
    """
    active = _active_identities(identities)
    researched = sorted(
        (item for item in active if item.provider == "wikipedia_sec_research"),
        key=lambda item: (item.security_id, item.effective_from, item.event_id),
    )

    extensions: list[dict] = []
    caps: list[dict] = []
    out_events: list[SecurityIdentityEvent] = []

    for claim in researched:
        own = [
            item
            for index_id, events in sorted(events_by_index.items())
            for item in _replay_membership(events)
            if item.security_id == claim.security_id and item.event_id not in churn_ids_by_index.get(index_id, set())
        ]
        removes = sorted(
            (item for item in own if item.action == "remove" and item.effective_at > claim.effective_from),
            key=lambda item: (item.effective_at, item.event_id),
        )
        end = removes[0].effective_at if removes else None
        # The index's own record is the evidence for the wider window: cite the
        # adds inside the claim and the remove that ends it, beside the claim's refs.
        cited = [
            item
            for item in own
            if item.action == "add"
            and claim.effective_from <= item.effective_at
            and (claim.effective_to is None or item.effective_at < claim.effective_to)
        ] + removes[:1]
        refs = dict(zip(claim.source_refs, claim.source_hashes, strict=True))
        for item in cited:
            refs.update(zip(item.source_refs, item.source_hashes, strict=True))

        capped_at = None
        for other in active:
            if other.security_id == claim.security_id or other.event_id == claim.event_id:
                continue
            if not _overlaps_interval(claim.effective_from, end, other.effective_from, other.effective_to):
                continue
            same_symbol_interval = (claim.provider, claim.symbol, claim.exchange_mic) == (
                other.provider,
                other.symbol,
                other.exchange_mic,
            )
            figi_collision = any(
                getattr(claim, field) is not None and getattr(claim, field) == getattr(other, field)
                for field in ("share_class_figi", "composite_figi")
            )
            if (same_symbol_interval or figi_collision) and (capped_at is None or other.effective_from < capped_at):
                capped_at = other.effective_from

        new_end = end
        if capped_at is not None and (end is None or capped_at < end):
            caps.append(
                {
                    "security_id": claim.security_id,
                    "event_id": claim.event_id,
                    "requested_end": end.isoformat() if end else None,
                    "capped_at": capped_at.isoformat(),
                }
            )
            new_end = capped_at

        if new_end is not None and new_end <= claim.effective_from:
            continue  # never forced: a cap at or before the window's own start extends nothing
        if new_end == claim.effective_to:
            continue

        revisions[claim.security_id] = revisions.get(claim.security_id, 0) + 1
        extended = replace(
            claim,
            event_id=_r2_extend_event_id(claim.event_id),
            revision=revisions[claim.security_id],
            effective_to=new_end,
            known_at=now,
            supersedes=claim.event_id,
            source_refs=tuple(refs),
            source_hashes=tuple(refs.values()),
        )
        out_events.append(extended)
        extensions.append(
            {
                "security_id": claim.security_id,
                "event_id": claim.event_id,
                "extended_event_id": extended.event_id,
                "old_effective_to": claim.effective_to.isoformat() if claim.effective_to else None,
                "new_effective_to": new_end.isoformat() if new_end else None,
            }
        )
    return extensions, caps, out_events


def _overlaps_interval(
    left_start: datetime, left_end: datetime | None, right_start: datetime, right_end: datetime | None
) -> bool:
    return (right_end is None or left_start < right_end) and (left_end is None or right_start < left_end)


def _plan_r4(
    events_by_index: dict[str, list[MembershipEvent]],
    churn_ids_by_index: dict[str, set[str]],
    now: datetime,
) -> tuple[dict[str, list[dict]], dict[str, list[MembershipEvent]]]:
    """R4: a `rejected` row superseding each still-active churn event."""
    rejections: dict[str, list[dict]] = {}
    events_out: dict[str, list[MembershipEvent]] = {}
    for index_id, events in sorted(events_by_index.items()):
        by_id = {item.event_id: item for item in events}
        revisions: dict[str, int] = {}
        for item in events:
            revisions[item.security_id] = max(revisions.get(item.security_id, 0), item.revision)
        rows: list[dict] = []
        new_events: list[MembershipEvent] = []
        for churn_id in sorted(churn_ids_by_index.get(index_id, set())):
            item = by_id[churn_id]
            revisions[item.security_id] = revisions.get(item.security_id, 0) + 1
            reject = replace(
                item,
                event_id=_r4_reject_event_id(item.event_id),
                revision=revisions[item.security_id],
                known_at=now,
                status="rejected",
                supersedes=item.event_id,
            )
            new_events.append(reject)
            rows.append(
                {
                    "index_id": index_id,
                    "security_id": item.security_id,
                    "action": item.action,
                    "effective_at": item.effective_at.isoformat(),
                    "rejected_event_id": item.event_id,
                    "rejection_event_id": reject.event_id,
                }
            )
        if new_events:
            rejections[index_id] = rows
            events_out[index_id] = new_events
    return rejections, events_out


def _member_counts(root: Path, indexes: list[str], as_of: datetime) -> dict[str, dict[str, int]]:
    master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=None)
    return {
        index_id: {
            d.date().isoformat(): len(store.members_effective_at(index_id, d, as_of)) for d in _MEMBER_COUNT_DATES
        }
        for index_id in indexes
    }


def _scratch_lake_copy(root: Path, dest: Path) -> Path:
    """Copy only the two stores repair-identity writes into `dest`.

    Never `raw/`: on the mini it holds the Massive flat files. Evidence is
    verified read-only against the real root's CAS instead.
    """
    for sub in ("security_master", "index_membership"):
        src = root / sub
        if src.exists():
            shutil.copytree(src, dest / sub)
    return dest


def _apply_repair(
    root: Path,
    verifier,
    security_master_events: list[SecurityIdentityEvent],
    membership_events_by_index: dict[str, list[MembershipEvent]],
) -> None:
    master = SecurityMaster(root, evidence_verifier=verifier)
    for event in security_master_events:
        master.append(event)
    reader_master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=reader_master, evidence_verifier=verifier)
    for _index_id, events in sorted(membership_events_by_index.items()):
        for event in events:
            store.append(event)


def repair_identity(
    *,
    indexes: list[str],
    data_lake_root: Path,
    now: datetime,
    apply: bool = False,
    output: Path | None = None,
) -> dict:
    """R1 (CIK merge), R2 (researched-window extension), R4 (churn rejection).

    Dry run by default: computes the plan, writes the JSON manifest (if
    `output` is given) and one ledger run + its measurements, without
    touching a store. `member_counts_after` is computed by replaying the plan
    onto a scratch copy of the lake, which is discarded either way.
    `--apply` appends security-master rows first (R1 then R2), then
    membership rejections (R4), to the real lake, in that order.

    Idempotent: every appended event id derives from the event it
    supersedes (or, for an R1 merge, from the claim and its target), and
    superseding a claim removes it from the next run's "active" set — so a
    rerun over the same input state has nothing left to merge, extend or
    reject, and appends nothing.
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    verifier = _evidence_verifier(evidence)
    reader_master = SecurityMaster(root, evidence_verifier=None)
    identities = reader_master.events(as_of=now)
    active_identities = _active_identities(identities)

    events_by_index = {
        index_id: IndexMembershipStore(root, security_master=reader_master, evidence_verifier=None).events(
            index_id, as_of=now
        )
        for index_id in indexes
    }
    churn_ids_by_index = _detect_churn(events_by_index, active_identities)

    revisions: dict[str, int] = {}
    for item in identities:
        revisions[item.security_id] = max(revisions.get(item.security_id, 0), item.revision)

    merges, conflicts, r1_events, merged_massive_ids = _plan_r1(identities, revisions, now)
    extensions, caps, r2_events = _plan_r2(identities, events_by_index, churn_ids_by_index, revisions, now)
    rejections_by_index, r4_events_by_index = _plan_r4(events_by_index, churn_ids_by_index, now)

    dangling: list[dict] = []
    for index_id, events in sorted(events_by_index.items()):
        churny = churn_ids_by_index.get(index_id, set())
        for item in _replay_membership(events):
            if item.status == "verified" and item.security_id in merged_massive_ids and item.event_id not in churny:
                dangling.append({"index_id": index_id, "event_id": item.event_id, "security_id": item.security_id})

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("membership-repair-identity")
    run_row = {
        "run_id": run_id,
        "job": "membership-repair-identity",
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
        print(f"abandoned {len(abandoned)} open run(s) of membership-repair-identity: {', '.join(abandoned)}")

    manifest = {
        "indexes": sorted(indexes),
        "apply": apply,
        "merges": merges,
        "conflicts": conflicts,
        "extensions": extensions,
        "caps": caps,
        "rejections": [row for rows in rejections_by_index.values() for row in rows],
        "dangling_verified_references": dangling,
        "member_counts_before": _member_counts(root, indexes, now),
    }
    security_master_events = r1_events + r2_events

    def close(exit_code: int, verdict: str) -> None:
        ledger.emit(
            "runs", [run_row | {"ended": datetime.now(UTC), "exit_code": exit_code, "verdict": verdict}], run_id=run_id
        )
        counts = {
            "identity_merges": len(merges),
            "identity_conflicts": len(conflicts),
            "identity_extensions": len(extensions),
            "identity_caps": len(caps),
            "membership_rejections": sum(len(rows) for rows in rejections_by_index.values()),
            "identity_dangling_references": len(dangling),
        }
        ledger.emit(
            "measurements",
            [
                {
                    "name": name,
                    "scope": ",".join(sorted(indexes)),
                    "measured_at": now,
                    "value": float(value),
                    "unit": "count",
                    "source": "measured",
                    "run_id": run_id,
                }
                for name, value in counts.items()
            ],
            run_id=run_id,
        )

    try:
        if dangling:
            manifest["member_counts_after"] = manifest["member_counts_before"]
            _write_manifest(output, manifest)
            raise ValueError(
                f"{len(dangling)} verified membership event(s) still reference a security_master identity "
                "R1 would reject; refusing to guess, nothing applied"
            )

        if apply:
            _apply_repair(root, verifier, security_master_events, r4_events_by_index)
            manifest["member_counts_after"] = _member_counts(root, indexes, now)
        else:
            with tempfile.TemporaryDirectory() as tmp:
                scratch = _scratch_lake_copy(root, Path(tmp) / "scratch")
                scratch_verifier = _evidence_verifier(SourceEvidenceStore(root))
                _apply_repair(scratch, scratch_verifier, security_master_events, r4_events_by_index)
                manifest["member_counts_after"] = _member_counts(scratch, indexes, now)
        _write_manifest(output, manifest)
    except BaseException:  # noqa: BLE001 - pm:2026-09-16-interrupted-runs-never-closed
        close(1, "FAILED")
        raise
    close(0, "OK")
    return manifest


def _write_manifest(output: Path | None, manifest: dict) -> None:
    if output is None:
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n")


def _repair_identity_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py membership-sync repair-identity",
        description="R1/R2/R4: merge CIK-duplicate identities, extend researched windows, reject 2026-09-17-style churn",
    )
    parser.add_argument(
        "--index",
        action="extend",
        nargs="+",
        choices=sorted(REPAIR_INDEXES),
        help="Indexes to repair (space-separated and/or repeatable; default: sp500 ndx100 djia)",
    )
    parser.add_argument("--apply", action="store_true", help="Append the planned events (default: dry run)")
    parser.add_argument("--output", type=Path, help="Write the JSON manifest to this path")
    args = parser.parse_args(argv)
    manifest = repair_identity(
        indexes=args.index or list(REPAIR_INDEXES),
        data_lake_root=data_lake_dir(),
        now=datetime.now(UTC),
        apply=args.apply,
        output=args.output,
    )
    print(json.dumps(manifest, sort_keys=True, default=str))
    return 0


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
    if argv[:1] == ["repair-identity"]:
        return _repair_identity_main(argv[1:])
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
