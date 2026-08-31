#!/usr/bin/env python3
"""Evidence-first reconciliation of current S&P 500 and Nasdaq-100 seeds."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.mediawiki_client import MediaWikiClient
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from clients.universe_client import UniverseFetchError, parse_constituent_table
from livewire_scripts.paths import data_lake_dir

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INDEXES = {
    "sp500": ("List of S&P 500 companies", PROJECT_ROOT / "presets" / "sp500.json"),
    "ndx100": ("Nasdaq-100", PROJECT_ROOT / "presets" / "ndx100.json"),
}
_HASH = re.compile(r"^[0-9a-f]{64}$")
_REF = re.compile(r"^artifact://sha256/([0-9a-f]{64})$")


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default) + "\n").encode()


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    raise TypeError(f"cannot serialize {type(value).__name__}")


def scope_hash_for(index_id: str, sources: list[dict[str, str]]) -> str:
    payload = {
        "indexId": index_id,
        "sources": sorted(
            ({"ref": item["ref"], "sha256": item["sha256"]} for item in sources),
            key=lambda item: item["ref"],
        ),
    }
    return f"sha256:{hashlib.sha256(_canonical(payload)).hexdigest()}"


def decision_payload_hash(payload: dict[str, Any]) -> str:
    decision = {
        key: payload.get(key)
        for key in ("version", "indexId", "scopeHash", "sourceEvidence", "identityEvents", "membershipEvents")
    }
    return f"sha256:{hashlib.sha256(_canonical(decision)).hexdigest()}"


def _record_preset(store: SourceEvidenceStore, path: Path, retrieved_at: datetime) -> tuple[SourceEvidence, set[str]]:
    raw = path.read_bytes()
    artifact = store.persist_raw(raw)
    payload = json.loads(raw)
    tickers = payload.get("tickers")
    if (
        not isinstance(tickers, list)
        or not tickers
        or any(not isinstance(item, str) or not item.strip() for item in tickers)
    ):
        raise ValueError(f"{path}: preset must contain a non-empty string ticker list")
    evidence = SourceEvidence(
        ref=artifact.ref,
        sha256=artifact.sha256,
        source_url=path.resolve().as_uri(),
        retrieved_at=retrieved_at,
        publication_time=None,
        mediawiki_revision_id=None,
        mediawiki_revision_time=None,
        content_type="application/json",
    )
    store.record(evidence)
    return evidence, {item.strip() for item in tickers}


def scan_index(
    index_id: str,
    *,
    store: SourceEvidenceStore,
    wiki: MediaWikiClient,
    preset_path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if index_id not in INDEXES:
        raise ValueError(f"unsupported index: {index_id}")
    retrieved_at = now or datetime.now(UTC)
    if retrieved_at.tzinfo is None:
        raise ValueError("scan clock must be timezone-aware")
    retrieved_at = retrieved_at.astimezone(UTC)
    title, default_preset = INDEXES[index_id]
    before = (preset_path or default_preset).read_bytes()
    snapshot = wiki.snapshot(title)
    if not _verify_ref(store, snapshot.evidence.ref, snapshot.evidence.sha256):
        raise ValueError("Wikipedia snapshot is not present in the scan evidence store")
    preset_evidence, preset = _record_preset(store, preset_path or default_preset, retrieved_at)
    if (preset_path or default_preset).read_bytes() != before:
        raise RuntimeError("scan changed the preset bytes")
    try:
        wikipedia_content = store.read(snapshot.evidence.ref).decode("utf-8")
        wikipedia = parse_constituent_table(wikipedia_content, index_id)
        wikipedia_state = "parsed"
        wikipedia_error = None
    except (UnicodeDecodeError, UniverseFetchError) as exc:
        wikipedia = set()
        wikipedia_state = "unparseable"
        wikipedia_error = str(exc)
    sources = [
        {"ref": snapshot.evidence.ref, "sha256": snapshot.evidence.sha256},
        {"ref": preset_evidence.ref, "sha256": preset_evidence.sha256},
    ]
    scope_hash = scope_hash_for(index_id, sources)
    claims = []
    for symbol in sorted(wikipedia | preset):
        positions = [name for name, members in (("wikipedia", wikipedia), ("preset", preset)) if symbol in members]
        evidence_refs = []
        if symbol in wikipedia:
            evidence_refs.append(sources[0]["ref"])
        if symbol in preset:
            evidence_refs.append(sources[1]["ref"])
        claims.append(
            {
                "claimKey": f"membership.{index_id}.{symbol}",
                "symbol": symbol,
                "kind": "current-membership-candidate",
                "sourcePositions": positions,
                "evidenceRefs": evidence_refs,
                "disposition": "agreed-candidate" if len(positions) == 2 else "source-conflict",
            }
        )
    conflicts = [claim["symbol"] for claim in claims if claim["disposition"] == "source-conflict"]
    result: dict[str, Any] = {
        "version": 1,
        "operation": "shepherd-universe-scan",
        "indexId": index_id,
        "scopeHash": scope_hash,
        "retrievedAt": retrieved_at,
        "sources": [
            {
                **sources[0],
                "kind": "wikipedia-revision",
                "sourceUrl": snapshot.evidence.source_url,
                "revisionId": snapshot.revision_id,
                "revisionTime": snapshot.revision_time,
                "state": wikipedia_state,
                "error": wikipedia_error,
            },
            {
                **sources[1],
                "kind": "livewire-preset",
                "sourceUrl": preset_evidence.source_url,
            },
        ],
        "counts": {"wikipedia": len(wikipedia), "preset": len(preset), "conflicts": len(conflicts)},
        "claims": claims,
        "teamCase": None
        if not conflicts
        else {
            "variant": "source-conflict",
            "scopeHash": scope_hash,
            "symbols": conflicts,
            "inputEvidence": sources,
            "sourceStates": {"wikipedia": wikipedia_state, "preset": "parsed"},
        },
        "mutated": False,
    }
    artifact = store.persist_raw(_canonical(result))
    result["scanArtifact"] = {"ref": artifact.ref, "sha256": artifact.sha256}
    return result


def _parse_time(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _verify_ref(store: SourceEvidenceStore, ref: str, digest: str) -> bool:
    if _HASH.fullmatch(digest) is None or _REF.fullmatch(ref) is None:
        return False
    try:
        return hashlib.sha256(store.read(ref)).hexdigest() == digest
    except (OSError, ValueError):
        return False


def _validate_source_kind(kind: object, evidence: SourceEvidence) -> None:
    if kind == "wikipedia-revision":
        parsed_url = urlparse(evidence.source_url)
        wikipedia_host = parsed_url.hostname == "wikipedia.org" or bool(
            parsed_url.hostname and parsed_url.hostname.endswith(".wikipedia.org")
        )
        if (
            evidence.mediawiki_revision_id is None
            or evidence.mediawiki_revision_time is None
            or evidence.mediawiki_revision_time > evidence.retrieved_at
            or evidence.publication_time != evidence.mediawiki_revision_time
            or parsed_url.scheme != "https"
            or not wikipedia_host
            or evidence.content_type not in {"text/html", "application/xhtml+xml"}
        ):
            raise ValueError("source declared as Wikipedia lacks revision-bound metadata")
        return
    if kind == "livewire-preset":
        if not evidence.source_url.startswith("file://") or evidence.content_type != "application/json":
            raise ValueError("source declared as preset is not a local JSON artifact")
        return
    raise ValueError("unsupported source evidence kind")


@contextmanager
def _import_lock(root: Path) -> Iterator[None]:
    lock_path = root / "shepherd" / "universe-import.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(lock_path.parent, 0o700)
    with lock_path.open("a", encoding="utf-8") as handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _event_sources(row: dict[str, Any], allowed: set[tuple[str, str]]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    sources = row.pop("sources", None)
    if not isinstance(sources, list) or not sources:
        raise ValueError("event sources must be non-empty")
    pairs: list[tuple[str, str]] = []
    for source in sources:
        if not isinstance(source, dict) or set(source) != {"ref", "sha256"}:
            raise ValueError("event source must contain only ref and sha256")
        pair = (source["ref"], source["sha256"])
        if pair not in allowed:
            raise ValueError("event cites source outside the verified manifest scope")
        pairs.append(pair)
    return tuple(ref for ref, _ in pairs), tuple(digest for _, digest in pairs)


def _identity_event(row: object, allowed: set[tuple[str, str]], accepted: set[str]) -> SecurityIdentityEvent:
    if not isinstance(row, dict):
        raise ValueError("identity event must be an object")
    value = dict(row)
    claim_key = value.pop("claimKey", None)
    if claim_key not in accepted:
        raise ValueError("identity event is not bound to an accepted claim")
    refs, hashes = _event_sources(value, allowed)
    for field in ("effective_from", "effective_to", "known_at"):
        if value.get(field) is not None:
            value[field] = _parse_time(value[field], field)
    value["source_refs"] = refs
    value["source_hashes"] = hashes
    return SecurityIdentityEvent(**value)


def _membership_event(row: object, allowed: set[tuple[str, str]], accepted: set[str]) -> MembershipEvent:
    if not isinstance(row, dict):
        raise ValueError("membership event must be an object")
    value = dict(row)
    claim_key = value.pop("claimKey", None)
    if claim_key not in accepted:
        raise ValueError("membership event is not bound to an accepted claim")
    refs, hashes = _event_sources(value, allowed)
    for field in ("announced_at", "effective_at", "known_at"):
        if value.get(field) is not None:
            value[field] = _parse_time(value[field], field)
    value["source_refs"] = refs
    value["source_hashes"] = hashes
    return MembershipEvent(**value)


def _evidence_verifier(store: SourceEvidenceStore):
    def verify(ref: str, digest: str) -> bool:
        return _verify_ref(store, ref, digest)

    return verify


def _preflight_events(
    root: Path,
    index_id: str,
    identities: list[SecurityIdentityEvent],
    memberships: list[MembershipEvent],
    store: SourceEvidenceStore,
) -> None:
    """Validate the complete append set against a disposable copy first."""

    with tempfile.TemporaryDirectory(prefix="livewire-shepherd-preflight-") as temporary:
        staged_root = Path(temporary)
        for relative in (
            Path("security_master/events.parquet"),
            Path("index_membership") / index_id / "events.parquet",
        ):
            source = root / relative
            if source.exists():
                target = staged_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        verifier = _evidence_verifier(store)
        staged_security = SecurityMaster(staged_root, evidence_verifier=verifier)
        staged_memberships = IndexMembershipStore(
            staged_root,
            security_master=staged_security,
            evidence_verifier=verifier,
        )
        for event in identities:
            staged_security.append(event)
        for event in memberships:
            staged_memberships.append(event)


def import_decision(path: Path, *, data_lake_root: Path, now: datetime | None = None) -> dict[str, Any]:
    if not path.is_absolute():
        raise ValueError("decision manifest path must be absolute")
    payload = json.loads(path.read_bytes())
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError("unsupported decision manifest")
    index_id = payload.get("indexId")
    if index_id not in INDEXES:
        raise ValueError("invalid decision index")
    source_rows = payload.get("sourceEvidence")
    if not isinstance(source_rows, list) or len(source_rows) < 2:
        raise ValueError("decision requires both index seed sources")
    store = SourceEvidenceStore(data_lake_root)
    known_sources = {(item.ref, item.sha256): item for item in store.list_verified()}
    allowed: set[tuple[str, str]] = set()
    source_kinds: set[str] = set()
    for item in source_rows:
        if not isinstance(item, dict) or set(item) != {"ref", "sha256", "kind"}:
            raise ValueError("invalid source evidence entry")
        pair = (item["ref"], item["sha256"])
        if pair not in known_sources or not _verify_ref(store, *pair):
            raise ValueError("decision source evidence is missing or corrupt")
        _validate_source_kind(item["kind"], known_sources[pair])
        if pair in allowed:
            raise ValueError("decision source evidence must be unique")
        allowed.add(pair)
        source_kinds.add(item["kind"])
    if not {"wikipedia-revision", "livewire-preset"}.issubset(source_kinds):
        raise ValueError("decision must bind both Wikipedia revision and preset bytes")
    sources_for_hash = [{"ref": ref, "sha256": digest} for ref, digest in allowed]
    if payload.get("scopeHash") != scope_hash_for(index_id, sources_for_hash):
        raise ValueError("decision scope hash mismatch")

    verifier = payload.get("verifier")
    if not isinstance(verifier, dict) or verifier.get("decision") != "pass" or not verifier.get("identity"):
        raise ValueError("decision requires an identified passing verifier")
    decided_at = _parse_time(verifier.get("decidedAt"), "verifier.decidedAt")
    current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None:
        raise ValueError("import clock must be timezone-aware")
    if decided_at > current_time.astimezone(UTC):
        raise ValueError("verifier decision cannot be in the future")
    verifier_ref, verifier_hash = verifier.get("evidenceRef"), verifier.get("evidenceHash")
    if (
        not isinstance(verifier_ref, str)
        or not isinstance(verifier_hash, str)
        or not _verify_ref(store, verifier_ref, verifier_hash)
    ):
        raise ValueError("verifier evidence is missing or corrupt")
    accepted_values = verifier.get("acceptedClaimKeys")
    if (
        not isinstance(accepted_values, list)
        or not accepted_values
        or any(not isinstance(key, str) for key in accepted_values)
    ):
        raise ValueError("verifier accepted claim keys are required")
    accepted = set(accepted_values)
    if len(accepted) != len(accepted_values):
        raise ValueError("verifier accepted claim keys must be unique")
    try:
        verifier_artifact = json.loads(store.read(verifier_ref))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("verifier evidence must be a valid JSON decision receipt") from exc
    expected_verifier_artifact = {
        "version": 1,
        "identity": verifier["identity"],
        "decision": "pass",
        "decidedAt": verifier["decidedAt"],
        "scopeHash": payload["scopeHash"],
        "acceptedClaimKeys": accepted_values,
        "decisionPayloadHash": decision_payload_hash(payload),
    }
    if verifier_artifact != expected_verifier_artifact:
        raise ValueError("verifier evidence is not bound to this exact decision payload")

    identity_rows = payload.get("identityEvents")
    membership_rows = payload.get("membershipEvents")
    if not isinstance(identity_rows, list) or not isinstance(membership_rows, list):
        raise ValueError("decision event collections must be lists")
    identities = [_identity_event(row, allowed, accepted) for row in identity_rows]
    memberships = [_membership_event(row, allowed, accepted) for row in membership_rows]
    if not identities and not memberships:
        raise ValueError("decision contains no events")
    retrieved_by_pair = {pair: item.retrieved_at for pair, item in known_sources.items()}
    for event in [*identities, *memberships]:
        if event.known_at > decided_at:
            raise ValueError("event known_at exceeds verifier decision time")
        if any(
            retrieved_by_pair[pair] > event.known_at
            for pair in zip(event.source_refs, event.source_hashes, strict=True)
        ):
            raise ValueError("event known_at precedes source retrieval")

    if any(event.index_id != index_id for event in memberships):
        raise ValueError("membership event index does not match decision scope")

    root = Path(data_lake_root).expanduser()
    verifier_fn = _evidence_verifier(store)
    with _import_lock(root):
        _preflight_events(root, index_id, identities, memberships, store)
        security = SecurityMaster(root, evidence_verifier=verifier_fn)
        membership_store = IndexMembershipStore(
            root,
            security_master=security,
            evidence_verifier=verifier_fn,
        )
        identity_appends = sum(security.append(event) for event in identities)
        membership_appends = sum(membership_store.append(event) for event in memberships)
    return {
        "version": 1,
        "operation": "shepherd-universe-import-decision",
        "indexId": index_id,
        "scopeHash": payload["scopeHash"],
        "identityEventsAppended": identity_appends,
        "membershipEventsAppended": membership_appends,
        "membershipRevision": len(membership_store.events(index_id)),
        "verifierEvidence": {"ref": verifier_ref, "sha256": verifier_hash},
    }


def verify_revision(
    index_id: str,
    revision: int,
    *,
    data_lake_root: Path,
    effective_at: datetime | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    store = SourceEvidenceStore(data_lake_root)
    verifier_fn = _evidence_verifier(store)
    security = SecurityMaster(data_lake_root, evidence_verifier=verifier_fn)
    memberships = IndexMembershipStore(data_lake_root, security_master=security, evidence_verifier=verifier_fn)
    events = memberships.events(index_id)
    if revision < 1 or revision > len(events):
        raise ValueError(f"revision must be between 1 and {len(events)}")
    clock = datetime.now(UTC)
    when = effective_at or clock
    known = as_of or clock
    if when.tzinfo is None or known.tzinfo is None:
        raise ValueError("verification times must be timezone-aware")
    when = when.astimezone(UTC)
    known = known.astimezone(UTC)
    prefix = events[:revision]
    visible = [event for event in prefix if event.known_at <= known]
    superseded = {event.supersedes for event in visible if event.supersedes is not None}
    applicable = sorted(
        (
            event
            for event in visible
            if event.event_id not in superseded and event.status == "verified" and event.effective_at <= when
        ),
        key=lambda event: (event.effective_at, event.known_at, event.revision, event.event_id),
    )
    members: set[str] = set()
    for event in applicable:
        if event.action == "add":
            members.add(event.security_id)
        else:
            members.discard(event.security_id)
    members = {security_id for security_id in members if security.is_verified(security_id, when, known)}
    identity_events = security.events(as_of=known)
    identity_superseded = {event.supersedes for event in identity_events if event.supersedes is not None}
    active_identity = {}
    active_identity_events: dict[str, SecurityIdentityEvent] = {}
    for event in identity_events:
        if (
            event.security_id in members
            and event.status == "verified"
            and event.effective_from <= when
            and (event.effective_to is None or when < event.effective_to)
            and event.event_id not in identity_superseded
        ):
            if event.security_id in active_identity:
                raise ValueError("membership revision has multiple active identities for one security")
            active_identity[event.security_id] = event.symbol
            active_identity_events[event.security_id] = event
    if set(active_identity) != members:
        raise ValueError("membership revision lacks an active verified identity")
    evidence_events = [*prefix, *active_identity_events.values()]
    evidence = sorted(
        {
            (ref, digest)
            for event in evidence_events
            for ref, digest in zip(event.source_refs, event.source_hashes, strict=True)
        }
    )
    for ref, digest in evidence:
        if not _verify_ref(store, ref, digest):
            raise ValueError("membership revision contains missing or corrupt evidence")
    receipt = {
        "version": 1,
        "operation": "shepherd-universe-verify",
        "indexId": index_id,
        "membershipRevision": revision,
        "revisionSemantics": "append-order-prefix",
        "effectiveAt": when,
        "asOf": known,
        "members": [
            {"securityId": security_id, "symbol": active_identity[security_id]} for security_id in sorted(members)
        ],
        "evidence": [{"ref": ref, "sha256": digest} for ref, digest in evidence],
        "outcome": "completed",
        "stateHint": "VERIFIED",
        "changedPaths": [],
    }
    receipt["receiptHash"] = f"sha256:{hashlib.sha256(_canonical(receipt)).hexdigest()}"
    return receipt


def scan_receipt(result: dict[str, Any]) -> dict[str, Any]:
    """Return the bounded CLI receipt; full claims remain in scanArtifact."""

    if result.get("operation") != "shepherd-universe-scan" or "scanArtifact" not in result:
        raise ValueError("scan result is incomplete")
    return {
        key: result[key]
        for key in (
            "version",
            "operation",
            "indexId",
            "scopeHash",
            "retrievedAt",
            "sources",
            "counts",
            "teamCase",
            "mutated",
            "scanArtifact",
        )
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-root", type=Path, default=data_lake_dir())
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("scan")
    scan.add_argument("--index", choices=sorted(INDEXES), required=True)
    scan.add_argument("--preset", type=Path)
    decision = sub.add_parser("import-decision")
    decision.add_argument("--manifest", type=Path, required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--index", choices=sorted(INDEXES), required=True)
    verify.add_argument("--revision", type=int, required=True)
    verify.add_argument("--effective-at")
    verify.add_argument("--as-of")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.data_lake_root.expanduser()
    if args.command == "scan":
        store = SourceEvidenceStore(root)
        result = scan_index(
            args.index,
            store=store,
            wiki=MediaWikiClient(store),
            preset_path=args.preset,
        )
        result = scan_receipt(result)
    elif args.command == "import-decision":
        result = import_decision(args.manifest, data_lake_root=root)
    else:
        result = verify_revision(
            args.index,
            args.revision,
            data_lake_root=root,
            effective_at=None if args.effective_at is None else _parse_time(args.effective_at, "effective-at"),
            as_of=None if args.as_of is None else _parse_time(args.as_of, "as-of"),
        )
    print(json.dumps(result, sort_keys=True, default=_json_default))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
