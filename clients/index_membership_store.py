"""Append-only point-in-time index membership events."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa

from clients.revisioned_parquet import AtomicParquetLog
from clients.security_master import SecurityMaster

MembershipAction = Literal["add", "remove"]
MembershipStatus = Literal["candidate", "verified", "rejected", "unresolved"]
EvidenceVerifier = Callable[[str, str], bool]

_INDEX_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")

_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("index_id", pa.string(), nullable=False),
        pa.field("security_id", pa.string(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("announced_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("effective_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("known_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source_refs", pa.list_(pa.string()), nullable=False),
        pa.field("source_hashes", pa.list_(pa.string()), nullable=False),
        pa.field("revision", pa.int64(), nullable=False),
        pa.field("supersedes", pa.string(), nullable=True),
        pa.field("status", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class MembershipEvent:
    event_id: str
    index_id: str
    security_id: str
    action: MembershipAction
    announced_at: datetime | None
    effective_at: datetime
    known_at: datetime
    source_refs: tuple[str, ...]
    source_hashes: tuple[str, ...]
    revision: int
    supersedes: str | None
    status: MembershipStatus


class IndexMembershipStore:
    """Persist and reconstruct evidence-backed index membership as-of time."""

    def __init__(
        self,
        data_lake_root: str | Path,
        *,
        security_master: SecurityMaster,
        evidence_verifier: EvidenceVerifier | None,
    ) -> None:
        self.root = Path(data_lake_root).expanduser() / "index_membership"
        self.security_master = security_master
        self._evidence_verifier = evidence_verifier

    def append(self, item: MembershipEvent) -> bool:
        _validate_event(item, self._evidence_verifier)
        log = self._log(item.index_id)
        row = _serialize(item)
        return log.append(row, key="event_id", validate=self._validate_append)

    def events(self, index_id: str, *, as_of: datetime | None = None) -> list[MembershipEvent]:
        items = [_deserialize(row) for row in self._log(index_id).read()]
        if as_of is not None:
            _require_aware(as_of, "as_of")
            items = [item for item in items if item.known_at <= as_of]
        return items

    def members_effective_at(self, index_id: str, effective_at: datetime, as_of: datetime) -> set[str]:
        _require_aware(effective_at, "effective_at")
        _require_aware(as_of, "as_of")
        items = self.events(index_id, as_of=as_of)
        superseded = {item.supersedes for item in items if item.supersedes is not None}
        applicable = [
            item
            for item in items
            if item.event_id not in superseded and item.status == "verified" and item.effective_at <= effective_at
        ]
        applicable.sort(key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id))
        members: set[str] = set()
        for item in applicable:
            if item.action == "add":
                members.add(item.security_id)
            else:
                members.discard(item.security_id)
        return {
            security_id for security_id in members if self.security_master.is_verified(security_id, effective_at, as_of)
        }

    def _log(self, index_id: str) -> AtomicParquetLog:
        if _INDEX_ID.fullmatch(index_id) is None:
            raise ValueError("invalid index_id")
        return AtomicParquetLog(self.root / index_id / "events.parquet", _SCHEMA)

    def _validate_append(self, rows: list[dict[str, object]], row: dict[str, object]) -> None:
        item = _deserialize(row)
        _validate_event(item, self._evidence_verifier)
        existing = [_deserialize(entry) for entry in rows]
        own = [entry for entry in existing if entry.security_id == item.security_id]
        expected_revision = max((entry.revision for entry in own), default=0) + 1
        if item.revision != expected_revision:
            raise ValueError(f"membership revision must be {expected_revision}")
        if item.supersedes is not None:
            prior = next((entry for entry in existing if entry.event_id == item.supersedes), None)
            if (
                prior is None
                or prior.security_id != item.security_id
                or prior.index_id != item.index_id
                or prior.revision >= item.revision
                or prior.known_at > item.known_at
            ):
                raise ValueError("supersedes must name an earlier event for the same index security")
        if item.status == "verified":
            if item.action == "add":
                identity_is_verified = self.security_master.is_verified(
                    item.security_id, item.effective_at, item.known_at
                )
            else:
                identity_is_verified = self.security_master.has_verified_identity(item.security_id, item.known_at)
            if not identity_is_verified:
                raise ValueError("verified membership requires a verified security identity")


def _validate_event(item: MembershipEvent, verifier: EvidenceVerifier | None) -> None:
    if not item.event_id or _INDEX_ID.fullmatch(item.index_id) is None or not item.security_id:
        raise ValueError("invalid membership identity")
    if item.action not in {"add", "remove"}:
        raise ValueError("invalid membership action")
    if item.status not in {"candidate", "verified", "rejected", "unresolved"}:
        raise ValueError("invalid membership status")
    if item.revision < 1:
        raise ValueError("membership revision must be positive")
    _require_aware(item.effective_at, "effective_at")
    _require_aware(item.known_at, "known_at")
    if item.announced_at is not None:
        _require_aware(item.announced_at, "announced_at")
        if item.announced_at > item.known_at:
            raise ValueError("announced_at cannot be after known_at")
    if len(item.source_refs) == 0 or len(item.source_refs) != len(item.source_hashes):
        raise ValueError("source refs and hashes must be non-empty and aligned")
    if verifier is None or any(
        _SHA256.fullmatch(digest) is None or not verifier(ref, digest)
        for ref, digest in zip(item.source_refs, item.source_hashes, strict=True)
    ):
        raise ValueError("evidence verification failed")


def _serialize(item: MembershipEvent) -> dict[str, object]:
    row = asdict(item)
    row["announced_at"] = None if item.announced_at is None else item.announced_at.astimezone(UTC)
    row["effective_at"] = item.effective_at.astimezone(UTC)
    row["known_at"] = item.known_at.astimezone(UTC)
    row["source_refs"] = list(item.source_refs)
    row["source_hashes"] = list(item.source_hashes)
    return row


def _deserialize(row: dict[str, object]) -> MembershipEvent:
    return MembershipEvent(
        **{
            **row,
            "source_refs": tuple(row["source_refs"]),
            "source_hashes": tuple(row["source_hashes"]),
        }
    )


def _require_aware(value: datetime, label: str) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
