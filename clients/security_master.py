"""Append-only stable-security identity intervals for Livewire Shepherd."""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa

from clients.revisioned_parquet import AtomicParquetLog
from clients.timeutils import require_aware

IdentityStatus = Literal["candidate", "verified", "rejected", "unresolved"]
EvidenceVerifier = Callable[[str, str], bool]

_SECURITY_ID = re.compile(r"^sec_[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CIK = re.compile(r"^[0-9]{10}$")
_FIGI = re.compile(r"^[A-Z0-9]{12}$")
_STRONG_BASES = {"responsible_publisher_action", "regulator_filing", "provider_figi"}

_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("security_id", pa.string(), nullable=False),
        pa.field("revision", pa.int64(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("provider", pa.string(), nullable=False),
        pa.field("exchange_mic", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("effective_from", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("effective_to", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("known_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("issuer_name", pa.string(), nullable=False),
        pa.field("cik", pa.string(), nullable=True),
        pa.field("composite_figi", pa.string(), nullable=True),
        pa.field("share_class_figi", pa.string(), nullable=True),
        pa.field("continuity_basis", pa.string(), nullable=False),
        pa.field("relationship_type", pa.string(), nullable=True),
        pa.field("related_security_id", pa.string(), nullable=True),
        pa.field("source_refs", pa.list_(pa.string()), nullable=False),
        pa.field("source_hashes", pa.list_(pa.string()), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("supersedes", pa.string(), nullable=True),
    ]
)


@dataclass(frozen=True)
class SecurityIdentityEvent:
    event_id: str
    security_id: str
    revision: int
    symbol: str
    provider: str
    exchange_mic: str
    currency: str
    effective_from: datetime
    effective_to: datetime | None
    known_at: datetime
    issuer_name: str
    cik: str | None
    composite_figi: str | None
    share_class_figi: str | None
    continuity_basis: str
    relationship_type: str | None
    related_security_id: str | None
    source_refs: tuple[str, ...]
    source_hashes: tuple[str, ...]
    status: IdentityStatus
    supersedes: str | None


class SecurityMaster:
    """Store and resolve evidence-backed security identity intervals."""

    def __init__(self, data_lake_root: str | Path, *, evidence_verifier: EvidenceVerifier | None) -> None:
        self.path = Path(data_lake_root).expanduser() / "security_master" / "events.parquet"
        self._log = AtomicParquetLog(self.path, _SCHEMA)
        self._evidence_verifier = evidence_verifier

    @staticmethod
    def new_security_id() -> str:
        return f"sec_{uuid.uuid4().hex}"

    def append(self, item: SecurityIdentityEvent) -> bool:
        _validate_event(item, self._evidence_verifier)
        row = _serialize(item)
        return self._log.append(row, key="event_id", validate=self._validate_append)

    def events(self, *, as_of: datetime | None = None) -> list[SecurityIdentityEvent]:
        rows = self._log.read()
        items = [_deserialize(row) for row in rows]
        if as_of is not None:
            require_aware(as_of, "as_of")
            items = [item for item in items if item.known_at <= as_of]
        return items

    def resolve_symbol(
        self,
        provider: str,
        symbol: str,
        exchange_mic: str,
        effective_at: datetime,
        as_of: datetime,
    ) -> str | None:
        require_aware(effective_at, "effective_at")
        require_aware(as_of, "as_of")
        matches = {
            item.security_id
            for item in self._active_events(as_of)
            if item.status == "verified"
            and item.provider == provider
            and item.symbol == symbol
            and item.exchange_mic == exchange_mic
            and _contains(item.effective_from, item.effective_to, effective_at)
        }
        if len(matches) > 1:
            raise ValueError("verified symbol resolves to multiple security identities")
        return next(iter(matches), None)

    def is_verified(self, security_id: str, effective_at: datetime, as_of: datetime) -> bool:
        require_aware(effective_at, "effective_at")
        require_aware(as_of, "as_of")
        return any(
            item.security_id == security_id
            and item.status == "verified"
            and _contains(item.effective_from, item.effective_to, effective_at)
            for item in self._active_events(as_of)
        )

    def has_verified_identity(self, security_id: str, as_of: datetime) -> bool:
        """Return whether this opaque identity has any verified claim known then."""

        require_aware(as_of, "as_of")
        return any(item.security_id == security_id and item.status == "verified" for item in self._active_events(as_of))

    def _active_events(self, as_of: datetime) -> list[SecurityIdentityEvent]:
        items = self.events(as_of=as_of)
        superseded = {item.supersedes for item in items if item.supersedes is not None}
        return [item for item in items if item.event_id not in superseded]

    def _validate_append(self, rows: list[dict[str, object]], row: dict[str, object]) -> None:
        item = _deserialize(row)
        _validate_event(item, self._evidence_verifier)
        existing = [_deserialize(entry) for entry in rows]
        own = [entry for entry in existing if entry.security_id == item.security_id]
        expected_revision = max((entry.revision for entry in own), default=0) + 1
        if item.revision != expected_revision:
            raise ValueError(f"security revision must be {expected_revision}")
        if item.supersedes is not None:
            prior = next((entry for entry in existing if entry.event_id == item.supersedes), None)
            if (
                prior is None
                or prior.security_id != item.security_id
                or prior.revision >= item.revision
                or prior.known_at > item.known_at
            ):
                raise ValueError("supersedes must name an earlier event for the same security")
        if item.related_security_id is not None and not any(
            entry.security_id == item.related_security_id for entry in existing
        ):
            raise ValueError("related_security_id is unknown")
        if item.status != "verified":
            return
        visible = [entry for entry in existing if entry.known_at <= item.known_at]
        superseded = {entry.supersedes for entry in visible if entry.supersedes is not None}
        for prior in (entry for entry in visible if entry.event_id not in superseded):
            if prior.status != "verified" or prior.security_id == item.security_id:
                continue
            if not _overlaps(item.effective_from, item.effective_to, prior.effective_from, prior.effective_to):
                continue
            if (item.provider, item.symbol, item.exchange_mic) == (
                prior.provider,
                prior.symbol,
                prior.exchange_mic,
            ):
                raise ValueError("symbol interval collision")
            for field in ("share_class_figi", "composite_figi"):
                value = getattr(item, field)
                if value is not None and value == getattr(prior, field):
                    raise ValueError(f"{field} collision")


def _validate_event(item: SecurityIdentityEvent, verifier: EvidenceVerifier | None) -> None:
    if not item.event_id or _SECURITY_ID.fullmatch(item.security_id) is None:
        raise ValueError("invalid event_id or security_id")
    if item.revision < 1 or not all(
        value for value in (item.symbol, item.provider, item.exchange_mic, item.currency, item.issuer_name)
    ):
        raise ValueError("identity interval fields must be non-empty")
    require_aware(item.effective_from, "effective_from")
    require_aware(item.known_at, "known_at")
    if item.effective_to is not None:
        require_aware(item.effective_to, "effective_to")
        if item.effective_to <= item.effective_from:
            raise ValueError("effective_to must be after effective_from")
    if item.status not in {"candidate", "verified", "rejected", "unresolved"}:
        raise ValueError("invalid identity status")
    if item.cik is not None and _CIK.fullmatch(item.cik) is None:
        raise ValueError("CIK must be a zero-padded ten-digit identifier")
    for label, value in (
        ("composite FIGI", item.composite_figi),
        ("share-class FIGI", item.share_class_figi),
    ):
        if value is not None and _FIGI.fullmatch(value) is None:
            raise ValueError(f"{label} must be a twelve-character identifier")
    if (item.relationship_type is None) != (item.related_security_id is None):
        raise ValueError("relationship_type and related_security_id must be supplied together")
    if item.related_security_id == item.security_id:
        raise ValueError("a security cannot relate to itself")
    if len(item.source_refs) == 0 or len(item.source_refs) != len(item.source_hashes):
        raise ValueError("source refs and hashes must be non-empty and aligned")
    if verifier is None or any(
        _SHA256.fullmatch(digest) is None or not verifier(ref, digest)
        for ref, digest in zip(item.source_refs, item.source_hashes, strict=True)
    ):
        raise ValueError("evidence verification failed")
    strong_identifier = item.share_class_figi is not None or item.composite_figi is not None
    if item.status == "verified" and not strong_identifier and item.continuity_basis not in _STRONG_BASES:
        raise ValueError("verified identity requires strong continuity evidence")


def _serialize(item: SecurityIdentityEvent) -> dict[str, object]:
    row = asdict(item)
    row["effective_from"] = item.effective_from.astimezone(UTC)
    row["effective_to"] = None if item.effective_to is None else item.effective_to.astimezone(UTC)
    row["known_at"] = item.known_at.astimezone(UTC)
    row["source_refs"] = list(item.source_refs)
    row["source_hashes"] = list(item.source_hashes)
    return row


def _deserialize(row: dict[str, object]) -> SecurityIdentityEvent:
    return SecurityIdentityEvent(
        **{
            **row,
            "source_refs": tuple(row["source_refs"]),
            "source_hashes": tuple(row["source_hashes"]),
        }
    )


def _contains(start: datetime, end: datetime | None, value: datetime) -> bool:
    return start <= value and (end is None or value < end)


def _overlaps(
    left_start: datetime,
    left_end: datetime | None,
    right_start: datetime,
    right_end: datetime | None,
) -> bool:
    return (right_end is None or left_start < right_end) and (left_end is None or right_start < left_end)
