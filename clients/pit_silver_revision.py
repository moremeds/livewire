"""Point-in-time lineage manifests over existing canonical Silver artifacts."""

from __future__ import annotations

import fcntl
import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from clients.index_membership_store import IndexMembershipStore
from clients.parquet_io import fsync_directory
from clients.security_master import SecurityMaster
from clients.source_evidence import SourceEvidenceStore, canonical_bytes, digest_bytes, jsonable
from clients.trading_calendar import (
    XNYS_SESSION_POLICY,
    is_trading_day,
    previous_trading_day,
    session_close_time,
)


def daily_bar_cutoff(as_of: datetime) -> date:
    """Return the latest conservatively closed US equity daily session."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as-of must be timezone-aware")
    local = as_of.astimezone(ZoneInfo("America/New_York"))
    if not is_trading_day(local.date()):
        return previous_trading_day(local.date() + timedelta(days=1))
    if local.time() < session_close_time(local.date()):
        return previous_trading_day(local.date())
    return local.date()


def _membership_session_date(value: datetime) -> date:
    """Map an effective instant to the first XNYS daily session it governs."""
    local = value.astimezone(ZoneInfo("America/New_York"))
    candidate = local.date()
    if local.time() >= session_close_time(candidate):
        candidate += timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


@dataclass(frozen=True)
class PitSilverRevision:
    revision: int
    status: Literal["PROVEN", "PARTIAL"]
    input_hash: str
    manifest_path: Path
    changed_paths: tuple[Path, ...]


class PitSilverRevisionPublisher:
    def __init__(self, data_lake_root: Path):
        self.root = Path(data_lake_root)
        self.silver_root = self.root / "silver"
        self.revisions = self.silver_root / "pit-revisions"
        self.current = self.revisions / "current.json"
        self.lock = self.silver_root / ".pit-revision.lock"

    def publish(
        self,
        *,
        index_id: str,
        membership_revision: int,
        as_of: datetime,
        actions_receipt: dict[str, Any],
    ) -> PitSilverRevision:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as-of must be timezone-aware")
        core = self._build_core(index_id, membership_revision, as_of.astimezone(UTC), actions_receipt)
        input_hash = f"sha256:{digest_bytes(canonical_bytes(core))}"
        self.silver_root.mkdir(parents=True, exist_ok=True)
        with self.lock.open("a", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                recovered_paths = self._recover_orphans()
                existing = self._read_current()
                if existing is not None and existing.get("input_hash") == input_hash:
                    self.verify()
                    return self._result(existing, recovered_paths)
                revision = 1 if existing is None else int(existing["revision"]) + 1
                published_at = datetime.now(UTC)
                status: Literal["PROVEN", "PARTIAL"] = (
                    "PROVEN" if all(item["state"] == "VERIFIED" for item in actions_receipt["symbols"]) else "PARTIAL"
                )
                actions_payload = canonical_bytes(actions_receipt)
                actions_hash = digest_bytes(actions_payload)
                evidence_dir = self.revisions / "evidence"
                evidence_path = evidence_dir / f"actions-{actions_hash}.json"
                evidence_existed = evidence_path.exists()
                manifest = {
                    "schema_version": 1,
                    "revision": revision,
                    "generation_id": f"{published_at.strftime('%Y%m%dT%H%M%SZ')}-{revision}",
                    "published_at": published_at.isoformat(),
                    "status": status,
                    "input_hash": input_hash,
                    **core,
                    "inputs": {
                        **core["inputs"],
                        "corporate_action_receipt": {
                            "path": str(evidence_path.relative_to(self.silver_root)),
                            "sha256": actions_hash,
                            "receipt_hash": actions_receipt["receiptHash"],
                        },
                    },
                }
                self.revisions.mkdir(parents=True, exist_ok=True)
                evidence_dir.mkdir(parents=True, exist_ok=True)
                self._write_immutable(evidence_path, actions_payload)
                immutable = self.revisions / f"revision={revision}.json"
                payload = canonical_bytes(manifest)
                try:
                    self._write_immutable(immutable, payload)
                    self._replace_current(payload)
                except Exception:
                    immutable.unlink(missing_ok=True)
                    if not any(
                        self._actions_ref(path) == actions_hash for path in self.revisions.glob("revision=*.json")
                    ):
                        evidence_path.unlink(missing_ok=True)
                    raise
                changed = [*recovered_paths, immutable, self.current]
                if not evidence_existed:
                    changed.insert(0, evidence_path)
                return PitSilverRevision(
                    revision,
                    status,
                    input_hash,
                    immutable,
                    tuple(dict.fromkeys(changed)),
                )
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def verify(self, manifest_path: Path | None = None) -> dict[str, Any]:
        path = self.current if manifest_path is None else Path(manifest_path)
        payload = json.loads(path.read_bytes())
        if path.name.startswith("revision=") and path.name.endswith(".json"):
            filename_revision = int(path.stem.split("=", 1)[1])
            if int(payload["revision"]) != filename_revision:
                raise ValueError("PIT Silver filename revision does not match payload")
        if manifest_path is None:
            revisions = [
                int(candidate.stem.split("=", 1)[1])
                for candidate in self.revisions.glob("revision=*.json")
                if candidate.stem.split("=", 1)[1].isdigit()
            ]
            if not revisions or int(payload["revision"]) != max(revisions):
                raise ValueError("PIT Silver current pointer is not the latest immutable revision")
        immutable = self.revisions / f"revision={int(payload['revision'])}.json"
        if not immutable.is_file() or immutable.read_bytes() != path.read_bytes():
            raise ValueError("PIT Silver pointer does not match immutable manifest")
        action_input = payload["inputs"]["corporate_action_receipt"]
        action_path = (self.silver_root / action_input["path"]).resolve()
        if not action_path.is_relative_to(self.silver_root.resolve()):
            raise ValueError("corporate-action receipt escapes Silver root")
        action_bytes = action_path.read_bytes()
        if digest_bytes(action_bytes) != action_input["sha256"]:
            raise ValueError("corporate-action receipt artifact hash mismatch")
        actions_receipt = json.loads(action_bytes)
        if action_input.get("receipt_hash") != actions_receipt.get("receiptHash"):
            raise ValueError("corporate-action receipt identity mismatch")
        core = self._build_core(
            str(payload["index_id"]),
            int(payload["membership_revision"]),
            datetime.fromisoformat(str(payload["as_of"])),
            actions_receipt,
            security_revision=int(payload["inputs"]["security_master"]["revision"]),
            silver_revision=int(payload["silver_revision"]),
        )
        expected = f"sha256:{digest_bytes(canonical_bytes(core))}"
        if payload["input_hash"] != expected:
            raise ValueError("PIT Silver input hash mismatch")
        if payload["members"] != core["members"]:
            raise ValueError("PIT Silver member scope mismatch")
        for key in (
            "policy_version",
            "as_of",
            "index_id",
            "membership_revision",
            "silver_revision",
            "corporate_actions_as_of",
            "daily_bar_cutoff",
            "session_policy",
        ):
            if payload.get(key) != core[key]:
                raise ValueError("PIT Silver lineage mismatch")
        manifest_core_inputs = dict(payload["inputs"])
        manifest_core_inputs.pop("corporate_action_receipt", None)
        if manifest_core_inputs != core["inputs"]:
            raise ValueError("PIT Silver input references mismatch")
        expected_status = (
            "PROVEN" if all(item["state"] == "VERIFIED" for item in actions_receipt["symbols"]) else "PARTIAL"
        )
        if payload.get("schema_version") != 1 or payload.get("status") != expected_status:
            raise ValueError("PIT Silver status mismatch")
        return {
            "version": 1,
            "operation": "shepherd-silver-verify",
            "revision": int(payload["revision"]),
            "silverRevision": int(payload["silver_revision"]),
            "status": payload["status"],
            "inputHash": expected,
            "manifestHash": f"sha256:{digest_bytes(path.read_bytes())}",
            "manifestPath": str(immutable),
            "changedPaths": [],
        }

    def _build_core(
        self,
        index_id: str,
        membership_revision: int,
        as_of: datetime,
        actions_receipt: dict[str, Any],
        *,
        security_revision: int | None = None,
        silver_revision: int | None = None,
    ) -> dict[str, Any]:
        receipt = dict(actions_receipt)
        claimed_receipt_hash = str(receipt.pop("receiptHash", ""))
        actual_receipt_hash = f"sha256:{digest_bytes(canonical_bytes(receipt))}"
        if claimed_receipt_hash != actual_receipt_hash:
            raise ValueError("corporate-action receipt hash mismatch")
        if actions_receipt.get("mutated") is not False:
            raise ValueError("corporate-action receipt must be read-only")
        receipt_as_of = datetime.fromisoformat(str(actions_receipt.get("asOf", "")))

        evidence = SourceEvidenceStore(self.root)

        def verify(ref: str, digest: str) -> bool:
            try:
                return digest_bytes(evidence.read(ref)) == digest
            except (OSError, ValueError):
                return False

        master = SecurityMaster(self.root, evidence_verifier=verify)
        membership = IndexMembershipStore(self.root, security_master=master, evidence_verifier=verify)
        events = membership.events(index_id)
        if membership_revision < 1 or membership_revision > len(events):
            raise ValueError("invalid membership revision")
        prefix = events[:membership_revision]
        for event in prefix:
            if any(not verify(ref, digest) for ref, digest in zip(event.source_refs, event.source_hashes, strict=True)):
                raise ValueError("membership evidence is missing or corrupt")
        known_prefix = [event for event in prefix if event.known_at <= as_of]
        superseded = {event.supersedes for event in known_prefix if event.supersedes is not None}
        visible = [event for event in known_prefix if event.event_id not in superseded]
        membership_spans: dict[str, list[tuple[datetime, datetime | None, str]]] = {}
        open_memberships: dict[str, tuple[datetime, str]] = {}
        for event in sorted(visible, key=lambda row: (row.effective_at, row.known_at, row.revision, row.event_id)):
            if event.status != "verified" or event.effective_at > as_of:
                continue
            if event.action == "add":
                if event.security_id in open_memberships:
                    raise ValueError("duplicate open membership interval")
                open_memberships[event.security_id] = (event.effective_at, event.event_id)
            else:
                opened = open_memberships.pop(event.security_id, None)
                if opened is None:
                    raise ValueError("membership removal has no open interval")
                membership_spans.setdefault(event.security_id, []).append((opened[0], event.effective_at, opened[1]))
        members = set(open_memberships)
        for security_id, (started_at, event_id) in open_memberships.items():
            membership_spans.setdefault(security_id, []).append((started_at, None, event_id))

        all_identity_events = master.events()
        if security_revision is None:
            security_revision = len(all_identity_events)
        if security_revision < 0 or security_revision > len(all_identity_events):
            raise ValueError("invalid security-master revision")
        identity_prefix = all_identity_events[:security_revision]
        identity_events = [event for event in identity_prefix if event.known_at <= as_of]
        identity_superseded = {event.supersedes for event in identity_events if event.supersedes is not None}
        identities = sorted(
            (
                event
                for event in identity_events
                if event.event_id not in identity_superseded
                and event.security_id in members
                and event.status == "verified"
                and event.effective_from <= as_of
            ),
            key=lambda row: (row.security_id, row.effective_from, row.revision),
        )
        for event in identities:
            if any(not verify(ref, digest) for ref, digest in zip(event.source_refs, event.source_hashes, strict=True)):
                raise ValueError("security identity evidence is missing or corrupt")
        member_scopes: list[dict[str, Any]] = []
        for identity in identities:
            for member_from, member_to, membership_event_id in membership_spans.get(identity.security_id, []):
                effective_from = max(identity.effective_from, member_from)
                ends = [value for value in (identity.effective_to, member_to) if value is not None]
                effective_to = min(ends) if ends else None
                if effective_to is not None and effective_from >= effective_to:
                    continue
                member_scopes.append(
                    {
                        "security_id": identity.security_id,
                        "identity_event_id": identity.event_id,
                        "membership_event_id": membership_event_id,
                        "symbol": identity.symbol,
                        "effective_from": effective_from.isoformat(),
                        "effective_to": None if effective_to is None else effective_to.isoformat(),
                        "session_from": _membership_session_date(effective_from).isoformat(),
                        "session_to": None
                        if effective_to is None
                        else _membership_session_date(effective_to).isoformat(),
                    }
                )
        identified = {item["security_id"] for item in member_scopes}
        if identified != members:
            raise ValueError("current membership has missing verified security identity")
        scopes_by_membership: dict[str, list[tuple[datetime, datetime | None]]] = {}
        for item in member_scopes:
            scopes_by_membership.setdefault(item["membership_event_id"], []).append(
                (
                    datetime.fromisoformat(item["effective_from"]),
                    None if item["effective_to"] is None else datetime.fromisoformat(item["effective_to"]),
                )
            )
        for security_id, spans in membership_spans.items():
            if security_id not in members:
                continue
            for member_from, member_to, membership_event_id in spans:
                required_end = min(member_to, as_of) if member_to is not None else as_of
                cursor = member_from
                for interval_from, interval_to in sorted(scopes_by_membership.get(membership_event_id, [])):
                    if interval_from > cursor:
                        raise ValueError("verified security identity has a gap inside membership interval")
                    interval_end = min(interval_to, as_of) if interval_to is not None else as_of
                    if interval_end > cursor:
                        cursor = interval_end
                    if cursor >= required_end:
                        break
                if cursor < required_end:
                    raise ValueError("verified security identity has a gap inside membership interval")
        member_scopes.sort(key=lambda item: (item["security_id"], item["effective_from"], item["symbol"]))
        expected_symbols = {item["symbol"] for item in member_scopes}
        receipt_symbols = {str(item["symbol"]) for item in actions_receipt.get("symbols", [])}
        if receipt_symbols != expected_symbols:
            raise ValueError("action receipt symbol scope does not match current membership identities")
        silver_current = self.silver_root / "revisions" / "current.json"
        if silver_revision is None:
            if not silver_current.is_file():
                raise ValueError("missing current Silver revision")
            silver_payload = json.loads(silver_current.read_bytes())
            silver_revision = int(silver_payload["revision"])
            immutable = self.silver_root / "revisions" / f"revision={silver_revision}.json"
            if not immutable.is_file() or immutable.read_bytes() != silver_current.read_bytes():
                raise ValueError("Silver revision pointer does not match immutable manifest")
        else:
            immutable = self.silver_root / "revisions" / f"revision={silver_revision}.json"
            if not immutable.is_file():
                raise ValueError("missing immutable Silver revision")
            silver_payload = json.loads(immutable.read_bytes())
            if int(silver_payload.get("revision", -1)) != silver_revision:
                raise ValueError("immutable Silver revision identity mismatch")
        silver_published_at = datetime.fromisoformat(
            str(silver_payload["published_at"]).replace("Z", "+00:00")
        ).astimezone(UTC)
        if silver_published_at > as_of:
            raise ValueError("Silver revision was published after PIT as-of")
        actions_as_of = datetime.fromisoformat(
            str(silver_payload["corporate_actions_as_of"]).replace("Z", "+00:00")
        ).astimezone(UTC)
        if actions_as_of > as_of:
            raise ValueError("Silver corporate-action cutoff is later than PIT as-of")
        if receipt_as_of.astimezone(UTC) != actions_as_of:
            raise ValueError("corporate-action receipt as-of does not match Silver cutoff")
        from livewire_scripts.shepherd_actions import export_actions

        regenerated = export_actions(sorted(expected_symbols), actions_as_of, data_lake_root=self.root)
        if regenerated != actions_receipt:
            raise ValueError("corporate-action receipt does not match local replay")
        silver_artifacts = []
        for artifact in silver_payload["artifacts"]:
            path = (self.silver_root / artifact["path"]).resolve()
            if not path.is_relative_to(self.silver_root.resolve()):
                raise ValueError("Silver artifact path escapes Silver root")
            if not path.is_file() or digest_bytes(path.read_bytes()) != artifact["sha256"]:
                raise ValueError("Silver artifact hash mismatch")
            silver_artifacts.append(dict(artifact))

        security_path = self.root / "security_master" / "events.parquet"
        membership_path = self.root / "index_membership" / index_id / "events.parquet"
        membership_snapshot = canonical_bytes([jsonable(asdict(event)) for event in prefix])
        security_snapshot = canonical_bytes([jsonable(asdict(event)) for event in identity_prefix])
        return {
            "policy_version": "pit-silver-v1",
            "as_of": as_of.isoformat(),
            "daily_bar_cutoff": daily_bar_cutoff(as_of).isoformat(),
            "session_policy": XNYS_SESSION_POLICY,
            "index_id": index_id,
            "membership_revision": membership_revision,
            "silver_revision": silver_revision,
            "corporate_actions_as_of": actions_as_of.isoformat(),
            "members": member_scopes,
            "inputs": {
                "silver_manifest": {
                    "path": str(immutable.relative_to(self.silver_root)),
                    "sha256": digest_bytes(immutable.read_bytes()),
                },
                "silver_artifacts": silver_artifacts,
                "security_master": {
                    "path": str(security_path.relative_to(self.root)),
                    "revision": security_revision,
                    "revision_semantics": "append-order-prefix",
                    "sha256": digest_bytes(security_snapshot),
                },
                "membership": {
                    "path": str(membership_path.relative_to(self.root)),
                    "revision": membership_revision,
                    "revision_semantics": "append-order-prefix",
                    "sha256": digest_bytes(membership_snapshot),
                },
                "corporate_action_receipt_hash": claimed_receipt_hash,
            },
        }

    def _read_current(self) -> dict[str, Any] | None:
        if not self.current.exists():
            return None
        return json.loads(self.current.read_bytes())

    def _recover_orphans(self) -> tuple[Path, ...]:
        """Adopt valid crash-complete revisions and quarantine invalid remnants."""
        changed: list[Path] = []
        current = self._read_current()
        current_revision = 0
        if current is not None:
            current_revision = int(current["revision"])
            immutable = self.revisions / f"revision={current_revision}.json"
            if not immutable.is_file() or immutable.read_bytes() != self.current.read_bytes():
                raise ValueError("PIT Silver current pointer does not match its immutable revision")
            self.verify(immutable)
        candidates = sorted(
            (
                (int(path.stem.split("=", 1)[1]), path)
                for path in self.revisions.glob("revision=*.json")
                if path.stem.split("=", 1)[1].isdigit() and int(path.stem.split("=", 1)[1]) > current_revision
            ),
            key=lambda item: item[0],
        )
        for revision, path in candidates:
            if revision != current_revision + 1:
                changed.append(self._quarantine(path))
                continue
            try:
                self.verify(path)
            except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                changed.append(self._quarantine(path))
                continue
            self._replace_current(path.read_bytes())
            changed.append(self.current)
            current_revision = revision
        return tuple(dict.fromkeys(changed))

    def _quarantine(self, path: Path) -> Path:
        quarantine = self.revisions / "quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        target = quarantine / f"{path.name}.{time.time_ns()}.orphan"
        os.replace(path, target)
        fsync_directory(quarantine)
        fsync_directory(self.revisions)
        return target

    def _result(self, payload: dict[str, Any], changed_paths: tuple[Path, ...]) -> PitSilverRevision:
        return PitSilverRevision(
            int(payload["revision"]),
            payload["status"],
            str(payload["input_hash"]),
            self.revisions / f"revision={int(payload['revision'])}.json",
            changed_paths,
        )

    @staticmethod
    def _actions_ref(path: Path) -> str | None:
        try:
            payload = json.loads(path.read_bytes())
            return str(payload["inputs"]["corporate_action_receipt"]["sha256"])
        except (OSError, KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def _write_immutable(path: Path, payload: bytes) -> None:
        if path.exists():
            if path.read_bytes() != payload:
                raise FileExistsError(path)
            return
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        fsync_directory(path.parent)

    def _replace_current(self, payload: bytes) -> None:
        temp = self.revisions / f".current.{os.getpid()}.{time.time_ns()}.tmp"
        try:
            with temp.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, self.current)
            fsync_directory(self.revisions)
        finally:
            temp.unlink(missing_ok=True)
