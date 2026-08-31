"""Manifest-bound, staged, reversible Livewire repair transactions."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from clients.parquet_io import symbol_lock, validate_parquet_file
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.source_evidence import SourceEvidenceStore
from clients.symbol_ids import stable_symbol_id
from clients.symbol_paths import encode_symbol
from clients.trading_calendar import XNYS_SESSION_POLICY, session_close_time, trading_dates_in_range

_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$")
_WORK_UNIT = re.compile(r"^lws-[0-9a-f]{32}$")
_SECURITY = re.compile(r"^sec_[0-9a-f]{32}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_SCOPE = re.compile(r"^sha256:[0-9a-f]{64}$")
_UTC_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
_OPERATIONS = {"daily-merge"}


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class HashedPath:
    path: str
    sha256: str


@dataclass(frozen=True)
class HashedRef:
    ref: str
    sha256: str


@dataclass(frozen=True)
class RepairManifest:
    version: Literal[1]
    operation_id: str
    work_unit_id: str
    scope_hash: str
    data_lake_root: Path
    layer: Literal["bronze", "silver", "query"]
    security_id: str
    symbol: str
    symbol_valid_from: datetime
    symbol_valid_to: datetime | None
    identity_as_of: datetime
    security_master_revision: int
    security_master_sha256: str
    session_policy: str
    date_from: date
    date_to: date
    timeframe: str
    prior_artifacts: tuple[HashedPath, ...]
    source_evidence: tuple[HashedRef, ...]
    max_rows: int
    max_bytes: int
    expires_at: datetime
    operation: str


@dataclass(frozen=True)
class LoadedManifest:
    manifest: RepairManifest
    path: Path
    sha256: str
    target_paths: tuple[Path, ...]


class ShepherdRepair:
    """Execute one exact repair without allowing a writer to widen its scope."""

    def __init__(self, data_lake_root: Path, *, failpoint: Callable[[str], None] | None = None):
        self.root = Path(data_lake_root).expanduser().resolve()
        self.failpoint = failpoint or (lambda _point: None)
        self.evidence = SourceEvidenceStore(self.root)

    def preflight(self, manifest_path: Path, *, now: datetime | None = None) -> dict[str, Any]:
        loaded = self._load_manifest(manifest_path, now=now, enforce_expiry=True, check_prior=True, check_source=True)
        self._verify_identity(loaded.manifest)
        return self._receipt(
            {
                "version": 1,
                "operation": "shepherd-repair-preflight",
                "operationId": loaded.manifest.operation_id,
                "workUnitId": loaded.manifest.work_unit_id,
                "scopeHash": loaded.manifest.scope_hash,
                "manifestHash": f"sha256:{loaded.sha256}",
                "state": "PREFLIGHT_OK",
                "targetPaths": [str(path) for path in loaded.target_paths],
                "changedPaths": [],
            }
        )

    def transaction(self, manifest_path: Path, *, now: datetime | None = None) -> dict[str, Any]:
        """Run or resume the one bounded repair and roll back a failed verification.

        Every durable step remains independently replayable.  This method only
        supplies the fixed orchestration that an unattended executor needs; it
        does not weaken any manifest, source, identity, or postcondition check.
        """
        loaded = self._load_manifest(
            manifest_path,
            now=now,
            enforce_expiry=False,
            check_prior=False,
            check_source=False,
        )
        state = self._state_dir(loaded.manifest)
        rollback_path = state / "rollback-receipt.json"
        if rollback_path.exists():
            rolled_back = self._load_receipt(
                rollback_path,
                loaded,
                "shepherd-repair-rollback",
            )
            return {
                "version": 1,
                "operation": "shepherd-repair-transaction",
                "state": "ROLLED_BACK",
                "error": "repair was previously rolled back",
                "rollbackReceipt": rolled_back,
            }

        preflight: dict[str, Any] | None = None
        stage_path = state / "stage-receipt.json"
        if not stage_path.exists():
            preflight = self.preflight(manifest_path, now=now)
            staged = self.stage(manifest_path, now=now)
            stage_path = Path(staged["receiptPath"])
        published = self.publish(manifest_path, stage_path, now=now)
        publish_path = Path(published["receiptPath"])
        try:
            verified = self.verify(manifest_path, publish_path, now=now)
        except Exception as error:
            rolled_back = self.rollback(manifest_path, publish_path)
            return {
                "version": 1,
                "operation": "shepherd-repair-transaction",
                "state": "ROLLED_BACK",
                "error": str(error),
                "publishReceipt": published,
                "rollbackReceipt": rolled_back,
                **({} if preflight is None else {"preflightReceipt": preflight}),
            }
        return {
            "version": 1,
            "operation": "shepherd-repair-transaction",
            "state": "VERIFIED",
            "publishReceipt": published,
            "verifyReceipt": verified,
            **({} if preflight is None else {"preflightReceipt": preflight}),
        }

    def postcondition(self, manifest_path: Path, *, now: datetime | None = None) -> dict[str, Any]:
        """Recheck an already-terminal transaction without initiating repair work."""
        loaded = self._load_manifest(
            manifest_path,
            now=now,
            enforce_expiry=False,
            check_prior=False,
            check_source=False,
        )
        state = self._state_dir(loaded.manifest)
        base = {
            "version": 1,
            "operation": "shepherd-repair-postcondition",
            "operationId": loaded.manifest.operation_id,
            "workUnitId": loaded.manifest.work_unit_id,
            "scopeHash": loaded.manifest.scope_hash,
            "manifestHash": f"sha256:{loaded.sha256}",
        }
        rollback_path = state / "rollback-receipt.json"
        if rollback_path.exists():
            rollback = self._load_receipt(rollback_path, loaded, "shepherd-repair-rollback")
            return {**base, "state": "NOT_VERIFIED", "reason": "repair rolled back", "receipt": rollback}
        publish_path = state / "publish-receipt.json"
        verify_path = state / "verify-receipt.json"
        if not publish_path.exists() or not verify_path.exists():
            return {**base, "state": "NOT_VERIFIED", "reason": "terminal verification receipt is absent"}
        try:
            verified = self.verify(manifest_path, publish_path, now=now)
        except Exception as error:
            return {**base, "state": "FAILED", "reason": str(error)}
        return {
            **base,
            "state": "VERIFIED",
            "receiptHash": verified["receiptHash"],
            "postconditions": verified["postconditions"],
        }

    def stage(self, manifest_path: Path, *, now: datetime | None = None) -> dict[str, Any]:
        loaded = self._load_manifest(manifest_path, now=now, enforce_expiry=True, check_prior=True, check_source=True)
        self._verify_identity(loaded.manifest)
        state = self._state_dir(loaded.manifest)
        with self._operation_lock(state):
            if any(
                (state / name).exists()
                for name in ("publish-receipt.json", "verify-receipt.json", "rollback-receipt.json")
            ):
                raise ValueError("repair operation is already terminal")
            receipt_path = state / "stage-receipt.json"
            if receipt_path.exists():
                receipt = self._load_receipt(receipt_path, loaded, "shepherd-repair-stage")
                candidate = receipt["candidates"][0]
                self._verify_candidate_path(Path(candidate["path"]), state, candidate["sha256"])
                return receipt

            target = loaded.target_paths[0]
            prior = loaded.manifest.prior_artifacts[0]
            source = loaded.manifest.source_evidence[0]
            self.failpoint("before-stage")
            with symbol_lock(target):
                if self._target_path(prior.path) != target or self._path_hash(target) != prior.sha256:
                    raise ValueError("prior artifact changed during repair staging")
                candidate_table, patch_rows = self._build_daily_candidate(loaded, target, source)
            if candidate_table.num_rows > loaded.manifest.max_rows:
                raise ValueError("candidate exceeds manifest row budget")
            candidate_dir = state / "candidates"
            candidate_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            candidate = candidate_dir / f"{hashlib.sha256(prior.path.encode()).hexdigest()}.parquet"
            self._write_parquet_candidate(candidate, candidate_table)
            candidate_hash = _digest(candidate.read_bytes())
            if candidate.stat().st_size > loaded.manifest.max_bytes:
                candidate.unlink(missing_ok=True)
                raise ValueError("candidate exceeds manifest byte budget")
            self.failpoint("after-stage")
            receipt = self._receipt(
                {
                    "version": 1,
                    "operation": "shepherd-repair-stage",
                    "operationId": loaded.manifest.operation_id,
                    "workUnitId": loaded.manifest.work_unit_id,
                    "scopeHash": loaded.manifest.scope_hash,
                    "manifestHash": f"sha256:{loaded.sha256}",
                    "state": "STAGED",
                    "candidates": [
                        {
                            "targetPath": str(target),
                            "path": str(candidate),
                            "sha256": candidate_hash,
                            "priorSha256": prior.sha256,
                            "rows": candidate_table.num_rows,
                            "patchRows": patch_rows,
                            "bytes": candidate.stat().st_size,
                        }
                    ],
                    "sourceEvidence": [source.__dict__],
                    "changedPaths": [str(candidate), str(receipt_path)],
                }
            )
            self._write_receipt(receipt_path, receipt)
            return {**receipt, "receiptPath": str(receipt_path)}

    def publish(
        self,
        manifest_path: Path,
        staged_receipt_path: Path,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        loaded = self._load_manifest(
            manifest_path, now=now, enforce_expiry=False, check_prior=False, check_source=False
        )
        state = self._state_dir(loaded.manifest)
        with self._operation_lock(state):
            if (state / "rollback-receipt.json").exists():
                raise ValueError("repair operation is already terminal")
            staged = self._load_receipt(staged_receipt_path, loaded, "shepherd-repair-stage")
            receipt_path = state / "publish-receipt.json"
            candidate_info = staged["candidates"][0]
            target = loaded.target_paths[0]
            candidate = Path(candidate_info["path"])
            prior_hash = loaded.manifest.prior_artifacts[0].sha256
            candidate_hash = str(candidate_info["sha256"])
            backup = state / "backups" / f"{prior_hash}.bin"

            with symbol_lock(target):
                if receipt_path.exists():
                    receipt = self._load_receipt(receipt_path, loaded, "shepherd-repair-publish")
                    if self._path_hash(target) != candidate_hash:
                        raise ValueError("published target no longer matches candidate")
                    return receipt
                if self._target_path(loaded.manifest.prior_artifacts[0].path) != target:
                    raise ValueError("repair target changed before publish")
                current_hash = self._path_hash(target)
                if current_hash not in {prior_hash, candidate_hash}:
                    raise ValueError("stale target blocks repair publish")
                if current_hash == prior_hash:
                    clock = (now or datetime.now(UTC)).astimezone(UTC)
                    if loaded.manifest.expires_at <= clock:
                        raise ValueError("repair manifest is expired before canonical publish")
                    if loaded.manifest.identity_as_of > clock:
                        raise ValueError("repair identity as-of is later than the publish clock")
                    self._verify_source_evidence(loaded.manifest.source_evidence)
                    self._verify_identity(loaded.manifest)
                    self._verify_candidate_path(candidate, state, candidate_hash)
                    candidate_bytes = candidate.read_bytes()
                    if _digest(candidate_bytes) != candidate_hash:
                        raise ValueError("candidate artifact changed before publish")
                    self._write_immutable(backup, target.read_bytes())
                    self.failpoint("after-backup")
                    self.failpoint("before-publish")
                    self._atomic_replace(target, candidate_bytes)
                else:
                    self._verify_state_path(backup, state, prior_hash)
                    if not candidate.exists():
                        self._write_immutable(candidate, target.read_bytes())
                    self._verify_candidate_path(candidate, state, candidate_hash)
                self.failpoint("after-publish")

            receipt = self._receipt(
                {
                    "version": 1,
                    "operation": "shepherd-repair-publish",
                    "operationId": loaded.manifest.operation_id,
                    "workUnitId": loaded.manifest.work_unit_id,
                    "scopeHash": loaded.manifest.scope_hash,
                    "manifestHash": f"sha256:{loaded.sha256}",
                    "stageReceiptHash": staged["receiptHash"],
                    "state": "PUBLISHED",
                    "artifacts": [
                        {
                            "targetPath": str(target),
                            "priorSha256": prior_hash,
                            "publishedSha256": candidate_hash,
                            "backupPath": str(backup),
                            "candidatePath": str(candidate),
                        }
                    ],
                    "changedPaths": [str(backup), str(target), str(receipt_path)],
                }
            )
            self._write_receipt(receipt_path, receipt)
            return {**receipt, "receiptPath": str(receipt_path)}

    def verify(
        self,
        manifest_path: Path,
        publish_receipt_path: Path,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        loaded = self._load_manifest(manifest_path, enforce_expiry=False, check_prior=False, check_source=True)
        state = self._state_dir(loaded.manifest)
        with self._operation_lock(state):
            receipt_path = state / "verify-receipt.json"
            published = self._load_receipt(publish_receipt_path, loaded, "shepherd-repair-publish")
            artifact = published["artifacts"][0]
            target = loaded.target_paths[0]
            backup = Path(artifact["backupPath"])
            candidate = Path(artifact["candidatePath"])
            self._verify_state_path(backup, state, artifact["priorSha256"])
            self._verify_candidate_path(candidate, state, artifact["publishedSha256"])
            with symbol_lock(target):
                if self._target_path(loaded.manifest.prior_artifacts[0].path) != target:
                    raise ValueError("repair target changed before verification")
                if self._path_hash(target) != artifact["publishedSha256"]:
                    raise ValueError("published target hash mismatch")
                expected, _patch_rows = self._build_daily_candidate(
                    loaded,
                    backup,
                    loaded.manifest.source_evidence[0],
                )
                expected_hash = self._table_digest(expected)
                if expected_hash != artifact["publishedSha256"]:
                    raise ValueError("published target does not replay from prior bytes and source evidence")
                verified_at = (now or datetime.now(UTC)).astimezone(UTC)
                expected_dates = set(trading_dates_in_range(loaded.manifest.date_from, loaded.manifest.date_to))
                if not expected_dates:
                    raise ValueError("repair scope contains no bound exchange session")
                final_session = max(expected_dates)
                session_close = datetime.combine(
                    final_session,
                    session_close_time(final_session),
                    ZoneInfo("America/New_York"),
                ).astimezone(UTC)
                if verified_at < session_close:
                    raise ValueError("repair cannot verify freshness before the final session close")
                identity = self._verify_identity(loaded.manifest)
                actual_dates = {
                    row["trade_date"]
                    for row in expected.to_pylist()
                    if loaded.manifest.date_from <= row["trade_date"] <= loaded.manifest.date_to
                }
                if actual_dates != expected_dates:
                    raise ValueError("repair candidate session coverage does not match the bound calendar")
                trading_dates = [day.isoformat() for day in sorted(actual_dates)]
                postconditions = {
                    "integrity": {
                        "state": "VERIFIED",
                        "sha256": artifact["publishedSha256"],
                        "rows": expected.num_rows,
                    },
                    "freshness": {
                        "state": "VERIFIED",
                        "sessionThrough": max(actual_dates).isoformat(),
                        "sessionClose": session_close.isoformat(),
                        "sessionPolicy": loaded.manifest.session_policy,
                    },
                    "coverage": {
                        "state": "VERIFIED",
                        "tradingDates": trading_dates,
                    },
                    "identity": identity,
                    "scope": {
                        "state": "VERIFIED",
                        "scopeHash": loaded.manifest.scope_hash,
                        "targetPath": str(target),
                        "dateFrom": loaded.manifest.date_from.isoformat(),
                        "dateTo": loaded.manifest.date_to.isoformat(),
                        "securityId": loaded.manifest.security_id,
                        "symbol": loaded.manifest.symbol,
                        "symbolValidFrom": loaded.manifest.symbol_valid_from.isoformat(),
                        "symbolValidTo": (
                            None
                            if loaded.manifest.symbol_valid_to is None
                            else loaded.manifest.symbol_valid_to.isoformat()
                        ),
                        "timeframe": loaded.manifest.timeframe,
                        "sessionPolicy": loaded.manifest.session_policy,
                    },
                    "lineage": {
                        "state": "VERIFIED",
                        "sourceEvidence": [item.__dict__ for item in loaded.manifest.source_evidence],
                        "priorSha256": artifact["priorSha256"],
                    },
                }
                if receipt_path.exists():
                    receipt = self._load_receipt(receipt_path, loaded, "shepherd-repair-verify")
                    if receipt.get("postconditions") != postconditions:
                        raise ValueError("cached verification no longer matches independently derived postconditions")
                    return receipt
                self.failpoint("before-verify")
                receipt = self._receipt(
                    {
                        "version": 1,
                        "operation": "shepherd-repair-verify",
                        "operationId": loaded.manifest.operation_id,
                        "workUnitId": loaded.manifest.work_unit_id,
                        "scopeHash": loaded.manifest.scope_hash,
                        "manifestHash": f"sha256:{loaded.sha256}",
                        "publishReceiptHash": published["receiptHash"],
                        "state": "VERIFIED",
                        "postconditions": postconditions,
                        "changedPaths": [str(receipt_path)],
                    }
                )
                self._write_receipt(receipt_path, receipt)
            return {**receipt, "receiptPath": str(receipt_path)}

    def rollback(self, manifest_path: Path, publish_receipt_path: Path) -> dict[str, Any]:
        loaded = self._load_manifest(manifest_path, enforce_expiry=False, check_prior=False, check_source=False)
        state = self._state_dir(loaded.manifest)
        with self._operation_lock(state):
            published = self._load_receipt(publish_receipt_path, loaded, "shepherd-repair-publish")
            receipt_path = state / "rollback-receipt.json"
            artifact = published["artifacts"][0]
            target = loaded.target_paths[0]
            backup = Path(artifact["backupPath"])
            self._verify_state_path(backup, state, artifact["priorSha256"])
            backup_bytes = backup.read_bytes()
            if _digest(backup_bytes) != artifact["priorSha256"]:
                raise ValueError("rollback backup changed before use")
            with symbol_lock(target):
                if receipt_path.exists():
                    receipt = self._load_receipt(receipt_path, loaded, "shepherd-repair-rollback")
                    if self._path_hash(target) != artifact["priorSha256"]:
                        raise ValueError("rolled-back target no longer matches prior bytes")
                    return receipt
                current_hash = self._path_hash(target)
                if current_hash not in {artifact["publishedSha256"], artifact["priorSha256"]}:
                    raise ValueError("stale target blocks rollback")
                if current_hash == artifact["publishedSha256"]:
                    self._atomic_replace(target, backup_bytes)
                self.failpoint("during-rollback")
            if self._path_hash(target) != artifact["priorSha256"]:
                raise ValueError("rollback hash mismatch")
            receipt = self._receipt(
                {
                    "version": 1,
                    "operation": "shepherd-repair-rollback",
                    "operationId": loaded.manifest.operation_id,
                    "workUnitId": loaded.manifest.work_unit_id,
                    "scopeHash": loaded.manifest.scope_hash,
                    "manifestHash": f"sha256:{loaded.sha256}",
                    "publishReceiptHash": published["receiptHash"],
                    "state": "ROLLED_BACK",
                    "restored": [{"targetPath": str(target), "sha256": artifact["priorSha256"]}],
                    "changedPaths": [str(target), str(receipt_path)],
                }
            )
            self._write_receipt(receipt_path, receipt)
            return {**receipt, "receiptPath": str(receipt_path)}

    def _load_manifest(
        self,
        path: Path,
        *,
        now: datetime | None = None,
        enforce_expiry: bool,
        check_prior: bool,
        check_source: bool,
    ) -> LoadedManifest:
        path = Path(path).expanduser()
        if not path.is_absolute():
            raise ValueError("repair manifest path must be absolute")
        raw = path.read_bytes()
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("repair manifest must be a JSON object")
        required = {
            "version",
            "operationId",
            "workUnitId",
            "scopeHash",
            "dataLakeRoot",
            "layer",
            "securityId",
            "symbol",
            "symbolValidFrom",
            "symbolValidTo",
            "identityAsOf",
            "securityMasterRevision",
            "securityMasterSha256",
            "sessionPolicy",
            "dateFrom",
            "dateTo",
            "timeframe",
            "priorArtifacts",
            "sourceEvidence",
            "maxRows",
            "maxBytes",
            "expiresAt",
            "operation",
        }
        if set(payload) != required:
            raise ValueError("repair manifest fields are incomplete or unsupported")
        string_fields = (
            "operationId",
            "workUnitId",
            "scopeHash",
            "dataLakeRoot",
            "layer",
            "securityId",
            "symbol",
            "symbolValidFrom",
            "identityAsOf",
            "securityMasterSha256",
            "sessionPolicy",
            "dateFrom",
            "dateTo",
            "timeframe",
            "expiresAt",
            "operation",
        )
        if any(not isinstance(payload[field], str) for field in string_fields):
            raise ValueError("repair manifest string fields must be strings")
        if payload["symbolValidTo"] is not None and not isinstance(payload["symbolValidTo"], str):
            raise ValueError("repair symbol validity end must be a string or null")
        if (
            not isinstance(payload["version"], int)
            or isinstance(payload["version"], bool)
            or not isinstance(payload["maxRows"], int)
            or isinstance(payload["maxRows"], bool)
            or not isinstance(payload["maxBytes"], int)
            or isinstance(payload["maxBytes"], bool)
            or not isinstance(payload["securityMasterRevision"], int)
            or isinstance(payload["securityMasterRevision"], bool)
        ):
            raise ValueError("repair manifest version, revision, and budgets must be integers")
        if not isinstance(payload["priorArtifacts"], list) or not isinstance(payload["sourceEvidence"], list):
            raise ValueError("repair manifest artifacts must be arrays")
        if any(not isinstance(item, dict) or set(item) != {"path", "sha256"} for item in payload["priorArtifacts"]):
            raise ValueError("repair prior artifact fields are unsupported")
        if any(not isinstance(item, dict) or set(item) != {"ref", "sha256"} for item in payload["sourceEvidence"]):
            raise ValueError("repair source evidence fields are unsupported")
        if any(
            not isinstance(item[key], str) for item in payload["priorArtifacts"] for key in ("path", "sha256")
        ) or any(not isinstance(item[key], str) for item in payload["sourceEvidence"] for key in ("ref", "sha256")):
            raise ValueError("repair artifact paths, refs, and hashes must be strings")
        timestamps = [payload["expiresAt"], payload["symbolValidFrom"], payload["identityAsOf"]]
        if payload["symbolValidTo"] is not None:
            timestamps.append(payload["symbolValidTo"])
        if any(_UTC_ISO.fullmatch(value) is None for value in timestamps):
            raise ValueError("repair timestamps must use the shared UTC Z format")
        expires = datetime.fromisoformat(str(payload["expiresAt"]))
        if expires.tzinfo is None or expires.utcoffset() is None:
            raise ValueError("repair manifest expiry must be timezone-aware")
        clock = (now or datetime.now(UTC)).astimezone(UTC)
        if enforce_expiry and expires.astimezone(UTC) <= clock:
            raise ValueError("repair manifest is expired")
        root = Path(payload["dataLakeRoot"]).expanduser()
        if not root.is_absolute() or root.resolve() != self.root:
            raise ValueError("repair manifest data-lake root does not match active root")
        operation_id = str(payload["operationId"])
        work_unit_id = str(payload["workUnitId"])
        if _ID.fullmatch(operation_id) is None or _WORK_UNIT.fullmatch(work_unit_id) is None:
            raise ValueError("invalid repair operation or work-unit id")
        if payload["version"] != 1 or payload["layer"] not in {"bronze", "silver", "query"}:
            raise ValueError("unsupported repair manifest version or layer")
        if payload["operation"] not in _OPERATIONS:
            raise ValueError("unsupported repair operation")
        if payload["sessionPolicy"] != XNYS_SESSION_POLICY:
            raise ValueError("unsupported repair session policy")
        if (
            _SCOPE.fullmatch(str(payload["scopeHash"])) is None
            or _SECURITY.fullmatch(str(payload["securityId"])) is None
            or _HASH.fullmatch(str(payload["securityMasterSha256"])) is None
        ):
            raise ValueError("invalid repair scope, security identity, or security-master hash")
        if payload["securityMasterRevision"] < 1:
            raise ValueError("repair security-master revision must be positive")
        symbol = str(payload["symbol"])
        if not symbol or len(symbol) > 64 or symbol.strip() != symbol or any(ord(char) < 32 for char in symbol):
            raise ValueError("invalid repair symbol")
        symbol_valid_from = datetime.fromisoformat(payload["symbolValidFrom"])
        symbol_valid_to = None if payload["symbolValidTo"] is None else datetime.fromisoformat(payload["symbolValidTo"])
        identity_as_of = datetime.fromisoformat(payload["identityAsOf"])
        if enforce_expiry and identity_as_of.astimezone(UTC) > clock:
            raise ValueError("repair identity as-of is later than the operation clock")
        if symbol_valid_from.tzinfo is None or symbol_valid_from.utcoffset() is None:
            raise ValueError("repair symbol validity start must be timezone-aware")
        if symbol_valid_to is not None and (
            symbol_valid_to.tzinfo is None
            or symbol_valid_to.utcoffset() is None
            or symbol_valid_to <= symbol_valid_from
        ):
            raise ValueError("repair symbol validity end is invalid")
        date_from = date.fromisoformat(str(payload["dateFrom"]))
        date_to = date.fromisoformat(str(payload["dateTo"]))
        if date_to < date_from:
            raise ValueError("repair date range is inverted")
        scope: dict[str, object] = {
            "kind": "security-interval",
            "securityId": payload["securityId"],
            "symbol": symbol,
            "symbolValidFrom": payload["symbolValidFrom"],
            "dateFrom": payload["dateFrom"],
            "dateTo": payload["dateTo"],
            "timeframe": payload["timeframe"],
            "layer": payload["layer"],
        }
        if payload["symbolValidTo"] is not None:
            scope["symbolValidTo"] = payload["symbolValidTo"]
        encoded_scope = json.dumps(scope, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        expected_scope_hash = f"sha256:{_digest(encoded_scope)}"
        if payload["scopeHash"] != expected_scope_hash:
            raise ValueError("repair scope hash does not match the canonical security interval")
        if work_unit_id != f"lws-{expected_scope_hash[len('sha256:') :][:32]}":
            raise ValueError("repair work-unit id does not match scope hash")
        prior = tuple(HashedPath(str(item["path"]), str(item["sha256"])) for item in payload["priorArtifacts"])
        sources = tuple(HashedRef(str(item["ref"]), str(item["sha256"])) for item in payload["sourceEvidence"])
        if len(prior) != 1 or len(sources) != 1:
            raise ValueError("repair v1 requires exactly one target and one source evidence artifact")
        if any(_HASH.fullmatch(item.sha256) is None for item in (*prior, *sources)):
            raise ValueError("invalid repair artifact hash")
        manifest = RepairManifest(
            version=1,
            operation_id=operation_id,
            work_unit_id=work_unit_id,
            scope_hash=str(payload["scopeHash"]),
            data_lake_root=root.resolve(),
            layer=payload["layer"],
            security_id=str(payload["securityId"]),
            symbol=symbol,
            symbol_valid_from=symbol_valid_from.astimezone(UTC),
            symbol_valid_to=None if symbol_valid_to is None else symbol_valid_to.astimezone(UTC),
            identity_as_of=identity_as_of.astimezone(UTC),
            security_master_revision=payload["securityMasterRevision"],
            security_master_sha256=payload["securityMasterSha256"],
            session_policy=payload["sessionPolicy"],
            date_from=date_from,
            date_to=date_to,
            timeframe=str(payload["timeframe"]),
            prior_artifacts=prior,
            source_evidence=sources,
            max_rows=payload["maxRows"],
            max_bytes=payload["maxBytes"],
            expires_at=expires.astimezone(UTC),
            operation=str(payload["operation"]),
        )
        if manifest.max_rows < 1 or manifest.max_bytes < 1:
            raise ValueError("repair budgets must be positive")
        target_paths = tuple(self._target_path(item.path) for item in prior)
        self._validate_operation_target(manifest, prior[0].path)
        if check_source:
            self._verify_source_evidence(sources)
        if check_prior:
            for target, item in zip(target_paths, prior, strict=True):
                if self._path_hash(target) != item.sha256:
                    raise ValueError("prior artifact hash does not match canonical target")
        return LoadedManifest(manifest, path, _digest(raw), target_paths)

    def _verify_source_evidence(self, sources: tuple[HashedRef, ...]) -> None:
        for source in sources:
            try:
                source_bytes = self.evidence.read(source.ref)
            except (OSError, ValueError) as exc:
                raise ValueError("source evidence is missing or corrupt") from exc
            if _digest(source_bytes) != source.sha256:
                raise ValueError("source evidence hash mismatch")

    def _verify_identity(self, manifest: RepairManifest) -> dict[str, Any]:
        master = SecurityMaster(self.root, evidence_verifier=None)
        all_events = master.events()
        if manifest.security_master_revision > len(all_events):
            raise ValueError("repair security-master revision exceeds the local append log")
        prefix = all_events[: manifest.security_master_revision]
        snapshot = _canonical([_jsonable(asdict(event)) for event in prefix])
        if _digest(snapshot) != manifest.security_master_sha256:
            raise ValueError("repair security-master append-prefix hash mismatch")
        events = [event for event in prefix if event.known_at <= manifest.identity_as_of]
        superseded = {event.supersedes for event in events if event.supersedes is not None}
        candidates = [
            event
            for event in events
            if event.event_id not in superseded
            and event.status == "verified"
            and event.security_id == manifest.security_id
            and event.symbol == manifest.symbol
            and event.effective_from.astimezone(UTC) == manifest.symbol_valid_from
            and (
                (event.effective_to is None and manifest.symbol_valid_to is None)
                or (
                    event.effective_to is not None
                    and manifest.symbol_valid_to is not None
                    and event.effective_to.astimezone(UTC) == manifest.symbol_valid_to
                )
            )
        ]
        used: dict[str, SecurityIdentityEvent] = {}
        for day in trading_dates_in_range(manifest.date_from, manifest.date_to):
            session_close = datetime.combine(
                day,
                session_close_time(day),
                ZoneInfo("America/New_York"),
            )
            if session_close < manifest.symbol_valid_from or (
                manifest.symbol_valid_to is not None and session_close >= manifest.symbol_valid_to
            ):
                raise ValueError("repair scope exceeds the declared symbol-validity interval")
            matching = [
                event
                for event in candidates
                if event.effective_from <= session_close
                and (event.effective_to is None or session_close < event.effective_to)
            ]
            if len(matching) != 1:
                raise ValueError("repair identity is not uniquely verified for the full date range")
            used[matching[0].event_id] = matching[0]
        evidence: list[dict[str, str]] = []
        for event in sorted(used.values(), key=lambda item: item.event_id):
            for ref, digest in zip(event.source_refs, event.source_hashes, strict=True):
                self._verify_source_evidence((HashedRef(ref, digest),))
                evidence.append({"ref": ref, "sha256": digest})
        return {
            "state": "VERIFIED",
            "securityId": manifest.security_id,
            "symbol": manifest.symbol,
            "identityAsOf": manifest.identity_as_of.isoformat(),
            "securityMasterRevision": manifest.security_master_revision,
            "securityMasterSha256": manifest.security_master_sha256,
            "eventIds": sorted(used),
            "eventHashes": [
                _digest(_canonical(_jsonable(asdict(event))))
                for event in sorted(used.values(), key=lambda item: item.event_id)
            ],
            "sourceEvidence": evidence,
        }

    def _validate_operation_target(self, manifest: RepairManifest, relative: str) -> None:
        if manifest.operation == "daily-merge":
            expected = f"bronze/asset_class=equity/symbol={encode_symbol(manifest.symbol)}/1d.parquet"
            if manifest.layer != "bronze" or manifest.timeframe != "1d" or relative != expected:
                raise ValueError("daily-merge target does not match exact manifest symbol and timeframe")

    def _target_path(self, relative: str) -> Path:
        pure = PurePosixPath(relative)
        if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
            raise ValueError("repair target path escapes data-lake root")
        current = self.root
        for part in pure.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("repair target path contains a symlink")
        target = self.root.joinpath(*pure.parts)
        if not target.resolve().is_relative_to(self.root):
            raise ValueError("repair target path escapes data-lake root")
        return target

    def _build_daily_candidate(
        self,
        loaded: LoadedManifest,
        prior_path: Path,
        source: HashedRef,
    ) -> tuple[pa.Table, int]:
        try:
            prior_table = pq.ParquetFile(prior_path).read()
            patch_table = pq.ParquetFile(pa.BufferReader(self.evidence.read(source.ref))).read()
        except Exception as exc:
            raise ValueError("repair source or prior artifact is not valid Parquet") from exc
        if patch_table.schema != prior_table.schema:
            raise ValueError("repair patch schema does not match prior artifact")
        patch_rows = patch_table.to_pylist()
        if not patch_rows:
            raise ValueError("repair patch is empty")
        expected_symbol_id = stable_symbol_id(loaded.manifest.symbol)
        prior_rows = prior_table.to_pylist()
        self._validate_daily_rows(prior_rows, expected_symbol_id)
        self._validate_daily_rows(patch_rows, expected_symbol_id)
        bound_sessions = set(trading_dates_in_range(loaded.manifest.date_from, loaded.manifest.date_to))
        for row in patch_rows:
            day = row.get("trade_date")
            if not isinstance(day, date) or not (loaded.manifest.date_from <= day <= loaded.manifest.date_to):
                raise ValueError("repair patch row is outside manifest date range")
            if day not in bound_sessions:
                raise ValueError("repair patch row is not a bound exchange session")
        if len(patch_rows) > loaded.manifest.max_rows:
            raise ValueError("repair patch exceeds manifest row budget")
        merged = {row["trade_date"]: row for row in prior_rows}
        for row in patch_rows:
            merged[row["trade_date"]] = row
        candidate_sessions = {day for day in merged if loaded.manifest.date_from <= day <= loaded.manifest.date_to}
        if candidate_sessions != bound_sessions:
            raise ValueError("repair candidate does not exactly cover the bound exchange sessions")
        ordered = [merged[key] for key in sorted(merged)]
        self._validate_daily_rows(ordered, expected_symbol_id)
        return pa.Table.from_pylist(ordered, schema=prior_table.schema), len(patch_rows)

    @staticmethod
    def _validate_daily_rows(rows: list[dict[str, Any]], expected_symbol_id: int) -> None:
        days = [row.get("trade_date") for row in rows]
        if any(not isinstance(day, date) for day in days) or len(set(days)) != len(days):
            raise ValueError("repair rows contain an invalid or duplicate trade date")
        for row in rows:
            if int(row.get("symbol_id", -1)) != expected_symbol_id:
                raise ValueError("repair rows contain an extra symbol identity")
            prices = tuple(float(row[field]) for field in ("open", "high", "low", "close", "adj_close"))
            opening, high, low, close, adjusted = prices
            if (
                not all(math.isfinite(value) for value in prices)
                or min(prices) <= 0
                or high < max(opening, low, close)
                or low > min(opening, high, close)
                or int(row["volume"]) < 0
            ):
                raise ValueError("repair rows violate OHLCV integrity")
            if row.get("source") not in {"ib", "massive", "nasdaq", "stooq", "legacy"} or row.get(
                "price_basis"
            ) not in {"raw", "split_adjusted", "unknown"}:
                raise ValueError("repair rows have invalid source lineage")

    def _state_dir(self, manifest: RepairManifest) -> Path:
        return self._target_path(f"operations/shepherd-repairs/{manifest.operation_id}")

    @contextmanager
    def _operation_lock(self, state: Path) -> Iterator[None]:
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock = state / ".lock"
        with lock.open("a", encoding="utf-8") as handle:
            os.chmod(lock, 0o600)
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _receipt(payload: dict[str, Any]) -> dict[str, Any]:
        receipt = dict(payload)
        receipt["receiptHash"] = f"sha256:{_digest(_canonical(receipt))}"
        return receipt

    def _load_receipt(self, path: Path, loaded: LoadedManifest, operation: str) -> dict[str, Any]:
        path = Path(path).expanduser()
        if not path.is_absolute():
            raise ValueError("repair receipt path must be absolute")
        if not path.resolve().is_relative_to(self._state_dir(loaded.manifest).resolve()):
            raise ValueError("repair receipt escapes operation state")
        expected_name = {
            "shepherd-repair-stage": "stage-receipt.json",
            "shepherd-repair-publish": "publish-receipt.json",
            "shepherd-repair-verify": "verify-receipt.json",
            "shepherd-repair-rollback": "rollback-receipt.json",
        }[operation]
        if path.resolve() != (self._state_dir(loaded.manifest) / expected_name).resolve():
            raise ValueError("repair receipt path is not the canonical operation receipt")
        receipt = json.loads(path.read_bytes())
        claimed = str(receipt.pop("receiptHash", ""))
        actual = f"sha256:{_digest(_canonical(receipt))}"
        receipt["receiptHash"] = claimed
        if claimed != actual:
            raise ValueError("repair receipt hash mismatch")
        if (
            receipt.get("operation") != operation
            or receipt.get("operationId") != loaded.manifest.operation_id
            or receipt.get("manifestHash") != f"sha256:{loaded.sha256}"
        ):
            raise ValueError("repair receipt is not bound to this manifest")
        return {**receipt, "receiptPath": str(path)}

    @staticmethod
    def _path_hash(path: Path) -> str | None:
        return _digest(path.read_bytes()) if path.is_file() else None

    def _verify_candidate_path(self, path: Path, state: Path, digest: str) -> None:
        if not path.resolve().is_relative_to((state / "candidates").resolve()):
            raise ValueError("candidate path escapes operation state")
        if self._path_hash(path) != digest:
            raise ValueError("candidate artifact hash mismatch")

    def _verify_state_path(self, path: Path, state: Path, digest: str) -> None:
        if not path.resolve().is_relative_to(state.resolve()) or self._path_hash(path) != digest:
            raise ValueError("operation evidence path or hash mismatch")

    @staticmethod
    def _write_parquet_candidate(path: Path, table: pa.Table) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            pq.write_table(table, temp, compression="zstd", compression_level=3)
            validate_parquet_file(temp, table.num_rows, "trade_date")
            with temp.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temp, path)
            _fsync_directory(path.parent)
        finally:
            temp.unlink(missing_ok=True)

    @staticmethod
    def _atomic_replace(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.repair.tmp")
        try:
            with temp.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
            _fsync_directory(path.parent)
        finally:
            temp.unlink(missing_ok=True)

    @staticmethod
    def _write_immutable(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            if path.read_bytes() != payload:
                raise ValueError("immutable operation artifact changed")
            return
        temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            with temp.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
            _fsync_directory(path.parent)
        finally:
            temp.unlink(missing_ok=True)

    def _write_receipt(self, path: Path, receipt: dict[str, Any]) -> None:
        self._write_immutable(path, _canonical(receipt))

    @staticmethod
    def _table_digest(table: pa.Table) -> str:
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd", compression_level=3)
        return _digest(sink.getvalue().to_pybytes())
