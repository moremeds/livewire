"""Locked transactional publisher for Silver revision manifests."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from clients.parquet_io import fsync_directory
from clients.silver_client import PublishedArtifact
from clients.symbol_paths import canonical_symbol

_THREAD_LOCKS: dict[Path, threading.RLock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()
_SUPPORTED_TIMEFRAMES = frozenset({"1d", "1m", "5m", "30m", "1h"})


@dataclass(frozen=True)
class AffectedSymbol:
    symbol: str
    earliest_date: date
    timeframes: tuple[str, ...]


@dataclass(frozen=True)
class ManifestArtifact:
    path: str
    sha256: str


@dataclass(frozen=True)
class SilverRevision:
    schema_version: int
    revision: int
    generation_id: str
    published_at: datetime
    corporate_actions_as_of: datetime
    affected: tuple[AffectedSymbol, ...]
    artifacts: tuple[ManifestArtifact, ...]


@dataclass
class SilverRevisionTransaction:
    publisher: SilverRevisionPublisher
    current: SilverRevision | None
    revision: int
    _committed: bool = False

    def commit(
        self,
        artifacts: list[PublishedArtifact],
        affected: list[AffectedSymbol],
        actions_as_of: datetime,
        *,
        generation_id: str | None = None,
    ) -> SilverRevision:
        if self._committed:
            raise RuntimeError("Silver revision transaction already committed")
        result = self.publisher._publish_locked(
            artifacts,
            affected,
            actions_as_of,
            current=self.current,
            revision=self.revision,
            generation_id=generation_id,
        )
        # An identical manifest is a no-op: _publish_locked writes nothing and returns
        # the CURRENT revision, leaving the reservation unused. That is success, not a
        # failed commit. The caller cannot predict it -- whether the assembled manifest
        # differs is only knowable here, after it is assembled -- so deciding it there
        # means adding one more condition every time a new no-change path appears.
        # Anything that is neither the reserved revision nor the one we read under the
        # lock still means another writer moved current.json, which is fatal.
        if result.revision not in (self.revision, self.current.revision if self.current else self.revision):
            raise RuntimeError("reserved Silver revision was not committed")
        self._committed = True
        return result


class SilverRevisionPublisher:
    """Advance ``current.json`` only after all artifact checks pass."""

    def __init__(self, silver_root: Path):
        self.root = Path(silver_root)
        self.revisions_dir = self.root / "revisions"
        self.current_path = self.revisions_dir / "current.json"
        self.lock_path = self.root / ".revision.lock"

    def publish(
        self,
        artifacts: list[PublishedArtifact],
        affected: list[AffectedSymbol],
        actions_as_of: datetime,
        published_at: datetime | None = None,
        generation_id: str | None = None,
    ) -> SilverRevision:
        with self._lock():
            current = self._read_current()
            self._quarantine_ahead_manifests(current)
            revision = 1 if current is None else current.revision + 1
            return self._publish_locked(
                artifacts,
                affected,
                actions_as_of,
                published_at,
                current=current,
                revision=revision,
                generation_id=generation_id,
            )

    def read_current(self) -> SilverRevision | None:
        """Read the current committed revision without creating the Silver root."""
        return self._read_current()

    @contextmanager
    def transaction(self) -> Iterator[SilverRevisionTransaction]:
        """Reserve the next revision while holding the cross-process publish lock."""
        with self._lock():
            current = self._read_current()
            self._quarantine_ahead_manifests(current)
            revision = 1 if current is None else current.revision + 1
            yield SilverRevisionTransaction(self, current, revision)

    def _publish_locked(
        self,
        artifacts: list[PublishedArtifact],
        affected: list[AffectedSymbol],
        actions_as_of: datetime,
        published_at: datetime | None = None,
        *,
        current: SilverRevision | None = None,
        revision: int | None = None,
        generation_id: str | None = None,
    ) -> SilverRevision:
        manifest_artifacts = self._validate_artifacts(artifacts)
        normalized_affected = self._validate_affected(affected)
        if current and current.artifacts == manifest_artifacts and current.affected == normalized_affected:
            return current

        if revision is None:
            current = self._read_current()
            revision = 1 if current is None else current.revision + 1
        previous = set(current.artifacts) if current is not None else set()
        directories: set[Path] = set()
        root = self.root.resolve()
        for artifact in manifest_artifacts:
            if artifact in previous:
                continue
            path = root / artifact.path
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
            directory = path.parent
            while directory.is_relative_to(root):
                directories.add(directory)
                if directory == root:
                    break
                directory = directory.parent
        # Flush each new directory once, after all its entries exist. Repeating
        # ancestry fsync for each artifact adds tens of thousands of disk flushes.
        for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
            fsync_directory(directory)
        # ponytail: an injectable clock, because a manifest stamped from the wall clock
        # cannot be tested against a frozen PIT as-of -- freeze one half and the pair
        # collides at some midnight. Production passes nothing and still gets now().
        published_at = (published_at or datetime.now(UTC)).astimezone(UTC)
        generation_id = generation_id or f"{published_at.strftime('%Y%m%dT%H%M%SZ')}-{revision}"
        silver_revision = SilverRevision(
            schema_version=1,
            revision=revision,
            generation_id=generation_id,
            published_at=published_at,
            corporate_actions_as_of=actions_as_of.astimezone(UTC),
            affected=normalized_affected,
            artifacts=manifest_artifacts,
        )
        payload = self._serialize(silver_revision)
        immutable_path = self.revisions_dir / f"revision={revision}.json"
        self.revisions_dir.mkdir(parents=True, exist_ok=True)
        self._write_immutable(immutable_path, payload)
        self._replace_current(payload)
        return silver_revision

    def _quarantine_ahead_manifests(self, current: SilverRevision | None) -> None:
        """Move all manifests ahead of ``current.json`` aside without adopting them.

        The pointer swap is the only commit point. A manifest left by a killed writer
        is therefore uncommitted even when its body is valid; its generation files are
        deliberately retained for later garbage collection.
        """
        current_revision = 0 if current is None else current.revision
        candidates = sorted(
            (int(path.stem.split("=", 1)[1]), path)
            for path in self.revisions_dir.glob("revision=*.json")
            if path.stem.split("=", 1)[1].isdigit() and int(path.stem.split("=", 1)[1]) > current_revision
        )
        for _, path in candidates:
            self._quarantine(path)

    def _quarantine(self, path: Path) -> None:
        quarantine = self.revisions_dir / "quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        os.replace(path, quarantine / f"{path.name}.{time.time_ns()}.orphan")

    def _validate_artifacts(
        self,
        artifacts: list[PublishedArtifact],
    ) -> tuple[ManifestArtifact, ...]:
        if not artifacts:
            raise ValueError("at least one Silver artifact is required")
        root = self.root.resolve()
        manifest: list[ManifestArtifact] = []
        for artifact in artifacts:
            path = artifact.path.resolve()
            try:
                relative = path.relative_to(root)
            except ValueError as exc:
                raise ValueError(f"artifact escapes Silver root: {artifact.path}") from exc
            if not path.is_file():
                raise ValueError(f"missing Silver artifact: {artifact.path}")
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            if actual != artifact.sha256:
                raise ValueError(f"artifact checksum mismatch: {relative}")
            manifest.append(ManifestArtifact(relative.as_posix(), actual))
        if len({item.path for item in manifest}) != len(manifest):
            raise ValueError("duplicate Silver artifact path")
        return tuple(sorted(manifest, key=lambda item: item.path))

    @staticmethod
    def _validate_affected(affected: list[AffectedSymbol]) -> tuple[AffectedSymbol, ...]:
        if not affected:
            raise ValueError("at least one affected symbol is required")
        normalized = [
            AffectedSymbol(canonical_symbol(item.symbol), item.earliest_date, tuple(item.timeframes))
            for item in affected
        ]
        symbols = [item.symbol for item in normalized]
        if len(symbols) != len(set(symbols)):
            raise ValueError("duplicate affected symbol")
        for item in normalized:
            if len(item.timeframes) != len(set(item.timeframes)):
                raise ValueError(f"duplicate timeframe for {item.symbol}")
            unsupported = set(item.timeframes) - _SUPPORTED_TIMEFRAMES
            if unsupported:
                raise ValueError(f"unsupported timeframe for {item.symbol}: {sorted(unsupported)}")
        return tuple(sorted(normalized, key=lambda item: item.symbol))

    def _read_current(self) -> SilverRevision | None:
        if not self.current_path.exists():
            return None
        current_bytes = self.current_path.read_bytes()
        payload = json.loads(current_bytes)
        if payload.get("schema_version") != 1:
            raise ValueError("unsupported Silver revision schema")
        revision = int(payload["revision"])
        immutable_path = self.revisions_dir / f"revision={revision}.json"
        if not immutable_path.is_file() or immutable_path.read_bytes() != current_bytes:
            raise ValueError("Silver current pointer differs from immutable manifest")
        return SilverRevision(
            schema_version=1,
            revision=revision,
            generation_id=str(payload["generation_id"]),
            published_at=self._parse_timestamp(payload["published_at"]),
            corporate_actions_as_of=self._parse_timestamp(payload["corporate_actions_as_of"]),
            affected=tuple(
                AffectedSymbol(
                    str(item["symbol"]),
                    date.fromisoformat(item["earliest_date"]),
                    tuple(item["timeframes"]),
                )
                for item in payload["affected"]
            ),
            artifacts=tuple(ManifestArtifact(str(item["path"]), str(item["sha256"])) for item in payload["artifacts"]),
        )

    @staticmethod
    def _serialize(revision: SilverRevision) -> bytes:
        payload = {
            "schema_version": revision.schema_version,
            "revision": revision.revision,
            "generation_id": revision.generation_id,
            "published_at": SilverRevisionPublisher._timestamp(revision.published_at),
            "corporate_actions_as_of": SilverRevisionPublisher._timestamp(revision.corporate_actions_as_of),
            "affected": [
                {
                    "symbol": item.symbol,
                    "earliest_date": item.earliest_date.isoformat(),
                    "timeframes": list(item.timeframes),
                }
                for item in revision.affected
            ],
            "artifacts": [asdict(item) for item in revision.artifacts],
        }
        return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)

    @staticmethod
    def _write_immutable(path: Path, payload: bytes) -> None:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        fsync_directory(path.parent)

    def _replace_current(self, payload: bytes) -> None:
        temp_path = self.revisions_dir / f".current.{os.getpid()}.{time.time_ns()}.tmp"
        try:
            with temp_path.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.current_path)
            fsync_directory(self.revisions_dir)
        finally:
            temp_path.unlink(missing_ok=True)

    @contextmanager
    def _lock(self) -> Iterator[None]:
        root = self.root.resolve()
        with _THREAD_LOCKS_GUARD:
            thread_lock = _THREAD_LOCKS.setdefault(root, threading.RLock())
        with thread_lock:
            self.root.mkdir(parents=True, exist_ok=True)
            with self.lock_path.open("a", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
