"""Immutable source evidence storage for Livewire Shepherd inputs.

Exact response bytes are stored by SHA-256.  The normalized manifest is
canonical Parquet metadata; consumers must verify the referenced bytes before
using a row as evidence or exposing it through a query layer.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock, get_ident

import pyarrow as pa
import pyarrow.parquet as pq

_CAS_REF = re.compile(r"^artifact://sha256/([0-9a-f]{64})$")

_MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("ref", pa.string(), nullable=False),
        pa.field("sha256", pa.string(), nullable=False),
        pa.field("source_url", pa.string(), nullable=False),
        pa.field("retrieved_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("publication_time", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("mediawiki_revision_id", pa.int64(), nullable=True),
        pa.field("mediawiki_revision_time", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("content_type", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class RawArtifact:
    """A content-addressed exact-byte artifact."""

    ref: str
    sha256: str
    size: int


@dataclass(frozen=True)
class SourceEvidence:
    """Normalized provenance for one immutable source response."""

    ref: str
    sha256: str
    source_url: str
    retrieved_at: datetime
    publication_time: datetime | None
    mediawiki_revision_id: int | None
    mediawiki_revision_time: datetime | None
    content_type: str


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a", encoding="utf-8") as lock_file:
        os.chmod(path, 0o600)
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


class SourceEvidenceStore:
    """Persist exact bytes and their canonical Parquet evidence manifest."""

    def __init__(self, data_lake_root: str | Path) -> None:
        self.data_lake_root = Path(data_lake_root).expanduser()
        self.raw_root = self.data_lake_root / "raw" / "shepherd" / "sha256"
        self.manifest_path = self.data_lake_root / "raw" / "shepherd" / "source_evidence.parquet"
        self.raw_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.raw_root, 0o700)
        # Digests this process has already confirmed on disk. Content-addressed
        # storage is immutable, so a digest verified once needs no second
        # read+rehash for the rest of the run -- skipping it is what turns a
        # mostly-duplicate-response run (e.g. yesterday's unchanged filings)
        # from O(responses) disk reads into O(distinct payloads).
        self._known_lock = Lock()
        self._known: set[str] = set()
        # Shard directories written since the last commit, fsynced in one pass
        # by `record_many`. See `persist_raw` for what that trades.
        self._unsynced_dirs: set[Path] = set()

    def _shard_path(self, sha256: str) -> Path:
        if re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise ValueError("invalid artifact sha256")
        return self.raw_root / sha256[0:2] / sha256[2:4] / sha256

    def raw_path(self, sha256: str) -> Path:
        """Resolve a digest to its artifact: sharded first, legacy flat second.

        Artifacts written before 2026-09-05 sit directly in `raw_root`; 137,504
        of them, and they are provider bytes that can never be refetched, so
        they stay readable in place rather than being migrated on a hot path.
        A digest with neither file resolves to the sharded path, which is where
        a new artifact is written.
        """
        sharded = self._shard_path(sha256)
        if sharded.exists():
            return sharded
        legacy = self.raw_root / sha256
        if legacy.exists():
            return legacy
        return sharded

    def persist_raw(self, payload: bytes, expected_sha256: str | None = None) -> RawArtifact:
        digest = hashlib.sha256(payload).hexdigest()
        if expected_sha256 is not None and expected_sha256 != digest:
            raise ValueError("declared sha256 does not match exact payload")

        with self._known_lock:
            already_known = digest in self._known
        if already_known:
            return RawArtifact(ref=f"artifact://sha256/{digest}", sha256=digest, size=len(payload))

        # Sharded path only, deliberately: a `raw_path` fallback here would put a
        # lookup in the 137k-entry legacy directory back on the write path, and
        # an exFAT directory op is linear in entry count -- the whole cost this
        # sharding removes. An artifact that already exists flat is simply
        # written again into its shard; the bytes are identical by construction.
        destination = self._shard_path(digest)
        if destination.exists():
            self._verify_path(destination, digest)
        else:
            shard = destination.parent
            shard.mkdir(parents=True, exist_ok=True, mode=0o700)
            # No per-artifact lock file. Content-addressed writes are idempotent,
            # so two racing writers produce byte-identical files and `os.replace`
            # is atomic; the lock only ever serialized them. The old lock file was
            # never unlinked -- 137,504 orphans, swept by `housekeeping
            # --evidence-locks`.
            temp_path = shard / f".{digest}.{os.getpid()}.{get_ident()}.{time.time_ns()}.tmp"
            try:
                with temp_path.open("xb") as output:
                    os.chmod(temp_path, 0o600)
                    output.write(payload)
                    output.flush()
                    os.fsync(output.fileno())
                self._verify_path(temp_path, digest)
                os.replace(temp_path, destination)
                os.chmod(destination, 0o600)
            finally:
                temp_path.unlink(missing_ok=True)
            # ponytail: the directory entry is fsynced once per commit instead of
            # once per artifact. Trade: a power loss between the write and the
            # commit can lose the *link* to bytes that are themselves durable, and
            # the manifest row that would have referenced them is lost with it --
            # so the lake stays self-consistent and the response is refetched.
            # Per-artifact it cost 29.6k directory fsyncs a night against one
            # exFAT directory.
            with self._known_lock:
                self._unsynced_dirs.add(shard)
        with self._known_lock:
            self._known.add(digest)
        return RawArtifact(ref=f"artifact://sha256/{digest}", sha256=digest, size=len(payload))

    def record(self, evidence: SourceEvidence) -> None:
        self.record_many([evidence])

    def record_many(self, evidence: Sequence[SourceEvidence]) -> None:
        """Commit a batch of evidence under one lock, one read and one write.

        The manifest is read whole and rewritten whole, so the cost of a commit
        is O(manifest). Committing per response made a caller that records N
        responses pay O(N * manifest) with every call serialized on the same
        global lock -- measured at 2.8 us/row/call, which is 41 minutes a night
        for the ~29.6k corporate-action responses against a ~29.6k-row manifest.
        Batching makes the same run pay that cost once.
        """
        self._sync_written_dirs()
        if not evidence:
            return
        for item in evidence:
            digest = self._digest_from_ref(item.ref)
            if digest != item.sha256:
                raise ValueError("evidence ref and sha256 disagree")
            with self._known_lock:
                known = digest in self._known
            # A digest this process wrote and verified needs no second
            # read+rehash: content-addressed storage is immutable, and re-reading
            # every pending artifact at commit was a second full pass over the
            # night's 29.6k responses.
            if not known:
                self._verify_path(self.raw_path(digest), digest)

        lock_path = self.manifest_path.with_suffix(".parquet.lock")
        with _exclusive_lock(lock_path):
            rows = self._read_manifest_rows()
            by_ref: dict[str, dict[str, object]] = {}
            for row in rows:
                ref = row["ref"]
                if not isinstance(ref, str):
                    raise ValueError("evidence manifest row has an invalid ref")
                if ref in by_ref:
                    raise ValueError("evidence ref has ambiguous metadata")
                by_ref[ref] = row
            appended = False
            for item in evidence:
                serialized = asdict(item)
                existing = by_ref.get(item.ref)
                if existing is None:
                    rows.append(serialized)
                    by_ref[item.ref] = serialized
                    appended = True
                    continue
                self._assert_same_evidence(existing, serialized)
            if appended:
                self._publish_manifest(rows)

    @staticmethod
    def _assert_same_evidence(existing: dict[str, object], incoming: dict[str, object]) -> None:
        immutable_existing = {key: value for key, value in existing.items() if key != "retrieved_at"}
        immutable_new = {key: value for key, value in incoming.items() if key != "retrieved_at"}
        if immutable_existing != immutable_new:
            raise ValueError("evidence ref already has different metadata")
        existing_retrieved_at = existing["retrieved_at"]
        if not isinstance(existing_retrieved_at, datetime):
            raise ValueError("evidence ref has invalid retrieval metadata")
        incoming_retrieved_at = incoming["retrieved_at"]
        if not isinstance(incoming_retrieved_at, datetime):
            raise ValueError("evidence ref has invalid retrieval metadata")
        if incoming_retrieved_at < existing_retrieved_at:
            raise ValueError("evidence ref cannot be backdated before its first retrieval")

    def read(self, ref: str) -> bytes:
        digest = self._digest_from_ref(ref)
        path = self.raw_path(digest)
        self._verify_path(path, digest)
        return path.read_bytes()

    def list_verified(self) -> list[SourceEvidence]:
        rows = self._read_manifest_rows()
        evidence = [SourceEvidence(**row) for row in rows]
        for item in evidence:
            digest = self._digest_from_ref(item.ref)
            if digest != item.sha256:
                raise ValueError("evidence ref and sha256 disagree")
            self._verify_path(self.raw_path(digest), digest)
        return evidence

    def _sync_written_dirs(self) -> None:
        """fsync every shard directory written since the last commit."""
        with self._known_lock:
            pending = sorted(self._unsynced_dirs)
            self._unsynced_dirs.clear()
        for directory in pending:
            _fsync_directory(directory)

    @staticmethod
    def _digest_from_ref(ref: str) -> str:
        match = _CAS_REF.fullmatch(ref)
        if match is None:
            raise ValueError("invalid artifact ref")
        return match.group(1)

    @staticmethod
    def _verify_path(path: Path, digest: str) -> None:
        if not path.is_file():
            raise ValueError(f"artifact missing for sha256 {digest}")
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != digest:
            raise ValueError(f"artifact hash mismatch: expected {digest}, got {actual}")

    def _read_manifest_rows(self) -> list[dict[str, object]]:
        if not self.manifest_path.exists():
            return []
        table = pq.ParquetFile(self.manifest_path).read()
        if table.schema != _MANIFEST_SCHEMA:
            raise ValueError("source evidence manifest schema mismatch")
        return table.to_pylist()

    def _publish_manifest(self, rows: list[dict[str, object]]) -> None:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.manifest_path.parent, 0o700)
        table = pa.Table.from_pylist(rows, schema=_MANIFEST_SCHEMA)
        temp_path = self.manifest_path.with_name(f".{self.manifest_path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            pq.write_table(table, temp_path, compression="zstd", compression_level=3)
            os.chmod(temp_path, 0o600)
            with temp_path.open("rb") as manifest:
                os.fsync(manifest.fileno())
            written = pq.ParquetFile(temp_path).read()
            if written.schema != _MANIFEST_SCHEMA or written.num_rows != len(rows):
                raise ValueError("source evidence manifest validation failed")
            os.replace(temp_path, self.manifest_path)
            os.chmod(self.manifest_path, 0o600)
            _fsync_directory(self.manifest_path.parent)
        finally:
            temp_path.unlink(missing_ok=True)
