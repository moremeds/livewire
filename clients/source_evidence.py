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

    def raw_path(self, sha256: str) -> Path:
        if re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise ValueError("invalid artifact sha256")
        return self.raw_root / sha256

    def persist_raw(self, payload: bytes, expected_sha256: str | None = None) -> RawArtifact:
        digest = hashlib.sha256(payload).hexdigest()
        if expected_sha256 is not None and expected_sha256 != digest:
            raise ValueError("declared sha256 does not match exact payload")

        destination = self.raw_path(digest)
        lock_path = self.raw_root / f".{digest}.lock"
        with _exclusive_lock(lock_path):
            if destination.exists():
                self._verify_path(destination, digest)
            else:
                temp_path = self.raw_root / f".{digest}.{os.getpid()}.{time.time_ns()}.tmp"
                try:
                    with temp_path.open("xb") as output:
                        os.chmod(temp_path, 0o600)
                        output.write(payload)
                        output.flush()
                        os.fsync(output.fileno())
                    self._verify_path(temp_path, digest)
                    os.replace(temp_path, destination)
                    os.chmod(destination, 0o600)
                    _fsync_directory(self.raw_root)
                finally:
                    temp_path.unlink(missing_ok=True)
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
        if not evidence:
            return
        for item in evidence:
            digest = self._digest_from_ref(item.ref)
            if digest != item.sha256:
                raise ValueError("evidence ref and sha256 disagree")
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
