from __future__ import annotations

import hashlib
import multiprocessing
import os
from dataclasses import asdict
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from clients.source_evidence import SourceEvidence, SourceEvidenceStore


def evidence(ref: str, digest: str) -> SourceEvidence:
    revision_time = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    return SourceEvidence(
        ref=ref,
        sha256=digest,
        source_url="https://en.wikipedia.org/w/api.php?title=fixture",
        retrieved_at=datetime(2026, 8, 31, 1, 0, tzinfo=UTC),
        publication_time=revision_time,
        mediawiki_revision_id=123,
        mediawiki_revision_time=revision_time,
        content_type="application/json",
    )


def _record_in_process(root: str, payload: bytes) -> None:
    store = SourceEvidenceStore(root)
    artifact = store.persist_raw(payload)
    store.record(evidence(artifact.ref, artifact.sha256))


def test_persists_exact_bytes_and_canonical_parquet_manifest(tmp_path):
    store = SourceEvidenceStore(tmp_path)
    payload = b'{"exact":"response bytes"}'
    artifact = store.persist_raw(payload)
    row = evidence(artifact.ref, artifact.sha256)
    store.record(row)

    assert store.read(artifact.ref) == payload
    assert store.list_verified() == [row]
    assert pq.ParquetFile(store.manifest_path).metadata.num_rows == 1
    assert store.raw_path(artifact.sha256).stat().st_mode & 0o777 == 0o600
    assert store.manifest_path.stat().st_mode & 0o777 == 0o600


def test_duplicate_bytes_and_manifest_row_are_idempotent(tmp_path):
    store = SourceEvidenceStore(tmp_path)
    first = store.persist_raw(b"same")
    second = store.persist_raw(b"same")
    assert first == second
    row = evidence(first.ref, first.sha256)
    store.record(row)
    store.record(row)
    assert store.list_verified() == [row]


def test_repeat_persist_within_one_run_skips_the_disk_reverify(tmp_path):
    """A digest this process already confirmed on disk is not re-read+rehashed.

    Corporate-actions responses are heavily duplicated within one run (e.g.
    thousands of tickers sharing one empty-results body); re-verifying an
    already-known digest from disk on every call turned that duplication into
    O(responses) reads against a single mechanical drive. Corrupting the file
    out from under the cache and confirming persist_raw still succeeds proves
    the second call never touched disk.
    """
    store = SourceEvidenceStore(tmp_path)
    first = store.persist_raw(b"shared body")
    store.raw_path(first.sha256).write_bytes(b"corrupted")

    second = store.persist_raw(b"shared body")

    assert second == first
    assert store.raw_path(first.sha256).read_bytes() == b"corrupted"


def test_later_retrieval_of_same_source_revision_preserves_first_known_time(tmp_path):
    store = SourceEvidenceStore(tmp_path)
    artifact = store.persist_raw(b"same revision")
    first = evidence(artifact.ref, artifact.sha256)
    later = SourceEvidence(**{**first.__dict__, "retrieved_at": datetime(2026, 8, 31, 2, 0, tzinfo=UTC)})
    store.record(first)
    store.record(later)
    assert store.list_verified() == [first]


def test_same_source_revision_cannot_be_backdated(tmp_path):
    store = SourceEvidenceStore(tmp_path)
    artifact = store.persist_raw(b"same revision")
    first = evidence(artifact.ref, artifact.sha256)
    earlier = SourceEvidence(**{**first.__dict__, "retrieved_at": datetime(2026, 8, 31, 0, 0, tzinfo=UTC)})
    store.record(first)
    with pytest.raises(ValueError, match="backdated"):
        store.record(earlier)


def test_conflicting_metadata_for_the_same_artifact_is_rejected(tmp_path):
    store = SourceEvidenceStore(tmp_path)
    artifact = store.persist_raw(b"same artifact")
    row = evidence(artifact.ref, artifact.sha256)
    store.record(row)
    conflicting = SourceEvidence(**{**row.__dict__, "source_url": "https://example.test/different"})
    with pytest.raises(ValueError, match="different metadata"):
        store.record(conflicting)


def test_concurrent_processes_do_not_lose_manifest_rows(tmp_path):
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(target=_record_in_process, args=(str(tmp_path), f"payload-{index}".encode()))
        for index in range(4)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    assert len(SourceEvidenceStore(tmp_path).list_verified()) == 4


def test_rejects_declared_hash_mismatch_and_detects_tampering(tmp_path):
    store = SourceEvidenceStore(tmp_path)
    with pytest.raises(ValueError, match="declared sha256"):
        store.persist_raw(b"bytes", expected_sha256="0" * 64)

    artifact = store.persist_raw(b"trusted")
    store.record(evidence(artifact.ref, artifact.sha256))
    store.raw_path(artifact.sha256).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="hash mismatch"):
        store.read(artifact.ref)
    with pytest.raises(ValueError, match="hash mismatch"):
        store.list_verified()


def test_rejects_path_traversal_and_non_cas_references(tmp_path):
    store = SourceEvidenceStore(tmp_path)
    for ref in ["../secret", "artifact://sha256/../secret", "artifact://logical/source"]:
        with pytest.raises(ValueError, match="artifact ref"):
            store.read(ref)


def test_manifest_fsync_precedes_atomic_replace(tmp_path, monkeypatch):
    store = SourceEvidenceStore(tmp_path)
    artifact = store.persist_raw(b"fixture")
    operations: list[str] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def observed_fsync(fd: int) -> None:
        operations.append("fsync")
        real_fsync(fd)

    def observed_replace(source, destination) -> None:
        operations.append("replace")
        real_replace(source, destination)

    monkeypatch.setattr("clients.source_evidence.os.fsync", observed_fsync)
    monkeypatch.setattr("clients.source_evidence.os.replace", observed_replace)
    store.record(evidence(artifact.ref, artifact.sha256))
    assert operations.index("fsync") < operations.index("replace")


def test_ref_is_the_sha256_of_exact_payload(tmp_path):
    artifact = SourceEvidenceStore(tmp_path).persist_raw(b"alpha")
    digest = hashlib.sha256(b"alpha").hexdigest()
    assert artifact.sha256 == digest
    assert artifact.ref == f"artifact://sha256/{digest}"


class TestBatchedCommit:
    """One commit per batch is the point: per-response commits cost O(N * manifest)."""

    def test_a_batch_of_many_refs_publishes_the_manifest_once(self, tmp_path, monkeypatch):
        store = SourceEvidenceStore(tmp_path)
        batch = []
        for index in range(25):
            artifact = store.persist_raw(f"payload-{index}".encode())
            batch.append(evidence(artifact.ref, artifact.sha256))

        publishes = []
        real_publish = SourceEvidenceStore._publish_manifest
        monkeypatch.setattr(
            SourceEvidenceStore,
            "_publish_manifest",
            lambda self, rows: (publishes.append(len(rows)), real_publish(self, rows))[1],
        )
        store.record_many(batch)

        assert publishes == [25]
        assert len(store.list_verified()) == 25

    def test_duplicate_refs_inside_one_batch_collapse(self, tmp_path):
        store = SourceEvidenceStore(tmp_path)
        artifact = store.persist_raw(b'{"status":"OK","results":[]}')
        item = evidence(artifact.ref, artifact.sha256)

        store.record_many([item, item, item])

        assert len(store.list_verified()) == 1

    def test_a_ref_already_in_the_manifest_is_not_appended_again(self, tmp_path):
        store = SourceEvidenceStore(tmp_path)
        artifact = store.persist_raw(b"alpha")
        item = evidence(artifact.ref, artifact.sha256)
        store.record_many([item])

        store.record_many([item])

        assert len(store.list_verified()) == 1

    def test_an_empty_batch_touches_nothing(self, tmp_path):
        store = SourceEvidenceStore(tmp_path)
        store.record_many([])
        assert not store.manifest_path.exists()

    def test_conflicting_metadata_for_a_known_ref_still_fails_closed(self, tmp_path):
        store = SourceEvidenceStore(tmp_path)
        artifact = store.persist_raw(b"alpha")
        store.record_many([evidence(artifact.ref, artifact.sha256)])
        conflicting = SourceEvidence(
            **{**asdict(evidence(artifact.ref, artifact.sha256)), "source_url": "https://example.test/other"}
        )

        with pytest.raises(ValueError, match="already has different metadata"):
            store.record_many([conflicting])

    def test_backdating_a_known_ref_still_fails_closed(self, tmp_path):
        store = SourceEvidenceStore(tmp_path)
        artifact = store.persist_raw(b"alpha")
        item = evidence(artifact.ref, artifact.sha256)
        store.record_many([item])
        earlier = SourceEvidence(**{**asdict(item), "retrieved_at": datetime(2020, 1, 1, tzinfo=UTC)})

        with pytest.raises(ValueError, match="cannot be backdated"):
            store.record_many([earlier])

    def test_a_batch_whose_bytes_are_missing_is_rejected_before_the_lock(self, tmp_path):
        store = SourceEvidenceStore(tmp_path)
        artifact = store.persist_raw(b"alpha")
        good = evidence(artifact.ref, artifact.sha256)
        missing_digest = hashlib.sha256(b"never-persisted").hexdigest()
        missing = SourceEvidence(
            **{
                **asdict(good),
                "ref": f"artifact://sha256/{missing_digest}",
                "sha256": missing_digest,
            }
        )

        with pytest.raises(ValueError, match="artifact missing"):
            store.record_many([good, missing])
        assert not store.manifest_path.exists()
