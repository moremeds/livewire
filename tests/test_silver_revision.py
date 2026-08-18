from __future__ import annotations

import hashlib
import json
import threading
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from clients.silver_client import PublishedArtifact
from clients.silver_revision import AffectedSymbol, SilverRevisionPublisher

ACTIONS_AS_OF = datetime(2026, 7, 13, 2, 0, tzinfo=UTC)
AFFECTED = [AffectedSymbol("NVDA", date(1999, 1, 22), ("1d", "1m", "5m", "30m", "1h"))]


def _artifact(root: Path, name: str, content: bytes) -> PublishedArtifact:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return PublishedArtifact(path, hashlib.sha256(content).hexdigest(), 1)


def test_publish_creates_monotonic_immutable_manifests_and_current_pointer(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    first = publisher.publish([_artifact(tmp_path, "a.parquet", b"one")], AFFECTED, ACTIONS_AS_OF)
    second = publisher.publish([_artifact(tmp_path, "b.parquet", b"two")], AFFECTED, ACTIONS_AS_OF)

    assert (first.revision, second.revision) == (1, 2)
    assert (tmp_path / "revisions/revision=1.json").exists()
    assert (tmp_path / "revisions/revision=2.json").exists()
    current = json.loads((tmp_path / "revisions/current.json").read_text())
    assert current["revision"] == 2
    assert current["schema_version"] == 1
    assert current["affected"][0]["timeframes"] == ["1d", "1m", "5m", "30m", "1h"]
    assert current["artifacts"][0]["path"] == "b.parquet"


def test_unchanged_artifacts_are_noop(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    artifact = _artifact(tmp_path, "a.parquet", b"same")
    first = publisher.publish([artifact], AFFECTED, ACTIONS_AS_OF)
    current_bytes = (tmp_path / "revisions/current.json").read_bytes()

    second = publisher.publish([artifact], AFFECTED, ACTIONS_AS_OF.replace(minute=1))

    assert second == first
    assert (tmp_path / "revisions/current.json").read_bytes() == current_bytes
    assert not (tmp_path / "revisions/revision=2.json").exists()


def test_affected_symbols_preserve_provider_significant_case(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    affected = [
        AffectedSymbol("BCPC", date(2026, 1, 1), ("1d",)),
        AffectedSymbol("BCpC", date(2026, 1, 1), ("1d",)),
    ]

    revision = publisher.publish(
        [_artifact(tmp_path, "BCPC.parquet", b"common"), _artifact(tmp_path, "BC%70C.parquet", b"preferred")],
        affected,
        ACTIONS_AS_OF,
    )

    assert [item.symbol for item in revision.affected] == ["BCPC", "BCpC"]


def test_checksum_mismatch_preserves_current(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    publisher.publish([_artifact(tmp_path, "a.parquet", b"valid")], AFFECTED, ACTIONS_AS_OF)
    current_bytes = (tmp_path / "revisions/current.json").read_bytes()
    bad = _artifact(tmp_path, "b.parquet", b"actual")
    bad = PublishedArtifact(bad.path, "0" * 64, bad.row_count)

    with pytest.raises(ValueError, match="checksum"):
        publisher.publish([bad], AFFECTED, ACTIONS_AS_OF)

    assert (tmp_path / "revisions/current.json").read_bytes() == current_bytes
    assert not (tmp_path / "revisions/revision=2.json").exists()


def test_missing_artifact_preserves_current(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    publisher.publish([_artifact(tmp_path, "a.parquet", b"valid")], AFFECTED, ACTIONS_AS_OF)
    current_bytes = (tmp_path / "revisions/current.json").read_bytes()
    missing = PublishedArtifact(tmp_path / "missing.parquet", hashlib.sha256(b"").hexdigest(), 0)

    with pytest.raises(ValueError, match="missing"):
        publisher.publish([missing], AFFECTED, ACTIONS_AS_OF)

    assert (tmp_path / "revisions/current.json").read_bytes() == current_bytes


def test_failed_current_replace_removes_uncommitted_manifest_and_preserves_pointer(tmp_path, monkeypatch):
    publisher = SilverRevisionPublisher(tmp_path)
    publisher.publish([_artifact(tmp_path, "a.parquet", b"valid")], AFFECTED, ACTIONS_AS_OF)
    current_path = tmp_path / "revisions/current.json"
    current_bytes = current_path.read_bytes()
    real_replace = __import__("os").replace

    def fail_current(source, destination):
        if Path(destination) == current_path:
            raise RuntimeError("current replace failed")
        return real_replace(source, destination)

    monkeypatch.setattr("clients.silver_revision.os.replace", fail_current)
    with pytest.raises(RuntimeError, match="current replace"):
        publisher.publish([_artifact(tmp_path, "b.parquet", b"new")], AFFECTED, ACTIONS_AS_OF)

    assert current_path.read_bytes() == current_bytes
    assert not (tmp_path / "revisions/revision=2.json").exists()


def test_existing_next_manifest_is_immutable(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    revisions = tmp_path / "revisions"
    revisions.mkdir()
    (revisions / "revision=1.json").write_text("occupied")

    with pytest.raises(FileExistsError):
        publisher.publish([_artifact(tmp_path, "a.parquet", b"valid")], AFFECTED, ACTIONS_AS_OF)

    assert not (revisions / "current.json").exists()


def test_concurrent_publishers_serialize_revision_assignment(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    artifacts = [
        [_artifact(tmp_path, "a.parquet", b"one")],
        [_artifact(tmp_path, "b.parquet", b"two")],
    ]
    barrier = threading.Barrier(2)
    revisions: list[int] = []

    def publish(index):
        barrier.wait()
        revisions.append(publisher.publish(artifacts[index], AFFECTED, ACTIONS_AS_OF).revision)

    threads = [threading.Thread(target=publish, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(revisions) == [1, 2]
    assert json.loads((tmp_path / "revisions/current.json").read_text())["revision"] == 2


@pytest.mark.parametrize(
    "affected, message",
    [
        ([], "affected"),
        ([*AFFECTED, *AFFECTED], "duplicate affected"),
        ([AffectedSymbol("NVDA", date(1999, 1, 22), ("1d", "1d"))], "duplicate timeframe"),
        ([AffectedSymbol("NVDA", date(1999, 1, 22), ("2h",))], "unsupported timeframe"),
    ],
)
def test_invalid_affected_contract_is_rejected(tmp_path, affected, message):
    publisher = SilverRevisionPublisher(tmp_path)
    with pytest.raises(ValueError, match=message):
        publisher.publish([_artifact(tmp_path, "a.parquet", b"valid")], affected, ACTIONS_AS_OF)
    assert not (tmp_path / "revisions/current.json").exists()


def test_an_identical_manifest_commits_as_a_noop_instead_of_crashing(tmp_path):
    """A quiet night rebuilds byte-identical artifacts. The publisher dedupes that by
    returning the CURRENT revision, leaving the transaction's reservation unused --
    which crashed the 2026-08-17 nightly Silver rebuild with
    ``reserved Silver revision was not committed``."""
    publisher = SilverRevisionPublisher(tmp_path)
    artifact = _artifact(tmp_path, "a.parquet", b"same")
    first = publisher.publish([artifact], AFFECTED, ACTIONS_AS_OF)

    with publisher.transaction() as transaction:
        assert transaction.revision == first.revision + 1
        result = transaction.commit([artifact], AFFECTED, ACTIONS_AS_OF)

    assert result.revision == first.revision
    assert not (tmp_path / f"revisions/revision={first.revision + 1}.json").exists()
    assert json.loads((tmp_path / "revisions/current.json").read_text())["revision"] == first.revision


def test_a_changed_manifest_still_commits_the_reserved_revision(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    first = publisher.publish([_artifact(tmp_path, "a.parquet", b"one")], AFFECTED, ACTIONS_AS_OF)

    with publisher.transaction() as transaction:
        result = transaction.commit([_artifact(tmp_path, "b.parquet", b"two")], AFFECTED, ACTIONS_AS_OF)

    assert result.revision == transaction.revision == first.revision + 1


def test_a_revision_moved_by_another_writer_is_still_fatal(tmp_path):
    publisher = SilverRevisionPublisher(tmp_path)
    publisher.publish([_artifact(tmp_path, "a.parquet", b"one")], AFFECTED, ACTIONS_AS_OF)

    with pytest.raises(RuntimeError, match="reserved Silver revision was not committed"):
        with publisher.transaction() as transaction:
            # Another process advanced current.json while we held our reservation.
            # Written through the already-locked path: publish() would re-take the
            # cross-process flock this transaction is holding and deadlock the test.
            publisher._publish_locked([_artifact(tmp_path, "b.parquet", b"two")], AFFECTED, ACTIONS_AS_OF)
            transaction.commit([_artifact(tmp_path, "c.parquet", b"three")], AFFECTED, ACTIONS_AS_OF)
