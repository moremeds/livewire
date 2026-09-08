from __future__ import annotations

import errno
import hashlib
import json
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import fields
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from clients.silver_client import PublishedArtifact, SilverClient
from clients.silver_revision import AffectedSymbol, SilverRevisionPublisher
from clients.silver_snapshot import SilverSnapshot
from livewire_scripts import rebuild_silver
from tests.test_rebuild_silver import _seed_bronze


def _manifest(silver: Path) -> dict:
    return json.loads((silver / "revisions/current.json").read_text())


def _path(silver: Path, symbol: str, suffix: str) -> Path:
    entry = next(
        item
        for item in _manifest(silver)["artifacts"]
        if f"symbol={symbol}" in item["path"] and item["path"].endswith(suffix)
    )
    return silver / entry["path"]


def _artifact(root: Path, generation: str, name: str, body: bytes) -> PublishedArtifact:
    path = root / "generations" / generation / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return PublishedArtifact(path, hashlib.sha256(body).hexdigest(), 1)


def test_current_swap_failure_keeps_old_pointer_and_retry_quarantines_manifest(tmp_path, monkeypatch):
    publisher = SilverRevisionPublisher(tmp_path)
    affected = [AffectedSymbol("AAPL", date(2024, 1, 2), ("1d",))]
    first = _artifact(tmp_path, "attempt-1", "a.parquet", b"old")
    publisher.publish([first], affected, datetime(2026, 9, 8, tzinfo=UTC), generation_id="attempt-1")
    current_before = (tmp_path / "revisions/current.json").read_bytes()
    second = _artifact(tmp_path, "attempt-2", "b.parquet", b"new")
    real_replace = __import__("os").replace

    def fail_pointer(source, destination):
        if Path(destination) == publisher.current_path:
            raise RuntimeError("stop before pointer")
        return real_replace(source, destination)

    monkeypatch.setattr("clients.silver_revision.os.replace", fail_pointer)
    with pytest.raises(RuntimeError, match="stop before pointer"):
        publisher.publish([second], affected, datetime(2026, 9, 8, tzinfo=UTC), generation_id="attempt-2")
    assert (tmp_path / "revisions/current.json").read_bytes() == current_before
    assert (tmp_path / "revisions/revision=2.json").is_file()
    monkeypatch.setattr("clients.silver_revision.os.replace", real_replace)

    third = _artifact(tmp_path, "attempt-3", "c.parquet", b"retry")
    result = publisher.publish([third], affected, datetime(2026, 9, 8, tzinfo=UTC), generation_id="attempt-3")

    assert result.revision == 2
    assert list((tmp_path / "revisions/quarantine").glob("revision=2.json.*.orphan"))
    assert second.path.read_bytes() == b"new"


def test_rebuild_preserves_bytes_pinned_by_prior_revision(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))
    old_manifest = _manifest(silver)
    publisher = SilverRevisionPublisher(silver)
    previous = publisher.read_current()
    assert previous is not None
    old_path = _path(silver, "AAPL", "/1d.parquet")
    old_bytes = old_path.read_bytes()

    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 12.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert old_path.read_bytes() == old_bytes
    assert (silver / old_manifest["artifacts"][0]["path"]).is_file()
    assert _path(silver, "AAPL", "/1d.parquet") != old_path

    # Restore retained data through a new commit; never rewind current.json.
    newer_snapshot = SilverSnapshot.pin(silver)
    restored = publisher.publish(
        [PublishedArtifact(silver / item.path, item.sha256, 0) for item in previous.artifacts],
        list(previous.affected),
        previous.corporate_actions_as_of,
        generation_id="restore-retained-data",
    )
    assert restored.revision == newer_snapshot.revision + 1
    assert SilverSnapshot.pin(silver).files("1d") == [str(old_path)]
    assert Path(newer_snapshot.files("1d")[0]).is_file()
    assert old_path.read_bytes() == old_bytes


def test_rebuild_preserves_provider_significant_case(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    for symbol, close in (("BCPC", 10.0), ("BCpC", 20.0)):
        _seed_bronze(root, symbol, [("2024-01-02", close)])
    kwargs = {"data_lake_root": root, "silver_root": silver, "as_of_date": date(2026, 9, 8)}
    rebuild_silver.run(["--full"], **kwargs)
    original = SilverSnapshot.pin(silver)
    _seed_bronze(root, "BCpC", [("2024-01-02", 21.0)])
    rebuild_silver.run(["--tickers", "BCpC"], **kwargs)
    changed = SilverSnapshot.pin(silver)
    assert changed.files("1d", {"BCPC"}) == original.files("1d", {"BCPC"})
    assert changed.files("1d", {"BCpC"}) != original.files("1d", {"BCpC"})
    triage = root / "triage.json"
    triage.write_text(json.dumps({"verdicts": [{"symbol": "BCpC", "verdict": "real_move", "date": "2024-01-02"}]}))
    assert rebuild_silver._load_keep_dates(root, triage) == {"BCpC": {"2024-01-02"}}
    (root / "bronze/asset_class=equity/symbol=BC%70C/1d.parquet").write_bytes(b"corrupt fixture")
    rebuild_silver.run(["--full"], **kwargs)
    assert {item.symbol for item in SilverSnapshot.pin(silver).artifacts} == {"BCPC"}


def test_tampered_carried_reference_never_blesses_disk_bytes(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 20.0), ("2024-01-03", 21.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))
    current_before = (silver / "revisions/current.json").read_bytes()
    carried = _path(silver, "MSFT", "/factors.parquet")
    pq.write_table(pq.ParquetFile(carried).read(), carried, compression="gzip")
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 12.0)])

    with pytest.raises(ValueError, match="checksum mismatch"):
        rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert (silver / "revisions/current.json").read_bytes() == current_before


def test_full_rebuild_migrates_legacy_fixed_paths_into_generation(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))
    payload = _manifest(silver)
    for entry in payload["artifacts"]:
        source = silver / entry["path"]
        relative = Path(*Path(entry["path"]).parts[2:])
        destination = silver / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        entry["path"] = relative.as_posix()
        entry["sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
    legacy = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (silver / "revisions/current.json").write_bytes(legacy)
    (silver / "revisions/revision=1.json").write_bytes(legacy)

    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert _manifest(silver)["revision"] == 2
    assert all(item["path"].startswith("generations/") for item in _manifest(silver)["artifacts"])


def test_targeted_rebuild_migrates_carried_legacy_pairs_without_changing_them(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 20.0), ("2024-01-03", 21.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    payload = _manifest(silver)
    fixed_msft: dict[Path, bytes] = {}
    for entry in payload["artifacts"]:
        source = silver / entry["path"]
        relative = Path(*Path(entry["path"]).parts[2:])
        destination = silver / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        entry["path"] = relative.as_posix()
        entry["sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        if "symbol=MSFT" in entry["path"]:
            fixed_msft[destination] = destination.read_bytes()
    legacy = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (silver / "revisions/current.json").write_bytes(legacy)
    (silver / "revisions/revision=1.json").write_bytes(legacy)

    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 12.0)])
    rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    manifest = _manifest(silver)
    assert manifest["revision"] == 2
    assert all(entry["path"].startswith("generations/") for entry in manifest["artifacts"])
    assert {path: path.read_bytes() for path in fixed_msft} == fixed_msft
    msft_hashes = {entry["sha256"] for entry in manifest["artifacts"] if "symbol=MSFT" in entry["path"]}
    assert msft_hashes == {hashlib.sha256(contents).hexdigest() for contents in fixed_msft.values()}


def test_full_rebuild_omits_symbol_removed_from_bronze_without_moving_old_generation(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    for symbol, price in (("AAPL", 10.0), ("MSFT", 20.0)):
        _seed_bronze(root, symbol, [("2024-01-02", price), ("2024-01-03", price + 1)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))
    old_msft = _path(silver, "MSFT", "/1d.parquet")
    shutil.rmtree(root / "bronze/asset_class=equity/symbol=MSFT")

    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert not any("symbol=MSFT" in item["path"] for item in _manifest(silver)["artifacts"])
    assert old_msft.is_file()


def test_input_lock_is_released_before_adjustment_work(tmp_path, monkeypatch):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    real_lock = rebuild_silver.silver_input_lock
    real_adjust = rebuild_silver.adjust_daily_rows
    held = False

    @contextmanager
    def tracked_lock(path):
        nonlocal held
        with real_lock(path):
            held = True
            try:
                yield
            finally:
                held = False

    def checked_adjust(*args, **kwargs):
        assert not held
        return real_adjust(*args, **kwargs)

    monkeypatch.setattr(rebuild_silver, "silver_input_lock", tracked_lock)
    monkeypatch.setattr(rebuild_silver, "adjust_daily_rows", checked_adjust)

    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))


def test_input_snapshot_stays_fixed_after_the_exclusive_lock_releases(tmp_path, monkeypatch):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    real_lock = rebuild_silver.silver_input_lock

    @contextmanager
    def changing_source(path):
        with real_lock(path):
            yield
        _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 99.0)])

    monkeypatch.setattr(rebuild_silver, "silver_input_lock", changing_source)
    rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    rows = pq.ParquetFile(_path(silver, "AAPL", "/1d.parquet")).read().to_pylist()
    assert [str(row["trade_date"]) for row in rows] == ["2024-01-02", "2024-01-03"]


def test_staged_symbols_hold_temp_paths_instead_of_row_lists(tmp_path, monkeypatch):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    captured = []

    def inspect_staged(_client, item, _current, _index):
        captured.append(item)
        assert item.rows_path.is_file()
        return False

    monkeypatch.setattr(rebuild_silver, "_matches_existing", inspect_staged)
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert captured
    assert "rows" not in {field.name for field in fields(rebuild_silver.StagedSymbol)}
    assert all(item.rows_path.suffix == ".parquet" for item in captured)


def test_staging_write_failure_isolated_and_owned_scratch_is_cleaned(tmp_path, monkeypatch):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 20.0), ("2024-01-03", 21.0)])
    real_stage = rebuild_silver._stage_rows
    real_tempdir = tempfile.TemporaryDirectory
    scratch_paths = []

    class TrackingTemporaryDirectory:
        def __init__(self, *args, **kwargs):
            self._inner = real_tempdir(*args, **kwargs)

        def __enter__(self):
            path = self._inner.__enter__()
            scratch_paths.append(Path(path))
            return path

        def __exit__(self, *args):
            return self._inner.__exit__(*args)

    def fail_one_stage(path, rows):
        if path.name == "AAPL.parquet":
            raise OSError("scratch write failed")
        return real_stage(path, rows)

    monkeypatch.setattr(rebuild_silver.tempfile, "TemporaryDirectory", TrackingTemporaryDirectory)
    monkeypatch.setattr(rebuild_silver, "_stage_rows", fail_one_stage)
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert any("symbol=MSFT" in entry["path"] for entry in _manifest(silver)["artifacts"])
    assert not any("symbol=AAPL" in entry["path"] for entry in _manifest(silver)["artifacts"])
    assert scratch_paths and not scratch_paths[0].exists()


def test_snapshot_enospc_aborts_before_publishing_a_partial_input_set(tmp_path, monkeypatch):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 20.0), ("2024-01-03", 21.0)])
    real_copy = rebuild_silver._copy_snapshot_file

    def exhaust_scratch(source, destination):
        if source.name == "1d.parquet" and "symbol=MSFT" in str(source):
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_copy(source, destination)

    monkeypatch.setattr(rebuild_silver, "_copy_snapshot_file", exhaust_scratch)
    with pytest.raises(RuntimeError, match="scratch became unavailable while copying MSFT"):
        rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert not (silver / "revisions/current.json").exists()


def test_stale_attempt_writes_no_generation_after_a_concurrent_commit(tmp_path, monkeypatch):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))
    generations_before = {path.name for path in (silver / "generations").iterdir()}
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 12.0)])
    real_adjust = rebuild_silver.adjust_daily_rows
    committed = False

    def commit_during_adjustment(*args, **kwargs):
        nonlocal committed
        if not committed:
            committed = True
            current = _manifest(silver)
            artifacts = []
            for entry in current["artifacts"]:
                source = silver / entry["path"]
                destination = silver / "generations" / "concurrent" / Path(*Path(entry["path"]).parts[2:])
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
                artifacts.append(
                    PublishedArtifact(destination, hashlib.sha256(destination.read_bytes()).hexdigest(), 0)
                )
            SilverRevisionPublisher(silver).publish(
                artifacts,
                [AffectedSymbol("AAPL", date(2024, 1, 2), ("1d", "1m", "5m", "30m", "1h"))],
                datetime.now(UTC),
                generation_id="concurrent",
            )
        return real_adjust(*args, **kwargs)

    monkeypatch.setattr(rebuild_silver, "adjust_daily_rows", commit_during_adjustment)
    with pytest.raises(RuntimeError, match="stale attempt published nothing"):
        rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    assert _manifest(silver)["revision"] == 2
    assert {path.name for path in (silver / "generations").iterdir()} == generations_before | {"concurrent"}


def test_failed_in_scope_symbol_is_omitted_while_healthy_change_advances(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 20.0), ("2024-01-03", 21.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 12.0)])
    # An empty canonical file makes MSFT fail staging while remaining discoverable.
    SilverClient(root / "bronze").daily_path("MSFT").write_bytes(b"not parquet")

    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))

    manifest = _manifest(silver)
    assert manifest["revision"] == 2
    assert any("symbol=AAPL" in item["path"] for item in manifest["artifacts"])
    assert not any("symbol=MSFT" in item["path"] for item in manifest["artifacts"])


def test_unreadable_source_failure_evidence_does_not_block_healthy_publication(tmp_path, monkeypatch):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    for symbol in ("AAPL", "RJF"):
        _seed_bronze(root, symbol, [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    original_open = Path.open

    def source_denied(path, *args, **kwargs):
        if path == root / "bronze/asset_class=equity/symbol=RJF/1d.parquet":
            raise PermissionError("source unreadable")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", source_denied)
    evidence = tmp_path / "failures.json"
    rebuild_silver.run(
        ["--full", "--failure-output", str(evidence)],
        data_lake_root=root,
        silver_root=silver,
        as_of_date=date(2026, 9, 8),
    )
    assert [item["symbol"] for item in _manifest(silver)["affected"]] == ["AAPL"]
    failure = json.loads(evidence.read_text())["failures"][0]
    assert failure["symbol"] == "RJF"
    assert failure["error_type"] == "PermissionError"
    assert failure["source_sha256"] is None


@pytest.mark.parametrize("readable_snapshot", [True, False])
def test_failure_evidence_uses_frozen_bytes_and_tolerates_bad_dates(tmp_path, monkeypatch, readable_snapshot):
    from clients.bronze_client import BronzeClient

    bronze = BronzeClient(tmp_path / "bronze", "equity")
    frozen = tmp_path / "frozen.parquet"
    frozen.write_bytes(b"frozen input")
    if not readable_snapshot:

        def denied(path):
            raise PermissionError("snapshot unreadable")

        monkeypatch.setattr(rebuild_silver, "_sha256", denied)
    failure = rebuild_silver._failure(
        "RJF",
        ValueError("invalid trade date"),
        bronze,
        [{"trade_date": "invalid"}, {"trade_date": "2024-01-02"}],
        [],
        frozen,
    )
    assert failure["error"] == "invalid trade date"
    assert failure["earliest_trade_date"] == failure["latest_trade_date"] == "2024-01-02"
    assert failure["source_sha256"] == (hashlib.sha256(b"frozen input").hexdigest() if readable_snapshot else None)


def test_generation_writer_refuses_to_overwrite_an_artifact(tmp_path):
    writer = SilverClient(tmp_path).for_generation("attempt")
    rows = [
        {
            "trade_date": date(2026, 1, 2),
            "symbol_id": 1,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "adj_close": 1.0,
            "volume": 1,
            "price_adjustment_factor": 1.0,
            "split_volume_factor": 1.0,
            "adjustment_revision": 1,
        }
    ]
    writer.publish_daily("AAPL", rows)

    with pytest.raises(FileExistsError, match="immutable"):
        writer.publish_daily("AAPL", rows)


@pytest.mark.parametrize(
    ("stop_point", "committed"),
    [
        ("daily", False),
        ("factor", False),
        ("validation", False),
        ("immutable_manifest", False),
        ("current", True),
    ],
)
def test_sigkill_at_each_publication_boundary_keeps_a_pinnable_complete_snapshot(tmp_path, stop_point, committed):
    """An OS kill may expose only the prior complete manifest or the next one.

    The child pauses inside the actual writer method after the named boundary;
    the parent then sends SIGKILL, so Python cleanup cannot make the assertion
    pass.  A real ``SilverSnapshot`` pinned before publication must remain
    readable throughout, and retrying must not need to clean the abandoned
    generation.
    """
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 20.0), ("2024-01-03", 21.0)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8))
    pinned_old = SilverSnapshot.pin(silver)
    old_aapl = pq.ParquetFile(pinned_old.files("1d", {"AAPL"})[0]).read().to_pylist()
    old_msft_path = pinned_old.files("1d", {"MSFT"})[0]

    _seed_bronze(root, "AAPL", [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 12.0)])
    ready = tmp_path / "child-ready"
    child = r"""
import os
import signal
import sys
from datetime import date
from pathlib import Path

from clients.silver_client import SilverClient
from clients.silver_revision import SilverRevisionPublisher
from livewire_scripts import rebuild_silver

stop_point, root, silver, ready = sys.argv[1:]

def halt():
    path = Path(ready)
    path.write_text("ready")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
    os.kill(os.getpid(), signal.SIGSTOP)

if stop_point == "daily":
    original = SilverClient.publish_daily
    def stopped(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        halt()
        return result
    SilverClient.publish_daily = stopped
elif stop_point == "factor":
    original = SilverClient.publish_factors
    def stopped(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        halt()
        return result
    SilverClient.publish_factors = stopped
elif stop_point == "validation":
    original = SilverRevisionPublisher._validate_affected
    def stopped(*args, **kwargs):
        result = original(*args, **kwargs)
        halt()
        return result
    SilverRevisionPublisher._validate_affected = staticmethod(stopped)
elif stop_point == "immutable_manifest":
    original = SilverRevisionPublisher._write_immutable
    def stopped(*args, **kwargs):
        result = original(*args, **kwargs)
        halt()
        return result
    SilverRevisionPublisher._write_immutable = staticmethod(stopped)
elif stop_point == "current":
    original = SilverRevisionPublisher._replace_current
    def stopped(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        halt()
        return result
    SilverRevisionPublisher._replace_current = stopped
else:
    raise ValueError(stop_point)

rebuild_silver.run(["--full"], data_lake_root=Path(root), silver_root=Path(silver), as_of_date=date(2026, 9, 8))
"""
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", child, stop_point, str(root), str(silver), str(ready)],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists(), process.communicate(timeout=1)[1]
        process.send_signal(signal.SIGKILL)
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    # The old object reads its already-pinned immutable paths, independent of
    # whether the child reached the one current-pointer commit point.
    assert pq.ParquetFile(pinned_old.files("1d", {"AAPL"})[0]).read().to_pylist() == old_aapl
    snapshot_after_kill = SilverSnapshot.pin(silver)
    assert snapshot_after_kill.revision == (2 if committed else 1)
    assert snapshot_after_kill.files("1d", {"MSFT"}) == [old_msft_path]
    after_kill_aapl = pq.ParquetFile(snapshot_after_kill.files("1d", {"AAPL"})[0]).read().to_pylist()
    assert [str(row["trade_date"]) for row in after_kill_aapl] == (
        ["2024-01-02", "2024-01-03", "2024-01-04"] if committed else ["2024-01-02", "2024-01-03"]
    )
    assert len(snapshot_after_kill.files("factors", {"AAPL"})) == 1

    assert rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 9, 8)) == 0
    retried = SilverSnapshot.pin(silver)
    assert retried.revision == 2
    retried_aapl = pq.ParquetFile(retried.files("1d", {"AAPL"})[0]).read().to_pylist()
    assert [str(row["trade_date"]) for row in retried_aapl] == ["2024-01-02", "2024-01-03", "2024-01-04"]
