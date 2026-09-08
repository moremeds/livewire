from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.duckdb_catalog import connect, ensure_view, read_symbols
from clients.silver_snapshot import SilverSnapshot


def _revision(root, revision, value):
    artifacts = []
    for _kind, suffix in (
        ("daily", "asset_class=equity/symbol=TEST/1d.parquet"),
        ("factors", "adjustments/asset_class=equity/symbol=TEST/factors.parquet"),
    ):
        relative = f"generations/{revision}/{suffix}"
        path = root / relative
        path.parent.mkdir(parents=True)
        pq.write_table(pa.table({"value": [value]}), path)
        artifacts.append({"path": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    payload = {"schema_version": 1, "revision": revision, "artifacts": artifacts, "affected": [{"symbol": "TEST"}]}
    _commit(root, payload)
    return payload


def _commit(root, payload):
    revisions = root / "revisions"
    revisions.mkdir(exist_ok=True)
    data = json.dumps(payload).encode()
    (revisions / f"revision={payload['revision']}.json").write_bytes(data)
    (revisions / "current.json").write_bytes(data)


def test_connection_pins_daily_and_factors_while_next_revision_commits(tmp_path):
    _revision(tmp_path, 1, 10)
    with connect() as con:
        assert read_symbols(con, "silver_equity_1d", ["TEST"], silver_root=tmp_path).fetchone()[0] == 10
        _revision(tmp_path, 2, 20)
        ensure_view(con, "silver_factors", silver_root=tmp_path)
        assert con.sql("select value from silver_factors").fetchone() == (10,)
    with connect() as con:
        assert read_symbols(con, "silver_equity_1d", ["TEST"], silver_root=tmp_path).fetchone()[0] == 20


def test_removed_symbol_cannot_fall_back_to_old_generation(tmp_path):
    _revision(tmp_path, 1, 10)
    pinned = SilverSnapshot.pin(tmp_path)
    _commit(tmp_path, {"schema_version": 1, "revision": 2, "artifacts": [], "affected": []})
    assert len(pinned.files("1d", {"TEST"})) == 1
    assert SilverSnapshot.pin(tmp_path).files("1d", {"TEST"}) == []


def test_selected_artifact_hash_is_checked_without_reading_unselected(tmp_path):
    data = _revision(tmp_path, 1, 10)
    snapshot = SilverSnapshot.pin(tmp_path)
    (tmp_path / data["artifacts"][1]["path"]).write_bytes(b"broken")
    assert len(snapshot.files("1d")) == 1
    with pytest.raises(ValueError, match="checksum mismatch"):
        snapshot.files("factors")


def test_partial_silver_request_fails_instead_of_silently_omitting_symbol(tmp_path):
    _revision(tmp_path, 1, 10)
    with connect() as con, pytest.raises(FileNotFoundError, match="symbols missing"):
        read_symbols(con, "silver_equity_1d", ["TEST", "MISSING"], silver_root=tmp_path)


@pytest.mark.parametrize(
    "fault", ["pointer", "escape", "duplicate", "checksum", "kind", "partition", "revision", "pair", "affected"]
)
def test_malformed_manifest_is_rejected(tmp_path, fault):
    data = _revision(tmp_path, 1, 10)
    if fault == "pointer":
        (tmp_path / "revisions/current.json").write_bytes(b'{"schema_version":1,"revision":1,"artifacts":[]}')
    else:
        if fault == "escape":
            data["artifacts"][0]["path"] = "../asset_class=equity/symbol=TEST/1d.parquet"
        elif fault == "duplicate":
            data["artifacts"].append(data["artifacts"][0])
        elif fault == "checksum":
            data["artifacts"][0]["sha256"] = "bogus"
        elif fault == "kind":
            data["artifacts"][0]["path"] = "asset_class=equity/symbol=TEST/unknown.parquet"
        elif fault == "partition":
            data["artifacts"][0]["path"] = "bad.parquet"
        elif fault == "pair":
            data["artifacts"].pop()
        elif fault == "affected":
            data["affected"].append({"symbol": "TEST"})
        else:
            data["revision"] = 0
        _commit(tmp_path, data)
    with pytest.raises(ValueError):
        SilverSnapshot.pin(tmp_path)
