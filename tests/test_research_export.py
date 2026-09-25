"""Real frozen GLD evidence; no network and no invented market values."""

import gzip
import hashlib
import json
import os
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.research_export import export_gld_research

CAPTURE = Path(__file__).resolve().parents[1] / "docs/audits/price-discovery/2026-09-22-capture"


def test_capture_is_immutable_verified_and_honest_about_unknown_lineage(tmp_path, monkeypatch):
    root = tmp_path / "silver"
    (root / "revisions").mkdir(parents=True)
    original = gzip.decompress((CAPTURE / "silver-revision-76.json.gz").read_bytes())
    manifest = json.loads(original)
    for entry in manifest["artifacts"]:
        if "/symbol=GLD/" in entry["path"]:
            source = "GLD-factors.parquet" if entry["path"].endswith("factors.parquet") else "GLD-silver-1d.parquet"
            target = root / entry["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((CAPTURE / source).read_bytes())
    (root / "revisions/current.json").write_bytes(original)
    (root / "revisions/revision=76.json").write_bytes(original)
    bronze = CAPTURE / "GLD-bronze-1d.parquet"
    capture = export_gld_research(root, bronze, tmp_path / "exports")
    checksum = hashlib.sha256(capture.read_bytes()).hexdigest()
    assert export_gld_research(root, bronze, tmp_path / "exports") == capture
    assert hashlib.sha256(capture.read_bytes()).hexdigest() == checksum

    # A failed publication leaves the already complete capture untouched.
    def fail_publish(*args):
        raise OSError("simulated publish failure")

    with monkeypatch.context() as patch:
        patch.setattr("clients.research_export.os.link", fail_publish)
        with pytest.raises(OSError, match="simulated publish"):
            export_gld_research(root, bronze, tmp_path / "failed")
    assert list((tmp_path / "failed").iterdir()) == []
    real_link = os.link

    def concurrent_publish(source, destination):
        real_link(source, destination)
        raise FileExistsError("another writer published the same complete capture")

    with monkeypatch.context() as patch:
        patch.setattr("clients.research_export.os.link", concurrent_publish)
        assert export_gld_research(root, bronze, tmp_path / "concurrent").is_file()
    empty = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"trade_date": pa.array([], type=pa.date32())}), empty)
    with pytest.raises(ValueError, match="empty research input"):
        export_gld_research(root, empty, tmp_path / "empty-output")
    with zipfile.ZipFile(capture) as archive:
        metadata = json.loads(archive.read("capture.json"))
        assert metadata["available_at"] is None and metadata["historical_pit"] is False
        assert metadata["silver_upstream_provider"] is None
        assert [item["provider"] for item in metadata["bronze_source_segments"]] == ["ib", "legacy", "massive"]
        assert archive.read("silver-manifest.json") == original
        entries = {name: archive.read(name) for name in archive.namelist()}
    # Corrupt an existing destination copy; retries must reject, not replace it.
    damaged = tmp_path / "damaged"
    damaged.mkdir()
    with zipfile.ZipFile(damaged / capture.name, "w") as archive:
        for name, payload in entries.items():
            archive.writestr(name, b"corrupt" if name == "bronze-GLD.parquet" else payload)
    with pytest.raises(ValueError, match="existing capture checksum"):
        export_gld_research(root, bronze, damaged)
    bad_metadata = tmp_path / "bad-metadata"
    bad_metadata.mkdir()
    with zipfile.ZipFile(bad_metadata / capture.name, "w") as archive:
        metadata["capture_id"] = "wrong"
        archive.writestr("capture.json", json.dumps(metadata))
    with pytest.raises(ValueError, match="existing capture metadata"):
        export_gld_research(root, bronze, bad_metadata)
    changed = json.loads(original)
    changed["affected"] = []
    changed["artifacts"] = []
    for filename in ("current.json", "revision=76.json"):
        (root / "revisions" / filename).write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="missing GLD"):
        export_gld_research(root, bronze, tmp_path / "missing")
    for filename in ("current.json", "revision=76.json"):
        (root / "revisions" / filename).write_bytes(original)
    target.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        export_gld_research(root, bronze, tmp_path / "exports")
    assert hashlib.sha256(capture.read_bytes()).hexdigest() == checksum
