from __future__ import annotations

import json

import pytest

from clients.bronze_client import BronzeClient
from livewire_scripts import audit_split_basis, repair_split_basis
from tests.test_audit_split_basis import _seed


def _manifest(tmp_path):
    output = tmp_path / "audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(output)], data_lake_root=tmp_path) == 0
    return output


def _approve(path):
    payload = json.loads(path.read_text())
    payload["symbols"][0]["approved"] = True
    path.write_text(json.dumps(payload, sort_keys=True))


def test_unapproved_manifest_does_not_mutate_bronze(tmp_path):
    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    before = bronze_path.read_bytes()

    with pytest.raises(ValueError, match="approved"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    assert bronze_path.read_bytes() == before


def test_stale_manifest_is_rejected(tmp_path):
    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    _approve(manifest)
    bronze_path.write_bytes(bronze_path.read_bytes() + b"stale")
    stale = bronze_path.read_bytes()

    with pytest.raises(ValueError, match="stale"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    assert bronze_path.read_bytes() == stale


def test_apply_and_rollback_restore_exact_bytes(tmp_path):
    bronze_path = _seed(tmp_path)
    original = bronze_path.read_bytes()
    manifest = _manifest(tmp_path)
    _approve(manifest)

    assert repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path) == 0

    row = BronzeClient(bronze_path.parents[1], "equity").read_symbol_rows("AAPL")[0]
    assert row["close"] == 100.0
    assert row["source"] == "legacy"
    assert row["price_basis"] == "raw"
    assert repair_split_basis.run(["--manifest", str(manifest), "--rollback"], data_lake_root=tmp_path) == 0
    assert bronze_path.read_bytes() == original
