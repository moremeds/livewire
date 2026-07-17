"""Unit tests for rollback-legacy-basis.

Reuses the repair suite's real NVDA fixtures (real closes around its real
2021-07-20 4:1 split, frozen 2026-07-17). No network, no IB.
"""

import hashlib
import json

import pytest

from clients.bronze_client import BronzeClient
from livewire_scripts import repair_legacy_basis, rollback_legacy_basis
from tests.test_repair_legacy_basis import (
    _audit_manifest,
    _clean_ib_fetcher,
    _clean_ib_rows_for,
    _seed_mixed,
)


def _repair(tmp_path):
    """Seed a mixed NVDA, repair it for real, and return (bronze_path, output_dir)."""
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    path = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").symbol_path("NVDA")
    before = path.read_bytes()
    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
    )
    assert rc == 0
    assert path.read_bytes() != before  # guard: nothing to roll back otherwise
    return path, output_dir, before


def test_rollback_restores_the_original_bytes(tmp_path):
    path, output_dir, before = _repair(tmp_path)
    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 0
    assert path.read_bytes() == before


def test_rollback_rejects_a_different_active_root(tmp_path):
    _, output_dir, _ = _repair(tmp_path)
    other = tmp_path / "other-lake"
    other.mkdir()
    with pytest.raises(ValueError, match="does not match active root"):
        rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=other)


def test_rollback_refuses_a_tampered_backup(tmp_path):
    path, output_dir, _ = _repair(tmp_path)
    repaired = path.read_bytes()
    backup = output_dir / "backup" / "NVDA.1d.parquet"
    backup.write_bytes(backup.read_bytes() + b"\0")
    with pytest.raises(ValueError, match="backup checksum mismatch"):
        rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path)
    assert path.read_bytes() == repaired  # refused, and left bronze alone


def test_rollback_restores_only_the_requested_tickers(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    _seed_mixed(tmp_path, "AMD")
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    manifest_path = tmp_path / "audit.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "data_lake_root": str(tmp_path.resolve()),
                "symbols": [
                    {
                        "symbol": s,
                        "path": str(bronze.symbol_path(s)),
                        "source_sha256": hashlib.sha256(bronze.symbol_path(s).read_bytes()).hexdigest(),
                        "klass": "mixed",
                        "break_date": "2021-06-18",
                    }
                    for s in ("NVDA", "AMD")
                ],
            }
        )
    )
    output_dir = tmp_path / "out"
    originals = {s: bronze.symbol_path(s).read_bytes() for s in ("NVDA", "AMD")}
    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest_path), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({s: _clean_ib_rows_for(s) for s in ("NVDA", "AMD")}),
    )
    repaired = {s: bronze.symbol_path(s).read_bytes() for s in ("NVDA", "AMD")}
    assert all(repaired[s] != originals[s] for s in ("NVDA", "AMD"))

    assert (
        rollback_legacy_basis.run(["--output-dir", str(output_dir), "--tickers", "NVDA"], data_lake_root=tmp_path) == 0
    )

    assert bronze.symbol_path("NVDA").read_bytes() == originals["NVDA"]
    assert bronze.symbol_path("AMD").read_bytes() == repaired["AMD"]  # untouched


def test_rollback_reports_a_missing_backup_and_exits_nonzero(tmp_path):
    path, output_dir, _ = _repair(tmp_path)
    repaired = path.read_bytes()
    (output_dir / "backup" / "NVDA.1d.parquet").unlink()

    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 1

    assert path.read_bytes() == repaired  # nothing restored, nothing corrupted


def test_rollback_without_a_repair_cursor_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="no repair cursor"):
        rollback_legacy_basis.run(["--output-dir", str(tmp_path / "nope")], data_lake_root=tmp_path)


def test_main_delegates_to_run(monkeypatch):
    seen = {}

    def _fake_run(argv):
        seen["argv"] = argv
        return 0

    monkeypatch.setattr(rollback_legacy_basis, "run", _fake_run)
    assert rollback_legacy_basis.main(["--output-dir", "out"]) == 0
    assert seen["argv"] == ["--output-dir", "out"]
