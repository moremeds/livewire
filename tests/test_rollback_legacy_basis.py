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


def test_rollback_ignores_appledouble_receipt_sidecars(tmp_path):
    path, output_dir, before = _repair(tmp_path)
    appledouble = output_dir / "symbols/._NVDA.json"
    appledouble.write_bytes(b"\xff\xfeMac OS X AppleDouble")

    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 0
    assert path.read_bytes() == before


def test_rollback_refuses_to_overwrite_newer_bronze(tmp_path):
    path, output_dir, _ = _repair(tmp_path)
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    newer = {**bronze.read_symbol_rows("NVDA")[-1], "trade_date": "2026-09-08"}
    bronze.merge_ticker_rows("NVDA", [newer])
    changed = path.read_bytes()

    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 1
    assert path.read_bytes() == changed
    assert any(row["trade_date"] == "2026-09-08" for row in bronze.read_symbol_rows("NVDA"))


def test_rollback_is_idempotent_when_target_is_already_restored(tmp_path):
    path, output_dir, before = _repair(tmp_path)
    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 0
    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 0
    assert path.read_bytes() == before
    assert json.loads((output_dir / "symbols/NVDA.json").read_text())["status"] == "rolled_back"


def test_rollback_participates_in_symbol_and_snapshot_lock_boundaries(tmp_path, monkeypatch):
    from clients.parquet_io import path_lock

    path, output_dir, before = _repair(tmp_path)
    original = rollback_legacy_basis.restore_parquet_exact

    def restore(backup, target, checksum):
        for lock in (target.with_suffix(".parquet.lock"), target.parent.parent / ".inputs.lock"):
            with path_lock(lock, blocking=False) as held:
                assert not held
        return original(backup, target, checksum)

    monkeypatch.setattr(rollback_legacy_basis, "restore_parquet_exact", restore)
    rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path)
    assert path.read_bytes() == before


def test_rollback_rejects_corrupt_backup_even_with_matching_hash(tmp_path):
    path, output_dir, _ = _repair(tmp_path)
    repaired = path.read_bytes()
    sidecar_path = output_dir / "symbols/NVDA.json"
    sidecar = json.loads(sidecar_path.read_text())
    from pathlib import Path

    Path(sidecar["backup_path"]).write_bytes(b"not parquet")
    sidecar["backup_sha256"] = hashlib.sha256(b"not parquet").hexdigest()
    sidecar_path.write_text(json.dumps(sidecar))
    with pytest.raises(Exception, match="Parquet"):
        rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path)
    assert path.read_bytes() == repaired


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


def test_targeted_rollback_preserves_mixed_case_identity(tmp_path):
    symbols = ("BCPC", "BCpC")
    for symbol in symbols:
        _seed_mixed(tmp_path, symbol)
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    manifest_path = tmp_path / "audit.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "data_lake_root": str(tmp_path.resolve()),
                "symbols": [
                    {
                        "symbol": symbol,
                        "path": str(bronze.symbol_path(symbol)),
                        "source_sha256": hashlib.sha256(bronze.symbol_path(symbol).read_bytes()).hexdigest(),
                        "klass": "mixed",
                        "break_date": "2021-06-18",
                    }
                    for symbol in symbols
                ],
            }
        )
    )
    originals = {symbol: bronze.symbol_path(symbol).read_bytes() for symbol in symbols}
    output_dir = tmp_path / "out"
    assert (
        repair_legacy_basis.run(
            ["--audit-manifest", str(manifest_path), "--output-dir", str(output_dir)],
            data_lake_root=tmp_path,
            ib_factory=lambda: object(),
            ib_fetcher_factory=_clean_ib_fetcher({symbol: _clean_ib_rows_for(symbol) for symbol in symbols}),
        )
        == 0
    )
    repaired = {symbol: bronze.symbol_path(symbol).read_bytes() for symbol in symbols}

    assert (
        rollback_legacy_basis.run(["--output-dir", str(output_dir), "--tickers", "BCpC"], data_lake_root=tmp_path) == 0
    )
    assert bronze.symbol_path("BCpC").read_bytes() == originals["BCpC"]
    assert bronze.symbol_path("BCPC").read_bytes() == repaired["BCPC"]


def test_old_sidecar_without_applied_hash_allows_already_restored_noop(tmp_path):
    path, output_dir, before = _repair(tmp_path)
    path.write_bytes(before)
    sidecar_path = output_dir / "symbols/NVDA.json"
    sidecar = json.loads(sidecar_path.read_text())
    sidecar.pop("applied_sha256")
    sidecar["status"] = "in_progress"
    sidecar_path.write_text(json.dumps(sidecar))

    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 0
    assert path.read_bytes() == before


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


def test_old_in_progress_sidecar_without_applied_hash_refuses_to_guess(tmp_path):
    """Bronze is the system of record, so every mutation must be undoable by the
    supplied command — not merely by an operator who knows the backup naming scheme.
    _repair_one mutates bronze and only then returns, and the caller writes the
    terminal sidecar after that; a kill in between (OOM, power cut) used to leave
    mutated bronze plus a backup nothing pointed at, which rollback skipped because it
    only restores symbols a sidecar names. The write-ahead intent sidecar closes it."""
    bronze_path, output_dir, before = _repair(tmp_path)
    mutated = bronze_path.read_bytes()
    sidecar_path = next((output_dir / "symbols").glob("*.json"))
    sidecar = json.loads(sidecar_path.read_text())

    # Rewind to the crash window: bronze is already mutated, the backup exists, but the
    # terminal sidecar never landed — only the write-ahead intent did.
    sidecar_path.write_text(
        json.dumps(
            {
                "symbol": sidecar["symbol"],
                "status": "in_progress",
                "backup_path": sidecar["backup_path"],
                "backup_sha256": sidecar["backup_sha256"],
            }
        )
    )

    rc = rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path)

    assert rc == 1
    assert bronze_path.read_bytes() == mutated
