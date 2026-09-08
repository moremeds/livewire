"""Tests for scripts/migrate_parquet_filename.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from livewire_scripts.migrate_parquet_filename import main, migrate_parquet_files


def test_filename_migration_waits_for_legacy_writer_before_renaming(tmp_path):
    import subprocess
    import sys
    import time

    from clients.parquet_io import path_lock

    root = tmp_path / "bronze"
    old = root / "asset_class=equity/symbol=AAPL/data.parquet"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"original")
    ready = tmp_path / "ready"
    child = """
import sys
from pathlib import Path
from livewire_scripts.migrate_parquet_filename import migrate_parquet_files
Path(sys.argv[2]).touch()
migrate_parquet_files(Path(sys.argv[1]))
"""
    proc = None
    try:
        with path_lock(old.with_suffix(".parquet.lock")):
            proc = subprocess.Popen(
                [sys.executable, "-c", child, str(root), str(ready)],
                cwd=Path(__file__).resolve().parents[1],
            )
            deadline = time.monotonic() + 10
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert ready.exists()
            with pytest.raises(subprocess.TimeoutExpired):
                proc.wait(timeout=0.1)
            assert old.exists() and not old.with_name("1d.parquet").exists()
            old.write_bytes(b"writer completed")
        assert proc.wait(timeout=10) == 0
        assert old.with_name("1d.parquet").read_bytes() == b"writer completed"
        assert not old.exists()
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


class TestMigrateParquetFilename:
    def test_renames_data_to_1d(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        old = sym_dir / "data.parquet"
        old.write_bytes(b"fake parquet")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=False)

        assert not old.exists()
        assert (sym_dir / "1d.parquet").exists()
        assert stats["renamed"] == 1
        assert stats["skipped"] == 0
        assert stats["errors"] == 0

    def test_skips_already_renamed(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        (sym_dir / "1d.parquet").write_bytes(b"fake parquet")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=False)

        assert stats["renamed"] == 0
        assert stats["skipped"] == 0

    def test_dry_run_does_not_rename(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        old = sym_dir / "data.parquet"
        old.write_bytes(b"fake parquet")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=True)

        assert old.exists()
        assert not (sym_dir / "1d.parquet").exists()
        assert stats["renamed"] == 1

    def test_aborts_on_split_brain(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        (sym_dir / "data.parquet").write_bytes(b"old")
        (sym_dir / "1d.parquet").write_bytes(b"new")

        with pytest.raises(RuntimeError, match="split-brain"):
            migrate_parquet_files(tmp_path / "bronze", dry_run=False)

    def test_handles_multiple_asset_classes(self, tmp_path):
        for ac in ("asset_class=equity", "asset_class=futures", "asset_class=volatility"):
            sym_dir = tmp_path / "bronze" / ac / "symbol=TEST"
            sym_dir.mkdir(parents=True)
            (sym_dir / "data.parquet").write_bytes(b"fake")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=False)

        assert stats["renamed"] == 3

    def test_handles_delisted_dir(self, tmp_path):
        sym_dir = tmp_path / "bronze-delisted" / "asset_class=equity" / "symbol=OLD"
        sym_dir.mkdir(parents=True)
        (sym_dir / "data.parquet").write_bytes(b"fake")

        stats = migrate_parquet_files(tmp_path / "bronze-delisted", dry_run=False)

        assert stats["renamed"] == 1
        assert (sym_dir / "1d.parquet").exists()

    def test_empty_dir_returns_zero(self, tmp_path):
        bronze = tmp_path / "bronze"
        bronze.mkdir()

        stats = migrate_parquet_files(bronze, dry_run=False)

        assert stats["renamed"] == 0

    def test_nonexistent_dir_returns_zero(self, tmp_path):
        stats = migrate_parquet_files(tmp_path / "nonexistent", dry_run=False)

        assert stats["renamed"] == 0


class TestMain:
    def test_main_with_custom_dir(self, tmp_path, monkeypatch):
        sym_dir = tmp_path / "bronze" / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        (sym_dir / "data.parquet").write_bytes(b"fake")

        monkeypatch.setattr("sys.argv", ["migrate_parquet_filename.py", "--dir", str(tmp_path / "bronze")])
        main()

        assert (sym_dir / "1d.parquet").exists()

    def test_main_default_dirs_migrates_bronze_and_delisted(self, tmp_path, monkeypatch):
        bronze = tmp_path / "market-warehouse" / "data-lake" / "bronze" / "symbol=AAPL"
        bronze.mkdir(parents=True)
        (bronze / "data.parquet").write_bytes(b"fake")

        delisted = tmp_path / "market-warehouse" / "data-lake" / "bronze-delisted" / "symbol=OLD"
        delisted.mkdir(parents=True)
        (delisted / "data.parquet").write_bytes(b"fake")

        monkeypatch.setattr("sys.argv", ["migrate_parquet_filename.py"])
        with patch.object(Path, "home", return_value=tmp_path):
            main()

        assert (bronze / "1d.parquet").exists()
        assert (delisted / "1d.parquet").exists()

    def test_main_default_dirs_skips_nonexistent(self, tmp_path, monkeypatch):
        # Neither bronze nor bronze-delisted exist — should run without error
        monkeypatch.setattr("sys.argv", ["migrate_parquet_filename.py"])
        with patch.object(Path, "home", return_value=tmp_path):
            main()  # Should not raise

    def test_main_dry_run_with_custom_dir(self, tmp_path, monkeypatch):
        sym_dir = tmp_path / "bronze" / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        old = sym_dir / "data.parquet"
        old.write_bytes(b"fake")

        monkeypatch.setattr(
            "sys.argv",
            ["migrate_parquet_filename.py", "--dry-run", "--dir", str(tmp_path / "bronze")],
        )
        main()

        assert old.exists()
        assert not (sym_dir / "1d.parquet").exists()
