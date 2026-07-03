"""Tests for livewire_scripts/archive_otc_symbols.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from livewire_scripts.archive_otc_symbols import (
    archive_symbol,
    find_non_sip_symbols,
    load_sip_universe,
    main,
    run_archive,
)


def _write_ticker_parquet(path: Path, tickers: list[str]) -> None:
    """Write a minimal day_aggs-style parquet file with a ticker column."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tbl = pa.table({"ticker": pa.array(tickers, type=pa.string())})
    pq.write_table(tbl, path)


def _make_sip_date(raw_base: Path, date: str, tickers: list[str]) -> None:
    """Create a single-bucket SIP date directory with given tickers."""
    _write_ticker_parquet(raw_base / f"date={date}" / "bucket_000.parquet", tickers)


def _make_bronze_sym(bronze_equity: Path, sym: str) -> None:
    """Create a symbol directory with a stub 1d.parquet."""
    sym_dir = bronze_equity / f"symbol={sym}"
    sym_dir.mkdir(parents=True, exist_ok=True)
    (sym_dir / "1d.parquet").write_bytes(b"stub")


class TestLoadSipUniverse:
    def test_loads_tickers_from_latest_date(self, tmp_path):
        raw_base = tmp_path / "day_aggs_v1"
        _make_sip_date(raw_base, "2026-06-10", ["AAPL", "MSFT"])
        _make_sip_date(raw_base, "2026-06-11", ["AAPL", "NVDA"])

        tickers, date_used = load_sip_universe(raw_base)

        assert tickers == {"AAPL", "NVDA"}
        assert date_used == "2026-06-11"

    def test_loads_tickers_from_specific_date(self, tmp_path):
        raw_base = tmp_path / "day_aggs_v1"
        _make_sip_date(raw_base, "2026-06-10", ["AAPL", "MSFT"])
        _make_sip_date(raw_base, "2026-06-11", ["AAPL", "NVDA"])

        tickers, date_used = load_sip_universe(raw_base, date="2026-06-10")

        assert tickers == {"AAPL", "MSFT"}
        assert date_used == "2026-06-10"

    def test_merges_multiple_bucket_files(self, tmp_path):
        raw_base = tmp_path / "day_aggs_v1"
        date_dir = raw_base / "date=2026-06-11"
        _write_ticker_parquet(date_dir / "bucket_000.parquet", ["AAPL", "MSFT"])
        _write_ticker_parquet(date_dir / "bucket_001.parquet", ["NVDA", "GOOG"])

        tickers, _ = load_sip_universe(raw_base)

        assert tickers == {"AAPL", "MSFT", "NVDA", "GOOG"}

    def test_raises_when_no_date_dirs(self, tmp_path):
        raw_base = tmp_path / "day_aggs_v1"
        raw_base.mkdir()

        with pytest.raises(FileNotFoundError, match="No day_aggs raw data"):
            load_sip_universe(raw_base)

    def test_raises_when_specific_date_missing(self, tmp_path):
        raw_base = tmp_path / "day_aggs_v1"
        _make_sip_date(raw_base, "2026-06-11", ["AAPL"])

        with pytest.raises(FileNotFoundError, match="SIP date not found"):
            load_sip_universe(raw_base, date="2026-06-01")

    def test_raises_when_date_dir_has_no_parquet(self, tmp_path):
        raw_base = tmp_path / "day_aggs_v1"
        (raw_base / "date=2026-06-11").mkdir(parents=True)

        with pytest.raises(FileNotFoundError, match="No parquet bucket files"):
            load_sip_universe(raw_base)

    def test_ignores_hidden_parquet_files(self, tmp_path):
        raw_base = tmp_path / "day_aggs_v1"
        date_dir = raw_base / "date=2026-06-11"
        _write_ticker_parquet(date_dir / "bucket_000.parquet", ["AAPL"])
        (date_dir / ".__symbols.parquet").write_bytes(b"not a real parquet file")

        tickers, _ = load_sip_universe(raw_base)

        assert tickers == {"AAPL"}


class TestFindNonSipSymbols:
    def test_returns_symbols_absent_from_sip(self, tmp_path):
        bronze_equity = tmp_path / "asset_class=equity"
        for sym in ["AAPL", "OTC1", "MSFT", "PNKSH"]:
            _make_bronze_sym(bronze_equity, sym)

        result = find_non_sip_symbols(bronze_equity, {"AAPL", "MSFT"})

        assert result == ["OTC1", "PNKSH"]

    def test_returns_empty_when_all_in_sip(self, tmp_path):
        bronze_equity = tmp_path / "asset_class=equity"
        _make_bronze_sym(bronze_equity, "AAPL")

        result = find_non_sip_symbols(bronze_equity, {"AAPL", "MSFT"})

        assert result == []

    def test_returns_all_when_none_in_sip(self, tmp_path):
        bronze_equity = tmp_path / "asset_class=equity"
        for sym in ["OTC1", "OTC2"]:
            _make_bronze_sym(bronze_equity, sym)

        result = find_non_sip_symbols(bronze_equity, {"AAPL"})

        assert result == ["OTC1", "OTC2"]

    def test_returns_sorted(self, tmp_path):
        bronze_equity = tmp_path / "asset_class=equity"
        for sym in ["ZZZ", "AAA", "MMM"]:
            _make_bronze_sym(bronze_equity, sym)

        result = find_non_sip_symbols(bronze_equity, set())

        assert result == ["AAA", "MMM", "ZZZ"]

    def test_returns_empty_when_bronze_is_empty(self, tmp_path):
        bronze_equity = tmp_path / "asset_class=equity"
        bronze_equity.mkdir(parents=True)

        result = find_non_sip_symbols(bronze_equity, {"AAPL"})

        assert result == []


class TestArchiveSymbol:
    def test_moves_symbol_dir(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        delisted = tmp_path / "delisted" / "asset_class=equity"
        _make_bronze_sym(bronze, "OTC1")

        result = archive_symbol("OTC1", bronze, delisted, dry_run=False)

        assert result == "archived"
        assert not (bronze / "symbol=OTC1").exists()
        assert (delisted / "symbol=OTC1" / "1d.parquet").exists()

    def test_creates_delisted_parent_dirs(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        delisted = tmp_path / "delisted" / "asset_class=equity"
        _make_bronze_sym(bronze, "OTC1")

        archive_symbol("OTC1", bronze, delisted, dry_run=False)

        assert delisted.is_dir()

    def test_skips_when_destination_exists(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        delisted = tmp_path / "delisted" / "asset_class=equity"
        _make_bronze_sym(bronze, "OTC1")
        _make_bronze_sym(delisted, "OTC1")

        result = archive_symbol("OTC1", bronze, delisted, dry_run=False)

        assert result == "skipped_exists"
        assert (bronze / "symbol=OTC1").exists()

    def test_dry_run_does_not_move(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        delisted = tmp_path / "delisted" / "asset_class=equity"
        _make_bronze_sym(bronze, "OTC1")

        result = archive_symbol("OTC1", bronze, delisted, dry_run=True)

        assert result == "dry_run"
        assert (bronze / "symbol=OTC1").exists()
        assert not (delisted / "symbol=OTC1").exists()

    def test_preserves_all_timeframe_files(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        delisted = tmp_path / "delisted" / "asset_class=equity"
        sym_dir = bronze / "symbol=OTC1"
        sym_dir.mkdir(parents=True)
        for tf in ["1d.parquet", "1m.parquet", "5m.parquet"]:
            (sym_dir / tf).write_bytes(b"stub")

        archive_symbol("OTC1", bronze, delisted, dry_run=False)

        dst = delisted / "symbol=OTC1"
        assert (dst / "1d.parquet").exists()
        assert (dst / "1m.parquet").exists()
        assert (dst / "5m.parquet").exists()


class TestRunArchive:
    def _setup(self, tmp_path, sip_tickers, bronze_syms):
        raw_base = tmp_path / "raw" / "day_aggs_v1"
        _make_sip_date(raw_base, "2026-06-11", sip_tickers)
        bronze_equity = tmp_path / "bronze" / "asset_class=equity"
        for sym in bronze_syms:
            _make_bronze_sym(bronze_equity, sym)
        delisted_equity = tmp_path / "delisted" / "asset_class=equity"
        return raw_base, bronze_equity, delisted_equity

    def test_archives_non_sip_tickers(self, tmp_path):
        raw_base, bronze, delisted = self._setup(
            tmp_path, sip_tickers=["AAPL"], bronze_syms=["AAPL", "OTC1", "OTC2"]
        )

        stats = run_archive(bronze, delisted, raw_base, sip_date=None, dry_run=False)

        assert stats["archived"] == 2
        assert stats["skipped_exists"] == 0
        assert (delisted / "symbol=OTC1").exists()
        assert (delisted / "symbol=OTC2").exists()
        assert (bronze / "symbol=AAPL").exists()

    def test_dry_run_does_not_move(self, tmp_path):
        raw_base, bronze, delisted = self._setup(
            tmp_path, sip_tickers=["AAPL"], bronze_syms=["AAPL", "OTC1"]
        )

        stats = run_archive(bronze, delisted, raw_base, sip_date=None, dry_run=True)

        assert stats["dry_run"] == 1
        assert (bronze / "symbol=OTC1").exists()
        assert not delisted.exists()

    def test_skips_already_delisted(self, tmp_path):
        raw_base, bronze, delisted = self._setup(
            tmp_path, sip_tickers=["AAPL"], bronze_syms=["AAPL", "OTC1"]
        )
        _make_bronze_sym(delisted, "OTC1")

        stats = run_archive(bronze, delisted, raw_base, sip_date=None, dry_run=False)

        assert stats["skipped_exists"] == 1
        assert stats["archived"] == 0

    def test_returns_zeros_when_nothing_to_archive(self, tmp_path):
        raw_base, bronze, delisted = self._setup(
            tmp_path, sip_tickers=["AAPL", "MSFT"], bronze_syms=["AAPL", "MSFT"]
        )

        stats = run_archive(bronze, delisted, raw_base, sip_date=None, dry_run=False)

        assert stats == {"archived": 0, "skipped_exists": 0, "dry_run": 0}

    def test_passes_sip_date_to_loader(self, tmp_path):
        raw_base, bronze, delisted = self._setup(
            tmp_path, sip_tickers=["AAPL"], bronze_syms=["AAPL"]
        )
        _make_sip_date(raw_base, "2026-06-10", ["AAPL", "EXTRA"])

        stats = run_archive(bronze, delisted, raw_base, sip_date="2026-06-10", dry_run=False)

        assert stats["archived"] == 0


class TestMain:
    def _setup_warehouse(self, tmp_path, sip_tickers, bronze_syms):
        raw_base = (
            tmp_path
            / "market-warehouse"
            / "data-lake"
            / "raw"
            / "massive"
            / "us_stocks_sip"
            / "day_aggs_v1"
        )
        _make_sip_date(raw_base, "2026-06-11", sip_tickers)
        bronze = (
            tmp_path / "market-warehouse" / "data-lake" / "bronze" / "asset_class=equity"
        )
        for sym in bronze_syms:
            _make_bronze_sym(bronze, sym)
        return tmp_path / "market-warehouse"

    def test_archives_with_warehouse_flag(self, tmp_path):
        warehouse = self._setup_warehouse(tmp_path, ["AAPL"], ["AAPL", "OTC1"])

        rc = main(["--warehouse", str(warehouse)])

        assert rc == 0
        delisted = warehouse / "data-lake" / "bronze-delisted" / "asset_class=equity"
        assert (delisted / "symbol=OTC1").exists()

    def test_dry_run_flag(self, tmp_path):
        warehouse = self._setup_warehouse(tmp_path, ["AAPL"], ["AAPL", "OTC1"])
        bronze = warehouse / "data-lake" / "bronze" / "asset_class=equity"

        rc = main(["--warehouse", str(warehouse), "--dry-run"])

        assert rc == 0
        assert (bronze / "symbol=OTC1").exists()

    def test_sip_date_flag(self, tmp_path):
        warehouse = self._setup_warehouse(tmp_path, ["AAPL"], ["AAPL", "OTC1"])
        raw_base = (
            warehouse
            / "data-lake"
            / "raw"
            / "massive"
            / "us_stocks_sip"
            / "day_aggs_v1"
        )
        _make_sip_date(raw_base, "2026-06-10", ["AAPL", "OTC1"])

        rc = main(["--warehouse", str(warehouse), "--sip-date", "2026-06-10"])

        assert rc == 0
        delisted = warehouse / "data-lake" / "bronze-delisted" / "asset_class=equity"
        assert not (delisted / "symbol=OTC1").exists()

    def test_returns_1_when_bronze_missing(self, tmp_path):
        rc = main(["--warehouse", str(tmp_path / "nonexistent")])

        assert rc == 1
