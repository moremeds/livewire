"""Tests for livewire_scripts/archive_otc_symbols.py."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from livewire_scripts.archive_otc_symbols import (
    archive_symbol,
    find_archivable_symbols,
    latest_1d_date,
    load_active_universe,
    main,
    run_archive,
    _staleness_cutoff,
)


def _write_minute_symbols(minute_base: Path, date: str, tickers: list[str]) -> None:
    """Create a minute_aggs date dir with a _symbols.parquet ticker set."""
    d = minute_base / f"date={date}"
    d.mkdir(parents=True, exist_ok=True)
    tbl = pa.table({"ticker": pa.array(tickers, type=pa.string())})
    pq.write_table(tbl, d / "_symbols.parquet")


def _make_bronze_sym(
    bronze_equity: Path,
    sym: str,
    latest_date: str | None = "2020-01-02",
    *,
    as_date_type: bool = False,
    stub: bool = False,
) -> None:
    """Create a symbol dir. Writes a real 1d.parquet with *latest_date* unless
    *stub* (writes unreadable bytes) or *latest_date* is None (empty table)."""
    sym_dir = bronze_equity / f"symbol={sym}"
    sym_dir.mkdir(parents=True, exist_ok=True)
    f = sym_dir / "1d.parquet"
    if stub:
        f.write_bytes(b"not a parquet file")
        return
    if latest_date is None:
        tbl = pa.table({"trade_date": pa.array([], type=pa.string())})
    elif as_date_type:
        tbl = pa.table({"trade_date": pa.array([_dt.date.fromisoformat(latest_date)])})
    else:
        # include an older row so max() is meaningful
        tbl = pa.table({"trade_date": pa.array(["2019-12-31", latest_date])})
    pq.write_table(tbl, f)


# ── load_active_universe ──────────────────────────────────────────────


class TestLoadActiveUniverse:
    def test_unions_over_window(self, tmp_path):
        base = tmp_path / "minute_aggs_v1"
        _write_minute_symbols(base, "2026-06-09", ["AAPL", "WRNT"])
        _write_minute_symbols(base, "2026-06-10", ["AAPL", "UNIT"])
        _write_minute_symbols(base, "2026-06-11", ["AAPL", "MSFT"])

        tickers, dates = load_active_universe(base, universe_days=3)

        assert tickers == {"AAPL", "WRNT", "UNIT", "MSFT"}
        assert dates == ["2026-06-09", "2026-06-10", "2026-06-11"]

    def test_window_limits_to_last_n_days(self, tmp_path):
        base = tmp_path / "minute_aggs_v1"
        _write_minute_symbols(base, "2026-06-09", ["OLD"])
        _write_minute_symbols(base, "2026-06-10", ["MID"])
        _write_minute_symbols(base, "2026-06-11", ["NEW"])

        tickers, dates = load_active_universe(base, universe_days=2)

        assert tickers == {"MID", "NEW"}
        assert dates == ["2026-06-10", "2026-06-11"]

    def test_as_of_filters_future_dates(self, tmp_path):
        base = tmp_path / "minute_aggs_v1"
        _write_minute_symbols(base, "2026-06-10", ["A"])
        _write_minute_symbols(base, "2026-06-11", ["B"])
        _write_minute_symbols(base, "2026-06-12", ["C"])

        tickers, dates = load_active_universe(base, universe_days=5, as_of="2026-06-11")

        assert tickers == {"A", "B"}
        assert dates == ["2026-06-10", "2026-06-11"]

    def test_skips_dates_missing_symbols_file(self, tmp_path):
        base = tmp_path / "minute_aggs_v1"
        _write_minute_symbols(base, "2026-06-10", ["A"])
        (base / "date=2026-06-11").mkdir(parents=True)  # no _symbols.parquet
        _write_minute_symbols(base, "2026-06-12", ["C"])

        tickers, dates = load_active_universe(base, universe_days=5)

        assert tickers == {"A", "C"}
        assert dates == ["2026-06-10", "2026-06-12"]

    def test_raises_when_no_date_dirs(self, tmp_path):
        base = tmp_path / "minute_aggs_v1"
        base.mkdir()
        with pytest.raises(FileNotFoundError, match="No minute_aggs raw data"):
            load_active_universe(base, universe_days=3)

    def test_raises_when_window_has_no_symbols_files(self, tmp_path):
        base = tmp_path / "minute_aggs_v1"
        (base / "date=2026-06-11").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="No _symbols.parquet"):
            load_active_universe(base, universe_days=3)


# ── latest_1d_date ────────────────────────────────────────────────────


class TestLatest1dDate:
    def test_reads_max_string_date(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        _make_bronze_sym(b, "AAPL", latest_date="2026-06-30")
        assert latest_1d_date(b / "symbol=AAPL") == "2026-06-30"

    def test_reads_max_date_type(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        _make_bronze_sym(b, "AAPL", latest_date="2026-06-30", as_date_type=True)
        assert latest_1d_date(b / "symbol=AAPL") == "2026-06-30"

    def test_returns_none_when_file_missing(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        (b / "symbol=AAPL").mkdir(parents=True)
        assert latest_1d_date(b / "symbol=AAPL") is None

    def test_returns_none_when_unreadable(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        _make_bronze_sym(b, "AAPL", stub=True)
        assert latest_1d_date(b / "symbol=AAPL") is None

    def test_returns_none_when_empty(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        _make_bronze_sym(b, "AAPL", latest_date=None)
        assert latest_1d_date(b / "symbol=AAPL") is None


# ── find_archivable_symbols ───────────────────────────────────────────


class TestFindArchivableSymbols:
    def test_absent_and_stale_is_archivable(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        _make_bronze_sym(b, "DEAD", latest_date="2020-01-02")
        assert find_archivable_symbols(b, set(), "2026-05-12") == ["DEAD"]

    def test_present_in_universe_excluded(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        _make_bronze_sym(b, "AAPL", latest_date="2020-01-02")
        assert find_archivable_symbols(b, {"AAPL"}, "2026-05-12") == []

    def test_absent_but_fresh_excluded(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        # warrant that traded recently but is not in the universe set
        _make_bronze_sym(b, "WRNT", latest_date="2026-07-01")
        assert find_archivable_symbols(b, set(), "2026-05-12") == []

    def test_absent_unreadable_date_skipped(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        _make_bronze_sym(b, "CORRUPT", stub=True)
        assert find_archivable_symbols(b, set(), "2026-05-12") == []

    def test_result_is_sorted(self, tmp_path):
        b = tmp_path / "asset_class=equity"
        for s in ["ZZZ", "AAA", "MMM"]:
            _make_bronze_sym(b, s, latest_date="2020-01-02")
        assert find_archivable_symbols(b, set(), "2026-05-12") == ["AAA", "MMM", "ZZZ"]


# ── _staleness_cutoff ─────────────────────────────────────────────────


class TestStalenessCutoff:
    def test_subtracts_days_from_latest_date(self):
        assert _staleness_cutoff(["2026-06-01", "2026-07-02"], 30) == "2026-06-02"

    def test_uses_max_date_regardless_of_order(self):
        assert _staleness_cutoff(["2026-07-02", "2026-06-01"], 0) == "2026-07-02"


# ── archive_symbol (unchanged behaviour) ──────────────────────────────


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


# ── run_archive ───────────────────────────────────────────────────────


class TestRunArchive:
    def _setup(self, tmp_path, universe, bronze_syms):
        """bronze_syms: dict[sym] = latest_date (str | None)."""
        minute_base = tmp_path / "raw" / "minute_aggs_v1"
        _write_minute_symbols(minute_base, "2026-07-02", universe)
        bronze = tmp_path / "bronze" / "asset_class=equity"
        for sym, latest in bronze_syms.items():
            _make_bronze_sym(bronze, sym, latest_date=latest)
        delisted = tmp_path / "delisted" / "asset_class=equity"
        return minute_base, bronze, delisted

    def test_archives_inactive_and_stale(self, tmp_path):
        minute, bronze, delisted = self._setup(
            tmp_path, universe=["AAPL"], bronze_syms={"AAPL": "2026-07-02", "DEAD": "2020-01-02"}
        )
        stats = run_archive(
            bronze, delisted, minute, as_of=None, universe_days=20, staleness_days=30, dry_run=False
        )
        assert stats["archived"] == 1
        assert (delisted / "symbol=DEAD").exists()
        assert (bronze / "symbol=AAPL").exists()

    def test_spares_recently_traded_non_universe_symbol(self, tmp_path):
        # A warrant absent from the universe but with a fresh bar must NOT be archived.
        minute, bronze, delisted = self._setup(
            tmp_path, universe=["AAPL"], bronze_syms={"AAPL": "2026-07-02", "WRNT": "2026-07-01"}
        )
        stats = run_archive(
            bronze, delisted, minute, as_of=None, universe_days=20, staleness_days=30, dry_run=False
        )
        assert stats["archived"] == 0
        assert (bronze / "symbol=WRNT").exists()

    def test_dry_run_does_not_move(self, tmp_path):
        minute, bronze, delisted = self._setup(
            tmp_path, universe=["AAPL"], bronze_syms={"AAPL": "2026-07-02", "DEAD": "2020-01-02"}
        )
        stats = run_archive(
            bronze, delisted, minute, as_of=None, universe_days=20, staleness_days=30, dry_run=True
        )
        assert stats["dry_run"] == 1
        assert (bronze / "symbol=DEAD").exists()
        assert not delisted.exists()

    def test_skips_already_delisted(self, tmp_path):
        minute, bronze, delisted = self._setup(
            tmp_path, universe=["AAPL"], bronze_syms={"AAPL": "2026-07-02", "DEAD": "2020-01-02"}
        )
        _make_bronze_sym(delisted, "DEAD", latest_date="2020-01-02")
        stats = run_archive(
            bronze, delisted, minute, as_of=None, universe_days=20, staleness_days=30, dry_run=False
        )
        assert stats["skipped_exists"] == 1
        assert stats["archived"] == 0

    def test_returns_zeros_when_nothing_to_archive(self, tmp_path):
        minute, bronze, delisted = self._setup(
            tmp_path, universe=["AAPL", "MSFT"], bronze_syms={"AAPL": "2026-07-02", "MSFT": "2026-07-02"}
        )
        stats = run_archive(
            bronze, delisted, minute, as_of=None, universe_days=20, staleness_days=30, dry_run=False
        )
        assert stats == {"archived": 0, "skipped_exists": 0, "dry_run": 0}


# ── main ──────────────────────────────────────────────────────────────


class TestMain:
    def _setup_warehouse(self, tmp_path, universe, bronze_syms):
        wh = tmp_path / "market-warehouse"
        minute_base = wh / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1"
        _write_minute_symbols(minute_base, "2026-07-02", universe)
        bronze = wh / "data-lake" / "bronze" / "asset_class=equity"
        for sym, latest in bronze_syms.items():
            _make_bronze_sym(bronze, sym, latest_date=latest)
        return wh

    def test_archives_with_warehouse_flag(self, tmp_path):
        wh = self._setup_warehouse(
            tmp_path, ["AAPL"], {"AAPL": "2026-07-02", "DEAD": "2020-01-02"}
        )
        rc = main(["--warehouse", str(wh)])
        assert rc == 0
        delisted = wh / "data-lake" / "bronze-delisted" / "asset_class=equity"
        assert (delisted / "symbol=DEAD").exists()

    def test_dry_run_flag(self, tmp_path):
        wh = self._setup_warehouse(
            tmp_path, ["AAPL"], {"AAPL": "2026-07-02", "DEAD": "2020-01-02"}
        )
        bronze = wh / "data-lake" / "bronze" / "asset_class=equity"
        rc = main(["--warehouse", str(wh), "--dry-run"])
        assert rc == 0
        assert (bronze / "symbol=DEAD").exists()

    def test_as_of_and_window_flags(self, tmp_path):
        wh = self._setup_warehouse(
            tmp_path, ["AAPL"], {"AAPL": "2026-07-02", "DEAD": "2020-01-02"}
        )
        rc = main(
            ["--warehouse", str(wh), "--as-of", "2026-07-02", "--universe-days", "5", "--staleness-days", "10"]
        )
        assert rc == 0
        delisted = wh / "data-lake" / "bronze-delisted" / "asset_class=equity"
        assert (delisted / "symbol=DEAD").exists()

    def test_staleness_flag_spares_recent(self, tmp_path):
        # With a huge staleness window, even an old-ish symbol is spared.
        wh = self._setup_warehouse(
            tmp_path, ["AAPL"], {"AAPL": "2026-07-02", "RECENT": "2026-06-20"}
        )
        bronze = wh / "data-lake" / "bronze" / "asset_class=equity"
        rc = main(["--warehouse", str(wh), "--staleness-days", "365"])
        assert rc == 0
        assert (bronze / "symbol=RECENT").exists()

    def test_returns_1_when_bronze_missing(self, tmp_path):
        rc = main(["--warehouse", str(tmp_path / "nonexistent")])
        assert rc == 1
