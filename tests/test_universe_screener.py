"""Tests for scripts/universe_screener.py — 100% coverage target."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from livewire_scripts.universe_screener import (
    _send_screener_alert,
    compare_universes,
    get_removals_after_grace,
    load_core_etfs,
    load_screener_state,
    log_changes,
    run_scanner_sweeps,
    save_screener_state,
    update_absent_counts,
    write_universe_preset,
    main,
)


# ══════════════════════════════════════════════════════════════════════
# compare_universes
# ══════════════════════════════════════════════════════════════════════


class TestCompareUniverses:
    def test_additions_and_removals(self):
        current = {"AAPL", "MSFT", "GOOG"}
        scanned = {"AAPL", "MSFT", "NVDA"}
        additions, removals = compare_universes(current, scanned)
        assert additions == {"NVDA"}
        assert removals == {"GOOG"}

    def test_no_changes(self):
        current = {"AAPL", "MSFT"}
        scanned = {"AAPL", "MSFT"}
        additions, removals = compare_universes(current, scanned)
        assert additions == set()
        assert removals == set()

    def test_all_new(self):
        current = set()
        scanned = {"AAPL", "MSFT", "NVDA"}
        additions, removals = compare_universes(current, scanned)
        assert additions == {"AAPL", "MSFT", "NVDA"}
        assert removals == set()


# ══════════════════════════════════════════════════════════════════════
# load/save screener state
# ══════════════════════════════════════════════════════════════════════


class TestScreenerState:
    def test_load_nonexistent_returns_none(self, tmp_path):
        path = tmp_path / "nonexistent.json"
        assert load_screener_state(path) is None

    def test_save_and_load_roundtrip(self, tmp_path):
        path = tmp_path / "subdir" / "state.json"
        state = {
            "run_date": "2026-04-05",
            "universe": ["AAPL", "MSFT"],
            "absent_counts": {"GOOG": 2},
        }
        save_screener_state(path, state)
        loaded = load_screener_state(path)
        assert loaded == state

    def test_save_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "a" / "b" / "c" / "state.json"
        save_screener_state(path, {"key": "value"})
        assert path.exists()


# ══════════════════════════════════════════════════════════════════════
# update_absent_counts
# ══════════════════════════════════════════════════════════════════════


class TestAbsentCounts:
    def test_increments_absent(self):
        absent = {"GOOG": 1}
        removals = {"GOOG", "AMZN"}
        scanned = {"AAPL", "MSFT"}
        result = update_absent_counts(absent, removals, scanned)
        assert result["GOOG"] == 2
        assert result["AMZN"] == 1

    def test_resets_present_tickers(self):
        absent = {"GOOG": 2, "META": 1}
        removals = set()
        scanned = {"AAPL", "MSFT", "GOOG"}  # GOOG is back
        result = update_absent_counts(absent, removals, scanned)
        assert "GOOG" not in result
        assert "META" in result  # still absent (not in scanned)

    def test_carries_forward_others(self):
        absent = {"TSLA": 1}
        removals = set()
        scanned = {"AAPL"}  # TSLA not in scanned, not in removals
        result = update_absent_counts(absent, removals, scanned)
        assert result["TSLA"] == 1

    def test_empty_state(self):
        result = update_absent_counts({}, set(), {"AAPL"})
        assert result == {}


# ══════════════════════════════════════════════════════════════════════
# get_removals_after_grace
# ══════════════════════════════════════════════════════════════════════


class TestGracePeriod:
    def test_returns_tickers_past_grace(self):
        absent = {"GOOG": 3, "META": 2, "TSLA": 4}
        result = get_removals_after_grace(absent, grace_days=3)
        assert result == {"GOOG", "TSLA"}

    def test_returns_empty_when_none_past_grace(self):
        absent = {"GOOG": 1, "META": 2}
        result = get_removals_after_grace(absent, grace_days=3)
        assert result == set()

    def test_default_grace_days(self):
        absent = {"GOOG": 3}
        result = get_removals_after_grace(absent)
        assert "GOOG" in result

    def test_empty_absent_counts(self):
        assert get_removals_after_grace({}) == set()


# ══════════════════════════════════════════════════════════════════════
# write_universe_preset
# ══════════════════════════════════════════════════════════════════════


class TestWritePreset:
    def test_writes_valid_json(self, tmp_path):
        path = tmp_path / "screened-universe.json"
        tickers = ["MSFT", "AAPL", "NVDA"]
        write_universe_preset(path, tickers)

        with open(path) as f:
            data = json.load(f)

        assert data["name"] == "screened-universe"
        assert data["tickers"] == sorted(tickers)
        assert "generated_at" in data
        assert "description" in data

    def test_tickers_are_sorted(self, tmp_path):
        path = tmp_path / "preset.json"
        write_universe_preset(path, ["ZZZZ", "AAAA", "MMMM"])
        with open(path) as f:
            data = json.load(f)
        assert data["tickers"] == ["AAAA", "MMMM", "ZZZZ"]

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "subdir" / "preset.json"
        write_universe_preset(path, ["AAPL"])
        assert path.exists()


# ══════════════════════════════════════════════════════════════════════
# log_changes
# ══════════════════════════════════════════════════════════════════════


class TestLogChanges:
    def test_writes_log_file(self, tmp_path):
        run_date = date(2026, 4, 5)
        additions = {"NVDA", "TSLA"}
        removals = {"GOOG"}
        log_path = log_changes(tmp_path, run_date, additions, removals)

        assert log_path.exists()
        content = log_path.read_text()
        assert "NVDA" in content or "TSLA" in content
        assert "GOOG" in content

    def test_log_filename_contains_date(self, tmp_path):
        run_date = date(2026, 4, 5)
        log_path = log_changes(tmp_path, run_date, set(), set())
        assert "2026-04-05" in log_path.name

    def test_creates_log_dir(self, tmp_path):
        log_dir = tmp_path / "new_subdir"
        log_path = log_changes(log_dir, date(2026, 4, 5), {"A"}, {"B"})
        assert log_path.exists()

    def test_returns_path(self, tmp_path):
        result = log_changes(tmp_path, date(2026, 4, 5), set(), set())
        assert isinstance(result, Path)


# ══════════════════════════════════════════════════════════════════════
# run_scanner_sweeps
# ══════════════════════════════════════════════════════════════════════


class TestRunScannerSweeps:
    def test_returns_union_of_scanner_results(self, monkeypatch):
        monkeypatch.setattr("livewire_scripts.universe_screener._SCANNER_THROTTLE_SECONDS", 0)
        ib = MagicMock()
        # Return different symbols for different calls
        call_count = 0

        async def fake_req_scanner(sub):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [SimpleNamespace(contractDetails=SimpleNamespace(contract=SimpleNamespace(symbol="AAPL")))]
            elif call_count == 2:
                return [SimpleNamespace(contractDetails=SimpleNamespace(contract=SimpleNamespace(symbol="MSFT")))]
            else:
                return []

        ib.reqScannerDataAsync = fake_req_scanner

        result = asyncio.new_event_loop().run_until_complete(run_scanner_sweeps(ib))
        assert "AAPL" in result
        assert "MSFT" in result

    def test_deduplicates_results(self, monkeypatch):
        monkeypatch.setattr("livewire_scripts.universe_screener._SCANNER_THROTTLE_SECONDS", 0)
        ib = MagicMock()

        async def fake_req_scanner(sub):
            return [
                SimpleNamespace(contractDetails=SimpleNamespace(contract=SimpleNamespace(symbol="AAPL"))),
                SimpleNamespace(contractDetails=SimpleNamespace(contract=SimpleNamespace(symbol="AAPL"))),
            ]

        ib.reqScannerDataAsync = fake_req_scanner

        result = asyncio.new_event_loop().run_until_complete(run_scanner_sweeps(ib))
        assert result.count("AAPL") if isinstance(result, list) else len([x for x in result if x == "AAPL"]) == 1

    def test_failed_scanner_request_caught_and_logged(self, monkeypatch, caplog):
        """An exception in reqScannerDataAsync should be caught and logged, not raised."""
        monkeypatch.setattr("livewire_scripts.universe_screener._SCANNER_THROTTLE_SECONDS", 0)
        ib = MagicMock()

        async def failing_req_scanner(sub):
            raise RuntimeError("IB scanner unavailable")

        ib.reqScannerDataAsync = failing_req_scanner

        import logging
        with caplog.at_level(logging.WARNING):
            result = asyncio.new_event_loop().run_until_complete(run_scanner_sweeps(ib))
        # No symbols returned, all sweeps failed silently
        assert result == set()
        # At least one warning logged
        assert any("Scanner" in r.message and "failed" in r.message for r in caplog.records)


# ══════════════════════════════════════════════════════════════════════
# _send_screener_alert
# ══════════════════════════════════════════════════════════════════════


class TestSendAlert:
    def test_calls_subprocess(self):
        run_date = date(2026, 4, 5)
        additions = {"NVDA"}
        removals = {"GOOG"}

        with patch("subprocess.run") as mock_run:
            _send_screener_alert(run_date, additions, removals)
            assert mock_run.called

    def test_passes_run_date(self):
        run_date = date(2026, 4, 5)
        with patch("subprocess.run") as mock_run:
            _send_screener_alert(run_date, set(), set())
            cmd = mock_run.call_args[0][0]
            assert "2026-04-05" in cmd

    def test_includes_error_summary(self):
        run_date = date(2026, 4, 5)
        with patch("subprocess.run") as mock_run:
            _send_screener_alert(run_date, {"NVDA"}, {"GOOG"})
            cmd = mock_run.call_args[0][0]
            assert "--error-summary" in cmd


# ══════════════════════════════════════════════════════════════════════
# main()
# ══════════════════════════════════════════════════════════════════════


def _make_mock_ib_client(symbols=None):
    """Create a mock IBClient (sync context manager) that returns scanner results."""
    if symbols is None:
        symbols = ["AAPL", "MSFT", "GOOG"]

    # The underlying ib_async.IB object
    ib_raw = MagicMock()

    async def fake_req_scanner(sub):
        return [
            SimpleNamespace(contractDetails=SimpleNamespace(contract=SimpleNamespace(symbol=s)))
            for s in symbols
        ]

    ib_raw.reqScannerDataAsync = fake_req_scanner
    def _run_coro(coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    ib_raw.run = _run_coro

    # The IBClient wrapper
    ib_client = MagicMock()
    ib_client.ib = ib_raw
    ib_client.connect = MagicMock()
    ib_client.__enter__ = MagicMock(return_value=ib_client)
    ib_client.__exit__ = MagicMock(return_value=False)
    return ib_client


class TestMain:
    @pytest.fixture(autouse=True)
    def _no_scanner_throttle(self, monkeypatch):
        """Disable scanner throttle in all TestMain tests."""
        monkeypatch.setattr("livewire_scripts.universe_screener._SCANNER_THROTTLE_SECONDS", 0)

    def test_not_trading_day_exits(self, tmp_path, monkeypatch):
        """Script exits 0 on non-trading day without --force."""
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", tmp_path / "data-lake")

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=False):
            with pytest.raises(SystemExit) as exc_info:
                main(["--dry-run"])
            assert exc_info.value.code == 0

    def test_dry_run_does_not_modify(self, tmp_path, monkeypatch):
        """Dry run does not write any files."""
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        preset_path = tmp_path / "preset.json"
        state_path = tmp_path / "state.json"
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        mock_ib_client = _make_mock_ib_client(["AAPL", "MSFT"])

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client), \
             patch("livewire_scripts.universe_screener._PRESET_PATH", preset_path), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir):
            main(["--dry-run", "--force"])

        # dry run should not write preset or state
        assert not preset_path.exists()
        assert not state_path.exists()

    def test_bootstrap_mode_skips_removals(self, tmp_path, monkeypatch):
        """First run (no state file) skips all removals."""
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        # Seed some existing tickers in bronze parquet
        existing_sym_dir = bronze_dir / "symbol=OLDTICKER"
        existing_sym_dir.mkdir(parents=True)
        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
            ("trade_date", pa.date32()), ("symbol_id", pa.int64()),
            ("open", pa.float64()), ("high", pa.float64()),
            ("low", pa.float64()), ("close", pa.float64()),
            ("adj_close", pa.float64()), ("volume", pa.int64()),
        ])
        table = pa.Table.from_pylist([{
            "trade_date": date(2026, 1, 2), "symbol_id": 1,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "adj_close": 1.5, "volume": 1000,
        }], schema=schema)
        pq.write_table(table, existing_sym_dir / "1d.parquet")

        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        preset_path = tmp_path / "preset.json"
        state_path = tmp_path / "state.json"  # does not exist — bootstrap mode
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        # Scanner returns new universe (no OLDTICKER)
        mock_ib_client = _make_mock_ib_client(["AAPL", "MSFT"])

        archived = []

        def fake_archive(src, dst):
            archived.append(src)

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client), \
             patch("livewire_scripts.universe_screener._PRESET_PATH", preset_path), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir), \
             patch("shutil.move", fake_archive), \
             patch("subprocess.run"):
            main(["--force"])

        # No tickers should have been archived in bootstrap mode
        assert len(archived) == 0

    def test_max_removals_cap_aborts(self, tmp_path, monkeypatch):
        """If pending removals exceed MAX_REMOVALS after grace, abort removals."""
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
            ("trade_date", pa.date32()), ("symbol_id", pa.int64()),
            ("open", pa.float64()), ("high", pa.float64()),
            ("low", pa.float64()), ("close", pa.float64()),
            ("adj_close", pa.float64()), ("volume", pa.int64()),
        ])

        # Create 60 tickers in bronze (enough to exceed MAX_REMOVALS=50)
        existing_tickers = [f"TICK{idx:02d}" for idx in range(60)]
        for sym_idx, ticker in enumerate(existing_tickers):
            sym_dir = bronze_dir / f"symbol={ticker}"
            sym_dir.mkdir(parents=True)
            table = pa.Table.from_pylist([{
                "trade_date": date(2026, 1, 2), "symbol_id": sym_idx,
                "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                "adj_close": 1.5, "volume": 1000,
            }], schema=schema)
            pq.write_table(table, sym_dir / "1d.parquet")

        # State with all 60 tickers absent for >GRACE_DAYS so they all pass grace
        absent_counts = {t: 5 for t in existing_tickers}  # 5 > GRACE_DAYS=3
        state = {
            "run_date": "2026-04-04",
            "universe": existing_tickers,
            "absent_counts": absent_counts,
        }
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps(state))

        preset_path = tmp_path / "preset.json"
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        # Scanner returns completely different tickers (all old ones are absent)
        new_tickers = [f"NEW{i:02d}" for i in range(10)]
        mock_ib_client = _make_mock_ib_client(new_tickers)

        archived = []

        def fake_move(src, dst):
            archived.append(src)

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client), \
             patch("livewire_scripts.universe_screener._PRESET_PATH", preset_path), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir), \
             patch("shutil.move", fake_move), \
             patch("subprocess.run"):
            main(["--force"])

        # No tickers should have been archived since removals capped
        assert len(archived) == 0

    def test_force_reruns_same_day(self, tmp_path, monkeypatch):
        """--force flag bypasses idempotency check."""
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        today = date.today().isoformat()
        state = {"run_date": today, "universe": [], "absent_counts": {}}
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps(state))

        preset_path = tmp_path / "preset.json"
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        mock_ib_client = _make_mock_ib_client(["AAPL", "MSFT"])

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client), \
             patch("livewire_scripts.universe_screener._PRESET_PATH", preset_path), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir), \
             patch("subprocess.run"):
            # Should not raise SystemExit — --force bypasses idempotency
            main(["--force"])

        assert preset_path.exists()

    def test_confirmed_removals_are_archived(self, tmp_path, monkeypatch):
        """Tickers that pass the grace period are moved to bronze-delisted/."""
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
            ("trade_date", pa.date32()), ("symbol_id", pa.int64()),
            ("open", pa.float64()), ("high", pa.float64()),
            ("low", pa.float64()), ("close", pa.float64()),
            ("adj_close", pa.float64()), ("volume", pa.int64()),
        ])

        # Create OLD ticker that will be removed
        old_sym_dir = bronze_dir / "symbol=OLDTICKER"
        old_sym_dir.mkdir(parents=True)
        table = pa.Table.from_pylist([{
            "trade_date": date(2026, 1, 2), "symbol_id": 99,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "adj_close": 1.5, "volume": 1000,
        }], schema=schema)
        pq.write_table(table, old_sym_dir / "1d.parquet")

        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        # State with OLDTICKER absent for >= GRACE_DAYS
        state = {
            "run_date": "2026-04-04",
            "universe": ["OLDTICKER"],
            "absent_counts": {"OLDTICKER": 3},  # exactly GRACE_DAYS
        }
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps(state))

        preset_path = tmp_path / "preset.json"
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        # Scanner returns only new tickers (OLDTICKER absent)
        mock_ib_client = _make_mock_ib_client(["AAPL"])

        moves = []

        def fake_move(src, dst):
            moves.append((src, dst))

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client), \
             patch("livewire_scripts.universe_screener._PRESET_PATH", preset_path), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir), \
             patch("shutil.move", fake_move), \
             patch("subprocess.run"):
            main(["--force"])

        # OLDTICKER should have been archived
        assert len(moves) == 1
        assert "OLDTICKER" in moves[0][0]
        assert "bronze-delisted" in moves[0][1]

    def test_already_ran_today_exits(self, tmp_path, monkeypatch):
        """Already ran today skips without --force."""
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        today = date.today().isoformat()
        state = {"run_date": today, "universe": [], "absent_counts": {}}
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps(state))

        preset_path = tmp_path / "preset.json"
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir):
            with pytest.raises(SystemExit) as exc_info:
                main([])
            assert exc_info.value.code == 0

    def test_new_additions_trigger_backfill(self, tmp_path, monkeypatch):
        """New tickers in universe trigger backfill subprocess call."""
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        preset_path = tmp_path / "preset.json"
        state_path = tmp_path / "state.json"
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        # Scanner returns new tickers not in bronze
        mock_ib_client = _make_mock_ib_client(["AAPL", "MSFT", "NVDA"])

        subprocess_calls = []

        def fake_run(cmd, **kwargs):
            subprocess_calls.append(cmd)
            return MagicMock(returncode=0)

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client), \
             patch("livewire_scripts.universe_screener._PRESET_PATH", preset_path), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir), \
             patch("subprocess.run", fake_run):
            main(["--force"])

        # At least one subprocess call should include livewire_ingest.py for backfill
        backfill_calls = [c for c in subprocess_calls if "livewire_ingest.py" in " ".join(c)]
        assert len(backfill_calls) > 0

    def test_sends_alert_for_large_changes(self, tmp_path, monkeypatch):
        """Email alert sent when additions + removals exceed EMAIL_THRESHOLD."""
        from livewire_scripts.universe_screener import EMAIL_THRESHOLD
        data_lake = tmp_path / "data-lake"
        bronze_dir = data_lake / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", data_lake)

        preset_path = tmp_path / "preset.json"
        state_path = tmp_path / "state.json"
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        # Create enough new tickers to exceed EMAIL_THRESHOLD
        n = EMAIL_THRESHOLD + 5
        new_tickers = [f"NEW{i:03d}" for i in range(n)]
        mock_ib_client = _make_mock_ib_client(new_tickers)

        with patch("livewire_scripts.universe_screener.is_trading_day", return_value=True), \
             patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client), \
             patch("livewire_scripts.universe_screener._PRESET_PATH", preset_path), \
             patch("livewire_scripts.universe_screener._STATE_PATH", state_path), \
             patch("livewire_scripts.universe_screener._LOG_DIR", log_dir), \
             patch("livewire_scripts.universe_screener._send_screener_alert") as mock_alert, \
             patch("subprocess.run"):
            main(["--force"])

        mock_alert.assert_called_once()


# ══════════════════════════════════════════════════════════════════════
# load_core_etfs
# ══════════════════════════════════════════════════════════════════════


class TestLoadCoreEtfs:
    def test_load_core_etfs_returns_set(self, tmp_path, monkeypatch):
        path = tmp_path / "core-etfs.json"
        path.write_text(json.dumps({"name": "core-etfs", "tickers": ["SPY", "QQQ", "GLD"]}))
        monkeypatch.setattr("livewire_scripts.universe_screener._CORE_ETFS_PATH", path)
        result = load_core_etfs()
        assert result == {"SPY", "QQQ", "GLD"}

    def test_load_core_etfs_missing_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.universe_screener._CORE_ETFS_PATH", tmp_path / "nonexistent.json")
        assert load_core_etfs() == set()


# ══════════════════════════════════════════════════════════════════════
# Core ETF integration tests
# ══════════════════════════════════════════════════════════════════════


class TestCoreEtfIntegration:
    @pytest.fixture(autouse=True)
    def _no_scanner_throttle(self, monkeypatch):
        monkeypatch.setattr("livewire_scripts.universe_screener._SCANNER_THROTTLE_SECONDS", 0)

    def test_core_etf_added_when_missing(self, tmp_path, monkeypatch):
        """A core ETF not in bronze should be added even if not scanned."""
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", tmp_path / "data-lake")
        monkeypatch.setattr("livewire_scripts.universe_screener._STATE_PATH", tmp_path / "state.json")
        monkeypatch.setattr("livewire_scripts.universe_screener._LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr("livewire_scripts.universe_screener._PRESET_PATH", tmp_path / "preset.json")
        monkeypatch.setattr("livewire_scripts.universe_screener._CORE_ETFS_PATH", tmp_path / "core.json")

        # Write a core ETF preset
        (tmp_path / "core.json").write_text(json.dumps({"name": "core-etfs", "tickers": ["SPY", "QQQ"]}))
        # Empty bronze
        (tmp_path / "data-lake" / "bronze" / "asset_class=equity").mkdir(parents=True)
        # Scanner returns nothing
        mock_ib_client = _make_mock_ib_client([])

        with patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client):
            with patch("subprocess.run") as mock_subproc:
                with patch("sys.argv", ["universe_screener.py", "--force"]):
                    main()

        # Backfill must have been triggered for SPY and QQQ
        assert mock_subproc.called
        cmd = mock_subproc.call_args[0][0]
        assert "SPY" in cmd
        assert "QQQ" in cmd

    def test_core_etf_never_in_removals(self, tmp_path, monkeypatch):
        """A core ETF in bronze but not scanned should NEVER be archived."""
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", tmp_path / "data-lake")
        monkeypatch.setattr("livewire_scripts.universe_screener._STATE_PATH", tmp_path / "state.json")
        monkeypatch.setattr("livewire_scripts.universe_screener._LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr("livewire_scripts.universe_screener._PRESET_PATH", tmp_path / "preset.json")
        monkeypatch.setattr("livewire_scripts.universe_screener._CORE_ETFS_PATH", tmp_path / "core.json")

        (tmp_path / "core.json").write_text(json.dumps({"name": "core-etfs", "tickers": ["SPY"]}))

        # Pre-existing state from a previous run, not bootstrap
        (tmp_path / "state.json").write_text(json.dumps({
            "run_date": "2026-04-01",
            "universe": ["SPY", "AAPL"],
            "absent_counts": {},
        }))

        # Bronze contains SPY and AAPL
        for sym in ("SPY", "AAPL"):
            sym_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity" / f"symbol={sym}"
            sym_dir.mkdir(parents=True)
            (sym_dir / "1d.parquet").write_bytes(b"x")

        # Scanner returns AAPL but not SPY
        mock_ib_client = _make_mock_ib_client(["AAPL"])

        with patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client):
            with patch("subprocess.run"):
                with patch("sys.argv", ["universe_screener.py", "--force"]):
                    main()

        # SPY must NOT be archived
        spy_archive = tmp_path / "data-lake" / "bronze-delisted" / "asset_class=equity" / "symbol=SPY"
        assert not spy_archive.exists()

        # SPY must NOT appear in absent_counts in the new state
        state = json.loads((tmp_path / "state.json").read_text())
        assert "SPY" not in state["absent_counts"]

    def test_existing_absent_count_for_core_etf_is_dropped(self, tmp_path, monkeypatch):
        """Defensive: if a previous buggy run added a core ETF to absent_counts, drop it."""
        monkeypatch.setattr("livewire_scripts.universe_screener._DATA_LAKE", tmp_path / "data-lake")
        monkeypatch.setattr("livewire_scripts.universe_screener._STATE_PATH", tmp_path / "state.json")
        monkeypatch.setattr("livewire_scripts.universe_screener._LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr("livewire_scripts.universe_screener._PRESET_PATH", tmp_path / "preset.json")
        monkeypatch.setattr("livewire_scripts.universe_screener._CORE_ETFS_PATH", tmp_path / "core.json")

        (tmp_path / "core.json").write_text(json.dumps({"name": "core-etfs", "tickers": ["SPY"]}))

        # Buggy state: SPY has absent_count = 99 (way past grace, would be removed)
        (tmp_path / "state.json").write_text(json.dumps({
            "run_date": "2026-04-01",
            "universe": ["SPY", "AAPL"],
            "absent_counts": {"SPY": 99},
        }))

        for sym in ("SPY", "AAPL"):
            sym_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity" / f"symbol={sym}"
            sym_dir.mkdir(parents=True)
            (sym_dir / "1d.parquet").write_bytes(b"x")

        mock_ib_client = _make_mock_ib_client(["AAPL"])

        with patch("livewire_scripts.universe_screener.IBClient", return_value=mock_ib_client):
            with patch("subprocess.run"):
                with patch("sys.argv", ["universe_screener.py", "--force"]):
                    main()

        # SPY must NOT be archived even though absent_count was 99
        spy_archive = tmp_path / "data-lake" / "bronze-delisted" / "asset_class=equity" / "symbol=SPY"
        assert not spy_archive.exists()

        state = json.loads((tmp_path / "state.json").read_text())
        assert "SPY" not in state["absent_counts"]
