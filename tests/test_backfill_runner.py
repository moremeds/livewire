"""Tests for livewire_scripts/backfill_runner.py — full warehouse backfill."""

from __future__ import annotations

import json
from datetime import UTC
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

import pytest

from livewire_scripts.backfill_runner import (
    EQUITY_INTRADAY_TIMEFRAMES,
    VOL_INTRADAY_TIMEFRAMES,
    BackfillConfig,
    _derive_vol_1h,
    _file_mtime,
    _kill_process,
    _run_equity_intraday,
    _run_volatility_lanes,
    build_config,
    cursor_completed,
    gap_aware_completed,
    main,
    preset_info,
    run_backfill,
    run_preset,
    run_until_done,
)


@pytest.fixture(autouse=True)
def _flatfile_credentials(monkeypatch):
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "test-access")
    monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "test-secret")


def _make_config(tmp_path: Path) -> BackfillConfig:
    for name in ("sp500", "ndx100", "r2k"):
        preset = tmp_path / f"{name}.json"
        tickers = {"sp500": ["AAPL", "MSFT"], "ndx100": ["GOOG"], "r2k": ["IWM"]}
        preset.write_text(json.dumps({"name": name, "tickers": tickers[name]}))

    vol = tmp_path / "vol.json"
    vol.write_text(
        json.dumps(
            {
                "name": "vol-intraday",
                "tickers": ["VIX", "SPX", "NDX", "RUT", "VXN", "RVX"],
            }
        )
    )
    vol_daily = tmp_path / "vol_daily.json"
    vol_daily.write_text(json.dumps({"name": "volatility", "tickers": ["VIX"]}))

    return BackfillConfig(
        python_bin="/usr/bin/python3",
        ingest_script=tmp_path / "livewire_ingest.py",
        store_script=tmp_path / "livewire_store.py",
        log_dir=tmp_path / "logs",
        cursor_dir=tmp_path / "cursors",
        equity_presets=(
            str(tmp_path / "sp500.json"),
            str(tmp_path / "ndx100.json"),
            str(tmp_path / "r2k.json"),
        ),
        vol_preset=str(vol),
        vol_daily_preset=str(vol_daily),
        stall_timeout=60,
        stall_cooldown=1,
        success_cooldown=0,
        no_progress_cooldown=0,
        poll_interval=0.01,
        max_stale=2,
        batch_size=5,
        max_concurrent=10,
    )


def test_historical_cursor_writer_matches_backfill_runner_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import livewire_scripts.fetch_ib_historical as historical

    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    monkeypatch.delenv("MDW_LOG_DIR", raising=False)
    monkeypatch.setattr(historical, "CURSOR_DIR", None)

    config = build_config()

    assert historical._cursor_path("sp500") == config.log_dir / "cursor_sp500.json"


class _MockClock:
    """Controllable clock that advances with each sleep call."""

    def __init__(self, step: float = 1.0):
        self._time = 0.0
        self._step = step

    def monotonic(self) -> float:
        return self._time

    def sleep(self, seconds: float) -> None:
        self._time += seconds


class _MockProc:
    """Mock subprocess with controlled poll/wait behavior."""

    def __init__(self, poll_count: int = 1, returncode: int = 0):
        self._polls_left = poll_count
        self.returncode = returncode
        self.pid = 12345

    def poll(self):
        if self._polls_left > 0:
            self._polls_left -= 1
            return None
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode


class TestBuildConfig:
    def test_defaults(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        for var in (
            "MDW_PYTHON_BIN",
            "MDW_LOG_DIR",
            "MDW_CURSOR_DIR",
            "MDW_BACKFILL_STALL_TIMEOUT",
            "MDW_BACKFILL_STALL_COOLDOWN",
            "MDW_BACKFILL_COOLDOWN",
            "MDW_BACKFILL_SUCCESS_COOLDOWN",
            "MDW_BACKFILL_NO_PROGRESS_COOLDOWN",
            "MDW_BACKFILL_POLL_INTERVAL",
            "MDW_BACKFILL_MAX_STALE",
            "MDW_BACKFILL_BATCH_SIZE",
            "MDW_BACKFILL_MAX_CONCURRENT",
        ):
            monkeypatch.delenv(var, raising=False)

        config = build_config(tmp_path)
        assert config.stall_timeout == 600
        assert config.stall_cooldown == 300
        assert config.success_cooldown == 0
        assert config.no_progress_cooldown == 30
        assert config.poll_interval == 30.0
        assert config.max_stale == 3
        assert config.batch_size == 5
        assert config.max_concurrent == 10

    def test_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.setenv("MDW_BACKFILL_STALL_TIMEOUT", "120")
        monkeypatch.setenv("MDW_BACKFILL_STALL_COOLDOWN", "60")
        monkeypatch.setenv("MDW_BACKFILL_SUCCESS_COOLDOWN", "10")
        monkeypatch.setenv("MDW_BACKFILL_POLL_INTERVAL", "5")
        monkeypatch.setenv("MDW_BACKFILL_MAX_STALE", "5")
        monkeypatch.delenv("MDW_BACKFILL_COOLDOWN", raising=False)

        config = build_config(tmp_path)
        assert config.stall_timeout == 120
        assert config.stall_cooldown == 60
        assert config.success_cooldown == 10
        assert config.poll_interval == 5.0
        assert config.max_stale == 5

    def test_stall_cooldown_fallback(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_BACKFILL_STALL_COOLDOWN", raising=False)
        monkeypatch.setenv("MDW_BACKFILL_COOLDOWN", "99")

        config = build_config(tmp_path)
        assert config.stall_cooldown == 99


class TestPresetInfo:
    def test_reads_name_and_count(self, tmp_path):
        p = tmp_path / "test.json"
        p.write_text(json.dumps({"name": "my-preset", "tickers": ["A", "B", "C"]}))
        name, total = preset_info(str(p))
        assert name == "my-preset"
        assert total == 3


class TestCursorCompleted:
    def test_reads_completed_count(self, tmp_path):
        cursor = tmp_path / "cursor.json"
        cursor.write_text(json.dumps({"completed": ["A", "B"]}))
        assert cursor_completed(cursor) == 2

    def test_missing_file_returns_zero(self, tmp_path):
        assert cursor_completed(tmp_path / "missing.json") == 0

    def test_empty_json_returns_zero(self, tmp_path):
        cursor = tmp_path / "cursor.json"
        cursor.write_text("{}")
        assert cursor_completed(cursor) == 0

    def test_malformed_json_returns_zero(self, tmp_path):
        cursor = tmp_path / "cursor.json"
        cursor.write_text("not json")
        assert cursor_completed(cursor) == 0


class TestFileMtime:
    def test_existing_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")
        assert _file_mtime(f) > 0

    def test_missing_file(self, tmp_path):
        assert _file_mtime(tmp_path / "missing") == 0.0


class TestKillProcess:
    def test_kills_process_group(self):
        proc = MagicMock()
        proc.pid = 12345
        proc.wait.return_value = 0

        with patch("os.getpgid", return_value=12345) as mock_getpgid:
            with patch("os.killpg") as mock_killpg:
                _kill_process(proc)
                mock_killpg.assert_called_once()

    def test_handles_already_dead_process(self):
        proc = MagicMock()
        proc.pid = 12345

        with patch("os.getpgid", side_effect=ProcessLookupError):
            _kill_process(proc)

    def test_escalates_to_sigkill_on_timeout(self):
        import signal
        import subprocess

        proc = MagicMock()
        proc.pid = 12345
        proc.wait.side_effect = [subprocess.TimeoutExpired("cmd", 3), 0]

        with patch("os.getpgid", return_value=12345):
            with patch("os.killpg") as mock_killpg:
                _kill_process(proc)
                calls = mock_killpg.call_args_list
                assert len(calls) == 2
                assert calls[0][0][1] == signal.SIGTERM
                assert calls[1][0][1] == signal.SIGKILL

    def test_handles_sigkill_lookup_error(self):
        import subprocess

        proc = MagicMock()
        proc.pid = 12345
        proc.wait.side_effect = [subprocess.TimeoutExpired("cmd", 3), 0]

        call_count = [0]

        def mock_getpgid(pid):
            call_count[0] += 1
            if call_count[0] > 1:
                raise ProcessLookupError
            return pid

        with patch("os.getpgid", side_effect=mock_getpgid):
            with patch("os.killpg"):
                _kill_process(proc)


class TestRunPreset:
    def test_clean_exit(self, tmp_path):
        cursor = tmp_path / "cursor.json"
        cursor.write_text(json.dumps({"completed": ["A"]}))
        clock = _MockClock()
        mtime_val = [1.0]

        exit_code, delta = run_preset(
            "test",
            cursor,
            ["echo"],
            tmp_path / "logs",
            stall_timeout=60,
            poll_interval=1.0,
            popen_fn=lambda *a, **kw: _MockProc(poll_count=0, returncode=0),
            sleep_fn=clock.sleep,
            clock_fn=clock.monotonic,
            mtime_fn=lambda p: mtime_val[0],
            completed_fn=lambda p: 1,
            kill_fn=lambda p: None,
        )
        assert exit_code == 0
        assert delta == 0

    def test_stall_detection_kills_process(self, tmp_path):
        cursor = tmp_path / "cursor.json"
        clock = _MockClock(step=100)
        killed = []

        exit_code, delta = run_preset(
            "test",
            cursor,
            ["hang"],
            tmp_path / "logs",
            stall_timeout=60,
            poll_interval=100,
            popen_fn=lambda *a, **kw: _MockProc(poll_count=5, returncode=0),
            sleep_fn=clock.sleep,
            clock_fn=clock.monotonic,
            mtime_fn=lambda p: 1.0,  # never changes
            completed_fn=lambda p: 0,
            kill_fn=lambda p: killed.append(True),
        )
        assert exit_code == -1
        assert len(killed) == 1

    def test_activity_resets_stall_timer(self, tmp_path):
        cursor = tmp_path / "cursor.json"
        clock = _MockClock(step=30)
        poll_count = [0]
        mtime_counter = [0]

        def advancing_mtime(p):
            mtime_counter[0] += 1
            return float(mtime_counter[0])

        proc = _MockProc(poll_count=3, returncode=0)

        exit_code, delta = run_preset(
            "test",
            cursor,
            ["cmd"],
            tmp_path / "logs",
            stall_timeout=60,
            poll_interval=30,
            popen_fn=lambda *a, **kw: proc,
            sleep_fn=clock.sleep,
            clock_fn=clock.monotonic,
            mtime_fn=advancing_mtime,
            completed_fn=lambda p: 0,
            kill_fn=lambda p: None,
        )
        assert exit_code == 0


class TestRunUntilDone:
    def test_already_complete(self, tmp_path):
        config = _make_config(tmp_path)
        cursor = tmp_path / "cursor.json"
        cursor.write_text(json.dumps({"completed": ["A", "B", "C"]}))

        result = run_until_done(
            "test",
            cursor,
            3,
            ["cmd"],
            config,
            completed_fn=lambda p: 3,
            popen_fn=lambda *a, **kw: _MockProc(),
            sleep_fn=lambda s: None,
            clock_fn=lambda: 0,
            mtime_fn=lambda p: 0,
            kill_fn=lambda p: None,
        )
        assert result == 3

    def test_completes_after_run(self, tmp_path):
        config = _make_config(tmp_path)
        cursor = tmp_path / "cursor.json"
        completed_values = iter([0, 0, 2, 2])

        result = run_until_done(
            "test",
            cursor,
            2,
            ["cmd"],
            config,
            completed_fn=lambda p: next(completed_values),
            popen_fn=lambda *a, **kw: _MockProc(poll_count=0),
            sleep_fn=lambda s: None,
            clock_fn=lambda: 0,
            mtime_fn=lambda p: 0,
            kill_fn=lambda p: None,
        )
        assert result == 2

    def test_gives_up_after_max_stale(self, tmp_path):
        config = _make_config(tmp_path)
        cursor = tmp_path / "cursor.json"

        result = run_until_done(
            "test",
            cursor,
            10,
            ["cmd"],
            config,
            completed_fn=lambda p: 3,
            popen_fn=lambda *a, **kw: _MockProc(poll_count=0),
            sleep_fn=lambda s: None,
            clock_fn=lambda: 0,
            mtime_fn=lambda p: 0,
            kill_fn=lambda p: None,
        )
        assert result == 3

    def test_progress_resets_stale_count(self, tmp_path):
        config = _make_config(tmp_path)
        cursor = tmp_path / "cursor.json"
        # Sequence: 0 (check) -> 0 (before) -> 1 (after run_preset start) -> 1 (after run_preset end)
        # -> 1 (check) -> 1 (before) -> 2 (after) -> 2 (after) -> 2 (check) = done
        values = iter([0, 0, 1, 1, 1, 1, 2, 2, 2])

        result = run_until_done(
            "test",
            cursor,
            2,
            ["cmd"],
            config,
            completed_fn=lambda p: next(values),
            popen_fn=lambda *a, **kw: _MockProc(poll_count=0),
            sleep_fn=lambda s: None,
            clock_fn=lambda: 0,
            mtime_fn=lambda p: 0,
            kill_fn=lambda p: None,
        )
        assert result == 2

    def test_nonzero_exit_increments_stale(self, tmp_path):
        """Process exits non-zero (not stall) — exercises lines 265-273."""
        config = _make_config(tmp_path)
        cursor = tmp_path / "cursor.json"
        mtime_counter = [0]

        def advancing_mtime(p):
            mtime_counter[0] += 1
            return float(mtime_counter[0])

        result = run_until_done(
            "test",
            cursor,
            10,
            ["cmd"],
            config,
            completed_fn=lambda p: 0,
            popen_fn=lambda *a, **kw: _MockProc(poll_count=0, returncode=1),
            sleep_fn=lambda s: None,
            clock_fn=lambda: 0,
            mtime_fn=advancing_mtime,
            kill_fn=lambda p: None,
        )
        assert result == 0

    def test_stall_kill_increments_stale(self, tmp_path):
        config = _make_config(tmp_path)
        cursor = tmp_path / "cursor.json"
        clock = _MockClock(step=1000)

        result = run_until_done(
            "test",
            cursor,
            10,
            ["cmd"],
            config,
            completed_fn=lambda p: 0,
            popen_fn=lambda *a, **kw: _MockProc(poll_count=2),
            sleep_fn=clock.sleep,
            clock_fn=clock.monotonic,
            mtime_fn=lambda p: 1.0,
            kill_fn=lambda p: None,
        )
        assert result == 0


class TestRunEquityIntraday:
    def test_runs_one_full_market_flatfile_backfill(self, tmp_path):
        config = _make_config(tmp_path)
        commands: list = []

        def mock_runner(cmd, **kw):
            commands.append(cmd)
            return CompletedProcess(args=cmd, returncode=0)

        assert _run_equity_intraday(config, runner=mock_runner) == 0
        assert len(commands) == 1
        cmd = commands[0]
        assert cmd[:4] == [config.python_bin, str(config.ingest_script), "flatfile-ingest", "backfill"]
        assert "--workers" in cmd
        assert int(cmd[cmd.index("--workers") + 1]) >= 1


class TestDeriveVol1h:
    def test_no_30m_data_returns_zero(self, tmp_path):
        preset = tmp_path / "vol.json"
        preset.write_text(json.dumps({"tickers": ["VIX", "SPX"]}))
        warehouse = tmp_path / "warehouse"
        (warehouse / "data-lake" / "bronze" / "asset_class=volatility").mkdir(parents=True)
        result = _derive_vol_1h(str(preset), warehouse_dir=warehouse)
        assert result == 0

    def test_derives_1h_from_30m(self, tmp_path):
        from datetime import datetime

        from clients.intraday_bronze_client import IntradayBronzeClient

        preset = tmp_path / "vol.json"
        preset.write_text(json.dumps({"tickers": ["VIX"]}))
        warehouse = tmp_path / "warehouse"
        bronze_dir = warehouse / "data-lake" / "bronze" / "asset_class=volatility"
        bronze_dir.mkdir(parents=True)

        bronze_30m = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="30m")
        rows = [
            {
                "bar_timestamp": datetime(2026, 5, 28, 14, 0, tzinfo=UTC),
                "symbol_id": 1,
                "open": 20.0,
                "high": 22.0,
                "low": 19.0,
                "close": 21.0,
                "volume": 1000,
            },
            {
                "bar_timestamp": datetime(2026, 5, 28, 14, 30, tzinfo=UTC),
                "symbol_id": 1,
                "open": 21.0,
                "high": 23.0,
                "low": 20.0,
                "close": 22.0,
                "volume": 2000,
            },
        ]
        bronze_30m.replace_ticker_rows("VIX", rows)

        result = _derive_vol_1h(str(preset), warehouse_dir=warehouse)
        assert result == 1
        bronze_1h = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="1h")
        h_rows = bronze_1h.read_symbol_rows("VIX")
        assert len(h_rows) == 1
        assert h_rows[0]["volume"] == 3000


class TestRunVolatilityLanes:
    def test_runs_cboe_and_ib_intraday(self, tmp_path):
        config = _make_config(tmp_path)
        config.cursor_dir.mkdir(parents=True, exist_ok=True)

        name, total = preset_info(config.vol_preset)
        for tf in VOL_INTRADAY_TIMEFRAMES:
            cursor = config.cursor_dir / f"cursor_intraday_{tf}_{name}.json"
            cursor.write_text(json.dumps({"completed": list(range(total))}))

        run_commands: list = []

        def mock_runner(command, **kwargs):
            run_commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.backfill_runner._derive_vol_1h", return_value=0):
            _run_volatility_lanes(
                config,
                runner=mock_runner,
                popen_fn=lambda *a, **kw: _MockProc(poll_count=0),
                sleep_fn=lambda s: None,
                clock_fn=lambda: 0,
                mtime_fn=lambda p: 0,
                completed_fn=cursor_completed,
                kill_fn=lambda p: None,
            )
        assert len(run_commands) == 1  # just the CBOE cboe-vol command
        assert "cboe-vol" in run_commands[0]


class TestRunBackfill:
    def test_missing_flatfile_credentials_fail_before_phases(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MASSIVE_S3_SECRET_KEY")
        commands, inject = self._inject(tmp_path)
        assert run_backfill(_make_config(tmp_path), **inject) == 2
        assert commands == []

    def _inject(self, tmp_path):
        commands: list = []

        def mock_runner(command, **kwargs):
            commands.append(("run", command))
            return CompletedProcess(args=command, returncode=0)

        def mock_popen(cmd, **kw):
            commands.append(("popen", cmd))
            return _MockProc(poll_count=0)

        return commands, dict(
            runner=mock_runner,
            popen_fn=mock_popen,
            sleep_fn=lambda s: None,
            clock_fn=lambda: 0,
            mtime_fn=lambda p: 0,
            kill_fn=lambda p: None,
        )

    def test_full_run_completes(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        config = _make_config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        config.cursor_dir.mkdir(parents=True, exist_ok=True)

        # Pre-populate all cursors as complete
        for preset_path in config.equity_presets:
            name, total = preset_info(preset_path)
            (config.log_dir / f"cursor_{name}.json").write_text(json.dumps({"completed": list(range(total))}))
            (config.log_dir / f"cursor_backfill_{name}.json").write_text(json.dumps({"completed": list(range(total))}))
            for tf in EQUITY_INTRADAY_TIMEFRAMES:
                (config.cursor_dir / f"cursor_intraday_{tf}_{name}.json").write_text(
                    json.dumps({"completed": list(range(total))})
                )

        vol_name, vol_total = preset_info(config.vol_preset)
        for tf in VOL_INTRADAY_TIMEFRAMES:
            (config.cursor_dir / f"cursor_intraday_{tf}_{vol_name}.json").write_text(
                json.dumps({"completed": list(range(vol_total))})
            )

        commands, inject = self._inject(tmp_path)
        with patch("livewire_scripts.backfill_runner._derive_vol_1h", return_value=0):
            rc = run_backfill(config, **inject)
        assert rc == 0

        run_cmds = [c for kind, c in commands if kind == "run"]
        joined = [" ".join(c) for c in run_cmds]
        assert any("fred-rates" in c for c in joined)
        assert any("cboe-vol" in c for c in joined)

    def test_postgres_rebuild_when_configured(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_POSTGRES_DSN", "postgresql://test/db")
        config = _make_config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        config.cursor_dir.mkdir(parents=True, exist_ok=True)

        # Pre-populate cursors
        for preset_path in config.equity_presets:
            name, total = preset_info(preset_path)
            for prefix in ("cursor_", "cursor_backfill_"):
                (config.log_dir / f"{prefix}{name}.json").write_text(json.dumps({"completed": list(range(total))}))
            for tf in EQUITY_INTRADAY_TIMEFRAMES:
                (config.cursor_dir / f"cursor_intraday_{tf}_{name}.json").write_text(
                    json.dumps({"completed": list(range(total))})
                )

        vol_name, vol_total = preset_info(config.vol_preset)
        for tf in VOL_INTRADAY_TIMEFRAMES:
            (config.cursor_dir / f"cursor_intraday_{tf}_{vol_name}.json").write_text(
                json.dumps({"completed": list(range(vol_total))})
            )

        commands, inject = self._inject(tmp_path)
        with patch("livewire_scripts.backfill_runner._derive_vol_1h", return_value=0):
            rc = run_backfill(config, **inject)
        assert rc == 0

        run_cmds = [" ".join(c) for kind, c in commands if kind == "run"]
        assert any("rebuild-postgres" in c and "equity" in c for c in run_cmds)
        assert any("rebuild-postgres" in c and "volatility" in c for c in run_cmds)

    def _prepopulate_cursors(self, config):
        config.log_dir.mkdir(parents=True, exist_ok=True)
        config.cursor_dir.mkdir(parents=True, exist_ok=True)
        for preset_path in config.equity_presets:
            name, total = preset_info(preset_path)
            for prefix in ("cursor_", "cursor_backfill_"):
                (config.log_dir / f"{prefix}{name}.json").write_text(json.dumps({"completed": list(range(total))}))
            for tf in EQUITY_INTRADAY_TIMEFRAMES:
                (config.cursor_dir / f"cursor_intraday_{tf}_{name}.json").write_text(
                    json.dumps({"completed": list(range(total))})
                )
        vol_name, vol_total = preset_info(config.vol_preset)
        for tf in VOL_INTRADAY_TIMEFRAMES:
            (config.cursor_dir / f"cursor_intraday_{tf}_{vol_name}.json").write_text(
                json.dumps({"completed": list(range(vol_total))})
            )

    def test_fred_failure_logged(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        config = _make_config(tmp_path)
        self._prepopulate_cursors(config)

        def fail_fred(command, **kwargs):
            if "fred-rates" in command:
                return CompletedProcess(args=command, returncode=1)
            return CompletedProcess(args=command, returncode=0)

        _, inject = self._inject(tmp_path)
        inject["runner"] = fail_fred
        with patch("livewire_scripts.backfill_runner._derive_vol_1h", return_value=0):
            rc = run_backfill(config, **inject)
        assert rc == 0

    def test_cboe_failure_logged(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        config = _make_config(tmp_path)
        self._prepopulate_cursors(config)

        def fail_cboe(command, **kwargs):
            if "cboe-vol" in command:
                return CompletedProcess(args=command, returncode=1)
            return CompletedProcess(args=command, returncode=0)

        _, inject = self._inject(tmp_path)
        inject["runner"] = fail_cboe
        with patch("livewire_scripts.backfill_runner._derive_vol_1h", return_value=0):
            rc = run_backfill(config, **inject)
        assert rc == 0

    def test_parallel_lane_failure(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        config = _make_config(tmp_path)
        self._prepopulate_cursors(config)

        _, inject = self._inject(tmp_path)
        with patch("livewire_scripts.backfill_runner._run_equity_intraday", return_value=1):
            with patch("livewire_scripts.backfill_runner._derive_vol_1h", return_value=0):
                rc = run_backfill(config, **inject)
        assert rc == 1

    def test_postgres_failure_logged(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_POSTGRES_DSN", "postgresql://test/db")
        config = _make_config(tmp_path)
        self._prepopulate_cursors(config)

        def fail_postgres(command, **kwargs):
            if "rebuild-postgres" in command:
                return CompletedProcess(args=command, returncode=1)
            return CompletedProcess(args=command, returncode=0)

        _, inject = self._inject(tmp_path)
        inject["runner"] = fail_postgres
        with patch("livewire_scripts.backfill_runner._derive_vol_1h", return_value=0):
            rc = run_backfill(config, **inject)
        assert rc == 0


class TestMain:
    def test_default(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)

        with patch("livewire_scripts.backfill_runner.run_backfill", return_value=0) as mock:
            rc = main([])
        assert rc == 0

    def test_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)

        with patch("livewire_scripts.backfill_runner.run_backfill", return_value=0) as mock:
            main(["--stall-timeout", "120", "--poll-interval", "5"])
        config = mock.call_args[0][0]
        assert config.stall_timeout == 120
        assert config.poll_interval == 5.0

    def test_no_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)

        with patch("livewire_scripts.backfill_runner.run_backfill", return_value=0) as mock:
            main([])
        config = mock.call_args[0][0]
        assert config.stall_timeout == 600


class TestGapAwareCompleted:
    def test_cursor_not_done_returns_cursor_count(self, tmp_path):
        """If cursor count < total, return cursor count regardless of gaps."""
        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"]}}))
        result = gap_aware_completed(
            cursor_file,
            total=5,
            preset_path=None,
            warehouse_dir=tmp_path,
        )
        assert result == 1

    def test_cursor_done_no_gaps_returns_total(self, tmp_path):
        """If cursor says done and no gaps found, return total."""
        from datetime import date

        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"], "MSFT": ["1d"]}}))
        registry_path = tmp_path / "registry.json"
        registry_data = {
            "version": 1,
            "updated_at": "2026-05-31T00:00:00",
            "tickers": {
                "AAPL": {
                    "tags": ["sp500"],
                    "status": "active",
                    "added_at": "2026-01-01",
                    "last_verified": "2026-01-01",
                    "earliest_available": "2026-05-29",
                },
                "MSFT": {
                    "tags": ["sp500"],
                    "status": "active",
                    "added_at": "2026-01-01",
                    "last_verified": "2026-01-01",
                    "earliest_available": "2026-05-29",
                },
            },
            "changelog": [],
        }
        registry_path.write_text(json.dumps(registry_data))

        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 29)],
            "MSFT": [date(2026, 5, 29)],
        }
        with patch(
            "clients.bronze_client.BronzeClient",
            return_value=mock_bronze,
        ):
            result = gap_aware_completed(
                cursor_file,
                total=2,
                preset_path=None,
                warehouse_dir=tmp_path,
                bronze_dir=bronze_dir,
                as_of=date(2026, 5, 29),
            )
        assert result == 2

    def test_cursor_done_with_gaps_returns_less_than_total(self, tmp_path):
        """If cursor says done but gaps exist, return less than total."""
        from datetime import date

        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"], "MSFT": ["1d"]}}))
        registry_path = tmp_path / "registry.json"
        registry_data = {
            "version": 1,
            "updated_at": "2026-05-31T00:00:00",
            "tickers": {
                "AAPL": {
                    "tags": ["sp500"],
                    "status": "active",
                    "added_at": "2026-01-01",
                    "last_verified": "2026-01-01",
                    "earliest_available": "2026-05-27",
                },
                "MSFT": {
                    "tags": ["sp500"],
                    "status": "active",
                    "added_at": "2026-01-01",
                    "last_verified": "2026-01-01",
                    "earliest_available": "2026-05-27",
                },
            },
            "changelog": [],
        }
        registry_path.write_text(json.dumps(registry_data))

        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 27), date(2026, 5, 28), date(2026, 5, 29)],
            "MSFT": [date(2026, 5, 27), date(2026, 5, 29)],
        }
        with patch(
            "clients.bronze_client.BronzeClient",
            return_value=mock_bronze,
        ):
            result = gap_aware_completed(
                cursor_file,
                total=2,
                preset_path=None,
                warehouse_dir=tmp_path,
                bronze_dir=bronze_dir,
                as_of=date(2026, 5, 29),
            )
        assert result == 1

    def test_no_registry_no_depth_info_returns_zero(self, tmp_path):
        """When registry.json doesn't exist and a *backfill* cursor has no backfill_depth,
        return 0 to force a re-run (old cursor format cannot be trusted)."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"], "MSFT": ["1d"]}}))
        result = gap_aware_completed(
            cursor_file,
            total=2,
            preset_path=None,
            warehouse_dir=tmp_path,
        )
        assert result == 0

    def test_no_registry_seed_cursor_trusts_count(self, tmp_path):
        """Seed cursors (no 'backfill_' in name) skip depth check and return cursor count."""
        cursor_file = tmp_path / "cursor_sp500.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"], "MSFT": ["1d"]}}))
        result = gap_aware_completed(
            cursor_file,
            total=2,
            preset_path=None,
            warehouse_dir=tmp_path,
        )
        assert result == 2

    def test_no_registry_deep_depth_counts_as_complete(self, tmp_path):
        """When registry is absent but cursor has deep backfill_depth, count as complete."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"], "MSFT": ["1d"]},
                    "backfill_depth": {
                        "AAPL": {"oldest_date": "1999-01-04", "noop": False},
                        "MSFT": {"oldest_date": "2001-06-01", "noop": False},
                    },
                }
            )
        )
        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"tickers": ["AAPL", "MSFT"]}))
        result = gap_aware_completed(
            cursor_file,
            total=2,
            preset_path=str(preset),
            warehouse_dir=tmp_path,
        )
        assert result == 2

    def test_no_registry_noop_entries_count_as_complete(self, tmp_path):
        """noop=True entries (IB confirmed no older data) count as complete."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"NEWCO": ["1d"]},
                    "backfill_depth": {
                        "NEWCO": {"oldest_date": "2022-03-15", "noop": True},
                    },
                }
            )
        )
        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"tickers": ["NEWCO"]}))
        result = gap_aware_completed(
            cursor_file,
            total=1,
            preset_path=str(preset),
            warehouse_dir=tmp_path,
        )
        assert result == 1

    def test_no_registry_shallow_depth_not_counted(self, tmp_path, monkeypatch):
        """Entries with oldest_date after MDW_BACKFILL_MIN_DEPTH_DATE are not counted."""
        monkeypatch.setenv("MDW_BACKFILL_MIN_DEPTH_DATE", "2010-01-01")
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"]},
                    "backfill_depth": {
                        # oldest_date is AFTER the min threshold — shallow history
                        "AAPL": {"oldest_date": "2021-01-04", "noop": False},
                    },
                }
            )
        )
        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"tickers": ["AAPL"]}))
        result = gap_aware_completed(
            cursor_file,
            total=1,
            preset_path=str(preset),
            warehouse_dir=tmp_path,
        )
        assert result == 0

    def test_no_registry_mixed_deep_and_shallow(self, tmp_path, monkeypatch):
        """Mixed: deep + noop count; shallow does not."""
        monkeypatch.setenv("MDW_BACKFILL_MIN_DEPTH_DATE", "2010-01-01")
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"], "NEWCO": ["1d"], "SHALLOW": ["1d"]},
                    "backfill_depth": {
                        "AAPL": {"oldest_date": "1999-01-04", "noop": False},  # deep
                        "NEWCO": {"oldest_date": "2022-03-15", "noop": True},  # noop
                        "SHALLOW": {"oldest_date": "2021-01-04", "noop": False},  # shallow
                    },
                }
            )
        )
        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"tickers": ["AAPL", "NEWCO", "SHALLOW"]}))
        result = gap_aware_completed(
            cursor_file,
            total=3,
            preset_path=str(preset),
            warehouse_dir=tmp_path,
        )
        assert result == 2  # AAPL + NEWCO, not SHALLOW

    def test_no_registry_no_preset_uses_depth_info_keys(self, tmp_path):
        """When no preset_path, tickers come from depth_info keys."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"]},
                    "backfill_depth": {
                        "AAPL": {"oldest_date": "1999-01-04", "noop": False},
                    },
                }
            )
        )
        result = gap_aware_completed(
            cursor_file,
            total=1,
            preset_path=None,
            warehouse_dir=tmp_path,
        )
        assert result == 1

    def test_no_registry_corrupt_cursor_returns_zero(self, tmp_path):
        """If cursor is unreadable when registry absent, return 0."""
        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text("not json")
        result = gap_aware_completed(
            cursor_file,
            total=1,
            preset_path=None,
            warehouse_dir=tmp_path,
        )
        assert result == 0

    def test_no_registry_preset_unreadable_falls_back_to_depth_keys(self, tmp_path):
        """When preset_path is given but unreadable, fall back to depth_info keys."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"]},
                    "backfill_depth": {
                        "AAPL": {"oldest_date": "1999-01-04", "noop": False},
                    },
                }
            )
        )
        result = gap_aware_completed(
            cursor_file,
            total=1,
            preset_path=str(tmp_path / "nonexistent.json"),
            warehouse_dir=tmp_path,
        )
        assert result == 1

    def test_preset_path_limits_ticker_scope(self, tmp_path):
        """When preset_path is provided, only those tickers are gap-checked."""
        from datetime import date

        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"], "MSFT": ["1d"]}}))
        registry_path = tmp_path / "registry.json"
        registry_data = {
            "version": 1,
            "updated_at": "2026-05-31T00:00:00",
            "tickers": {
                "AAPL": {
                    "tags": ["sp500"],
                    "status": "active",
                    "added_at": "2026-01-01",
                    "last_verified": "2026-01-01",
                    "earliest_available": "2026-05-29",
                },
            },
            "changelog": [],
        }
        registry_path.write_text(json.dumps(registry_data))
        preset_path = tmp_path / "preset.json"
        preset_path.write_text(json.dumps({"tickers": ["AAPL"]}))

        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 29)],
        }
        with patch(
            "clients.bronze_client.BronzeClient",
            return_value=mock_bronze,
        ):
            result = gap_aware_completed(
                cursor_file,
                total=2,
                preset_path=str(preset_path),
                warehouse_dir=tmp_path,
                bronze_dir=bronze_dir,
                as_of=date(2026, 5, 29),
            )
        assert result == 2

    def test_tickers_without_earliest_are_skipped(self, tmp_path):
        """Tickers in registry but without earliest_available are skipped."""
        from datetime import date

        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"], "MSFT": ["1d"]}}))
        registry_path = tmp_path / "registry.json"
        registry_data = {
            "version": 1,
            "updated_at": "2026-05-31T00:00:00",
            "tickers": {
                "AAPL": {
                    "tags": ["sp500"],
                    "status": "active",
                    "added_at": "2026-01-01",
                    "last_verified": "2026-01-01",
                    "earliest_available": "2026-05-29",
                },
                "MSFT": {
                    "tags": ["sp500"],
                    "status": "active",
                    "added_at": "2026-01-01",
                    "last_verified": "2026-01-01",
                },
            },
            "changelog": [],
        }
        registry_path.write_text(json.dumps(registry_data))

        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 29)],
            "MSFT": [date(2026, 5, 29)],
        }
        with patch(
            "clients.bronze_client.BronzeClient",
            return_value=mock_bronze,
        ):
            result = gap_aware_completed(
                cursor_file,
                total=2,
                preset_path=None,
                warehouse_dir=tmp_path,
                bronze_dir=bronze_dir,
                as_of=date(2026, 5, 29),
            )
        # MSFT has no earliest_available so it's skipped; AAPL is complete
        assert result == 2

    def test_exception_falls_back_to_cursor_count(self, tmp_path):
        """If gap analysis raises, fall back to cursor count."""
        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"], "MSFT": ["1d"]}}))
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "updated_at": "2026-05-31T00:00:00",
                    "tickers": {
                        "AAPL": {
                            "tags": ["sp500"],
                            "status": "active",
                            "added_at": "2026-01-01",
                            "last_verified": "2026-01-01",
                            "earliest_available": "2026-05-29",
                        },
                    },
                    "changelog": [],
                }
            )
        )

        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        with patch(
            "clients.bronze_client.BronzeClient",
            side_effect=RuntimeError("boom"),
        ):
            result = gap_aware_completed(
                cursor_file,
                total=2,
                preset_path=None,
                warehouse_dir=tmp_path,
                bronze_dir=bronze_dir,
            )
        assert result == 2

    def test_no_registry_cursor_read_error_returns_zero(self, tmp_path):
        """If cursor file read_text raises an OS-level error, depth_info is empty → return 0."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        # Write a valid cursor so cursor_completed (which uses .open()) returns >= total.
        # Then patch read_text so the second read inside the registry-absent branch fails.
        cursor_file.write_text(json.dumps({"completed": {"AAPL": ["1d"]}}))

        with patch.object(cursor_file.__class__, "read_text", side_effect=OSError("simulated")):
            result = gap_aware_completed(
                cursor_file,
                total=1,
                preset_path=None,
                warehouse_dir=tmp_path,
            )
        assert result == 0

    def test_no_registry_ticker_not_in_depth_info_skipped(self, tmp_path):
        """Preset tickers absent from depth_info are skipped (not counted)."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"], "MSFT": ["1d"]},
                    "backfill_depth": {
                        # AAPL deep; MSFT absent (not yet processed)
                        "AAPL": {"oldest_date": "1999-01-04", "noop": False},
                    },
                }
            )
        )
        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"tickers": ["AAPL", "MSFT"]}))
        result = gap_aware_completed(
            cursor_file,
            total=2,
            preset_path=str(preset),
            warehouse_dir=tmp_path,
        )
        # Only AAPL is counted; MSFT is absent from depth_info → skipped
        assert result == 1

    def test_no_registry_bad_oldest_date_format_skipped(self, tmp_path):
        """Entries with malformed oldest_date don't crash and are not counted."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"]},
                    "backfill_depth": {
                        "AAPL": {"oldest_date": "not-a-date", "noop": False},
                    },
                }
            )
        )
        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"tickers": ["AAPL"]}))
        result = gap_aware_completed(
            cursor_file,
            total=1,
            preset_path=str(preset),
            warehouse_dir=tmp_path,
        )
        assert result == 0

    def test_no_registry_missing_oldest_date_key_skipped(self, tmp_path):
        """Entries missing the oldest_date key entirely don't crash and are not counted."""
        cursor_file = tmp_path / "cursor_backfill_sp500.json"
        cursor_file.write_text(
            json.dumps(
                {
                    "completed": {"AAPL": ["1d"]},
                    "backfill_depth": {
                        "AAPL": {"noop": False},  # no oldest_date key
                    },
                }
            )
        )
        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"tickers": ["AAPL"]}))
        result = gap_aware_completed(
            cursor_file,
            total=1,
            preset_path=str(preset),
            warehouse_dir=tmp_path,
        )
        assert result == 0
