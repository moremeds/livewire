"""Tests for livewire_scripts/sync_runner.py — daily sync orchestrator."""

from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest

from livewire_scripts.sync_runner import (
    EQUITY_INTRADAY_TIMEFRAMES,
    VOL_INTRADAY_TIMEFRAMES,
    SyncConfig,
    _format_command,
    build_config,
    latest_complete_trading_day,
    load_tickers,
    main,
    run_phase,
    run_sync,
    ticker_union,
)


def _make_config(tmp_path: Path) -> SyncConfig:
    for name in ("sp500", "ndx100", "r2k"):
        preset = tmp_path / f"{name}.json"
        if name == "sp500":
            preset.write_text(json.dumps({"name": name, "tickers": ["AAPL", "MSFT"]}))
        elif name == "ndx100":
            preset.write_text(json.dumps({"name": name, "tickers": ["MSFT", "GOOG"]}))
        else:
            preset.write_text(json.dumps({"name": name, "tickers": ["IWM"]}))

    vol = tmp_path / "vol.json"
    vol.write_text(
        json.dumps({"name": "volatility-intraday", "tickers": ["VIX", "SPX"]})
    )
    vol_daily = tmp_path / "vol_daily.json"
    vol_daily.write_text(json.dumps({"name": "volatility", "tickers": ["VIX"]}))

    return SyncConfig(
        python_bin="/usr/bin/python3",
        ingest_script=tmp_path / "livewire_ingest.py",
        store_script=tmp_path / "livewire_store.py",
        log_dir=tmp_path / "logs",
        equity_presets=(
            str(tmp_path / "sp500.json"),
            str(tmp_path / "ndx100.json"),
            str(tmp_path / "r2k.json"),
        ),
        vol_preset=str(vol),
        vol_daily_preset=str(vol_daily),
        intraday_days=3,
        intraday_concurrent=10,
        target_date="2026-05-28",
    )


def _ok_runner(command, **kwargs):
    return CompletedProcess(args=command, returncode=0)


def _fail_runner(command, **kwargs):
    return CompletedProcess(args=command, returncode=1)


class TestBuildConfig:
    def test_defaults(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_PYTHON_BIN", raising=False)
        monkeypatch.delenv("MDW_LOG_DIR", raising=False)
        monkeypatch.delenv("MDW_DAILY_BACKFILL_INTRADAY_DAYS", raising=False)
        monkeypatch.delenv("MDW_DAILY_BACKFILL_INTRADAY_CONCURRENT", raising=False)
        monkeypatch.delenv("MDW_DAILY_BACKFILL_TARGET_DATE", raising=False)

        config = build_config(tmp_path)
        assert config.log_dir == tmp_path / "logs"
        assert config.intraday_days == 7
        assert config.intraday_concurrent == 20
        assert config.target_date is None

    def test_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.setenv("MDW_PYTHON_BIN", "/venv/bin/python")
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "custom"))
        monkeypatch.setenv("MDW_DAILY_BACKFILL_INTRADAY_DAYS", "14")
        monkeypatch.setenv("MDW_DAILY_BACKFILL_INTRADAY_CONCURRENT", "5")
        monkeypatch.setenv("MDW_DAILY_BACKFILL_TARGET_DATE", "2026-05-20")

        config = build_config(tmp_path)
        assert config.python_bin == "/venv/bin/python"
        assert config.log_dir == tmp_path / "custom"
        assert config.intraday_days == 14
        assert config.intraday_concurrent == 5
        assert config.target_date == "2026-05-20"

    def test_empty_target_date_becomes_none(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.setenv("MDW_DAILY_BACKFILL_TARGET_DATE", "")
        config = build_config(tmp_path)
        assert config.target_date is None


class TestLoadTickers:
    def test_reads_preset(self, tmp_path):
        preset = tmp_path / "test.json"
        preset.write_text(json.dumps({"name": "test", "tickers": ["aapl", "MSFT"]}))
        result = load_tickers(str(preset))
        assert result == ["AAPL", "MSFT"]

    def test_missing_tickers_key(self, tmp_path):
        preset = tmp_path / "test.json"
        preset.write_text(json.dumps({"name": "test"}))
        assert load_tickers(str(preset)) == []


class TestTickerUnion:
    def test_deduplicates_and_sorts(self, tmp_path):
        p1 = tmp_path / "a.json"
        p1.write_text(json.dumps({"tickers": ["AAPL", "MSFT"]}))
        p2 = tmp_path / "b.json"
        p2.write_text(json.dumps({"tickers": ["MSFT", "GOOG"]}))

        result = ticker_union([str(p1), str(p2)])
        assert result == ["AAPL", "GOOG", "MSFT"]


class TestLatestCompleteTradingDay:
    def test_weekend_returns_friday(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        with patch("livewire_scripts.sync_runner.datetime") as mock_dt:
            sat = datetime(2026, 5, 30, 12, 0, tzinfo=ZoneInfo("America/New_York"))
            mock_dt.now.return_value = sat
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            with patch(
                "livewire_scripts.daily_update.is_trading_day", return_value=False
            ):
                from datetime import date

                with patch(
                    "livewire_scripts.daily_update.previous_trading_day",
                    return_value=date(2026, 5, 29),
                ):
                    result = latest_complete_trading_day()
                    assert result == "2026-05-29"

    def test_trading_day_after_close(self):
        from datetime import date, datetime, time
        from zoneinfo import ZoneInfo

        with patch("livewire_scripts.sync_runner.datetime") as mock_dt:
            after_close = datetime(
                2026, 5, 28, 17, 0, tzinfo=ZoneInfo("America/New_York")
            )
            mock_dt.now.return_value = after_close
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            with patch(
                "livewire_scripts.daily_update.is_trading_day", return_value=True
            ):
                with patch(
                    "livewire_scripts.daily_update.session_close_time",
                    return_value=time(16, 0),
                ):
                    result = latest_complete_trading_day()
                    assert result == "2026-05-28"

    def test_trading_day_before_close(self):
        from datetime import date, datetime, time
        from zoneinfo import ZoneInfo

        with patch("livewire_scripts.sync_runner.datetime") as mock_dt:
            before_close = datetime(
                2026, 5, 28, 15, 0, tzinfo=ZoneInfo("America/New_York")
            )
            mock_dt.now.return_value = before_close
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            with patch(
                "livewire_scripts.daily_update.is_trading_day", return_value=True
            ):
                with patch(
                    "livewire_scripts.daily_update.session_close_time",
                    return_value=time(16, 0),
                ):
                    with patch(
                        "livewire_scripts.daily_update.previous_trading_day",
                        return_value=date(2026, 5, 27),
                    ):
                        result = latest_complete_trading_day()
                        assert result == "2026-05-27"


class TestFormatCommand:
    def test_short_command(self):
        assert _format_command(["python", "-m", "pytest"]) == "python -m pytest"

    def test_long_command_truncated(self):
        parts = [f"arg{i}" for i in range(30)]
        result = _format_command(parts, limit=5)
        assert result == "arg0 arg1 arg2 arg3 arg4 ... [25 more args]"


class TestRunPhase:
    def test_success(self, tmp_path):
        rc = run_phase("test", ["echo", "hi"], tmp_path, runner=_ok_runner)
        assert rc == 0
        assert (tmp_path / "test.log").exists()

    def test_failure(self, tmp_path):
        rc = run_phase("test", ["fail"], tmp_path, runner=_fail_runner)
        assert rc == 1

    def test_failure_with_completed_summary(self, tmp_path):
        def runner_with_marker(command, **kwargs):
            stdout = kwargs.get("stdout")
            if stdout is not None:
                stdout.write("Daily Update Complete\n")
            return CompletedProcess(args=command, returncode=1)

        rc = run_phase(
            "test",
            ["cmd"],
            tmp_path,
            allow_completed_summary=True,
            runner=runner_with_marker,
        )
        assert rc == 0

    def test_failure_without_marker_not_suppressed(self, tmp_path):
        rc = run_phase(
            "test",
            ["cmd"],
            tmp_path,
            allow_completed_summary=True,
            runner=_fail_runner,
        )
        assert rc == 1

    def test_completed_summary_log_file_missing(self, tmp_path):
        """FileNotFoundError branch when log file vanishes between write and read."""
        log_dir = tmp_path / "logs"

        def runner_that_deletes_log(command, **kwargs):
            stdout = kwargs.get("stdout")
            if stdout is not None:
                stdout.close()
            log_file = log_dir / "vanish.log"
            if log_file.exists():
                log_file.unlink()
            return CompletedProcess(args=command, returncode=1)

        rc = run_phase(
            "vanish",
            ["cmd"],
            log_dir,
            allow_completed_summary=True,
            runner=runner_that_deletes_log,
        )
        assert rc == 1


class TestRunSync:
    def test_all_phases_succeed(self, tmp_path):
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        rc = run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        assert rc == 0

        joined = [" ".join(c) for c in commands]
        assert any("daily" in c and "--source massive" in c for c in joined)
        assert any("fred-rates" in c for c in joined)
        assert any("cboe-vol" in c for c in joined)
        assert any("--timeframe 1m" in c for c in joined)
        assert any("--timeframe 5m" in c and "equity" in c for c in joined)
        assert any("--timeframe 1h" in c and "equity" in c for c in joined)
        assert any("--timeframe 5m" in c and "volatility" in c for c in joined)
        assert any("--timeframe 1h" in c and "volatility" in c for c in joined)

    def test_uses_target_date_from_config(self, tmp_path):
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        run_sync(config, runner=capture, trading_day_fn=lambda: "should-not-use")
        assert any("2026-05-28" in c for c in commands[0])

    def test_auto_detects_trading_day(self, tmp_path):
        config = _make_config(tmp_path)
        config = SyncConfig(**{**vars(config), "target_date": None})
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-27")
        assert any("2026-05-27" in c for c in commands[0])

    def test_phase_failure_returns_nonzero(self, tmp_path):
        config = _make_config(tmp_path)
        rc = run_sync(config, runner=_fail_runner, trading_day_fn=lambda: "2026-05-28")
        assert rc == 1

    def test_postgres_rebuild_when_dsn_set(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_POSTGRES_DSN", "postgresql://test/db")
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        joined = [" ".join(c) for c in commands]
        assert any("rebuild-postgres" in c and "equity" in c for c in joined)
        assert any("rebuild-postgres" in c and "volatility" in c for c in joined)

    def test_postgres_failure_returns_nonzero(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_POSTGRES_DSN", "postgresql://test/db")
        config = _make_config(tmp_path)

        def selective_runner(command, **kwargs):
            if "rebuild-postgres" in command:
                return CompletedProcess(args=command, returncode=1)
            return CompletedProcess(args=command, returncode=0)

        rc = run_sync(
            config, runner=selective_runner, trading_day_fn=lambda: "2026-05-28"
        )
        assert rc == 1

    def test_postgres_skipped_when_dsn_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        joined = [" ".join(c) for c in commands]
        assert not any("rebuild-postgres" in c for c in joined)

    def test_expected_phase_count(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        # 1 equity daily + 1 FRED + 1 CBOE + 3 equity intraday + 2 vol intraday = 8
        assert len(commands) == 8


class TestMain:
    def test_default_args(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        monkeypatch.delenv("MDW_DAILY_BACKFILL_TARGET_DATE", raising=False)

        with patch("livewire_scripts.sync_runner.run_sync", return_value=0) as mock:
            rc = main(["--target-date", "2026-05-28"])
        assert rc == 0
        config = mock.call_args[0][0]
        assert config.target_date == "2026-05-28"

    def test_all_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)

        with patch("livewire_scripts.sync_runner.run_sync", return_value=0) as mock:
            main(
                [
                    "--target-date",
                    "2026-05-20",
                    "--intraday-days",
                    "14",
                    "--intraday-concurrent",
                    "5",
                ]
            )
        config = mock.call_args[0][0]
        assert config.target_date == "2026-05-20"
        assert config.intraday_days == 14
        assert config.intraday_concurrent == 5

    def test_no_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)
        monkeypatch.delenv("MDW_DAILY_BACKFILL_TARGET_DATE", raising=False)

        with patch("livewire_scripts.sync_runner.run_sync", return_value=0) as mock:
            main([])
        config = mock.call_args[0][0]
        assert config.target_date is None
