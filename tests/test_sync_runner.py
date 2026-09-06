"""Tests for livewire_scripts/sync_runner.py — daily sync orchestrator."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE
from livewire_scripts import sync_runner
from livewire_scripts.daily_outcomes import SUMMARY_PREFIX
from livewire_scripts.sync_runner import (
    SyncConfig,
    _derive_vol_1h,
    _format_command,
    build_config,
    latest_complete_trading_day,
    load_tickers,
    main,
    run_phase,
    run_sync,
    ticker_union,
)


def _summary_line(*, updated: int = 1, errors: int = 0) -> str:
    payload = {
        "job": "daily_update",
        "asset_class": "equity",
        "source": "massive",
        "target_date": "2026-05-28",
        "updated": updated,
        "no_trade": 0,
        "partial": 0,
        "errors": errors,
        "bars_inserted": updated,
        "validation_issues": 0,
        "top_errors": [],
    }
    return "SUMMARY_JSON " + json.dumps(payload, separators=(",", ":")) + "\n"


@pytest.fixture(autouse=True)
def _flatfile_credentials(monkeypatch):
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "test-access")
    monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "test-secret")


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
        json.dumps(
            {
                "name": "volatility-intraday",
                "tickers": ["VIX", "SPX", "NDX", "RUT", "VXN", "RVX"],
            }
        )
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
        monkeypatch.delenv("MDW_DAILY_BACKFILL_TARGET_DATE", raising=False)

        config = build_config(tmp_path)
        assert config.log_dir == tmp_path / "logs"
        assert config.intraday_days == 7
        assert config.target_date is None

    def test_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.setenv("MDW_PYTHON_BIN", "/venv/bin/python")
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "custom"))
        monkeypatch.setenv("MDW_DAILY_BACKFILL_INTRADAY_DAYS", "14")
        monkeypatch.setenv("MDW_DAILY_BACKFILL_TARGET_DATE", "2026-05-20")

        config = build_config(tmp_path)
        assert config.python_bin == "/venv/bin/python"
        assert config.log_dir == tmp_path / "custom"
        assert config.intraday_days == 14
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

            with patch("livewire_scripts.daily_update.is_trading_day", return_value=False):
                from datetime import date

                with patch(
                    "livewire_scripts.daily_update.previous_trading_day",
                    return_value=date(2026, 5, 29),
                ):
                    result = latest_complete_trading_day()
                    assert result == "2026-05-29"

    def test_trading_day_after_close(self):
        from datetime import datetime, time
        from zoneinfo import ZoneInfo

        with patch("livewire_scripts.sync_runner.datetime") as mock_dt:
            after_close = datetime(2026, 5, 28, 17, 0, tzinfo=ZoneInfo("America/New_York"))
            mock_dt.now.return_value = after_close
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            with patch("livewire_scripts.daily_update.is_trading_day", return_value=True):
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
            before_close = datetime(2026, 5, 28, 15, 0, tzinfo=ZoneInfo("America/New_York"))
            mock_dt.now.return_value = before_close
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            with patch("livewire_scripts.daily_update.is_trading_day", return_value=True):
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
        def runner_with_summary(command, **kwargs):
            stdout = kwargs.get("stdout")
            if stdout is not None:
                stdout.write(_summary_line(updated=1, errors=0))
            return CompletedProcess(args=command, returncode=1)

        rc = run_phase(
            "test",
            ["cmd"],
            tmp_path,
            allow_completed_summary=True,
            runner=runner_with_summary,
        )
        assert rc == 0

    def test_stale_summary_from_previous_run_not_suppressed(self, tmp_path):
        log_file = tmp_path / "test.log"
        log_file.write_text(_summary_line(updated=4, errors=0) + "Daily Update Complete\n")

        rc = run_phase(
            "test",
            ["cmd"],
            tmp_path,
            allow_completed_summary=True,
            runner=_fail_runner,
        )
        assert rc == 1

    def test_summary_with_errors_not_suppressed(self, tmp_path):
        def runner_with_errors(command, **kwargs):
            stdout = kwargs.get("stdout")
            if stdout is not None:
                stdout.write(_summary_line(updated=1, errors=2))
            return CompletedProcess(args=command, returncode=1)

        rc = run_phase(
            "test",
            ["cmd"],
            tmp_path,
            allow_completed_summary=True,
            runner=runner_with_errors,
        )
        assert rc == 1

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
        """FileNotFoundError branch when the log file vanishes before summary parse."""
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


class TestDeriveVol1h:
    def test_no_30m_data_returns_zero(self, tmp_path):
        preset = tmp_path / "vol.json"
        preset.write_text(json.dumps({"tickers": ["VIX"]}))
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


class TestRunSync:
    def test_missing_flatfile_credentials_fail_before_phases(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY")
        commands = []
        assert run_sync(_make_config(tmp_path), runner=lambda command, **kwargs: commands.append(command)) == 2
        assert commands == []

    def test_all_phases_succeed(self, tmp_path):
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            rc = run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        assert rc == 0

        joined = [" ".join(c) for c in commands]
        assert any("daily" in c and "--source massive" in c for c in joined)
        assert any("fred-rates" in c for c in joined)
        assert any("cboe-vol" in c for c in joined)
        assert any("flatfile-ingest catch-up" in c for c in joined)
        assert any("flatfile-ingest-daily catch-up" in c for c in joined)
        assert any("--timeframe 30m" in c and "volatility" in c for c in joined)
        # day_aggs lane runs before the intraday flatfile lane
        labels = [c for c in joined]
        day_aggs_idx = next(i for i, c in enumerate(labels) if "flatfile-ingest-daily" in c)
        intraday_idx = next(i for i, c in enumerate(labels) if "flatfile-ingest catch-up" in c)
        assert day_aggs_idx < intraday_idx

    def test_emits_summary_json_with_phases(self, tmp_path, capsys):
        from livewire_scripts.daily_outcomes import parse_last_summary_json

        config = _make_config(tmp_path)

        def capture(command, **kwargs):
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        summary = parse_last_summary_json(capsys.readouterr().out)
        assert summary["job"] == "daily_backfill"
        assert summary["failed"] == []
        labels = [p["label"] for p in summary["phases"]]
        assert "daily_backfill_equity_day_aggs" in labels
        assert all("duration_s" in p and "exit" in p for p in summary["phases"])

    def test_summary_json_lists_failed_phases(self, tmp_path, capsys):
        from livewire_scripts.daily_outcomes import parse_last_summary_json

        config = _make_config(tmp_path)

        def selective(command, **kwargs):
            rc = 1 if "fred-rates" in command else 0
            return CompletedProcess(args=command, returncode=rc)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            run_sync(config, runner=selective, trading_day_fn=lambda: "2026-05-28")
        summary = parse_last_summary_json(capsys.readouterr().out)
        assert summary["failed"] == ["daily_backfill_fred_rates"]

    def test_uses_target_date_from_config(self, tmp_path):
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            run_sync(config, runner=capture, trading_day_fn=lambda: "should-not-use")
        assert any("2026-05-28" in c for c in commands[0])

    def test_auto_detects_trading_day(self, tmp_path):
        config = _make_config(tmp_path)
        config = SyncConfig(**{**vars(config), "target_date": None})
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-27")
        assert any("2026-05-27" in c for c in commands[0])

    def test_phase_failure_returns_nonzero(self, tmp_path):
        config = _make_config(tmp_path)
        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            rc = run_sync(config, runner=_fail_runner, trading_day_fn=lambda: "2026-05-28")
        assert rc == 1

    def test_derive_failure_reaches_the_summary_not_just_the_exit_code(self, tmp_path, capsys):
        """SUMMARY_JSON["failed"] is built from phase_results, not from `failures`.

        The derivation appended to `failures` alone, so a broken 1h derive
        exited 1 while the machine-readable summary reported "failed": [] —
        and the digest and watchdog both read the summary, not the exit code.
        """
        config = _make_config(tmp_path)

        def ok(command, **kwargs):
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", side_effect=OSError("unreadable 30m parquet")):
            rc = run_sync(config, runner=ok, trading_day_fn=lambda: "2026-05-28")

        assert rc == 1
        summary = json.loads(
            next(
                line for line in capsys.readouterr().out.splitlines() if line.startswith("SUMMARY_JSON ")
            ).removeprefix("SUMMARY_JSON ")
        )
        assert "vol_1h_derive" in summary["failed"]

    def test_duckdb_coverage_refresh_runs(self, tmp_path):
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        joined = [" ".join(c) for c in commands]
        assert any("duckdb build" in c for c in joined)

    def test_duckdb_coverage_failure_returns_nonzero(self, tmp_path):
        """Freshness reporting going stale must fail the run, not pass quietly."""
        config = _make_config(tmp_path)

        def selective_runner(command, **kwargs):
            if "duckdb" in command:
                return CompletedProcess(args=command, returncode=1)
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            rc = run_sync(config, runner=selective_runner, trading_day_fn=lambda: "2026-05-28")
        assert rc == 1

    def test_expected_phase_count(self, tmp_path):
        config = _make_config(tmp_path)
        commands: list[list[str]] = []

        def capture(command, **kwargs):
            commands.append(command)
            return CompletedProcess(args=command, returncode=0)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            run_sync(config, runner=capture, trading_day_fn=lambda: "2026-05-28")
        # 1 equity daily + 1 FRED + 1 CBOE + 1 day_aggs + 1 full-market equity
        # intraday + 2 vol intraday (30m, 5m) + 1 DuckDB coverage refresh
        assert len(commands) == 8


class TestMain:
    def test_default_args(self, tmp_path, monkeypatch):
        from clients import ledger

        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.setenv("LW_RUN_ID", "intraday-catchup-main-test")
        monkeypatch.delenv("MDW_DAILY_BACKFILL_TARGET_DATE", raising=False)

        with patch("livewire_scripts.sync_runner.run_sync", return_value=0) as mock:
            rc = main(["--target-date", "2026-05-28"])
        assert rc == 0
        config = mock.call_args[0][0]
        assert config.target_date == "2026-05-28"
        assert ledger.query("select job, verdict from runs order by ended nulls first") == [
            {"job": "intraday-catchup", "verdict": None},
            {"job": "intraday-catchup", "verdict": "OK"},
        ]

    def test_all_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))

        with patch("livewire_scripts.sync_runner.run_sync", return_value=0) as mock:
            main(
                [
                    "--target-date",
                    "2026-05-20",
                    "--intraday-days",
                    "14",
                ]
            )
        config = mock.call_args[0][0]
        assert config.target_date == "2026-05-20"
        assert config.intraday_days == 14

    def test_no_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        monkeypatch.delenv("MDW_DAILY_BACKFILL_TARGET_DATE", raising=False)

        with patch("livewire_scripts.sync_runner.run_sync", return_value=0) as mock:
            main([])
        config = mock.call_args[0][0]
        assert config.target_date is None


class TestPhaseTimeout:
    """There was no timeout on this path at all.

    The wrapper's docstring claimed daily-backfill owns "activity-based stall
    detection"; grep found none. A wedged IB call blocked the phase forever and
    launchd will not start a second instance while the first lives, so the
    nightly job silently stopped running.
    """

    def test_phase_that_exceeds_its_budget_is_killed(self, tmp_path):
        def fake_runner(cmd, **kwargs):
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 1))

        rc = sync_runner.run_phase("stuck", ["sleep", "99999"], tmp_path, runner=fake_runner, timeout=1)

        assert rc == sync_runner.TIMEOUT_EXIT_CODE

    def test_timeout_is_passed_to_the_runner(self, tmp_path):
        seen = {}

        def fake_runner(cmd, **kwargs):
            seen.update(kwargs)
            return CompletedProcess(args=cmd, returncode=0)

        sync_runner.run_phase("ok", ["true"], tmp_path, runner=fake_runner, timeout=42)

        assert seen["timeout"] == 42

    def test_phase_writes_entry_and_terminal_ledger_rows(self, tmp_path, monkeypatch):
        from clients import ledger

        monkeypatch.setenv("LW_RUN_ID", "intraday-catchup-test")
        assert (
            sync_runner.run_phase(
                "equity_1m",
                ["ok"],
                tmp_path,
                runner=lambda *a, **k: CompletedProcess(a[0], 0),
                timeout=10,
            )
            == 0
        )
        assert ledger.query("select outcome from lane_results order by ended nulls first") == [
            {"outcome": None},
            {"outcome": "done"},
        ]

    def test_budget_is_env_tunable(self, monkeypatch):
        monkeypatch.setenv("MDW_SYNC_PHASE_TIMEOUT_SECONDS", "900")
        assert sync_runner.phase_timeout_seconds() == 900


class TestAGatewayOutageDegradesRatherThanFails:
    """A Gateway outage must degrade the run, not fail it.

    Task 1 lets the seven non-IB phases run when IB is down. This is the other
    half: the two IB phases exit 86, and without this the orchestrator still
    returns 1 and reports them in SUMMARY_JSON["failed"] — so the wrapper pages
    and the digest shows a red run for a dependency outage the design calls
    degraded.
    """

    @staticmethod
    def _summary(capsys) -> dict:
        lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SUMMARY_PREFIX)]
        return json.loads(lines[-1].removeprefix(SUMMARY_PREFIX))

    def test_ib_phases_exiting_86_do_not_fail_the_run(self, tmp_path, capsys):
        config = _make_config(tmp_path)

        def runner(command, **kwargs):
            rc = GATEWAY_DOWN_EXIT_CODE if "intraday-backfill" in command else 0
            return CompletedProcess(args=command, returncode=rc)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            rc = run_sync(config, runner=runner, trading_day_fn=lambda: "2026-08-07")

        assert rc == 0, "a Gateway outage is degraded, not failed"
        summary = self._summary(capsys)
        assert summary["failed"] == []
        assert sorted(summary["degraded"]) == [
            "daily_backfill_intraday_30m_volatility",
            "daily_backfill_intraday_5m_volatility",
        ]

    def test_a_real_phase_failure_still_fails_the_run(self, tmp_path, capsys):
        config = _make_config(tmp_path)

        def runner(command, **kwargs):
            rc = 1 if "flatfile-ingest-daily" in command else 0
            return CompletedProcess(args=command, returncode=rc)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            rc = run_sync(config, runner=runner, trading_day_fn=lambda: "2026-08-07")

        assert rc == 1
        summary = self._summary(capsys)
        assert "daily_backfill_equity_day_aggs" in summary["failed"]
        assert summary["degraded"] == []

    def test_a_non_ib_phase_at_86_is_still_a_failure(self, tmp_path, capsys):
        """86 is livewire's own preflight code, not a universal 'IB is down'.

        A Massive/FRED/CBOE/DuckDB phase returning it for an unrelated reason
        must not be swallowed — degrade eligibility is membership of the IB
        phase set, never the exit code alone.
        """
        config = _make_config(tmp_path)

        def runner(command, **kwargs):
            rc = GATEWAY_DOWN_EXIT_CODE if "duckdb" in command else 0
            return CompletedProcess(args=command, returncode=rc)

        with patch("livewire_scripts.sync_runner._derive_vol_1h", return_value=0):
            rc = run_sync(config, runner=runner, trading_day_fn=lambda: "2026-08-07")

        assert rc == 1
        summary = self._summary(capsys)
        assert summary["failed"] == ["daily_backfill_duckdb_coverage"]
        assert summary["degraded"] == []


class TestTheIntradayPhasesWaitForTheLake:
    """The 6h flat-file phase is what crowded the daily lanes out for four nights."""

    @pytest.fixture(autouse=True)
    def warehouse(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
        monkeypatch.setenv("LW_RUN_ID", "intraday-catchup-20260906T100000Z-1")

    def test_a_phase_records_the_time_it_waited(self, tmp_path):
        from clients import ledger

        rc = run_phase("daily_backfill_fred_rates", ["echo", "hi"], tmp_path, runner=_ok_runner)

        assert rc == 0
        assert ledger.query("select name, scope, source from measurements where name = 'lake_lock_wait_s'") == [
            {
                "name": "lake_lock_wait_s",
                "scope": "daily_backfill_fred_rates",
                "source": "measured",
            }
        ]

    def test_a_phase_that_never_gets_the_lock_is_blocked_and_never_runs(self, tmp_path, monkeypatch):
        from clients import ledger
        from clients.parquet_io import path_lock
        from livewire_scripts.paths import lake_lock_path

        started = []

        def _recording_runner(command, **kwargs):
            started.append(command)
            return CompletedProcess(args=command, returncode=0)

        with path_lock(lake_lock_path()):
            rc = run_phase(
                "daily_backfill_intraday_equity_flatfiles",
                ["cmd"],
                tmp_path,
                runner=_recording_runner,
                timeout=0,
            )

        assert rc == 0  # a deferred phase must not page; it is the low-priority job
        assert started == []
        assert ledger.query("select lane, outcome, blocker from lane_results where outcome is not null") == [
            {
                "lane": "daily_backfill_intraday_equity_flatfiles",
                "outcome": "blocked",
                "blocker": "lake_lock",
            }
        ]

    def test_the_intraday_job_polls_at_the_intraday_interval(self):
        """Low priority is a real mechanism: it looks once a minute, not once a second."""
        from clients import constants

        assert constants.declared("lake_lock_poll_s/intraday") == sync_runner.LAKE_LOCK_POLL_S
        assert constants.declared("lake_lock_poll_s/daily") < sync_runner.LAKE_LOCK_POLL_S
