"""Tests for scripts/run_daily_update_job.py."""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from subprocess import CompletedProcess
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE
from livewire_scripts import run_daily_update_job as daily_runner
from livewire_scripts.run_daily_update_job import (
    ASSET_CLASSES,
    FX_CATCHUP_DAYS,
    AlertRequest,
    RunnerConfig,
    _utc_now,
    append_log,
    build_alert_command,
    build_cboe_volatility_command,
    build_config,
    build_corporate_action_command,
    build_daily_update_command,
    build_fx_command,
    build_log_file,
    build_silver_rebuild_command,
    extract_error_summary,
    main,
    node_binary_exists,
    run_cboe_volatility_sync,
    run_daily_update_attempt,
    run_fx_sync,
    run_post_success_quality,
    run_with_retries,
    send_failure_alert,
)


class TestPerLaneBudgets:
    @pytest.fixture(autouse=True)
    def ledger_root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
        monkeypatch.setenv("LW_RUN_ID", "daily-update-20260902T060000Z-1")

    def _lane(self, config, lane, command):
        return daily_runner._run_scheduled_lane(
            config,
            command,
            lane,
            lane,
            env=None,
            runner=daily_runner._run_in_own_process_group,
            now_fn=daily_runner._utc_now,
        )

    def test_a_lane_over_budget_is_killed_and_the_next_lane_still_runs(self, tmp_path, monkeypatch):
        from clients import ledger

        config = _config(tmp_path)
        monkeypatch.setitem(daily_runner.LANE_BUDGET_S, "cmdty", 1.0)
        self._lane(config, "futures", ["true"])
        self._lane(config, "cmdty", [sys.executable, "-c", "import time; time.sleep(30)"])
        self._lane(config, "cboe", ["true"])
        assert ledger.query(
            "select lane, outcome, exit_code from lane_results where outcome is not null order by lane"
        ) == [
            {"lane": "cboe", "outcome": "done", "exit_code": 0},
            {"lane": "cmdty", "outcome": "timeout", "exit_code": 124},
            {"lane": "futures", "outcome": "done", "exit_code": 0},
        ]

    def test_a_lane_that_exits_86_blocks_and_the_next_lane_starts(self, tmp_path):
        from clients import ledger

        started = time.monotonic()
        config = _config(tmp_path)
        self._lane(config, "futures", [sys.executable, "-c", "raise SystemExit(86)"])
        self._lane(config, "cmdty", ["true"])
        assert time.monotonic() - started < 5.0
        assert ledger.query(
            "select lane, outcome, blocker from lane_results where outcome is not null order by lane"
        ) == [
            {"lane": "cmdty", "outcome": "done", "blocker": None},
            {"lane": "futures", "outcome": "blocked", "blocker": "ib_unreachable"},
        ]

    def test_each_lane_writes_an_entry_row_before_it_runs(self, tmp_path):
        from clients import ledger

        self._lane(_config(tmp_path), "cboe", ["true"])
        assert [
            row["outcome"] for row in ledger.query("select outcome from lane_results order by ended nulls first")
        ] == [None, "done"]

    def test_run_with_retries_records_a_lane_row_and_uses_the_lane_budget(self, tmp_path, monkeypatch):
        from clients import ledger

        monkeypatch.setitem(daily_runner.LANE_BUDGET_S, "futures", 1234.0)
        seen = {}

        def runner(command, stdout=None, env=None, timeout=None, **_):
            seen["timeout"] = timeout
            return SimpleNamespace(returncode=0, stdout="")

        assert (
            daily_runner.run_with_retries(
                _config(tmp_path),
                ["--asset-class", "futures"],
                runner=runner,
                sleep_fn=lambda _: None,
                completion_scope="futures",
            )
            == 0
        )
        assert seen["timeout"] == 1234.0
        assert ledger.query("select lane, outcome, budget_s from lane_results order by ended nulls first") == [
            {"lane": "futures", "outcome": None, "budget_s": 1234.0},
            {"lane": "futures", "outcome": "done", "budget_s": 1234.0},
        ]

    def test_run_with_retries_records_a_blocked_lane_on_86(self, tmp_path):
        from clients import ledger

        def runner(command, stdout=None, env=None, timeout=None, **_):
            return SimpleNamespace(returncode=GATEWAY_DOWN_EXIT_CODE, stdout="")

        daily_runner.run_with_retries(
            _config(tmp_path),
            ["--asset-class", "cmdty"],
            runner=runner,
            sleep_fn=lambda _: None,
            completion_scope="cmdty",
        )
        assert ledger.query("select outcome, blocker from lane_results where outcome is not null") == [
            {"outcome": "blocked", "blocker": "ib_unreachable"}
        ]

    def test_main_runs_the_no_fallback_lanes_before_the_expensive_ones(self, tmp_path, monkeypatch):
        order = []
        monkeypatch.setattr(daily_runner, "build_config", lambda: _config(tmp_path))
        monkeypatch.setattr(
            daily_runner,
            "_run_scheduled_lane",
            lambda config, command, label, scope, **kwargs: order.append(scope) or 0,
        )
        monkeypatch.setattr(
            daily_runner,
            "run_with_retries",
            lambda config, args, **kwargs: order.append(kwargs["completion_scope"]) or 0,
        )
        monkeypatch.setattr(daily_runner, "silver_is_blocked", lambda: None)
        daily_runner.main([])
        assert order.index("futures") < order.index("corporate-actions")
        assert order.index("cmdty") < order.index("corporate-actions")
        assert order.index("corporate-actions") < order.index("equity") < order.index("silver")

    def test_main_opens_and_closes_one_runs_row(self, tmp_path, monkeypatch):
        from clients import ledger

        monkeypatch.setattr(daily_runner, "build_config", lambda: _config(tmp_path))
        monkeypatch.setattr(daily_runner, "run_with_retries", lambda *args, **kwargs: 0)
        monkeypatch.setattr(daily_runner, "_run_scheduled_lane", lambda *args, **kwargs: 0)
        assert daily_runner.main([]) == 0
        runs = ledger.query("select job, host, verdict from runs order by ended nulls first")
        assert [row["job"] for row in runs] == ["daily-update", "daily-update"]
        assert runs[0]["verdict"] is None and runs[1]["verdict"] == "OK"
        assert runs[0]["host"] == socket.gethostname()

    def test_main_emits_every_declared_constant_as_a_measurement(self, tmp_path, monkeypatch):
        from clients import constants, ledger

        monkeypatch.setattr(daily_runner, "build_config", lambda: _config(tmp_path))
        monkeypatch.setattr(daily_runner, "run_with_retries", lambda *args, **kwargs: 0)
        monkeypatch.setattr(daily_runner, "_run_scheduled_lane", lambda *args, **kwargs: 0)
        assert daily_runner.main([]) == 0
        rows = ledger.query(
            "select name, scope, value, unit from measurements where source = 'declared' order by name, scope"
        )
        expected = {
            constants.split_scope(key): (float(value), unit) for key, (value, unit) in constants.DECLARED.items()
        }
        seen = {(row["name"], row["scope"]): (row["value"], row["unit"]) for row in rows}
        assert seen == expected

    def test_emit_lane_also_writes_a_measured_lane_budget_row(self):
        """The declared budget is only gradeable if the lane also measures itself."""
        from clients import ledger

        now = datetime.now(UTC)
        daily_runner._emit_lane("equity", started=now, ended=now, exit_code=0, elapsed_s=1234.0, outcome="done")
        lanes = ledger.query("select lane, elapsed_s, run_id from lane_results")
        measured = ledger.query(
            "select name, scope, value, unit, source, run_id from measurements where source = 'measured'"
        )
        assert lanes == [{"lane": "equity", "elapsed_s": 1234.0, "run_id": "daily-update-20260902T060000Z-1"}]
        assert measured == [
            {
                "name": "lane_budget_s",
                "scope": "equity",
                "value": 1234.0,
                "unit": "s",
                "source": "measured",
                "run_id": lanes[0]["run_id"],
            }
        ]

    def test_an_unexpected_crash_closes_the_run_as_failed(self, tmp_path, monkeypatch):
        from clients import ledger

        monkeypatch.setattr(daily_runner, "build_config", lambda: _config(tmp_path))
        monkeypatch.setattr(
            daily_runner, "run_with_retries", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        with pytest.raises(RuntimeError, match="boom"):
            daily_runner.main([])
        rows = ledger.query("select ended, exit_code, verdict from runs where verdict is not null")
        assert len(rows) == 1
        assert rows[0]["ended"] is not None
        assert rows[0]["exit_code"] == 1
        assert rows[0]["verdict"] == "FAILED"

    def test_a_systemexit_also_closes_the_run_as_failed(self, tmp_path, monkeypatch):
        # 2026-09-03: daily-update-20260903T060005Z-49009 left runs.ended null
        # with no traceback anywhere in the log — a plain Exception reaching
        # `except Exception:` always prints a traceback when it escapes
        # main(), so whatever killed that run wasn't one. SystemExit is the
        # one Python-catchable candidate that doesn't: it exits silently.
        from clients import ledger

        monkeypatch.setattr(daily_runner, "build_config", lambda: _config(tmp_path))
        monkeypatch.setattr(
            daily_runner, "run_with_retries", lambda *args, **kwargs: (_ for _ in ()).throw(SystemExit(1))
        )
        with pytest.raises(SystemExit):
            daily_runner.main([])
        rows = ledger.query("select ended, exit_code, verdict from runs where verdict is not null")
        assert len(rows) == 1
        assert rows[0]["ended"] is not None
        assert rows[0]["exit_code"] == 1
        assert rows[0]["verdict"] == "FAILED"

    def test_the_silver_gate_reads_the_equity_lane_row_not_an_in_process_dict(self):
        from clients import ledger

        now = datetime.now(UTC)
        ledger.emit(
            "lane_results",
            [
                {
                    "run_id": "daily-update-20260902T060000Z-1",
                    "lane": "corporate-actions",
                    "started": now,
                    "ended": now,
                    "exit_code": 0,
                    "budget_s": 10800.0,
                    "elapsed_s": 1.0,
                    "outcome": "done",
                    "blocker": None,
                },
                {
                    "run_id": "daily-update-20260902T060000Z-1",
                    "lane": "equity",
                    "started": now,
                    "ended": now,
                    "exit_code": 1,
                    "budget_s": 7200.0,
                    "elapsed_s": 1.0,
                    "outcome": "failed",
                    "blocker": None,
                },
            ],
            run_id="daily-update-20260902T060000Z-1",
        )
        assert daily_runner.silver_is_blocked() == "equity"

    def test_the_silver_gate_fails_closed_when_a_prerequisite_fact_is_missing(self):
        assert daily_runner.silver_is_blocked() == "corporate-actions"

    def test_a_lane_records_the_session_it_reached(self):
        from clients import ledger

        daily_runner._emit_last_session("futures", date(2026, 9, 1))
        assert ledger.query("select name, scope, value, unit, source from measurements") == [
            {
                "name": "last_session",
                "scope": "futures",
                "value": 20697.0,
                "unit": "epoch_days",
                "source": "measured",
            }
        ]

    def test_a_silent_lane_does_not_inherit_a_previous_lanes_session(self, tmp_path):
        log_file = tmp_path / "daily_update_2026-09-02.log"
        log_file.write_text('SUMMARY_JSON {"target_date":"2026-09-01"}\n', encoding="utf-8")
        assert daily_runner._last_session_from_log(log_file, log_file.stat().st_size) is None


@pytest.fixture(autouse=True)
def no_real_quality_spawn(tmp_path, monkeypatch):
    """Keep main() from shelling out to the real quality CLI.

    main() ends by spawning weekly/digest/housekeeping. Unpatched, a unit test
    launches `livewire_quality.py coverage` against the operator's live
    warehouse — and coverage runs auto-recovery subprocesses that write
    bronze. Autouse so a new main() test cannot forget it.
    """
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("LW_RUN_ID", "daily-update-test")
    with patch("livewire_scripts.run_daily_update_job.run_post_success_quality") as spawn:
        yield spawn


def _config(tmp_path: Path, *, node_bin: str = "/opt/homebrew/bin/node") -> RunnerConfig:
    repo_root = tmp_path / "repo"
    script_dir = repo_root / "scripts"
    return RunnerConfig(
        warehouse_dir=tmp_path / "warehouse",
        log_dir=tmp_path / "warehouse" / "logs",
        daily_update_script=script_dir / "livewire_ingest.py",
        alert_script=script_dir / "livewire_ops.py",
        python_bin="/usr/bin/python3",
        node_bin=node_bin,
        max_attempts=3,
        retry_delay_seconds=300,
    )


def _emit_test_lane(scope: str, code: int) -> None:
    from clients import ledger

    now = datetime.now(UTC)
    ledger.emit(
        "lane_results",
        [
            {
                "run_id": daily_runner.run_id(),
                "lane": scope,
                "started": now,
                "ended": now,
                "exit_code": code,
                "budget_s": 1.0,
                "elapsed_s": 0.0,
                "outcome": "done" if code == 0 else "failed",
                "blocker": None,
            }
        ],
        run_id=daily_runner.run_id(),
    )


class TestBuildConfig:
    def test_defaults(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.delenv("MDW_DAILY_UPDATE_LOG_DIR", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_ALERT_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_PYTHON_BIN", raising=False)
        monkeypatch.delenv("MDW_NODE_BIN", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_MAX_ATTEMPTS", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", raising=False)

        with patch(
            "livewire_scripts.run_daily_update_job.shutil.which",
            return_value="/usr/local/bin/node",
        ):
            config = build_config()

        assert config.warehouse_dir == tmp_path / "warehouse"
        assert config.log_dir == config.warehouse_dir / "logs"
        assert config.node_bin == "/usr/local/bin/node"
        assert config.max_attempts == 3
        assert config.retry_delay_seconds == 300
        assert config.python_bin == os.sys.executable

    def test_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_LOG_DIR", str(tmp_path / "custom-logs"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_SCRIPT", str(tmp_path / "daily.py"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_ALERT_SCRIPT", str(tmp_path / "alert.mjs"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_PYTHON_BIN", "/venv/bin/python")
        monkeypatch.setenv("MDW_NODE_BIN", "/custom/node")
        monkeypatch.setenv("MDW_DAILY_UPDATE_MAX_ATTEMPTS", "4")
        monkeypatch.setenv("MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", "9")

        config = build_config()

        assert config.warehouse_dir == tmp_path / "warehouse"
        assert config.log_dir == tmp_path / "custom-logs"
        assert config.daily_update_script == tmp_path / "daily.py"
        assert config.alert_script == tmp_path / "alert.mjs"
        assert config.python_bin == "/venv/bin/python"
        assert config.node_bin == "/custom/node"
        assert config.max_attempts == 4
        assert config.retry_delay_seconds == 9

    def test_invalid_positive_int_env(self, monkeypatch):
        monkeypatch.setenv("MDW_DAILY_UPDATE_MAX_ATTEMPTS", "0")

        with pytest.raises(ValueError, match="must be >= 1"):
            build_config()


class TestHelpers:
    def test_utc_now_returns_utc_datetime(self):
        assert _utc_now().tzinfo == UTC

    def test_build_log_file(self, tmp_path):
        current = datetime(2026, 3, 11, 13, 5, tzinfo=UTC)
        assert build_log_file(tmp_path, current) == tmp_path / "daily_update_2026-03-11.log"

    def test_build_log_file_defaults_to_utc(self, tmp_path):
        class FrozenDateTime:
            @classmethod
            def now(cls, tz=None):
                if tz is UTC:
                    return datetime(2026, 4, 6, 1, 0, tzinfo=UTC)
                return datetime(2026, 4, 5, 18, 0)

        with patch("livewire_scripts.run_daily_update_job.datetime", FrozenDateTime):
            assert build_log_file(tmp_path) == tmp_path / "daily_update_2026-04-06.log"

    def test_append_log_adds_newline(self, tmp_path):
        log_file = tmp_path / "logs" / "daily.log"
        append_log(log_file, "line one")
        append_log(log_file, "line two\n")

        assert log_file.read_text(encoding="utf-8") == "line one\nline two\n"

    def test_build_commands_with_and_without_optional_alert_fields(self, tmp_path):
        config = _config(tmp_path)

        assert build_daily_update_command(config, ["--force"]) == [
            "/usr/bin/python3",
            str(config.daily_update_script),
            "daily",
            "--force",
        ]

        full_request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=3,
            exit_code=9,
            error_summary="boom",
            repo_root=tmp_path / "repo",
        )
        full_command = build_alert_command(config, full_request)
        assert full_command[:3] == [
            "/usr/bin/python3",
            str(config.alert_script),
            "send-alert",
        ]
        assert "--attempts" in full_command
        assert "--exit-code" in full_command

        assert build_corporate_action_command(config, full_reconcile=False, dry_run=False) == [
            "/usr/bin/python3",
            str(daily_runner.INGEST_SCRIPT),
            "corporate-actions",
        ]
        assert build_corporate_action_command(config, full_reconcile=True, dry_run=True)[-2:] == [
            "--full-reconcile",
            "--dry-run",
        ]
        assert build_silver_rebuild_command(config, dry_run=True)[-2:] == ["--full", "--dry-run"]

        watchdog_request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=None,
            exit_code=None,
            error_summary="missing log",
            repo_root=tmp_path / "repo",
        )
        watchdog_command = build_alert_command(config, watchdog_request)
        assert "--attempts" not in watchdog_command
        assert "--exit-code" not in watchdog_command

    def test_extract_error_summary_handles_missing_and_empty_logs(self, tmp_path):
        missing_log = tmp_path / "missing.log"
        assert extract_error_summary(missing_log) == "Daily update failed, and the log file was not found."

        empty_log = tmp_path / "empty.log"
        empty_log.write_text(
            "=== Daily Update 2026-03-11T20:05:07Z ===\n=== Failed 2026-03-11T20:05:10Z ===\n",
            encoding="utf-8",
        )
        assert extract_error_summary(empty_log) == "Daily update failed with no error summary captured in the log."

    def test_extract_error_summary(self, tmp_path):
        log_file = tmp_path / "daily.log"
        log_file.write_text(
            "\n".join(
                [
                    "=== Daily Update 2026-03-11T20:05:07Z ===",
                    "Traceback: boom",
                    "=== Done equity 2026-03-11T20:05:08Z (attempt 1/3) ===",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        assert extract_error_summary(log_file) == "Traceback: boom"

    def test_extract_error_summary_prefers_summary_json(self, tmp_path):
        from livewire_scripts.daily_outcomes import build_summary_line

        log_file = tmp_path / "daily_update_2026-07-03.log"
        line = build_summary_line(
            job="daily_update",
            asset_class="equity",
            source="massive",
            target_date="2026-07-02",
            updated=9091,
            no_trade=277,
            partial=95,
            errors=12,
            bars_inserted=9186,
            validation_issues=0,
            top_errors=[("ConnectionError: Massive timeout", 12)],
        )
        log_file.write_text("  AAPL: 1 bar published from Massive\n" + line + "\n", encoding="utf-8")

        summary = extract_error_summary(log_file)
        assert "updated=9091" in summary
        assert "no_trade=277" in summary
        assert 'dominant error (12x): "ConnectionError: Massive timeout"' in summary
        assert "1 bar published" not in summary  # success lines never surface as errors

    def test_extract_error_summary_legacy_fallback_no_ticker_counting(self, tmp_path):
        log_file = tmp_path / "x.log"
        log_file.write_text("  AAPL: 1 bar published from Massive\nsome tail line\n", encoding="utf-8")
        assert extract_error_summary(log_file) == "some tail line"

    def test_node_binary_exists(self):
        with patch("livewire_scripts.run_daily_update_job.Path.exists", return_value=True):
            assert node_binary_exists("/opt/homebrew/bin/node") is True

        with patch("livewire_scripts.run_daily_update_job.Path.exists", return_value=False):
            assert node_binary_exists("/opt/homebrew/bin/node") is False

        with patch(
            "livewire_scripts.run_daily_update_job.shutil.which",
            return_value="/usr/local/bin/node",
        ):
            assert node_binary_exists("node") is True

        with patch("livewire_scripts.run_daily_update_job.shutil.which", return_value=None):
            assert node_binary_exists("node") is False


class TestSubprocessPaths:
    def test_run_daily_update_attempt(self, tmp_path):
        log_file = tmp_path / "daily.log"

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            assert command[-1] == "--dry-run"
            stdout.write("hello from sync\n")
            return SimpleNamespace(returncode=0)

        result = run_daily_update_attempt(
            ["/usr/bin/python3", "/repo/scripts/daily_update.py", "--dry-run"],
            log_file,
            env={"X": "1"},
            runner=_runner,
        )

        assert result.returncode == 0
        assert "hello from sync" in log_file.read_text(encoding="utf-8")


class TestEndOfDayQualityReport:
    """These jobs run once, after Silver — not inside each lane's success branch.

    They used to fire from run_with_retries, so four asset classes produced
    four coverage runs and four digest emails, all before the Silver rebuild.
    """

    _LOG_TS = datetime(2026, 5, 18, 20, 0, tzinfo=UTC)

    def _run(self, config, fake_runner):
        log_file = build_log_file(config.log_dir, self._LOG_TS)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        run_post_success_quality(config, log_file, runner=fake_runner)
        return log_file

    def test_quality_jobs_not_spawned_by_run_with_retries(self, tmp_path):
        config = _config(tmp_path)
        calls = []

        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")

        rc = run_with_retries(
            config,
            daily_update_args=[],
            runner=fake_runner,
            sleep_fn=lambda s: None,
            now_fn=lambda: self._LOG_TS,
        )
        assert rc == 0
        assert not [c for c in calls if any("livewire_quality.py" in str(x) for x in c)]

    def test_digest_invoked_with_email_and_run_date(self, tmp_path):
        config = _config(tmp_path)
        calls = []

        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")

        self._run(config, fake_runner)
        quality_calls = [c for c in calls if any("livewire_quality.py" in str(x) for x in c)]
        # The emailed nightly digest replaces the old report --view summary email.
        digest_cmd = next(c for c in quality_calls if "digest" in c)
        assert "--email" in digest_cmd
        assert "--run-date" in digest_cmd
        assert not any("report" in c and "summary" in c for c in quality_calls)

    def test_weekly_spawned(self, tmp_path):
        config = _config(tmp_path)
        calls = []

        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")

        self._run(config, fake_runner)
        quality_calls = [c for c in calls if any("livewire_quality.py" in str(x) for x in c)]
        assert any("weekly" in c for c in quality_calls)

    def test_weekly_spawn_failure_is_logged_not_raised(self, tmp_path):
        """A post-success job must never flip a successful ingest run to failure.

        The WARNING is not cosmetic: the terminal tail row records the failure
        without turning a successful ingest into a failed one.
        """
        config = _config(tmp_path)

        def fake_runner(cmd, **kwargs):
            is_weekly = any("livewire_quality.py" in str(x) for x in cmd) and "weekly" in cmd
            return CompletedProcess(args=cmd, returncode=3 if is_weekly else 0, stdout=b"", stderr=b"")

        log_file = self._run(config, fake_runner)
        assert "WARNING: weekly quality report failed" in log_file.read_text(encoding="utf-8")

    def test_digest_failure_is_logged_not_raised(self, tmp_path):
        from clients import ledger

        config = _config(tmp_path)

        def fake_runner(cmd, **kwargs):
            return CompletedProcess(args=cmd, returncode=2, stdout=b"", stderr=b"digest failed")

        log_file = self._run(config, fake_runner)
        assert "WARNING: nightly digest failed" in log_file.read_text(encoding="utf-8")
        assert ledger.query("select outcome, exit_code from lane_results where lane = 'digest'")[-1] == {
            "outcome": "failed",
            "exit_code": 2,
        }
        assert ledger.query("select exit_code from executions where script = 'send_alert'") == [{"exit_code": 2}]

    def test_send_failure_alert_skips_when_node_missing(self, tmp_path):
        config = _config(tmp_path, node_bin="/missing/node")
        request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=3,
            exit_code=5,
            error_summary="sync failed",
            repo_root=tmp_path / "repo",
        )
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('x')\n", encoding="utf-8")

        with patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=False):
            result = send_failure_alert(config, request, request.log_file, env={})

        assert result is None
        assert "node binary not found" in request.log_file.read_text(encoding="utf-8")

    def test_send_failure_alert_skips_when_script_missing(self, tmp_path):
        config = _config(tmp_path)
        request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=3,
            exit_code=5,
            error_summary="sync failed",
            repo_root=tmp_path / "repo",
        )

        with patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=True):
            result = send_failure_alert(config, request, request.log_file, env={})

        assert result is None
        assert "alert script not found" in request.log_file.read_text(encoding="utf-8")

    def test_send_failure_alert_invokes_runner(self, tmp_path):
        config = _config(tmp_path)
        request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=None,
            exit_code=None,
            error_summary="sync failed",
            repo_root=tmp_path / "repo",
        )
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("print('x')\n", encoding="utf-8")

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            assert command[0] == "/usr/bin/python3"
            assert command[2] == "send-alert"
            assert any(a.startswith("--error-summary=") for a in command)
            assert "--attempts" not in command
            return SimpleNamespace(returncode=0, stdout="sent")

        with patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=True):
            result = send_failure_alert(
                config,
                request,
                request.log_file,
                env={"A": "1"},
                runner=_runner,
            )

        assert result.returncode == 0
        assert "Triggering failure alert via:" in request.log_file.read_text(encoding="utf-8")


class TestRunWithRetries:
    def test_success_first_attempt(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            stdout.write("sync ok\n")
            return SimpleNamespace(returncode=0, stdout="")

        with patch("livewire_scripts.run_daily_update_job.socket.gethostname", return_value="warehouse.local"):
            rc = run_with_retries(
                config,
                ["--dry-run"],
                env={"A": "1"},
                runner=_runner,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 0
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(encoding="utf-8")
        assert "Runner config: attempts=3 retry_delay_seconds=300 budget_s=1800 hostname=warehouse.local" in log_text
        assert "=== Done daily 2026-03-11T20:05:09Z (attempt 1/3) ===" in log_text

    def test_retry_then_success(self, tmp_path):
        config = RunnerConfig(**(_config(tmp_path).__dict__ | {"max_attempts": 2, "retry_delay_seconds": 7}))
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 11, tzinfo=UTC),
            ]
        )
        results = iter(
            [
                SimpleNamespace(returncode=9, stdout=""),
                SimpleNamespace(returncode=0, stdout=""),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            stdout.write("attempt output\n")
            return next(results)

        sleep_calls: list[int] = []

        with patch("livewire_scripts.run_daily_update_job.socket.gethostname", return_value="warehouse.local"):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                sleep_fn=sleep_calls.append,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 0
        assert sleep_calls == [7]
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(encoding="utf-8")
        assert "Retrying in 7 seconds..." in log_text
        assert "attempt 2/2" in log_text

    def test_terminal_failure_sends_alert(self, tmp_path):
        config = RunnerConfig(**(_config(tmp_path).__dict__ | {"max_attempts": 2, "retry_delay_seconds": 5}))
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 11, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 12, tzinfo=UTC),
            ]
        )
        results = iter(
            [
                SimpleNamespace(returncode=4, stdout=""),
                SimpleNamespace(returncode=4, stdout=""),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            if hasattr(stdout, "write"):
                stdout.write("sync failed\n")
            return next(results)

        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('send');\n", encoding="utf-8")
        sleep_calls: list[int] = []

        # The alert has its own runner — see TestTheLaneRunnerNeverRunsTheAlert.
        with (
            patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=True),
            patch.object(
                daily_runner.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="alert sent")
            ),
        ):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                sleep_fn=sleep_calls.append,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 4
        assert sleep_calls == [5]
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(encoding="utf-8")
        assert "Failure alert sent successfully. alert sent" in log_text
        assert "=== Failed 2026-03-11T20:05:12Z after 2 attempt(s) ===" in log_text

    def test_terminal_failure_without_alert_result(self, tmp_path):
        config = RunnerConfig(**(_config(tmp_path, node_bin="/missing/node").__dict__ | {"max_attempts": 1}))
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            stdout.write("sync failed\n")
            return SimpleNamespace(returncode=6, stdout="")

        with patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=False):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 6
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(encoding="utf-8")
        assert "skipping failure email" in log_text

    def test_terminal_failure_alert_non_zero(self, tmp_path):
        config = RunnerConfig(**(_config(tmp_path).__dict__ | {"max_attempts": 1}))
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=UTC),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            if hasattr(stdout, "write"):
                stdout.write("sync failed\n")
            return SimpleNamespace(returncode=3, stdout="")

        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('send');\n", encoding="utf-8")

        with (
            patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=True),
            patch.object(
                daily_runner.subprocess, "run", return_value=SimpleNamespace(returncode=2, stdout="smtp down")
            ),
        ):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 3
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(encoding="utf-8")
        assert "WARNING: failure alert returned non-zero exit code 2. smtp down" in log_text
        from clients import ledger

        assert ledger.query("select script, exit_code from executions where script = 'send_alert'") == [
            {"script": "send_alert", "exit_code": 2}
        ]


class TestCboeVolatilitySync:
    def test_build_cboe_volatility_command(self, tmp_path):
        config = _config(tmp_path)
        command = build_cboe_volatility_command(config)
        assert command[0] == "/usr/bin/python3"
        assert "livewire_ingest.py" in command[1]
        assert command[2] == "cboe-vol"
        assert len(command) == 3  # No extra args, uses preset by default

    def test_run_cboe_volatility_sync_success(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 18, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 18, 20, 5, 8, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            stdout.write("CBOE fetch ok\n")
            return SimpleNamespace(returncode=0)

        rc = run_cboe_volatility_sync(
            config,
            env={},
            runner=_runner,
            now_fn=lambda: next(timestamps),
        )

        assert rc == 0
        log_text = (config.log_dir / "daily_update_2026-03-18.log").read_text(encoding="utf-8")
        assert "CBOE Volatility Sync" in log_text
        assert "=== Done cboe 2026-03-18T20:05:08Z ===" in log_text

    def test_build_fx_command_bounds_the_nightly_window(self):
        config = _config(Path("/tmp/test"))
        command = build_fx_command(config)

        assert command[0] == "/usr/bin/python3"
        assert "livewire_ingest.py" in command[1]
        assert command[2] == "fx"
        # The nightly run catches up; the deep seed is a separate manual run without
        # --days. Both merge, so accumulated intraday history survives either way.
        assert command[3:] == ["--days", str(FX_CATCHUP_DAYS)]

    def test_run_fx_sync_logs_its_completion_scope(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 18, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 18, 20, 5, 8, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            stdout.write("fx ok\n")
            return SimpleNamespace(returncode=0)

        rc = run_fx_sync(config, env={}, runner=_runner, now_fn=lambda: next(timestamps))

        assert rc == 0
        log_text = (config.log_dir / "daily_update_2026-03-18.log").read_text(encoding="utf-8")
        assert "FX Sync" in log_text
        # The watchdog requires this scope; without it a silent fx failure goes unnoticed.
        assert "=== Done fx 2026-03-18T20:05:08Z ===" in log_text

    def test_run_fx_sync_failure_is_reported(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 18, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 18, 20, 5, 8, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            stdout.write("fx failed\n")
            return SimpleNamespace(returncode=1)

        rc = run_fx_sync(config, env={}, runner=_runner, now_fn=lambda: next(timestamps))

        assert rc == 1
        log_text = (config.log_dir / "daily_update_2026-03-18.log").read_text(encoding="utf-8")
        assert "FX Sync Failed" in log_text
        assert "=== Done fx" not in log_text

    def test_run_cboe_volatility_sync_failure(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 18, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 18, 20, 5, 8, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout=None, env=None, timeout=None, **_):
            stdout.write("CBOE fetch failed\n")
            return SimpleNamespace(returncode=1)

        rc = run_cboe_volatility_sync(
            config,
            env={},
            runner=_runner,
            now_fn=lambda: next(timestamps),
        )

        assert rc == 1
        log_text = (config.log_dir / "daily_update_2026-03-18.log").read_text(encoding="utf-8")
        assert "CBOE Volatility Sync Failed" in log_text


class TestSilverScheduledLanes:
    def test_sunday_action_sync_requests_full_reconciliation(self, tmp_path):
        config = _config(tmp_path)
        calls = []

        def runner(command, stdout=None, env=None, timeout=None, **_):
            calls.append(command)
            return SimpleNamespace(returncode=0)

        assert (
            daily_runner.run_corporate_action_sync(
                config,
                dry_run=False,
                env={},
                runner=runner,
                now_fn=lambda: datetime(2026, 7, 12, 6, 0, tzinfo=UTC),
            )
            == 0
        )
        assert "--full-reconcile" in calls[0]


class TestMain:
    def test_main_orders_actions_before_ingestion_and_silver_after_all_success(self):
        config = _config(Path("/tmp/test"))
        calls = []

        def actions(*args, **kwargs):
            calls.append("actions")
            return 0

        def daily(cfg, args, env, completion_scope=None, **kwargs):
            calls.append(completion_scope)
            return 0

        def cboe(*args, **kwargs):
            calls.append("cboe")
            return 0

        def fx(*args, **kwargs):
            calls.append("fx")
            return 0

        def silver(*args, **kwargs):
            calls.append("silver")
            return 0

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", side_effect=actions),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=daily),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", side_effect=cboe),
            patch("livewire_scripts.run_daily_update_job.run_fx_sync", side_effect=fx),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild", side_effect=silver),
            patch("livewire_scripts.run_daily_update_job.silver_is_blocked", return_value=None),
        ):
            assert main(["--dry-run"]) == 0

        assert calls == ["futures", "cmdty", "cboe", "fx", "actions", "equity", "silver"]

    def test_failed_action_sync_prevents_silver_rebuild(self):
        config = _config(Path("/tmp/test"))

        def actions(*args, **kwargs):
            _emit_test_lane("corporate-actions", 2)
            return 2

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", side_effect=actions),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_fx_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild") as silver,
        ):
            assert main([]) == 2
        silver.assert_not_called()

    def test_failed_ingestion_prevents_silver_rebuild(self):
        config = _config(Path("/tmp/test"))

        def daily(cfg, args, env, completion_scope=None, **kwargs):
            code = 3 if completion_scope == "equity" else 0
            _emit_test_lane(completion_scope, code)
            return code

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=daily),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_fx_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild") as silver,
        ):
            assert main([]) == 3
        silver.assert_not_called()

    def test_failed_silver_publication_fails_scheduled_job(self):
        config = _config(Path("/tmp/test"))
        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_fx_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild", return_value=4),
            patch("livewire_scripts.run_daily_update_job.silver_is_blocked", return_value=None),
        ):
            assert main([]) == 4

    def test_main_runs_all_asset_classes_and_cboe_by_default(self):
        config = _config(Path("/tmp/test"))
        ib_calls: list[list[str]] = []
        cboe_called = []
        fx_called = []

        def _run_ib(cfg, args, env, completion_scope=None, **kwargs):
            ib_calls.append(args)
            assert completion_scope == args[args.index("--asset-class") + 1]
            return 0

        def _run_cboe(cfg, env, **kwargs):
            cboe_called.append(True)
            return 0

        def _run_fx(cfg, env, **kwargs):
            fx_called.append(True)
            return 0

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=_run_ib),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", side_effect=_run_cboe),
            patch("livewire_scripts.run_daily_update_job.run_fx_sync", side_effect=_run_fx),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild", return_value=0),
        ):
            assert main(["--dry-run"]) == 0

        # IB syncs equity, futures and cmdty; volatility via CBOE, fx via Yahoo/Massive
        assert ib_calls == [
            ["--dry-run", "--asset-class", asset_class] for asset_class in ("futures", "cmdty", "equity")
        ]
        assert cboe_called == [True]
        assert fx_called == [True]
        assert "cmdty" in ASSET_CLASSES
        # fx left the IB loop: resolve_fx_pair() cannot express NDF pairs or DXY.
        assert "fx" not in ASSET_CLASSES

    def test_main_explicit_asset_class_skips_cboe(self):
        config = _config(Path("/tmp/test"))

        with patch("livewire_scripts.run_daily_update_job.build_config", return_value=config):
            with patch("livewire_scripts.run_daily_update_job.run_with_retries", return_value=0) as run_mock:
                with (
                    patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync") as cboe_mock,
                    patch("livewire_scripts.run_daily_update_job.run_fx_sync") as fx_mock,
                ):
                    assert main(["--dry-run", "--asset-class", "equity"]) == 0

        assert run_mock.call_count == 1
        call = run_mock.call_args
        assert call.args == (config, ["--dry-run", "--asset-class", "equity"])
        assert call.kwargs["env"] == os.environ.copy()
        assert call.kwargs["completion_scope"] == "equity"
        cboe_mock.assert_not_called()
        fx_mock.assert_not_called()

    def test_explicit_ib_lane_preserves_gateway_down_exit_and_records_degraded(self):
        from clients import ledger

        config = _config(Path("/tmp/test"))
        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", return_value=GATEWAY_DOWN_EXIT_CODE),
        ):
            assert main(["--asset-class", "futures"]) == GATEWAY_DOWN_EXIT_CODE
        assert ledger.query("select verdict from runs where verdict is not null") == [{"verdict": "DEGRADED"}]

    def _main_with(self, *, lane_codes=None, action=0, cboe=0, fx=0, gateway_down=()):
        """Run main() with each lane's exit code stubbed. Returns (rc, silver_mock)."""
        config = _config(Path("/tmp/test"))
        codes = dict(lane_codes or {})

        def _emit(scope, code):
            from clients import ledger

            now = datetime.now(UTC)
            ledger.emit(
                "lane_results",
                [
                    {
                        "run_id": daily_runner.run_id(),
                        "lane": scope,
                        "started": now,
                        "ended": now,
                        "exit_code": code,
                        "budget_s": 1.0,
                        "elapsed_s": 0.0,
                        "outcome": "done" if code == 0 else "failed",
                        "blocker": None,
                    }
                ],
                run_id=daily_runner.run_id(),
            )

        def _run(cfg, args, env, completion_scope=None, **kwargs):
            name = args[args.index("--asset-class") + 1]
            if name in gateway_down:
                code = GATEWAY_DOWN_EXIT_CODE
            else:
                code = codes.get(name, 0)
            _emit(name, code)
            return code

        def _action(*args, **kwargs):
            _emit("corporate-actions", action)
            return action

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", side_effect=_action),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=_run),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=cboe),
            patch("livewire_scripts.run_daily_update_job.run_fx_sync", return_value=fx),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild", return_value=0) as silver,
            patch("livewire_scripts.run_daily_update_job.append_log"),
        ):
            return main([]), silver

    def test_main_returns_nonzero_if_any_asset_class_fails(self):
        rc, _ = self._main_with(lane_codes={"futures": 1})
        assert rc == 1

    def test_ib_lane_failure_does_not_block_silver(self):
        """Silver reads equity bronze + corporate actions. Nothing else gates it.

        A stale FX contract or a failed futures lane used to skip the adjusted
        rebuild for the whole ~13K equity universe.
        """
        rc, silver = self._main_with(lane_codes={"futures": 1, "cmdty": 1})
        assert rc == 1
        silver.assert_called_once()

    def test_cboe_failure_does_not_block_silver(self):
        rc, silver = self._main_with(cboe=1)
        assert rc == 1
        silver.assert_called_once()

    def test_equity_failure_blocks_silver(self):
        rc, silver = self._main_with(lane_codes={"equity": 1})
        assert rc == 1
        silver.assert_not_called()

    def test_corporate_action_failure_blocks_silver(self):
        """Silver would otherwise rebuild against a stale action store."""
        rc, silver = self._main_with(action=1)
        assert rc == 1
        silver.assert_not_called()

    def test_gateway_down_is_degraded_not_failed(self):
        """A 2FA-gated Gateway is an expected state, not a data failure."""
        rc, silver = self._main_with(gateway_down=("futures", "cmdty"))
        assert rc == 0
        silver.assert_called_once()
        from clients import ledger

        assert ledger.query("select verdict from runs where verdict is not null") == [{"verdict": "DEGRADED"}]

    def test_quality_jobs_run_once_after_silver(self, no_real_quality_spawn):
        """Four asset classes used to mean four coverage runs and four digests.

        All of them fired before the Silver rebuild, so the digest's Silver
        section — the only channel carrying window_regressions — parsed a log
        that could not yet contain Silver's SUMMARY_JSON.
        """
        rc, silver = self._main_with()
        assert rc == 0
        silver.assert_called_once()
        assert no_real_quality_spawn.call_count == 1


class TestAttemptTimeout:
    def test_a_hung_attempt_is_killed_and_reported_as_timeout(self, tmp_path):
        log_file = tmp_path / "job.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        def hang(cmd, **kwargs):
            raise subprocess.TimeoutExpired(cmd="daily", timeout=kwargs["timeout"])

        result = run_daily_update_attempt(["x"], log_file, runner=hang, timeout=10)

        assert result.returncode == daily_runner.TIMEOUT_EXIT_CODE
        assert "process group killed" in log_file.read_text(encoding="utf-8")


class TestTimeoutPages:
    """send_failure_alert sits at the END of run_with_retries and is reachable
    only by falling out of the retry loop. An early `return` would make the
    timeout the one failure mode that never pages."""

    def test_a_timeout_pages_exactly_once_and_is_not_retried(self, tmp_path):
        config = _config(tmp_path)
        attempts = []

        def hang(cmd, **kwargs):
            attempts.append(1)
            raise subprocess.TimeoutExpired(cmd="daily", timeout=kwargs.get("timeout") or 1)

        sent = []
        with patch.object(
            daily_runner,
            "send_failure_alert",
            side_effect=lambda *a, **k: sent.append(1) or SimpleNamespace(returncode=0, stdout=""),
        ):
            rc = run_with_retries(config, ["--asset-class", "equity"], runner=hang, sleep_fn=lambda _: None)

        assert rc == daily_runner.TIMEOUT_EXIT_CODE
        assert len(attempts) == 1, "a wedge is not transient; retrying spends the deadline for nothing"
        assert sent == [1], "the timeout must page"


class TestScheduledLanePages:
    """corporate-actions / CBOE / FX / Silver run through _run_scheduled_lane,
    which had no alert path at all — which is why the 2026-07-28 corporate-action
    wedge produced no alert from this job."""

    def _lane(self, tmp_path, returncode, sent):
        config = _config(tmp_path)
        with patch.object(
            daily_runner,
            "send_failure_alert",
            side_effect=lambda *a, **k: sent.append(1) or SimpleNamespace(returncode=0, stdout=""),
        ):
            return daily_runner._run_scheduled_lane(
                config,
                ["x"],
                "Corporate Action Sync",
                "corporate-actions",
                env=None,
                runner=lambda cmd, **kw: SimpleNamespace(returncode=returncode, stdout=""),
                now_fn=_utc_now,
            )

    def test_a_failing_lane_pages(self, tmp_path):
        sent = []
        assert self._lane(tmp_path, 1, sent) == 1
        assert sent == [1]

    def test_a_successful_lane_does_not_page(self, tmp_path):
        sent = []
        assert self._lane(tmp_path, 0, sent) == 0
        assert sent == []

    def test_a_gateway_down_lane_does_not_page(self, tmp_path):
        """Degraded is not failed — an unreachable Gateway must stay silent."""
        sent = []
        assert self._lane(tmp_path, GATEWAY_DOWN_EXIT_CODE, sent) == GATEWAY_DOWN_EXIT_CODE
        assert sent == []


class TestTheLaneRunnerNeverRunsTheAlert:
    """The lane runner and the alert runner are not interchangeable.

    2026-08-02: `_run_in_own_process_group` was threaded into the alert path.
    It is keyword-only on `stdout/env/timeout`, so `send_failure_alert`'s
    `stderr=`/`text=`/`check=` raised `TypeError` out of `main()`. One failed
    symbol out of 14,577 in corporate-actions took down the whole nightly job —
    equity, futures, cmdty, CBOE, FX and Silver never ran, and no alert was
    sent. Only the watchdog noticed, four hours later.

    Every other test in this file passes a fake runner that swallows `**kwargs`,
    which is exactly why nothing caught it. These two use the real signature.
    """

    @staticmethod
    def _strict_lane_runner(calls, returncode):
        def runner(command, *, stdout, env, timeout):  # the REAL signature — no **kwargs
            calls.append(command)
            return CompletedProcess(list(command), returncode)

        return runner

    def test_a_failing_lane_pages_without_touching_the_lane_runner(self, tmp_path):
        config = _config(tmp_path)
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("x\n", encoding="utf-8")
        lane_calls = []

        with (
            patch.object(daily_runner, "node_binary_exists", return_value=True),
            patch.object(
                daily_runner.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="sent")
            ) as alert_run,
        ):
            rc = daily_runner._run_scheduled_lane(
                config,
                ["lane-cmd"],
                "Corporate Action Sync",
                "corporate-actions",
                env=None,
                runner=self._strict_lane_runner(lane_calls, 1),
                now_fn=_utc_now,
            )

        assert rc == 1
        assert len(lane_calls) == 1, "the lane runner runs the lane, never the alert"
        assert alert_run.call_count == 1, "the alert goes through subprocess.run"
        assert "Failure alert sent successfully" in daily_runner.build_log_file(config.log_dir, _utc_now()).read_text(
            encoding="utf-8"
        )

    def test_the_retry_path_pages_without_touching_the_lane_runner(self, tmp_path):
        config = _config(tmp_path)
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("x\n", encoding="utf-8")
        lane_calls = []

        with (
            patch.object(daily_runner, "node_binary_exists", return_value=True),
            patch.object(
                daily_runner.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="sent")
            ) as alert_run,
        ):
            rc = run_with_retries(
                config,
                ["--asset-class", "equity"],
                runner=self._strict_lane_runner(lane_calls, 1),
                sleep_fn=lambda _: None,
            )

        assert rc == 1
        assert len(lane_calls) == config.max_attempts
        assert alert_run.call_count == 1


class TestTheEquityLaneFallsBackToMassive:
    """Silver must not be hostage to IB.

    Silver reads equity bronze and the corporate-action store, both
    Massive-backed. But the equity lane runs on IB by default, so a down
    Gateway skipped it and `silver_inputs_ok` then blocked the rebuild for the
    whole ~13K universe — the exact cascade CLAUDE.md says must not happen,
    arriving by an indirect route.

    Futures and cmdty get NO fallback: Massive does not carry those asset
    classes, so a fallback there would be a fabricated success.

    Consequence worth stating rather than discovering: if IB is down AND
    Massive cannot answer either, equity's code is no longer 86, so the lane
    leaves `degraded` for `failed` and the job pages. That is right — no source
    produced the session's bars — but it fires only when both providers are gone.
    """

    @staticmethod
    @contextlib.contextmanager
    def _lanes(config, daily, silver_code):
        """Everything main() calls except the equity retry under test."""

        def recorded_daily(*args, **kwargs):
            code = daily(*args, **kwargs)
            scope = kwargs.get("completion_scope")
            _emit_test_lane(scope, code)
            return code

        def actions(*args, **kwargs):
            _emit_test_lane("corporate-actions", 0)
            return 0

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", side_effect=actions),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=recorded_daily),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_fx_sync", return_value=0),
            patch(
                "livewire_scripts.run_daily_update_job.run_silver_rebuild",
                return_value=silver_code,
            ) as silver,
        ):
            yield silver

    def test_a_down_gateway_retries_equity_on_massive(self, tmp_path):
        config = _config(tmp_path)
        calls: list[list[str]] = []

        def daily(cfg, daily_update_args, **kwargs):
            args = list(daily_update_args)
            calls.append(args)
            if "--source" in args and args[args.index("--source") + 1] == "massive":
                return 0
            return GATEWAY_DOWN_EXIT_CODE

        with self._lanes(config, daily, 0) as silver:
            main([])

        equity_calls = [c for c in calls if "equity" in c]
        assert len(equity_calls) == 2, "equity should be retried exactly once"
        assert equity_calls[1][equity_calls[1].index("--source") + 1] == "massive"
        silver.assert_called_once()

    def test_futures_and_cmdty_get_no_fallback(self, tmp_path):
        config = _config(tmp_path)
        calls: list[list[str]] = []

        def daily(cfg, daily_update_args, **kwargs):
            calls.append(list(daily_update_args))
            return GATEWAY_DOWN_EXIT_CODE

        with self._lanes(config, daily, 0) as silver:
            main([])

        for asset_class in ("futures", "cmdty"):
            lane_calls = [c for c in calls if asset_class in c]
            assert len(lane_calls) == 1, f"{asset_class} must not be retried — Massive has no such data"
            assert "--source" not in lane_calls[0]
        silver.assert_not_called()

    def test_an_explicit_source_ib_does_not_defeat_the_fallback(self, tmp_path):
        """argparse honours the LAST --source; _requires_ib_preflight reads the FIRST.

        So appending --source massive to args that already carry --source ib
        would leave the preflight gating on ib, exit 86 again, and the retry
        would look like it ran while nothing changed.
        """
        config = _config(tmp_path)
        calls: list[list[str]] = []

        def daily(cfg, daily_update_args, **kwargs):
            args = list(daily_update_args)
            calls.append(args)
            if args.count("--source") > 1:
                raise AssertionError(f"ambiguous --source in {args}")
            if "--source" in args and args[args.index("--source") + 1] == "massive":
                return 0
            return GATEWAY_DOWN_EXIT_CODE

        with self._lanes(config, daily, 0):
            main(["--source", "ib"])

        fallback = [c for c in calls if "massive" in c]
        assert len(fallback) == 1
        assert fallback[0].count("--source") == 1, "exactly one --source reaches the preflight"

    def test_a_fallback_that_also_exits_86_fails_rather_than_degrades(self, tmp_path):
        """Equity stops being an IB lane once Massive answers for it.

        Classifying the FALLBACK's 86 as a Gateway outage would leave `failed`
        empty, exit 0, and skip Silver — reporting success for a night where
        no provider produced the session's bars. Same rule as sync_runner:
        degrade eligibility is membership of the IB set, not the exit code.
        """
        config = _config(tmp_path)

        def daily(cfg, daily_update_args, **kwargs):
            return GATEWAY_DOWN_EXIT_CODE  # both IB and the Massive fallback

        with self._lanes(config, daily, 0) as silver:
            assert main([]) == GATEWAY_DOWN_EXIT_CODE, "no source produced bars — that is a failure"
        silver.assert_not_called()

    def test_futures_at_86_still_degrades(self, tmp_path):
        """The narrowing must not break the case it was built for."""
        config = _config(tmp_path)

        def daily(cfg, daily_update_args, **kwargs):
            args = list(daily_update_args)
            if "--source" in args and args[args.index("--source") + 1] == "massive":
                return 0
            return GATEWAY_DOWN_EXIT_CODE

        with self._lanes(config, daily, 0) as silver:
            assert main([]) == 0, "futures/cmdty down on IB is degraded, not failed"
        silver.assert_called_once(), "equity recovered via Massive, so Silver runs"

    def test_both_providers_down_fails_rather_than_degrades(self, tmp_path):
        """No source produced the bars. That is a failure, not a degrade."""
        config = _config(tmp_path)

        def daily(cfg, daily_update_args, **kwargs):
            args = list(daily_update_args)
            if "--source" in args and args[args.index("--source") + 1] == "massive":
                return 7
            return GATEWAY_DOWN_EXIT_CODE

        with self._lanes(config, daily, 0) as silver:
            assert main([]) == 7
        silver.assert_not_called()


class TestTheAlertCommandCarriesTheSummaryAsOneToken:
    """The Python side must emit the single-token form.

    Fixing the parser alone leaves the callers still passing two tokens, which
    still breaks the moment the summary begins with "--".
    """

    def test_error_summary_is_a_single_equals_token(self, tmp_path):
        summary = "--- Runbook: /Users/moremeds/runbooks/trading-stack/ib-gateway-ibc.md ---"
        request = AlertRequest(
            run_date="2026-08-08",
            log_file=tmp_path / "daily_update_2026-08-08.log",
            attempts=1,
            exit_code=86,
            error_summary=summary,
            repo_root=tmp_path / "repo",
        )

        command = build_alert_command(_config(tmp_path), request)

        assert f"--error-summary={summary}" in command
        assert "--error-summary" not in command, "the bare two-token form must be gone"


class TestTheDailyJobNoLongerRunsCoverage:
    """Coverage does not belong on the nightly job's critical path.

    It was given a 600s budget, then 1800s; both were guesses against a warm
    cache and both expired. An arbitrary timeout around a job whose runtime is
    dominated by cold external-volume I/O is the bug, not the number.

    This calls `run_post_success_quality` directly rather than `main([])`: the
    autouse `no_real_quality_spawn` fixture patches it wholesale, so a `main([])`
    test can never observe what it spawns.
    """

    @staticmethod
    def _spawned(tmp_path) -> list[list[str]]:
        commands: list[list[str]] = []

        def fake_runner(command, **kwargs):
            commands.append(list(command))
            return CompletedProcess(command, 0, stdout="", stderr="")

        run_post_success_quality(
            _config(tmp_path),
            tmp_path / "daily_update_2026-08-08.log",
            runner=fake_runner,
        )
        return commands

    def test_no_coverage_subcommand_is_spawned(self, tmp_path):
        subcommands = [c[2:] for c in self._spawned(tmp_path)]

        assert not any(sub[:1] == ["coverage"] for sub in subcommands), "coverage has its own launchd job now"
        assert ["weekly"] in subcommands, "weekly still runs here"
        assert any(sub[:1] == ["digest"] for sub in subcommands), "the digest still runs here"

    def test_no_interior_gap_scan_is_spawned_even_on_a_sunday(self, tmp_path):
        """2026-08-16 is a Sunday — the only day the scan used to fire.

        It had a 3600s budget against ~3115s of measured work (302s cold glob
        + 191.5 ms x 14,687 symbols), ran exactly once, and was killed at the
        budget. It has its own launchd job now, with no budget at all, so this
        tail must not spawn it.
        """
        commands: list[list[str]] = []

        def fake_runner(command, **kwargs):
            commands.append(list(command))
            return CompletedProcess(command, 0, stdout="", stderr="")

        run_post_success_quality(
            _config(tmp_path),
            tmp_path / "daily_update_2026-08-16.log",
            runner=fake_runner,
        )

        assert not any("health" in c for c in commands), "the interior gap scan has its own job now"
        # And nothing else regressed on the day it used to fire.
        subcommands = [c[2:] for c in commands]
        assert ["weekly"] in subcommands
        assert any(sub[:1] == ["digest"] for sub in subcommands)


class TestHousekeepingRunsAfterTheDigest:
    def test_the_nightly_job_runs_a_housekeeping_sweep(self, tmp_path):
        commands: list[list[str]] = []

        def fake_runner(command, **kwargs):
            commands.append(list(command))
            return CompletedProcess(command, 0, stdout="", stderr="")

        run_post_success_quality(
            _config(tmp_path),
            tmp_path / "daily_update_2026-08-08.log",
            runner=fake_runner,
        )

        sweeps = [c for c in commands if "housekeeping" in c]
        assert len(sweeps) == 1
        assert sweeps[0][1].endswith("livewire_ops.py"), "housekeeping is an ops command"
        assert "--apply" in sweeps[0]
        # It runs last: the digest must already have been sent.
        assert commands.index(sweeps[0]) == len(commands) - 1

    def test_a_failed_sweep_only_warns(self, tmp_path):
        """A sweep that deleted nothing is never worth failing a good ingest run.

        The warning is load-bearing operator detail; the terminal tail row also
        records the failure without failing the ingest.
        """
        log_file = tmp_path / "daily_update_2026-08-08.log"

        def fake_runner(command, **kwargs):
            failed = "housekeeping" in command
            return CompletedProcess(command, 1 if failed else 0, stdout="", stderr="")

        run_post_success_quality(_config(tmp_path), log_file, runner=fake_runner)

        assert "WARNING: housekeeping failed" in log_file.read_text(encoding="utf-8")
