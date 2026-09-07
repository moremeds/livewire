"""Tests for livewire_scripts/run_intraday_catchup_job.py."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from livewire_scripts.run_intraday_catchup_job import (
    AlertRequest,
    IntradayCatchupConfig,
    _extract_error_summary,
    _node_binary_exists,
    _utc_now,
    build_alert_command,
    build_config,
    build_intraday_catchup_command,
    build_log_file,
    main,
    run_intraday_catchup,
)


def _config(tmp_path: Path, *, node_bin: str = "/opt/homebrew/bin/node") -> IntradayCatchupConfig:
    repo_root = tmp_path / "repo"
    script_dir = repo_root / "scripts"
    return IntradayCatchupConfig(
        warehouse_dir=tmp_path / "warehouse",
        log_dir=tmp_path / "warehouse" / "logs",
        ingest_script=script_dir / "livewire_ingest.py",
        alert_script=script_dir / "livewire_ops.py",
        python_bin="/usr/bin/python3",
        node_bin=node_bin,
    )


class TestUtcNow:
    def test_returns_utc_aware_datetime(self):
        result = _utc_now()
        assert result.tzinfo == UTC


class TestBuildConfig:
    def test_defaults(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        for key in (
            "MDW_INTRADAY_CATCHUP_LOG_DIR",
            "MDW_INTRADAY_CATCHUP_SCRIPT",
            "MDW_INTRADAY_CATCHUP_ALERT_SCRIPT",
            "MDW_INTRADAY_CATCHUP_PYTHON_BIN",
            "MDW_NODE_BIN",
        ):
            monkeypatch.delenv(key, raising=False)

        with patch(
            "livewire_scripts.run_intraday_catchup_job.shutil.which",
            return_value="/usr/local/bin/node",
        ):
            config = build_config()

        assert config.warehouse_dir == tmp_path / "warehouse"
        assert config.log_dir == config.warehouse_dir / "logs"
        assert config.node_bin == "/usr/local/bin/node"
        assert config.python_bin == sys.executable
        assert config.ingest_script.name == "livewire_ingest.py"
        assert config.alert_script.name == "livewire_ops.py"

    def test_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_LOG_DIR", str(tmp_path / "custom-logs"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_SCRIPT", str(tmp_path / "ingest.py"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_ALERT_SCRIPT", str(tmp_path / "ops.py"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_PYTHON_BIN", "/venv/bin/python")
        monkeypatch.setenv("MDW_NODE_BIN", "/usr/bin/node")

        config = build_config()

        assert config.log_dir == tmp_path / "custom-logs"
        assert config.ingest_script == tmp_path / "ingest.py"
        assert config.alert_script == tmp_path / "ops.py"
        assert config.python_bin == "/venv/bin/python"
        assert config.node_bin == "/usr/bin/node"


class TestBuildLogFile:
    def test_uses_utc_date(self, tmp_path):
        log_dir = tmp_path / "logs"
        # 16:00 PT == 00:00 UTC next day during PST (Nov-Mar) or 23:00 UTC same day during PDT
        when = datetime(2026, 6, 5, 23, 0, tzinfo=UTC)
        path = build_log_file(log_dir, when)
        assert path == log_dir / "intraday_catchup_2026-06-05.log"


class TestBuildIntradayCatchupCommand:
    def test_invokes_daily_backfill(self, tmp_path):
        config = _config(tmp_path)
        cmd = build_intraday_catchup_command(config)
        assert cmd == [
            "/usr/bin/python3",
            str(config.ingest_script),
            "daily-backfill",
        ]


class TestBuildAlertCommand:
    def test_includes_job_name_intraday_catchup(self, tmp_path):
        config = _config(tmp_path)
        request = AlertRequest(
            run_date="2026-06-05",
            log_file=tmp_path / "log.log",
            attempts=1,
            exit_code=42,
            error_summary="something broke",
            repo_root=tmp_path / "repo",
        )
        cmd = build_alert_command(config, request)
        assert "--job-name" in cmd
        assert cmd[cmd.index("--job-name") + 1] == "intraday_catchup"
        assert "--attempts" in cmd
        assert cmd[cmd.index("--attempts") + 1] == "1"
        assert "--exit-code" in cmd
        assert cmd[cmd.index("--exit-code") + 1] == "42"
        assert "--run-date" in cmd
        assert cmd[cmd.index("--run-date") + 1] == "2026-06-05"


class TestRunIntradayCatchup:
    def test_success_no_alert(self, tmp_path, monkeypatch):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True)
        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            handle = kwargs.get("stdout")
            if handle is not None and hasattr(handle, "write"):
                handle.write("ok\n")
            return CompletedProcess(args=cmd, returncode=0)

        rc = run_intraday_catchup(
            config,
            env={"FOO": "bar"},
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 0
        # Only one subprocess invocation (the daily-backfill command) and no alert.
        assert len(runner_calls) == 1
        assert runner_calls[0] == build_intraday_catchup_command(config)

        log_file = config.log_dir / "intraday_catchup_2026-06-05.log"
        contents = log_file.read_text(encoding="utf-8")
        assert "=== Intraday Catchup" in contents
        assert "=== Done" in contents

    def test_failure_triggers_alert(self, tmp_path, monkeypatch):
        node_bin = str(tmp_path / "bin" / "node")
        config = _config(tmp_path, node_bin=node_bin)
        config.log_dir.mkdir(parents=True)
        # Create alert script and node binary so the alert path runs.
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("# stub\n", encoding="utf-8")
        Path(config.node_bin).parent.mkdir(parents=True, exist_ok=True)
        Path(config.node_bin).write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")

        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            handle = kwargs.get("stdout")
            if handle is not None and hasattr(handle, "write"):
                handle.write("boom: ConnectionError: Socket disconnect\n")
            # Daily-backfill fails; alert subprocess succeeds.
            return CompletedProcess(
                args=cmd,
                returncode=2 if cmd[1] == str(config.ingest_script) else 0,
                stdout="alert sent",
            )

        rc = run_intraday_catchup(
            config,
            env=None,
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 2
        assert len(runner_calls) == 2
        assert runner_calls[0] == build_intraday_catchup_command(config)
        # Alert invocation includes --job-name intraday_catchup and exit-code 2.
        alert_cmd = runner_calls[1]
        assert "--job-name" in alert_cmd and alert_cmd[alert_cmd.index("--job-name") + 1] == "intraday_catchup"
        assert "--exit-code" in alert_cmd and alert_cmd[alert_cmd.index("--exit-code") + 1] == "2"

    def test_failure_with_missing_node_skips_alert_and_returns_exit_code(self, tmp_path, monkeypatch):
        config = _config(tmp_path, node_bin="/does/not/exist/node")
        config.log_dir.mkdir(parents=True)

        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=3)

        rc = run_intraday_catchup(
            config,
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 3
        assert len(runner_calls) == 1  # alert was skipped

        log_file = config.log_dir / "intraday_catchup_2026-06-05.log"
        contents = log_file.read_text(encoding="utf-8")
        assert "node binary not found" in contents


class TestMain:
    def test_main_builds_config_and_dispatches(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_LOG_DIR", str(tmp_path / "warehouse" / "logs"))
        monkeypatch.delenv("MDW_INTRADAY_CATCHUP_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_INTRADAY_CATCHUP_ALERT_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_INTRADAY_CATCHUP_PYTHON_BIN", raising=False)
        monkeypatch.delenv("MDW_NODE_BIN", raising=False)

        captured: dict[str, object] = {}

        def fake_runner(cmd, **kwargs):
            captured["cmd"] = list(cmd)
            handle = kwargs.get("stdout")
            if handle is not None and hasattr(handle, "write"):
                handle.write("daily-backfill ok\n")
            return CompletedProcess(args=cmd, returncode=0)

        with (
            patch(
                "livewire_scripts.run_intraday_catchup_job.subprocess.run",
                side_effect=fake_runner,
            ),
            patch(
                "livewire_scripts.run_intraday_catchup_job.shutil.which",
                return_value="/usr/local/bin/node",
            ),
        ):
            rc = main([])

        assert rc == 0
        assert captured["cmd"][1].endswith("scripts/livewire_ingest.py")
        assert captured["cmd"][2] == "daily-backfill"


class TestNodeBinaryExists:
    def test_absolute_path_that_exists(self, tmp_path):
        node = tmp_path / "node"
        node.write_text("#!/bin/bash\n", encoding="utf-8")
        assert _node_binary_exists(str(node)) is True

    def test_absolute_path_that_does_not_exist(self, tmp_path):
        assert _node_binary_exists(str(tmp_path / "does_not_exist")) is False

    def test_relative_name_found_on_path(self):
        with patch("livewire_scripts.run_intraday_catchup_job.shutil.which", return_value="/usr/bin/node"):
            assert _node_binary_exists("node") is True

    def test_relative_name_not_found_on_path(self):
        with patch("livewire_scripts.run_intraday_catchup_job.shutil.which", return_value=None):
            assert _node_binary_exists("node") is False


class TestExtractErrorSummary:
    def test_returns_last_non_header_line(self, tmp_path):
        log = tmp_path / "test.log"
        log.write_text("=== Header ===\nsome error\n=== Failed ===\n", encoding="utf-8")
        assert _extract_error_summary(log) == "some error"

    def test_file_not_found_returns_fallback(self, tmp_path):
        result = _extract_error_summary(tmp_path / "nonexistent.log")
        assert "log file was not found" in result

    def test_all_header_lines_returns_no_summary_fallback(self, tmp_path):
        log = tmp_path / "test.log"
        log.write_text("=== Header ===\n=== Done ===\n", encoding="utf-8")
        result = _extract_error_summary(log)
        assert "no error summary" in result

    def test_prefers_summary_json_failed_phases(self, tmp_path):
        from livewire_scripts.daily_outcomes import SUMMARY_PREFIX

        log = tmp_path / "test.log"
        summary = SUMMARY_PREFIX + (
            '{"job":"daily_backfill","target_date":"2026-07-02",'
            '"phases":[{"label":"daily_backfill_fred_rates","exit":1,"duration_s":0.1}],'
            '"failed":["daily_backfill_fred_rates"]}'
        )
        log.write_text("some noise\n" + summary + "\n", encoding="utf-8")
        result = _extract_error_summary(log)
        assert result == "Intraday catchup failed — phases failed: daily_backfill_fred_rates"


class TestRunIntradayCatchupAdditional:
    def test_failure_with_alert_script_missing_skips_alert(self, tmp_path):
        node_bin = str(tmp_path / "bin" / "node")
        Path(node_bin).parent.mkdir(parents=True)
        Path(node_bin).write_text("#!/bin/bash\n", encoding="utf-8")
        config = _config(tmp_path, node_bin=node_bin)
        config.log_dir.mkdir(parents=True)
        # alert_script does NOT exist

        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=5)

        rc = run_intraday_catchup(
            config,
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 5
        assert len(runner_calls) == 1  # no alert dispatched
        log_file = config.log_dir / "intraday_catchup_2026-06-05.log"
        assert "alert script not found" in log_file.read_text(encoding="utf-8")

    def test_failure_with_alert_subprocess_non_zero(self, tmp_path):
        node_bin = str(tmp_path / "bin" / "node")
        Path(node_bin).parent.mkdir(parents=True)
        Path(node_bin).write_text("#!/bin/bash\n", encoding="utf-8")
        config = _config(tmp_path, node_bin=node_bin)
        config.log_dir.mkdir(parents=True)
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("# stub\n", encoding="utf-8")

        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            handle = kwargs.get("stdout")
            if handle is not None and hasattr(handle, "write"):
                handle.write("some failure output\n")
            rc = 4 if cmd[1] == str(config.ingest_script) else 1
            return CompletedProcess(args=cmd, returncode=rc, stdout="alert failed")

        rc = run_intraday_catchup(
            config,
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 4
        assert len(runner_calls) == 2
        log_file = config.log_dir / "intraday_catchup_2026-06-05.log"
        contents = log_file.read_text(encoding="utf-8")
        assert "failure alert returned non-zero exit code" in contents


class TestLaunchdTemplate:
    def test_plist_template_exists_and_parses(self):
        repo_root = Path(__file__).resolve().parent.parent
        plist_path = repo_root / "launchd" / "com.livewire.intraday-catchup.plist.example"
        assert plist_path.exists(), f"missing plist template at {plist_path}"

        # Avoid plistlib XML strictness — just check the human-meaningful invariants.
        text = plist_path.read_text(encoding="utf-8")
        assert "<string>com.livewire.intraday-catchup</string>" in text
        assert "run-intraday-catchup-job" in text
        assert "<key>Hour</key>" in text
        assert "<integer>18</integer>" in text  # 10:00Z: the second lake writer
        assert "<key>Minute</key>" in text
        assert "<integer>0</integer>" in text
        # Same substitution sentinel as the daily-update example — and it names the
        # WAREHOUSE, not a repo: the job runs the immutable release `current` points
        # at, using that release's own venv, never the dev checkout.
        assert "/path/to/warehouse/current" in text
        assert ".venv/bin/python scripts/livewire_ops.py run-intraday-catchup-job" in text
        assert "/path/to/repo" not in text


class TestBothRunnersTakeOneLock:
    """CLAUDE.md rule 5, fix the twin: two lock files is the same as no lock."""

    def test_the_daily_and_intraday_runners_lock_the_same_file(self, tmp_path, monkeypatch):
        import subprocess

        from livewire_scripts import run_daily_update_job as daily_runner
        from livewire_scripts.sync_runner import run_phase

        warehouse = tmp_path / "warehouse"
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
        monkeypatch.setenv("LW_RUN_ID", "daily-update-20260906T050000Z-1")

        config = daily_runner.RunnerConfig(
            warehouse_dir=warehouse,
            log_dir=tmp_path / "logs",
            daily_update_script=tmp_path / "daily.py",
            alert_script=tmp_path / "alert.py",
            python_bin="python3",
            node_bin="node",
            max_attempts=1,
            retry_delay_seconds=0,
        )
        daily_runner._run_scheduled_lane(
            config,
            ["true"],
            "FX Sync",
            "fx",
            env=None,
            runner=daily_runner._run_in_own_process_group,
            now_fn=daily_runner._utc_now,
        )
        run_phase("daily_backfill_fred_rates", ["true"], tmp_path / "logs", runner=subprocess.run)

        assert sorted((warehouse / "locks").iterdir()) == [warehouse / "locks" / "lake-io.lock"]

    def test_the_lock_is_not_inside_the_lake(self, tmp_path, monkeypatch):
        from livewire_scripts.paths import data_lake_dir, lake_lock_path

        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        assert not lake_lock_path().is_relative_to(data_lake_dir())
