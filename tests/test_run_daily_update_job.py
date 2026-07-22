"""Tests for scripts/run_daily_update_job.py."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from subprocess import CompletedProcess
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE
from livewire_scripts import run_daily_update_job as daily_runner
from livewire_scripts.run_daily_update_job import (
    ASSET_CLASSES,
    AlertRequest,
    RunnerConfig,
    _utc_now,
    append_log,
    build_alert_command,
    build_cboe_volatility_command,
    build_config,
    build_corporate_action_command,
    build_daily_update_command,
    build_log_file,
    build_silver_rebuild_command,
    extract_error_summary,
    log_has_completion_marker,
    main,
    node_binary_exists,
    run_cboe_volatility_sync,
    run_daily_update_attempt,
    run_post_success_quality,
    run_with_retries,
    send_failure_alert,
)


@pytest.fixture(autouse=True)
def no_real_quality_spawn():
    """Keep main() from shelling out to the real quality CLI.

    main() ends by spawning coverage/weekly/digest. Unpatched, a unit test
    launches `livewire_quality.py coverage` against the operator's live
    warehouse — and coverage runs auto-recovery subprocesses that write
    bronze. Autouse so a new main() test cannot forget it.
    """
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

    def test_extract_error_summary_and_completion_marker(self, tmp_path):
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
        assert log_has_completion_marker(log_file) is True
        assert log_has_completion_marker(tmp_path / "nope.log") is False

        no_marker = tmp_path / "no_marker.log"
        no_marker.write_text("started\nfailed\n", encoding="utf-8")
        assert log_has_completion_marker(no_marker) is False

    def test_completed_scopes_parses_per_asset_markers(self, tmp_path):
        log_file = tmp_path / "daily.log"
        log_file.write_text(
            "\n".join(
                [
                    "=== Done equity 2026-03-11T20:05:08Z (attempt 1/3) ===",
                    "=== Done futures 2026-03-11T20:05:09Z (attempt 1/3) ===",
                    "=== Done cboe 2026-03-11T20:05:10Z ===",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        assert daily_runner.completed_scopes(log_file) == {"equity", "futures", "cboe"}

    def test_legacy_done_line_counts_as_wildcard(self, tmp_path):
        log_file = tmp_path / "daily.log"
        log_file.write_text("=== Done 2026-03-11T20:05:08Z (attempt 1/3) ===\n", encoding="utf-8")

        assert daily_runner.completed_scopes(log_file) == {"*"}

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

        def _runner(command, stdout, stderr, text, env, check):
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

    def test_coverage_and_weekly_spawned(self, tmp_path):
        config = _config(tmp_path)
        calls = []

        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")

        self._run(config, fake_runner)
        quality_calls = [c for c in calls if any("livewire_quality.py" in str(x) for x in c)]
        assert any("coverage" in c for c in quality_calls)
        assert any("weekly" in c for c in quality_calls)

    def test_coverage_spawn_failure_is_logged_not_raised(self, tmp_path):
        config = _config(tmp_path)

        def fake_runner(cmd, **kwargs):
            # Match the quality subcommand arg, not the worktree path (which
            # itself contains the word "coverage").
            is_coverage = any("livewire_quality.py" in str(x) for x in cmd) and "coverage" in cmd
            return CompletedProcess(args=cmd, returncode=3 if is_coverage else 0, stdout=b"", stderr=b"")

        log_file = self._run(config, fake_runner)
        assert "WARNING: coverage report failed" in log_file.read_text(encoding="utf-8")

    def test_digest_failure_is_logged_not_raised(self, tmp_path):
        config = _config(tmp_path)

        def fake_runner(cmd, **kwargs):
            return CompletedProcess(args=cmd, returncode=2, stdout=b"", stderr=b"digest failed")

        log_file = self._run(config, fake_runner)
        assert "WARNING: nightly digest failed" in log_file.read_text(encoding="utf-8")

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

        def _runner(command, stdout, stderr, text, env, check):
            assert command[0] == "/usr/bin/python3"
            assert command[2] == "send-alert"
            assert "--error-summary" in command
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

        def _runner(command, stdout, stderr, text, env, check):
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
        assert "Runner config: attempts=3 retry_delay_seconds=300 hostname=warehouse.local" in log_text
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

        def _runner(command, stdout, stderr, text, env, check):
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
                SimpleNamespace(returncode=0, stdout="alert sent"),
            ]
        )

        def _runner(command, stdout=None, stderr=None, text=None, env=None, check=None):
            if hasattr(stdout, "write"):
                stdout.write("sync failed\n")
            return next(results)

        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('send');\n", encoding="utf-8")
        sleep_calls: list[int] = []

        with patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=True):
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

        def _runner(command, stdout=None, stderr=None, text=None, env=None, check=None):
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
        results = iter(
            [
                SimpleNamespace(returncode=3, stdout=""),
                SimpleNamespace(returncode=2, stdout="smtp down"),
            ]
        )

        def _runner(command, stdout=None, stderr=None, text=None, env=None, check=None):
            if hasattr(stdout, "write"):
                stdout.write("sync failed\n")
            return next(results)

        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('send');\n", encoding="utf-8")

        with patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=True):
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

        def _runner(command, stdout, stderr, text, env, check):
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

    def test_run_cboe_volatility_sync_failure(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 18, 20, 5, 7, tzinfo=UTC),
                datetime(2026, 3, 18, 20, 5, 8, tzinfo=UTC),
            ]
        )

        def _runner(command, stdout, stderr, text, env, check):
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

        def runner(command, stdout, stderr, text, env, check):
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

        def daily(cfg, args, env, completion_scope=None):
            calls.append(completion_scope)
            return 0

        def cboe(*args, **kwargs):
            calls.append("cboe")
            return 0

        def silver(*args, **kwargs):
            calls.append("silver")
            return 0

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", side_effect=actions),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=daily),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", side_effect=cboe),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild", side_effect=silver),
        ):
            assert main(["--dry-run"]) == 0

        assert calls == ["actions", *ASSET_CLASSES, "cboe", "silver"]

    def test_failed_action_sync_prevents_silver_rebuild(self):
        config = _config(Path("/tmp/test"))
        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", return_value=2),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild") as silver,
        ):
            assert main([]) == 2
        silver.assert_not_called()

    def test_failed_ingestion_prevents_silver_rebuild(self):
        config = _config(Path("/tmp/test"))

        def daily(cfg, args, env, completion_scope=None):
            return 3 if completion_scope == "equity" else 0

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=daily),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=0),
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
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild", return_value=4),
        ):
            assert main([]) == 4

    def test_main_runs_all_asset_classes_and_cboe_by_default(self):
        config = _config(Path("/tmp/test"))
        ib_calls: list[list[str]] = []
        cboe_called = []

        def _run_ib(cfg, args, env, completion_scope=None):
            ib_calls.append(args)
            assert completion_scope == args[args.index("--asset-class") + 1]
            return 0

        def _run_cboe(cfg, env, **kwargs):
            cboe_called.append(True)
            return 0

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", return_value=0),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=_run_ib),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", side_effect=_run_cboe),
            patch("livewire_scripts.run_daily_update_job.run_silver_rebuild", return_value=0),
        ):
            assert main(["--dry-run"]) == 0

        # IB syncs equity, futures, cmdty and fx; volatility via CBOE
        assert ib_calls == [["--dry-run", "--asset-class", ac] for ac in ASSET_CLASSES]
        assert cboe_called == [True]
        assert "cmdty" in ASSET_CLASSES and "fx" in ASSET_CLASSES

    def test_main_explicit_asset_class_skips_cboe(self):
        config = _config(Path("/tmp/test"))

        with patch("livewire_scripts.run_daily_update_job.build_config", return_value=config):
            with patch("livewire_scripts.run_daily_update_job.run_with_retries", return_value=0) as run_mock:
                with patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync") as cboe_mock:
                    assert main(["--dry-run", "--asset-class", "equity"]) == 0

        run_mock.assert_called_once_with(
            config,
            ["--dry-run", "--asset-class", "equity"],
            env=os.environ.copy(),
            completion_scope="equity",
        )
        cboe_mock.assert_not_called()

    def _main_with(self, *, lane_codes=None, action=0, cboe=0, gateway_down=()):
        """Run main() with each lane's exit code stubbed. Returns (rc, silver_mock)."""
        config = _config(Path("/tmp/test"))
        codes = dict(lane_codes or {})

        def _run(cfg, args, env, completion_scope=None):
            name = args[args.index("--asset-class") + 1]
            if name in gateway_down:
                return GATEWAY_DOWN_EXIT_CODE
            return codes.get(name, 0)

        with (
            patch("livewire_scripts.run_daily_update_job.build_config", return_value=config),
            patch("livewire_scripts.run_daily_update_job.run_corporate_action_sync", return_value=action),
            patch("livewire_scripts.run_daily_update_job.run_with_retries", side_effect=_run),
            patch("livewire_scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=cboe),
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
        rc, silver = self._main_with(lane_codes={"futures": 1, "fx": 1, "cmdty": 1})
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
        rc, silver = self._main_with(gateway_down=("futures", "cmdty", "fx"))
        assert rc == 0
        silver.assert_called_once()

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
