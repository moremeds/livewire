"""Tests for scripts/check_daily_update_watchdog.py."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from subprocess import CompletedProcess
from types import SimpleNamespace
from unittest.mock import patch

from livewire_scripts.check_daily_update_watchdog import (
    WATCHDOG_ALERT_FAILED_EXIT_CODE,
    WATCHDOG_ALERT_SENT_EXIT_CODE,
    build_daily_log_file,
    build_watchdog_log_file,
    build_watchdog_marker_file,
    determine_watchdog_error,
    main,
    parse_args,
    record_alert_marker,
    run_watchdog,
)
from livewire_scripts.run_daily_update_job import ASSET_CLASSES, RunnerConfig


def _config(tmp_path: Path, *, node_bin: str = "/opt/homebrew/bin/node") -> RunnerConfig:
    repo_root = tmp_path / "repo"
    script_dir = repo_root / "scripts"
    return RunnerConfig(
        warehouse_dir=tmp_path / "warehouse",
        log_dir=tmp_path / "warehouse" / "logs",
        daily_update_script=script_dir / "daily_update.py",
        alert_script=script_dir / "livewire_ops.py",
        python_bin="/usr/bin/python3",
        node_bin=node_bin,
        max_attempts=3,
        retry_delay_seconds=300,
    )


def _all_daily_done_markers() -> str:
    lines = [f"=== Done {asset_class} 2026-03-11T20:05:09Z (attempt 1/3) ===" for asset_class in ASSET_CLASSES]
    lines.append("=== Done cboe 2026-03-11T20:05:10Z ===")
    lines.append("=== Done fx 2026-03-11T20:05:10Z ===")
    lines.append("=== Done silver 2026-03-11T20:05:11Z ===")
    return "\n".join(lines) + "\n"


def _healthy_run(config, *, daily_log_extra: str = "") -> None:
    log_file = build_daily_log_file(config.log_dir, "2026-03-11")
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text(_all_daily_done_markers() + daily_log_extra, encoding="utf-8")
    (config.log_dir / "quality_summary_2026-03-11.marker").write_text("ok\n")
    (config.log_dir / "intraday_catchup_2026-03-11.log").write_text(
        "=== Done 2026-03-11T05:02:29Z ===\n", encoding="utf-8"
    )


class TestHelpers:
    def test_parse_args_and_path_builders(self, tmp_path):
        args = parse_args(["--run-date", "2026-03-11"])
        assert args.run_date == "2026-03-11"
        assert parse_args([]).run_date is None

        assert build_daily_log_file(tmp_path, "2026-03-11") == tmp_path / "daily_update_2026-03-11.log"
        assert build_watchdog_log_file(tmp_path, "2026-03-11") == tmp_path / "daily_update_watchdog_2026-03-11.log"
        assert (
            build_watchdog_marker_file(tmp_path, "2026-03-11")
            == tmp_path / "state" / "daily-update-watchdog" / "2026-03-11.alerted"
        )

    def test_determine_error_and_marker_recording(self, tmp_path):
        missing_log = tmp_path / "missing.log"
        assert "did not start" in determine_watchdog_error(missing_log, "2026-03-11")

        incomplete_log = tmp_path / "daily.log"
        incomplete_log.write_text("=== Daily Update 2026-03-11T20:05:07Z ===\n", encoding="utf-8")
        assert "did not complete successfully" in determine_watchdog_error(incomplete_log, "2026-03-11")

        marker_file = build_watchdog_marker_file(tmp_path, "2026-03-11")
        record_alert_marker(marker_file, "sent")
        assert marker_file.read_text(encoding="utf-8") == "sent\n"


class TestRunWatchdog:
    def test_returns_healthy_when_daily_log_completed(self, tmp_path):
        config = _config(tmp_path)
        _healthy_run(config)

        assert run_watchdog(config, run_date="2026-03-11", env={}) == 0

    def test_missing_silver_scope_is_not_healthy(self, tmp_path):
        """Silver is the served artifact; a rebuild that never ran must alert."""
        config = _config(tmp_path)
        _healthy_run(config)
        log_file = build_daily_log_file(config.log_dir, "2026-03-11")
        log_file.write_text(
            "\n".join(line for line in log_file.read_text(encoding="utf-8").splitlines() if "Done silver" not in line)
            + "\n",
            encoding="utf-8",
        )

        assert run_watchdog(config, run_date="2026-03-11", env={}) != 0

    def test_skipped_scope_reports_degraded_not_missing(self, tmp_path):
        """A 2FA-gated Gateway is degraded, not 'the sync never ran'."""
        config = _config(tmp_path)
        _healthy_run(config)
        log_file = build_daily_log_file(config.log_dir, "2026-03-11")
        kept = [line for line in log_file.read_text(encoding="utf-8").splitlines() if "Done fx" not in line]
        kept.append("=== Skipped fx 2026-03-11T20:05:12Z (IB Gateway unreachable) ===")
        log_file.write_text("\n".join(kept) + "\n", encoding="utf-8")

        assert run_watchdog(config, run_date="2026-03-11", env={}) != 0
        watchdog_log = (config.log_dir / "daily_update_watchdog_2026-03-11.log").read_text(encoding="utf-8")
        assert "DEGRADED" in watchdog_log
        assert "missing completion scopes" not in watchdog_log

    def test_equity_lane_that_published_nothing_is_not_healthy(self, tmp_path):
        """`=== Done equity ===` only proves the process finished."""
        config = _config(tmp_path)
        summary = (
            'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"ib",'
            '"target_date":"2026-03-11","updated":0,"no_trade":0,"partial":0,"errors":2500,'
            '"bars_inserted":0,"validation_issues":0,"top_errors":[]}\n'
        )
        _healthy_run(config, daily_log_extra=summary)

        assert run_watchdog(config, run_date="2026-03-11", env={}) != 0
        watchdog_log = (config.log_dir / "daily_update_watchdog_2026-03-11.log").read_text(encoding="utf-8")
        assert "published nothing" in watchdog_log

    def test_healthy_equity_summary_stays_healthy(self, tmp_path):
        config = _config(tmp_path)
        summary = (
            'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"massive",'
            '"target_date":"2026-03-11","updated":2400,"no_trade":80,"partial":0,"errors":3,'
            '"bars_inserted":2400,"validation_issues":0,"top_errors":[]}\n'
        )
        _healthy_run(config, daily_log_extra=summary)

        assert run_watchdog(config, run_date="2026-03-11", env={}) == 0

    def test_undelivered_alerts_are_surfaced(self, tmp_path):
        """The alert channel dying silently is what hid a six-day outage."""
        config = _config(tmp_path)
        _healthy_run(config)
        queued = config.log_dir / "alerts_undelivered"
        queued.mkdir(parents=True, exist_ok=True)
        (queued / "2026-03-10_daily_update_2026-03-10.txt").write_text("stuck\n", encoding="utf-8")

        assert run_watchdog(config, run_date="2026-03-11", env={}) != 0
        watchdog_log = (config.log_dir / "daily_update_watchdog_2026-03-11.log").read_text(encoding="utf-8")
        assert "could not be delivered" in watchdog_log

    def test_skips_duplicate_alert_when_marker_exists(self, tmp_path):
        config = _config(tmp_path)
        marker_file = build_watchdog_marker_file(config.warehouse_dir, "2026-03-11")
        marker_file.parent.mkdir(parents=True, exist_ok=True)
        marker_file.write_text("already sent\n", encoding="utf-8")

        rc = run_watchdog(config, run_date="2026-03-11", env={})

        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "skipping duplicate failure email" in watchdog_log.read_text(encoding="utf-8")

    def test_sends_alert_for_incomplete_log(self, tmp_path):
        config = _config(tmp_path)
        daily_log = build_daily_log_file(config.log_dir, "2026-03-11")
        daily_log.parent.mkdir(parents=True, exist_ok=True)
        daily_log.write_text("partial run only\n", encoding="utf-8")
        captured = {}

        def _send_failure_alert(config_arg, request, log_file, env=None, runner=None):
            captured["config"] = config_arg
            captured["request"] = request
            captured["log_file"] = log_file
            return SimpleNamespace(returncode=0, stdout="sent")

        with patch(
            "livewire_scripts.check_daily_update_watchdog.send_failure_alert",
            side_effect=_send_failure_alert,
        ):
            rc = run_watchdog(config, run_date="2026-03-11", env={"A": "1"})

        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        assert captured["config"] == config
        assert captured["request"].attempts is None
        assert captured["request"].exit_code is None
        assert captured["request"].log_file == daily_log
        assert captured["log_file"] == build_watchdog_log_file(config.log_dir, "2026-03-11")
        marker_file = build_watchdog_marker_file(config.warehouse_dir, "2026-03-11")
        assert marker_file.exists() is True
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "Watchdog failure alert sent successfully. sent" in watchdog_log.read_text(encoding="utf-8")

    def test_returns_failed_exit_code_when_alert_cannot_be_sent(self, tmp_path):
        config = _config(tmp_path)

        with patch("livewire_scripts.check_daily_update_watchdog.send_failure_alert", return_value=None):
            rc = run_watchdog(config, run_date="2026-03-11", env={})

        assert rc == WATCHDOG_ALERT_FAILED_EXIT_CODE
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "could not send a failure alert" in watchdog_log.read_text(encoding="utf-8")

    def test_returns_failed_exit_code_when_alert_command_fails(self, tmp_path):
        config = _config(tmp_path)

        with patch(
            "livewire_scripts.check_daily_update_watchdog.send_failure_alert",
            return_value=SimpleNamespace(returncode=2, stdout="smtp down"),
        ):
            rc = run_watchdog(config, run_date="2026-03-11", env={})

        assert rc == WATCHDOG_ALERT_FAILED_EXIT_CODE
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "WARNING: watchdog failure alert returned non-zero exit code 2. smtp down" in watchdog_log.read_text(
            encoding="utf-8"
        )

    def test_alerts_when_subset_of_asset_classes_done(self, tmp_path):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        daily_log = build_daily_log_file(config.log_dir, "2026-03-11")
        daily_log.write_text("=== Done equity 2026-03-11T20:05:09Z (attempt 1/3) ===\n", encoding="utf-8")
        (config.log_dir / "quality_summary_2026-03-11.marker").write_text("ok\n", encoding="utf-8")
        (config.log_dir / "intraday_catchup_2026-03-11.log").write_text(
            "=== Done 2026-03-11T05:02:29Z ===\n", encoding="utf-8"
        )
        captured = {}

        def _send(config_arg, request, log_file, env=None, runner=None):
            captured["request"] = request
            return SimpleNamespace(returncode=0, stdout="sent")

        with patch(
            "livewire_scripts.check_daily_update_watchdog.send_failure_alert",
            side_effect=_send,
        ):
            rc = run_watchdog(config, run_date="2026-03-11", env={})

        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        assert "missing completion scopes" in captured["request"].error_summary
        assert "futures" in captured["request"].error_summary
        assert "cmdty" in captured["request"].error_summary
        assert "fx" in captured["request"].error_summary
        assert "cboe" in captured["request"].error_summary


class TestQualitySummaryMarker:
    def test_passes_when_both_markers_present(self, tmp_path):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        daily_log = build_daily_log_file(config.log_dir, "2026-05-18")
        daily_log.write_text("=== Done 2026-05-18T20:00:00Z (attempt 1/3) ===\n")
        (config.log_dir / "quality_summary_2026-05-18.marker").write_text("ok\n")
        (config.log_dir / "intraday_catchup_2026-05-18.log").write_text(
            "=== Done 2026-05-18T05:02:29Z ===\n", encoding="utf-8"
        )

        rc = run_watchdog(config, run_date="2026-05-18")
        assert rc == 0

    def test_alerts_when_intraday_marker_missing(self, tmp_path):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        daily_log = build_daily_log_file(config.log_dir, "2026-05-18")
        daily_log.write_text("=== Done 2026-05-18T20:00:00Z (attempt 1/3) ===\n")
        (config.log_dir / "quality_summary_2026-05-18.marker").write_text("ok\n")
        # intraday_catchup log intentionally absent
        captured = {}

        def _send(config_arg, request, log_file, env=None, runner=None):
            captured["request"] = request
            return SimpleNamespace(returncode=0, stdout="sent")

        with patch(
            "livewire_scripts.check_daily_update_watchdog.send_failure_alert",
            side_effect=_send,
        ):
            rc = run_watchdog(config, run_date="2026-05-18", env={})

        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        assert "intraday catch-up did not start" in captured["request"].error_summary

    def test_alerts_when_intraday_log_incomplete(self, tmp_path):
        from livewire_scripts.check_daily_update_watchdog import (
            build_intraday_log_file,
            determine_intraday_watchdog_error,
        )

        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        intraday_log = build_intraday_log_file(config.log_dir, "2026-05-18")
        intraday_log.write_text("started but never finished\n", encoding="utf-8")
        msg = determine_intraday_watchdog_error(intraday_log, "2026-05-18")
        assert "did not complete successfully" in msg

    def test_alerts_when_quality_marker_missing(self, tmp_path):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('sent')\n", encoding="utf-8")
        daily_log = build_daily_log_file(config.log_dir, "2026-05-18")
        daily_log.write_text("=== Done 2026-05-18T20:00:00Z (attempt 1/3) ===\n")

        calls = []

        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=0, stdout="sent", stderr="")

        with patch("livewire_scripts.run_daily_update_job.node_binary_exists", return_value=True):
            rc = run_watchdog(config, run_date="2026-05-18", runner=fake_runner)

        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        assert calls, "watchdog should have spawned the alert subprocess"


class TestMain:
    def test_main_uses_build_config_and_run_watchdog(self, tmp_path):
        config = _config(tmp_path)

        with patch("livewire_scripts.check_daily_update_watchdog.build_config", return_value=config):
            with patch("livewire_scripts.check_daily_update_watchdog.run_watchdog", return_value=1) as run_mock:
                assert main(["--run-date", "2026-03-11"]) == 1

        run_mock.assert_called_once_with(
            config,
            run_date="2026-03-11",
            env=os.environ.copy(),
        )

    def test_default_run_date_is_utc(self, tmp_path):
        config = _config(tmp_path)

        class FrozenDateTime:
            @classmethod
            def now(cls, tz=None):
                if tz is UTC:
                    return datetime(2026, 4, 6, 1, 0, tzinfo=UTC)
                return datetime(2026, 4, 5, 18, 0)

        with patch("livewire_scripts.check_daily_update_watchdog.datetime", FrozenDateTime):
            with patch("livewire_scripts.check_daily_update_watchdog.build_config", return_value=config):
                with patch("livewire_scripts.check_daily_update_watchdog.sys.argv", ["watchdog"]):
                    with patch("livewire_scripts.check_daily_update_watchdog.run_watchdog", return_value=0) as run_mock:
                        assert main([]) == 0

        assert run_mock.call_args.kwargs["run_date"] == "2026-04-06"
