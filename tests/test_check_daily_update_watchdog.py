"""Tests for the watchdog as a caller of the shared status surface."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from livewire_scripts import check_daily_update_watchdog as watchdog
from livewire_scripts.run_daily_update_job import RunnerConfig
from livewire_scripts.status import Section, Verdict


def _config(tmp_path: Path) -> RunnerConfig:
    return RunnerConfig(
        warehouse_dir=tmp_path / "warehouse",
        log_dir=tmp_path / "warehouse" / "logs",
        daily_update_script=tmp_path / "livewire_ingest.py",
        alert_script=tmp_path / "livewire_ops.py",
        python_bin="python",
        node_bin="node",
        max_attempts=3,
        retry_delay_seconds=300,
    )


def _section(verdict: Verdict, name: str = "X") -> Section:
    return Section(name=name, verdict=verdict, lines=[name])


def _ok() -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess([], 0, stdout="sent")


class TestTheWatchdogIsAStatusCaller:
    @pytest.fixture(autouse=True)
    def root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
        monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T103000Z-1")

    def test_an_all_green_status_pages_nobody(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.OK)])
        sent = []
        monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == 0
        assert sent == []

    def test_one_bad_section_pages_once(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD, "Lanes terminal")])
        sent = []
        monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == 0
        assert len(sent) == 1

    def test_a_second_run_the_same_day_does_not_page_again(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD)])
        sent = []
        monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
        watchdog.run_watchdog(_config(tmp_path), "2026-09-02")
        watchdog.run_watchdog(_config(tmp_path), "2026-09-02")
        assert len(sent) == 1

    @pytest.mark.parametrize("verdict", [Verdict.UNKNOWN, Verdict.WARN])
    def test_only_bad_pages(self, tmp_path, monkeypatch, verdict):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(verdict)])
        sent = []
        monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == 0
        assert sent == []

    def test_a_failed_send_is_recorded_as_an_execution_row(self, tmp_path, monkeypatch):
        from clients import ledger

        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD)])
        monkeypatch.setattr(
            watchdog,
            "send_failure_alert",
            lambda *a, **k: subprocess.CompletedProcess([], 7, stdout="smtp down"),
        )
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == watchdog.ALERT_FAILED_EXIT_CODE
        assert ledger.query("select exit_code from executions where script = 'send_alert'") == [{"exit_code": 7}]


def test_parse_args_and_path_builders(tmp_path):
    assert watchdog.parse_args(["--run-date", "2026-09-02"]).run_date == "2026-09-02"
    assert watchdog.build_daily_log_file(tmp_path, "2026-09-02") == tmp_path / "daily_update_2026-09-02.log"
    assert watchdog.build_watchdog_marker_file(tmp_path, "2026-09-02").name == "2026-09-02.alerted"
