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


def _send_runner(sent: list, returncode: int = 0):
    """The notify send-runner signature: (command, timeout=...) -> CompletedProcess."""

    def runner(command, timeout=None):
        sent.append(list(command))
        return subprocess.CompletedProcess(command, returncode, stdout="sent")

    return runner


class TestTheWatchdogIsAStatusCaller:
    @pytest.fixture(autouse=True)
    def root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
        monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T103000Z-1")

    def test_an_all_green_status_pages_nobody(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.OK)])
        sent = []
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=_send_runner(sent)) == 0
        assert sent == []

    def test_one_bad_section_pages_once(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD, "Lanes terminal")])
        sent = []
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=_send_runner(sent)) == 0
        assert len(sent) == 1
        assert sent[0][1].endswith("send_mail.mjs")
        assert any(t.startswith("--subject=PAGE 2026-09-02") for t in sent[0])

    def test_a_daily_run_that_never_started_pages(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            watchdog,
            "collect",
            lambda *a, **k: [
                _section(Verdict.UNKNOWN, "Daily update ran"),
                _section(Verdict.OK, "Daily update finished"),
            ],
        )
        sent = []
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=_send_runner(sent)) == 0
        assert len(sent) == 1

    def test_intraday_catchup_that_never_started_pages(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            watchdog,
            "collect",
            lambda *a, **k: [
                _section(Verdict.UNKNOWN, "Intraday catch-up ran"),
                _section(Verdict.OK, "Intraday catch-up finished"),
            ],
        )
        sent = []
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=_send_runner(sent)) == 0
        assert len(sent) == 1

    def test_a_second_run_the_same_day_does_not_page_again(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD)])
        sent = []
        runner = _send_runner(sent)
        watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=runner)
        watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=runner)
        assert len(sent) == 1

    @pytest.mark.parametrize("verdict", [Verdict.UNKNOWN, Verdict.WARN])
    def test_only_bad_pages(self, tmp_path, monkeypatch, verdict):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(verdict)])
        sent = []
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=_send_runner(sent)) == 0
        assert sent == []

    def test_a_failed_send_is_recorded_as_an_execution_row(self, tmp_path, monkeypatch):
        from clients import ledger

        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD)])
        runner = _send_runner([], returncode=7)
        assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02", runner=runner) == watchdog.ALERT_FAILED_EXIT_CODE
        assert ledger.query("select exit_code from executions where script = 'notify'") == [{"exit_code": 7}]


def test_parse_args_and_path_builders(tmp_path):
    assert watchdog.parse_args(["--run-date", "2026-09-02"]).run_date == "2026-09-02"
    assert watchdog.build_daily_log_file(tmp_path, "2026-09-02") == tmp_path / "daily_update_2026-09-02.log"
    assert watchdog.build_watchdog_marker_file(tmp_path, "2026-09-02").name == "2026-09-02.alerted"
