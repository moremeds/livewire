"""Tests for the watchdog as a caller of the shared status surface."""

from __future__ import annotations

import subprocess
from datetime import UTC, date, datetime

import pytest

from livewire_scripts import check_daily_update_watchdog as watchdog
from livewire_scripts import notify
from livewire_scripts.status import Section, Verdict


def _section(verdict: Verdict, name: str = "X") -> Section:
    return Section(name=name, verdict=verdict, lines=[name])


def _send_runner(sent: list, returncode: int = 0):
    """The notify send-runner signature: (command, timeout=...) -> CompletedProcess."""

    def runner(command, timeout=None):
        sent.append(list(command))
        return subprocess.CompletedProcess(command, returncode, stdout="sent")

    return runner


def _skipped_rows() -> list[str]:
    from clients import ledger

    return sorted(
        row["skipped"]
        for row in ledger.query(
            "select json_extract_string(receipt_json,'$.skipped') as skipped from executions where script = 'notify'"
        )
    )


class TestTheWatchdogIsAStatusCaller:
    @pytest.fixture(autouse=True)
    def root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
        monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T103000Z-1")

    def test_bad_pages_once_per_state_across_runs(self, tmp_path, monkeypatch):
        """The same BAD at 10:30Z and 12:00Z is one send and one dedup skip row."""
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD, "Lanes terminal")])
        sent = []
        runner = _send_runner(sent)

        assert watchdog.run_watchdog(date(2026, 9, 2), now=datetime(2026, 9, 2, 10, 30, tzinfo=UTC), runner=runner) == 0
        assert watchdog.run_watchdog(date(2026, 9, 2), now=datetime(2026, 9, 2, 12, 0, tzinfo=UTC), runner=runner) == 0

        assert len(sent) == 1, "the second run dedups on the same fingerprint"
        assert sent[0][1].endswith("send_mail.mjs")
        assert any(t.startswith("--subject=PAGE 2026-09-02") for t in sent[0])
        assert _skipped_rows() == ["false", "true"]

    def test_a_new_bad_pages_again(self, tmp_path, monkeypatch):
        """Coverage turning BAD at 12:00Z is a different fingerprint → a second send."""
        sections = [
            [_section(Verdict.BAD, "Lanes terminal")],
            [_section(Verdict.BAD, "Lanes terminal"), _section(Verdict.BAD, "Coverage")],
        ]
        states = iter(sections)
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: next(states))
        sent = []
        runner = _send_runner(sent)

        assert watchdog.run_watchdog(date(2026, 9, 2), now=datetime(2026, 9, 2, 10, 30, tzinfo=UTC), runner=runner) == 0
        assert watchdog.run_watchdog(date(2026, 9, 2), now=datetime(2026, 9, 2, 12, 0, tzinfo=UTC), runner=runner) == 0

        assert len(sent) == 2, "a new BAD section is a new fingerprint, not a repeat"
        assert _skipped_rows() == ["false", "false"]

    def test_nothing_bad_sends_nothing_and_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.OK)])
        sent = []

        assert watchdog.run_watchdog(date(2026, 9, 2), runner=_send_runner(sent)) == 0

        assert sent == []
        from clients import ledger

        assert ledger.query("select 1 as hit from executions where script = 'notify'") == []

    def test_a_failed_send_is_exit_3_and_a_row(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD)])
        runner = _send_runner([], returncode=7)

        assert watchdog.run_watchdog(date(2026, 9, 2), runner=runner) == watchdog.ALERT_FAILED_EXIT_CODE

        from clients import ledger

        assert ledger.query("select exit_code from executions where script = 'notify'") == [{"exit_code": 7}]

    def test_no_marker_file_is_written(self, tmp_path, monkeypatch):
        """Dedup lives in the ledger; state/daily-update-watchdog is never created."""
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD)])
        assert watchdog.run_watchdog(date(2026, 9, 2), runner=_send_runner([])) == 0
        assert not (tmp_path / "warehouse" / "state" / "daily-update-watchdog").exists()

    def test_unknown_and_warn_do_not_page(self, tmp_path, monkeypatch):
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.UNKNOWN), _section(Verdict.WARN)])
        sent = []
        assert watchdog.run_watchdog(date(2026, 9, 2), runner=_send_runner(sent)) == 0
        assert sent == []


def test_parse_args():
    assert watchdog.parse_args(["--run-date", "2026-09-02"]).run_date == "2026-09-02"
    assert watchdog.parse_args([]).run_date is None


def test_main_runs_the_watchdog_for_the_parsed_date(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    seen = {}

    def fake_collect(run_date, *args, **kwargs):
        seen["run_date"] = run_date
        return [_section(Verdict.OK)]

    monkeypatch.setattr(watchdog, "collect", fake_collect)
    monkeypatch.setattr(notify, "send", lambda *a, **k: 0)

    assert watchdog.main(["--run-date", "2026-09-02"]) == 0
    assert seen["run_date"] == date(2026, 9, 2)


class TestTheWatchdogDoesNotRestateALanesOwnPage:
    """A lane pages the moment it fails; the watchdog reads that same failure out
    of the ledger an hour later and paged again, because the two build their
    fingerprints in unrelated namespaces. One outage, two emails.
    pm:2026-09-16-one-failure-paged-twice
    """

    @pytest.fixture(autouse=True)
    def root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))

    def _lane_pages(self, monkeypatch, sent, run_id: str) -> None:
        """The lane runner's own page, sent while `run_id` was executing."""
        monkeypatch.setenv("LW_RUN_ID", run_id)
        notice = notify.page_for_lane(date(2026, 9, 2), "intraday_catchup", 1, "boom", "tail")
        assert notify.send(notice, runner=_send_runner(sent)) == 0

    def test_the_watchdog_stays_quiet_about_a_run_that_already_paged(self, monkeypatch):
        run_id = "intraday-catchup-20260902T100002Z-96742"
        sent: list = []
        self._lane_pages(monkeypatch, sent, run_id)

        monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T120000Z-1")
        monkeypatch.setattr(
            watchdog,
            "collect",
            lambda *a, **k: [Section("Intraday catch-up ran", Verdict.BAD, ["boom"], run_id=run_id)],
        )
        assert (
            watchdog.run_watchdog(
                date(2026, 9, 2), now=datetime(2026, 9, 2, 12, 0, tzinfo=UTC), runner=_send_runner(sent)
            )
            == 0
        )

        assert len(sent) == 1, "the lane's page is the only one"

    def test_a_failed_lane_page_leaves_the_watchdog_as_the_safety_net(self, monkeypatch):
        """A send that never left the machine must not silence the watchdog."""
        run_id = "intraday-catchup-20260902T100002Z-96742"
        sent: list = []
        monkeypatch.setenv("LW_RUN_ID", run_id)
        notice = notify.page_for_lane(date(2026, 9, 2), "intraday_catchup", 1, "boom", "tail")
        assert notify.send(notice, runner=_send_runner(sent, returncode=1)) != 0

        monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T120000Z-1")
        monkeypatch.setattr(
            watchdog,
            "collect",
            lambda *a, **k: [Section("Intraday catch-up ran", Verdict.BAD, ["boom"], run_id=run_id)],
        )
        watchdog.run_watchdog(date(2026, 9, 2), now=datetime(2026, 9, 2, 12, 0, tzinfo=UTC), runner=_send_runner(sent))

        assert len(sent) == 2, "the watchdog still pages when the lane's page failed"

    def test_a_bad_section_belonging_to_no_run_still_pages(self, monkeypatch):
        """Coverage, disk, a stale catalog: nothing paged for these, so the
        watchdog is the only surface that will."""
        sent: list = []
        monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T120000Z-1")
        monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD, "Coverage")])

        assert (
            watchdog.run_watchdog(
                date(2026, 9, 2), now=datetime(2026, 9, 2, 12, 0, tzinfo=UTC), runner=_send_runner(sent)
            )
            == 0
        )
        assert len(sent) == 1

    def test_one_suppressed_section_does_not_suppress_the_others(self, monkeypatch):
        run_id = "intraday-catchup-20260902T100002Z-96742"
        sent: list = []
        self._lane_pages(monkeypatch, sent, run_id)

        monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T120000Z-1")
        monkeypatch.setattr(
            watchdog,
            "collect",
            lambda *a, **k: [
                Section("Intraday catch-up ran", Verdict.BAD, ["boom"], run_id=run_id),
                Section("Coverage", Verdict.BAD, ["26.5%"]),
            ],
        )
        watchdog.run_watchdog(date(2026, 9, 2), now=datetime(2026, 9, 2, 12, 0, tzinfo=UTC), runner=_send_runner(sent))

        assert len(sent) == 2
        subject = next(token for token in sent[1] if token.startswith("--subject="))
        assert "Coverage" in subject and "Intraday catch-up ran" not in subject
