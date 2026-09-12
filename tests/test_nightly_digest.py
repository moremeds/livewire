"""Tests for livewire_scripts.nightly_digest — the unconditional daily digest."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from subprocess import CompletedProcess

import pytest

from clients import ledger
from livewire_scripts import nightly_digest, run_daily_update_job, status
from livewire_scripts.nightly_digest import build_digest, main
from livewire_scripts.status import Section, Verdict


@pytest.fixture(autouse=True)
def _isolated_ledger(tmp_path, monkeypatch):
    """collect() reaches launchctl and the real analytics.duckdb; nothing here
    may depend on which plists are loaded on the machine running the test."""
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("LW_RUN_ID", "digest-test")
    monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(
        status,
        "_coverage_headline",
        lambda _db: (_ for _ in ()).throw(FileNotFoundError("analytics.duckdb")),
    )
    monkeypatch.setattr(
        status.subprocess,
        "run",
        lambda *_a, **_kw: CompletedProcess(
            [], 0, stdout="".join(f"-\t0\t{label}\n" for label in status._LAUNCHD_JOBS), stderr=""
        ),
    )


def _measurement(name, scope, value, *, measured_at):
    ledger.emit(
        "measurements",
        [
            {
                "name": name,
                "scope": scope,
                "measured_at": measured_at,
                "value": float(value),
                "unit": "ratio",
                "source": "measured",
                "run_id": "coverage-run",
            }
        ],
        run_id="coverage-run",
    )


def _ok_runner(calls: list) -> object:
    def runner(command, timeout=None):
        calls.append(command)
        return CompletedProcess(command, 0, stdout='{"accepted":["ops@example.com"],"messageId":"m"}')

    return runner


def _sections(*verdicts) -> list[Section]:
    return [Section(f"Check {i}", v, [f"{v.name} detail"]) for i, v in enumerate(verdicts)]


class TestBuildDigest:
    def test_coverage_block_is_today_and_per_scope(self):
        now = datetime(2026, 9, 13, 11, 14, tzinfo=UTC)
        for name, value in (("coverage_pct", 0.9996), ("coverage_total", 13547)):
            _measurement(name, "1d", value, measured_at=now)
            _measurement(name, "1d", 0.9993 if name == "coverage_pct" else 13540, measured_at=now - timedelta(hours=2))
        _measurement("coverage_pct", "1m", 0.356, measured_at=now)
        _measurement("coverage_total", "1m", 11956, measured_at=now)
        _measurement("coverage_recovery_deferred", "1m", 1, measured_at=now)
        _measurement("coverage_recovery_deferred", "1m", 1, measured_at=now - timedelta(hours=2))
        _measurement("coverage_still_missing", "1m", 7700, measured_at=now)
        _measurement("coverage_scan_ok", "all", 1, measured_at=now)
        _measurement("coverage_elapsed_s", "all", 1402, measured_at=now)
        rows = nightly_digest._coverage_rows()
        body = build_digest(date(2026, 9, 13), [], previous_verdicts={}, coverage_rows=rows, sent_rows=[], now=now)
        assert f"COVERAGE as of {now:%Y-%m-%d %H:%M}Z (scan ok=1, 1402s)" in body
        assert "1d" in body and "99.96%" in body and "99.93%" in body
        assert "1m" in body and "35.60%" in body
        assert "recovery=deferred x2" in body
        assert "still_missing=7700" in body

    def test_a_missing_coverage_row_renders_UNKNOWN_not_blank(self):
        now = datetime(2026, 9, 13, 11, 14, tzinfo=UTC)
        _measurement("coverage_pct", "1d", 0.9996, measured_at=now)
        _measurement("coverage_total", "1d", 13547, measured_at=now)
        body = build_digest(
            date(2026, 9, 13), [], previous_verdicts={}, coverage_rows=nightly_digest._coverage_rows(), sent_rows=[]
        )
        assert "1d" in body
        assert "1m" in body and "UNKNOWN" in body

    def test_build_never_raises_on_empty_ledger(self):
        body = build_digest(date(2026, 9, 13), [], previous_verdicts={}, coverage_rows=[], sent_rows=[])
        assert "Livewire digest — 2026-09-13" in body
        assert "UNKNOWN" in body or "no coverage" in body.lower()

    def test_status_block_renders_every_section(self):
        sections = _sections(Verdict.OK, Verdict.WARN)
        body = build_digest(date(2026, 9, 13), sections, previous_verdicts={}, coverage_rows=[], sent_rows=[])
        assert "STATUS" in body
        assert "[OK ] Check 0" in body
        assert "[WARN] Check 1" in body

    def test_sent_block_lists_notify_rows(self):
        rows = [
            {
                "started": datetime(2026, 9, 13, 10, 31, tzinfo=UTC),
                "exit_code": 0,
                "kind": "page",
                "subject": "PAGE 2026-09-13: Coverage recovery",
                "skipped": "false",
            },
            {
                "started": datetime(2026, 9, 13, 12, 1, tzinfo=UTC),
                "exit_code": 0,
                "kind": "page",
                "subject": "PAGE 2026-09-13: Coverage recovery",
                "skipped": "true",
            },
        ]
        body = build_digest(date(2026, 9, 13), [], previous_verdicts={}, coverage_rows=[], sent_rows=rows)
        assert "SENT to you" in body
        assert "PAGE 2026-09-13: Coverage recovery" in body
        assert "skipped" in body

    def test_changed_block_diffs_verdicts_against_the_previous_digest(self):
        sections = [
            Section("Coverage recovery", Verdict.BAD),
            Section("Silver failures", Verdict.WARN),
            Section("Daily update ran", Verdict.OK),
        ]
        previous = {"Coverage recovery": "WARN", "Silver failures": "WARN", "Daily update ran": "OK"}
        body = build_digest(date(2026, 9, 13), sections, previous_verdicts=previous, coverage_rows=[], sent_rows=[])
        assert "CHANGED" in body
        assert "Coverage recovery: WARN -> BAD" in body
        assert "Silver failures" not in body.split("CHANGED")[1].split("STATUS")[0]


class TestMain:
    def test_digest_is_sent_every_day_even_when_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: _sections(Verdict.OK))
        calls = []
        args = ["--run-date", "2026-09-13", "--email"]
        assert main(args, runner=_ok_runner(calls)) == 0
        assert main(args, runner=_ok_runner(calls)) == 0
        assert len(calls) == 2
        rows = ledger.query(
            "select json_extract_string(receipt_json,'$.skipped') as skipped, "
            "json_extract_string(receipt_json,'$.kind') as kind "
            "from executions where script = 'notify' order by started"
        )
        assert len(rows) == 2
        assert all(r["kind"] == "digest" and r["skipped"] == "false" for r in rows)

    def test_the_digest_row_carries_todays_verdicts(self, tmp_path, monkeypatch):
        sections = [Section("Daily update ran", Verdict.OK), Section("Coverage", Verdict.WARN)]
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: sections)
        assert main(["--run-date", "2026-09-13", "--email"], runner=_ok_runner([])) == 0
        row = ledger.query("select receipt_json from executions where script = 'notify'")[0]
        verdicts = json.loads(row["receipt_json"])["verdicts"]
        assert verdicts == {"Daily update ran": "OK", "Coverage": "WARN"}

    def test_previous_verdicts_come_from_the_last_digest_row(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: [Section("Coverage", Verdict.WARN)])
        assert main(["--run-date", "2026-09-12", "--email"], runner=_ok_runner([])) == 0
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: [Section("Coverage", Verdict.BAD)])
        assert main(["--run-date", "2026-09-13", "--email"], runner=_ok_runner([])) == 0
        body = capsys.readouterr().out
        assert "Coverage: WARN -> BAD" in body

    def test_sent_block_lists_todays_notify_rows(self, tmp_path, monkeypatch, capsys):
        from livewire_scripts import notify

        page = notify.page_for_lane(date(2026, 9, 13), "equity", 1, "boom", "tail")
        notify.send(page, runner=_ok_runner([]))
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: _sections(Verdict.OK))
        assert main(["--run-date", "2026-09-13", "--email"], runner=_ok_runner([])) == 0
        body = capsys.readouterr().out
        assert "SENT to you" in body
        assert "lane equity failed" in body

    def test_body_out_writes_the_body_and_sends_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: _sections(Verdict.BAD))
        out = tmp_path / "digest.txt"
        calls = []

        def runner(command, timeout=None):
            calls.append(command)
            raise AssertionError("send must not run for --body-out")

        assert main(["--run-date", "2026-09-13", "--body-out", str(out)], runner=runner) == 0
        assert "Livewire digest — 2026-09-13" in out.read_text(encoding="utf-8")
        assert calls == []
        assert ledger.query("select * from executions") == []

    def test_a_failed_send_is_the_exit_code_and_still_a_row(self, tmp_path, monkeypatch):
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: _sections(Verdict.OK))

        def runner(command, timeout=None):
            return CompletedProcess(command, 1, stdout="smtp refused")

        assert main(["--run-date", "2026-09-13", "--email"], runner=runner) == 1
        rows = ledger.query("select exit_code from executions where script = 'notify'")
        assert rows == [{"exit_code": 1}]

    def test_default_run_date_is_utc_today(self, tmp_path, monkeypatch, capsys):
        class FrozenDateTime:
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 4, 6, 1, 0, tzinfo=UTC)

        monkeypatch.setattr(nightly_digest, "datetime", FrozenDateTime, raising=False)
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: _sections(Verdict.OK))
        assert main([], runner=_ok_runner([])) == 0
        assert "Livewire digest — 2026-04-06" in capsys.readouterr().out


def test_the_tail_lane_is_recorded_in_the_ledger(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260902T060000Z-1")
    config = run_daily_update_job.RunnerConfig(
        warehouse_dir=tmp_path,
        log_dir=tmp_path,
        daily_update_script=tmp_path / "livewire_ingest.py",
        alert_script=tmp_path / "livewire_ops.py",
        python_bin="python",
        node_bin="node",
        max_attempts=1,
        retry_delay_seconds=0,
    )
    run_daily_update_job.run_post_success_quality(
        config,
        tmp_path / "daily_update_2026-09-02.log",
        runner=lambda *args, **kwargs: CompletedProcess([], 0),
    )
    assert ledger.query("select lane, outcome from lane_results where lane = 'tail'") == [
        {"lane": "tail", "outcome": "done"}
    ]
