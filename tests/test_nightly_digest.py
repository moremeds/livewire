"""Tests for livewire_scripts.nightly_digest — the unconditional daily digest."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from subprocess import CompletedProcess

import pytest

from clients import ledger
from livewire_scripts import nightly_digest, run_daily_update_job, status
from livewire_scripts.nightly_digest import build_digest, main, wait_for_coverage_fact
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
    # The send path waits on today's coverage fact (up to 4h real time);
    # tests that exercise send/render stub the gate open and TestWaitForCoverageFact
    # covers the wait itself.
    monkeypatch.setattr(nightly_digest, "wait_for_coverage_fact", lambda *a, **k: True, raising=False)


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


class _Clock:
    """Fake wall clock for the coverage-fact wait: now() reads it, sleep() advances it."""

    def __init__(self, start: datetime):
        self.t = start

    def now(self) -> datetime:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)


class TestWaitForCoverageFact:
    def test_digest_waits_for_todays_coverage_fact(self, tmp_path, monkeypatch):
        """Polls until a coverage_scan_ok OR coverage_skipped row dated today exists."""
        today = date.today()
        clock = _Clock(datetime.now(UTC))
        emitted = {"done": False}

        def sleep_then_emit(seconds: float) -> None:
            clock.sleep(seconds)
            if not emitted["done"]:
                _measurement("coverage_scan_ok", "all", 1, measured_at=clock.now())
                emitted["done"] = True

        assert wait_for_coverage_fact(today, poll_s=300, max_wait_s=3600, now_fn=clock.now, sleep_fn=sleep_then_emit)
        assert emitted["done"]

        # A skip row is also a fact — coverage deliberately stood down.
        skipped_day = today - timedelta(days=1)
        _measurement(
            "coverage_skipped",
            "session_not_due",
            1,
            measured_at=datetime.combine(skipped_day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=21),
        )
        assert wait_for_coverage_fact(skipped_day, poll_s=300, max_wait_s=3600, now_fn=clock.now, sleep_fn=clock.sleep)

    def test_wait_times_out_when_no_fact_lands(self, tmp_path, monkeypatch):
        clock = _Clock(datetime.now(UTC))
        assert not wait_for_coverage_fact(
            date.today(), poll_s=300, max_wait_s=0, now_fn=clock.now, sleep_fn=clock.sleep
        )

    def test_digest_after_max_wait_sends_anyway_with_a_first_line_saying_coverage_did_not_finish(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(nightly_digest, "collect", lambda *a, **k: _sections(Verdict.OK))
        monkeypatch.setattr(
            nightly_digest,
            "wait_for_coverage_fact",
            lambda d: wait_for_coverage_fact(d, poll_s=0, max_wait_s=0),
        )
        bodies = []
        monkeypatch.setattr(nightly_digest.notify, "send", lambda notice, **kw: bodies.append(notice.body) or 0)
        assert main(["--run-date", date.today().isoformat(), "--email"]) == 0
        assert len(bodies) == 1
        assert bodies[0].startswith("COVERAGE DID NOT FINISH TODAY (waited 4h)")
        assert "status below is as of" in bodies[0].splitlines()[0]
