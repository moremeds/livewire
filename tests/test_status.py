"""Tests for livewire_scripts/status.py — one reader, over the ledger."""

from __future__ import annotations

import collections
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from clients import ledger
from livewire_scripts import status
from livewire_scripts.status import (
    _LAUNCHD_JOBS,
    Section,
    Verdict,
    _disk_section,
    _launchd_section,
    main,
    render,
)

RUN = "daily-update-20260902T060000Z-1"
NOW = datetime.now(UTC)
EPOCH = date(1970, 1, 1)


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    return tmp_path / "ledger"


def _run(**over):
    ledger.emit(
        "runs",
        [
            {
                "run_id": RUN,
                "job": "daily-update",
                "host": "macmini",
                "release_sha": "deadbeef",
                "presets_sha": "p",
                "registry_sha": "r",
                "started": NOW,
                "ended": NOW,
                "exit_code": 0,
                "verdict": "OK",
            }
            | over
        ],
        run_id=RUN,
    )


def _lane(lane, **over):
    ledger.emit(
        "lane_results",
        [
            {
                "run_id": RUN,
                "lane": lane,
                "started": NOW,
                "ended": NOW,
                "exit_code": 0,
                "budget_s": 1800.0,
                "elapsed_s": 12.0,
                "outcome": "done",
                "blocker": None,
            }
            | over
        ],
        run_id=RUN,
    )


def _all_lanes(**overrides):
    for lane in ("futures", "cmdty", "cboe", "fx", "corporate-actions", "equity", "silver"):
        _lane(lane, **overrides.get(lane, {}))


def _last_session(scope, session: date):
    ledger.emit(
        "measurements",
        [
            {
                "name": "last_session",
                "scope": scope,
                "measured_at": NOW,
                "value": float((session - EPOCH).days),
                "unit": "epoch_days",
                "source": "measured",
                "run_id": RUN,
            }
        ],
        run_id=RUN,
    )


def _measurement(name, scope, value, *, measured_at=NOW):
    ledger.emit(
        "measurements",
        [
            {
                "name": name,
                "scope": scope,
                "measured_at": measured_at,
                "value": float(value),
                "unit": "ratio" if name == "coverage_pct" else "symbols",
                "source": "measured",
                "run_id": RUN,
            }
        ],
        run_id=RUN,
    )


def _section(name, **kw):
    sections = status.collect(
        date.today(),
        Path("/nonexistent"),
        Path("/nonexistent"),
        runner=_fake_launchctl,
        database=None,
        **kw,
    )
    return next(section for section in sections if section.name == name)


def _declared_or_measured(name, scope, value, source, run_id=RUN, unit="s", measured_at=None):
    ledger.emit(
        "measurements",
        [
            {
                "name": name,
                "scope": scope,
                "measured_at": measured_at or NOW,
                "value": float(value),
                "unit": unit,
                "source": source,
                "run_id": run_id,
            }
        ],
        run_id=run_id,
    )


def _seed_drift(name, scope, declared_value, measured_values, unit="s"):
    _declared_or_measured(name, scope, declared_value, "declared", unit=unit)
    for index, value in enumerate(measured_values):
        _declared_or_measured(name, scope, value, "measured", run_id=f"{RUN}-{index}", unit=unit)


_DRIFT = "Declared constants match reality"


def test_declared_vs_measured_warns_on_a_2x_drift():
    _seed_drift("lane_budget_s", "corporate-actions", 10800.0, [25000.0] * 5)
    assert _section(_DRIFT).verdict is Verdict.WARN


def test_declared_vs_measured_is_ok_within_2x():
    _seed_drift("lane_budget_s", "corporate-actions", 10800.0, [9000.0] * 5)
    assert _section(_DRIFT).verdict is Verdict.OK


def test_declared_vs_measured_is_unknown_when_a_lane_stopped_running():
    # a declared lane budget with no elapsed_s in the window = that lane did not run
    _seed_drift("lane_budget_s", "cmdty", 1800.0, [])
    assert _section(_DRIFT).verdict is Verdict.UNKNOWN


def test_declared_vs_measured_reports_the_worst_drift_first():
    _seed_drift("lane_budget_s", "equity", 7200.0, [7000.0] * 5)
    _seed_drift("lane_budget_s", "corporate-actions", 10800.0, [25000.0] * 5)
    section = _section(_DRIFT)
    assert section.verdict is Verdict.WARN
    assert "corporate-actions" in section.lines[1]


def test_declared_vs_measured_ignores_a_threshold_with_no_measurable_counterpart():
    _seed_drift("failure_rate_tolerance", "", 0.05, [], unit="ratio")
    _seed_drift("lane_budget_s", "equity", 7200.0, [7000.0] * 5)
    assert _section(_DRIFT).verdict is Verdict.OK


def test_declared_vs_measured_ignores_the_default_lane_budget_fallback():
    # no lane is named 'default', so it can never have a measured counterpart
    _seed_drift("lane_budget_s", "default", 1800.0, [])
    _seed_drift("lane_budget_s", "equity", 7200.0, [7000.0] * 5)
    assert _section(_DRIFT).verdict is Verdict.OK


def test_declared_vs_measured_ignores_rows_older_than_the_window():
    _declared_or_measured("lane_budget_s", "equity", 7200.0, "declared")
    _declared_or_measured("lane_budget_s", "equity", 7000.0, "measured", measured_at=NOW - timedelta(days=15))
    assert _section(_DRIFT).verdict is Verdict.UNKNOWN


def test_the_latest_declared_value_is_the_one_graded():
    # the constant was lowered; max() would keep grading against the old, higher one
    _declared_or_measured("lane_budget_s", "equity", 30000.0, "declared", measured_at=NOW - timedelta(days=3))
    _declared_or_measured("lane_budget_s", "equity", 7200.0, "declared")
    for index in range(5):
        _declared_or_measured("lane_budget_s", "equity", 25000.0, "measured", run_id=f"{RUN}-{index}")
    assert _section(_DRIFT).verdict is Verdict.WARN


def test_no_run_row_at_all_is_unknown_not_ok():
    assert _section("Daily update ran").verdict is Verdict.UNKNOWN


def test_a_run_today_is_ok():
    _run()
    assert _section("Daily update ran").verdict is Verdict.OK


def test_a_failed_run_is_bad():
    _run(verdict="FAILED", exit_code=1)
    assert _section("Daily update ran").verdict is Verdict.BAD


def test_a_degraded_run_is_warn():
    _run(verdict="DEGRADED")
    assert _section("Daily update ran").verdict is Verdict.WARN


def test_a_failed_intraday_catchup_is_bad():
    _run(job="intraday-catchup", verdict="FAILED", exit_code=1)
    assert _section("Intraday catch-up ran").verdict is Verdict.BAD


def test_a_closed_run_reads_finished():
    _run()
    assert _section("Daily update finished").verdict is Verdict.OK


def test_a_run_still_open_at_watchdog_time_warns_and_grades_no_lane_bad():
    _run(ended=None, exit_code=None, verdict=None)
    _lane("equity", outcome=None, ended=None, exit_code=None)
    finished = _section("Daily update finished")
    assert finished.verdict is Verdict.WARN
    assert "running_minutes" in "\n".join(finished.lines)
    for name in ("Lanes terminal", "Silver advanced", "Lanes within budget"):
        assert _section(name).verdict is Verdict.UNKNOWN


def test_a_lane_with_no_terminal_row_is_bad():
    _run()
    _lane("equity", outcome=None, ended=None, exit_code=None)
    assert _section("Lanes terminal").verdict is Verdict.BAD


def test_every_lane_terminal_is_ok():
    _run()
    _all_lanes()
    assert _section("Lanes terminal").verdict is Verdict.OK


def test_silver_that_did_not_run_is_unknown():
    _run()
    _lane("equity")
    assert _section("Silver advanced").verdict is Verdict.UNKNOWN


def test_silver_blocked_is_bad():
    _run()
    _lane("silver", outcome="blocked", blocker="equity", exit_code=None)
    assert _section("Silver advanced").verdict is Verdict.BAD


def test_a_failed_digest_is_bad():
    _run()
    _lane("digest", outcome="failed", exit_code=2)
    assert _section("Post-success tail").verdict is Verdict.BAD


def test_any_undelivered_alert_is_a_warning():
    _run()
    ledger.emit(
        "executions",
        [
            {
                "evidence_hash": None,
                "script": "send_alert",
                "attempt": 1,
                "args_json": "{}",
                "release_sha": "deadbeef",
                "started": NOW,
                "ended": NOW,
                "exit_code": 3,
                "receipt_json": "{}",
                "run_id": RUN,
            }
        ],
        run_id=RUN,
    )
    section = _section("Undelivered alerts")
    assert section.verdict is Verdict.WARN
    assert "send_alert" in "\n".join(section.lines)


def test_a_delivered_alert_is_ok():
    _run()
    assert _section("Undelivered alerts").verdict is Verdict.OK


def test_an_ib_phase_at_86_reads_degraded_not_failed():
    _run(verdict="DEGRADED")
    _all_lanes(futures={"exit_code": 86, "outcome": "blocked", "blocker": "ib_unreachable"})
    assert _section("Lanes terminal").verdict is Verdict.OK
    assert _section("Release matches main", main_sha="deadbeef").verdict is Verdict.OK


def test_a_release_behind_main_is_bad():
    _run()
    assert _section("Release matches main", main_sha="0ther").verdict is Verdict.BAD


def test_no_main_sha_supplied_is_unknown_never_green():
    _run()
    assert _section("Release matches main").verdict is Verdict.UNKNOWN


def test_a_lane_over_its_budget_warns():
    _run()
    _lane("corporate-actions", budget_s=10800.0, elapsed_s=31140.0, outcome="timeout", exit_code=124)
    section = _section("Lanes within budget")
    assert section.verdict is Verdict.WARN
    assert "corporate-actions" in "\n".join(section.lines)


def test_a_lane_inside_its_budget_is_ok():
    _run()
    _lane("cboe")
    assert _section("Lanes within budget").verdict is Verdict.OK


def test_an_ib_only_lane_days_behind_warns_and_names_its_blocker():
    _run()
    _lane("futures", exit_code=86, outcome="blocked", blocker="ib_unreachable")
    _last_session("futures", date.today() - timedelta(days=9))
    _last_session("cmdty", date.today() - timedelta(days=1))
    section = _section("IB-only lanes behind")
    assert section.verdict is Verdict.WARN
    body = "\n".join(section.lines)
    assert "futures" in body and "ib_unreachable" in body


def test_an_ib_only_lane_current_is_ok():
    _run()
    _lane("futures")
    _last_session("futures", date.today() - timedelta(days=1))
    _last_session("cmdty", date.today() - timedelta(days=1))
    assert _section("IB-only lanes behind").verdict is Verdict.OK


def test_a_weekend_gap_is_not_a_backlog():
    _run()
    _lane("futures")
    _last_session("futures", date.today() - timedelta(days=3))
    _last_session("cmdty", date.today() - timedelta(days=3))
    assert _section("IB-only lanes behind").verdict is Verdict.OK


def test_an_ib_only_lane_that_never_reported_a_session_is_unknown():
    _run()
    _lane("futures")
    assert _section("IB-only lanes behind").verdict is Verdict.UNKNOWN


def test_one_missing_ib_only_lane_is_unknown_not_green():
    _last_session("futures", date.today())
    assert _section("IB-only lanes behind").verdict is Verdict.UNKNOWN


def test_coverage_below_threshold_is_bad():
    for timeframe in ("1d", "1m", "1h", "5m", "30m"):
        _measurement("coverage_pct", timeframe, 0.90 if timeframe == "1d" else 1.0)
        _measurement("coverage_total", timeframe, 100)
    assert _section("Coverage").verdict is Verdict.BAD


def test_fresh_complete_coverage_is_ok():
    for timeframe in ("1d", "1m", "1h", "5m", "30m"):
        _measurement("coverage_pct", timeframe, 1.0)
        _measurement("coverage_total", timeframe, 100)
    assert _section("Coverage").verdict is Verdict.OK


def test_missing_coverage_timeframe_is_unknown():
    _measurement("coverage_pct", "1d", 1.0)
    assert _section("Coverage").verdict is Verdict.UNKNOWN


def test_zero_total_coverage_is_unknown_not_green():
    for timeframe in ("1d", "1m", "1h", "5m", "30m"):
        _measurement("coverage_pct", timeframe, 1.0)
        _measurement("coverage_total", timeframe, 0)
    assert _section("Coverage").verdict is Verdict.UNKNOWN


def test_failed_coverage_scan_warns():
    _measurement("coverage_scan_ok", "all", 0)
    assert _section("Coverage scan").verdict is Verdict.WARN


def test_silver_window_regressions_warn():
    _measurement("silver_window_regressions", "silver", 2)
    assert _section("Silver window regressions").verdict is Verdict.WARN


def _silver_failed(value: float, at: datetime):
    ledger.emit(
        "measurements",
        [
            {
                "name": "silver_failed",
                "scope": "silver",
                "measured_at": at,
                "value": value,
                "unit": "symbols",
                "source": "measured",
                "run_id": RUN,
            }
        ],
        run_id=RUN,
    )


def test_one_silver_measurement_is_not_a_change():
    _run()
    _silver_failed(4.0, NOW)
    assert _section("Silver failures did not grow").verdict is Verdict.UNKNOWN


def test_growing_silver_failures_warn():
    _run()
    _silver_failed(4.0, NOW - timedelta(days=1))
    _silver_failed(9.0, NOW)
    section = _section("Silver failures did not grow")
    assert section.verdict is Verdict.WARN
    assert "9" in "\n".join(section.lines)


def test_shrinking_silver_failures_are_ok():
    _run()
    _silver_failed(9.0, NOW - timedelta(days=1))
    _silver_failed(4.0, NOW)
    assert _section("Silver failures did not grow").verdict is Verdict.OK


def test_a_broken_check_never_takes_the_report_down(monkeypatch):
    monkeypatch.setattr(status.ledger, "query", lambda sql: (_ for _ in ()).throw(RuntimeError("boom")))
    sections = status.collect(date.today(), Path("/nonexistent"), Path("/nonexistent"), runner=_fake_launchctl)
    assert any(section.verdict is Verdict.UNKNOWN for section in sections)


def test_every_check_is_a_name_and_a_select():
    assert status.CHECKS
    assert all(sql.strip().lower().startswith("select") for _, sql in status.CHECKS)


def test_section_is_frozen() -> None:
    section = Section(name="x", verdict=Verdict.OK, lines=["x"])
    assert section.fix is None


def test_unknown_outranks_ok_so_a_run_verdict_can_never_be_green_on_a_gap() -> None:
    assert max(Verdict.OK, Verdict.UNKNOWN) is Verdict.UNKNOWN
    assert max(Verdict.OK, Verdict.UNKNOWN, Verdict.WARN, Verdict.BAD) is Verdict.BAD


_Usage = collections.namedtuple("_Usage", "total used free")
_GIB = 1024**3


class TestTheDiskCheckWatchesBothVolumes:
    @staticmethod
    def _dirs(tmp_path):
        lake = tmp_path / "lake"
        warehouse = tmp_path / "warehouse"
        lake.mkdir()
        warehouse.mkdir()
        return lake, warehouse

    def test_a_full_warehouse_volume_warns_even_when_the_lake_is_empty(self, tmp_path, monkeypatch):
        lake, warehouse = self._dirs(tmp_path)

        def fake_usage(path):
            if Path(path) == lake:
                return _Usage(13_000 * _GIB, 6_400 * _GIB, 6_600 * _GIB)
            return _Usage(228 * _GIB, 214 * _GIB, 14 * _GIB)

        monkeypatch.setattr(status.shutil, "disk_usage", fake_usage)
        section = _disk_section(lake, warehouse)
        text = "\n".join(section.lines)
        assert "6600.0 GiB" in text and "14.0 GiB" in text and "⚠" in text
        assert section.verdict is Verdict.BAD

    def test_both_healthy_does_not_warn(self, tmp_path, monkeypatch):
        lake, warehouse = self._dirs(tmp_path)
        monkeypatch.setattr(
            status.shutil,
            "disk_usage",
            lambda path: _Usage(13_000 * _GIB, 6_400 * _GIB, 6_600 * _GIB),
        )
        section = _disk_section(lake, warehouse)
        assert "⚠" not in "\n".join(section.lines)
        assert section.verdict is Verdict.OK

    def test_one_volume_reports_once_when_both_paths_share_it(self, tmp_path, monkeypatch):
        shared = tmp_path / "everything"
        shared.mkdir()
        monkeypatch.setattr(status.shutil, "disk_usage", lambda path: _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB))
        assert len([line for line in _disk_section(shared, shared).lines if line.startswith("Disk")]) == 1

    def test_an_unreadable_path_is_skipped_not_fatal(self, tmp_path, monkeypatch):
        lake, warehouse = self._dirs(tmp_path)

        def fake_usage(path):
            if Path(path) == lake:
                raise OSError("volume not mounted")
            return _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB)

        monkeypatch.setattr(status.shutil, "disk_usage", fake_usage)
        lines = _disk_section(lake, warehouse).lines
        assert len([line for line in lines if line.startswith("Disk")]) == 1
        assert "128.0 GiB" in lines[0]


def test_render_shows_the_fix_for_anything_not_ok() -> None:
    out = render([Section(name="Coverage", verdict=Verdict.BAD, lines=["Coverage:"], fix="run me")])
    assert "run me" in out and "BAD" in out


def test_render_omits_the_fix_when_ok() -> None:
    out = render([Section(name="Disk", verdict=Verdict.OK, lines=["Disk: fine"], fix="run me")])
    assert "run me" not in out


def test_render_names_a_section_that_produced_no_lines() -> None:
    assert "Coverage" in render([Section(name="Coverage", verdict=Verdict.UNKNOWN)])


def test_render_survives_markup_in_log_derived_text(capsys) -> None:
    from rich.console import Console

    hostile = Section(
        name="Quality jobs",
        verdict=Verdict.WARN,
        lines=["Quality jobs: 1 FAILED", "  coverage failed: timed out [/] after [bold red]1800[/bold red]s"],
    )
    Console().print(render([hostile]))
    out = capsys.readouterr().out
    assert "[/]" in out and "1800" in out


def _no_catalog(_db):
    raise FileNotFoundError("analytics.duckdb")


def _fake_launchctl(_cmd, **_kw):
    return SimpleNamespace(stdout="".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS), returncode=0)


def test_main_exits_zero_even_when_everything_is_broken(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.setattr("livewire_scripts.status.subprocess.run", _fake_launchctl)
    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _no_catalog)
    rc = main(["--run-date", date.today().isoformat(), "--log-dir", str(tmp_path), "--data-lake", str(tmp_path)])
    assert rc == 0
    assert "Livewire status" in capsys.readouterr().out


def test_a_job_that_is_not_loaded_is_bad() -> None:
    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout="-\t0\tcom.livewire.daily-update\n", returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.BAD
    assert "com.livewire.coverage" in "\n".join(section.lines)


def test_a_nonzero_exit_is_printed_but_never_graded() -> None:
    stdout = "".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS)
    stdout = stdout.replace("-\t0\tcom.livewire.intraday-catchup", "-\t86\tcom.livewire.intraday-catchup")

    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout=stdout, returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.OK
    rendered = "\n".join(section.lines)
    assert "86" in rendered and "no timestamp" in rendered


def test_all_jobs_green_is_ok() -> None:
    assert _launchd_section(runner=_fake_launchctl).verdict is Verdict.OK


def test_launchctl_missing_is_unknown() -> None:
    def _runner(_cmd, **_kw):
        raise FileNotFoundError("launchctl")

    assert _launchd_section(runner=_runner).verdict is Verdict.UNKNOWN
