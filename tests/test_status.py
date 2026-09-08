"""Tests for livewire_scripts/status.py — one reader, over the ledger."""

from __future__ import annotations

import collections
import importlib
import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from clients import constants, ledger
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
INTRADAY_RUN = "intraday-catchup-20260902T100000Z-1"
NOW = datetime.now(UTC)
EPOCH = date(1970, 1, 1)


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("MDW_DUCKDB_PATH", str(tmp_path / "analytics.duckdb"))
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
    """Seeded from the declared lane order -- the last hand-written copy of it (spec section 7)."""
    for lane in constants.LANE_ORDER:
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


def _committed_silver(data_lake: Path, revision: int = 7) -> Path:
    revisions = data_lake / "silver" / "revisions"
    revisions.mkdir(parents=True)
    payload = {
        "schema_version": 1,
        "revision": revision,
        "generation_id": f"20260902T060000Z-{revision}",
        "published_at": "2026-09-02T06:00:00Z",
        "corporate_actions_as_of": "2026-09-02T05:00:00Z",
        "affected": [],
        "artifacts": [],
    }
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (revisions / f"revision={revision}.json").write_bytes(encoded)
    (revisions / "current.json").write_bytes(encoded)
    return data_lake


def _section(name, **kw):
    sections = status.collect(
        NOW.date(),
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


@pytest.mark.parametrize("lane", ["catalog", "daily_backfill_duckdb_coverage"])
@pytest.mark.parametrize("outcome, verdict", [("failed", Verdict.BAD), ("blocked", Verdict.WARN), ("done", Verdict.OK)])
def test_catalog_build_reads_terminal_lane_before_run_closes(lane, outcome, verdict):
    _run(started=NOW - timedelta(hours=2), ended=NOW - timedelta(hours=1))
    _lane("catalog", started=NOW - timedelta(hours=2), ended=NOW - timedelta(hours=1))
    _run(run_id="current-open", ended=None, verdict=None, exit_code=None)
    _lane(lane, run_id="current-open", outcome=outcome, exit_code=7 if outcome == "failed" else 0)

    section = _section("Catalog build")
    assert section.verdict == verdict
    assert "current-open" in " ".join(section.lines)


def test_catalog_build_without_evidence_is_unknown():
    assert _section("Catalog build").verdict == Verdict.UNKNOWN


def test_silver_publication_keeps_the_committed_revision_separate_from_a_failed_attempt(tmp_path):
    lake = _committed_silver(tmp_path / "lake")
    _lane("silver", outcome="failed", exit_code=1, blocker="RJF/1d validation")

    section = status._silver_publication_section(lake)

    assert section.verdict is Verdict.BAD
    body = "\n".join(section.lines)
    for field in ("Impact:", "Evidence:", "Last valid:", "Automatic handling:", "Next action:", "Clear condition:"):
        assert field in body
    assert "revision=7" in body and "latest rebuild attempt failed" in body and "Sustained incident: attempts=1" in body
    assert "RJF/1d validation" in section.notification_key
    assert "healthy subset may have advanced current.json" in body
    assert "failed attempt did not replace" not in body
    assert "artifact hashes were not checked by status" in body
    assert "Attempt linkage: unknown" in body


def test_silver_publication_done_is_a_commit_fact_not_a_reader_run(tmp_path):
    lake = _committed_silver(tmp_path / "lake")
    _lane("silver")

    section = status._silver_publication_section(lake)

    assert section.verdict is Verdict.OK
    assert "not proof that a consumer has run on it" in "\n".join(section.lines)


@pytest.mark.parametrize("latest_done", [False, True])
def test_silver_incident_starts_after_previous_success(tmp_path, latest_done):
    lake = _committed_silver(tmp_path / "lake")
    for hours, outcome in [(5, "failed"), (4, "done"), (3, "failed"), (2, "failed")]:
        when = NOW - timedelta(hours=hours)
        _lane("silver", started=when, ended=when, outcome=outcome)
    if latest_done:
        _lane("silver", started=NOW, ended=NOW)
    body = "\n".join(status._silver_publication_section(lake).lines)
    assert ("2 prior non-success attempt(s)" if latest_done else "Sustained incident: attempts=2") in body
    assert (NOW - timedelta(hours=5)).isoformat() not in body


def test_silver_manifest_reference_does_not_claim_artifact_hash_validation(tmp_path):
    lake = _committed_silver(tmp_path / "lake")
    revisions = lake / "silver" / "revisions"
    payload = json.loads((revisions / "current.json").read_text())
    payload["artifacts"] = [
        {"path": "generations/example/asset_class=equity/symbol=AAPL/1d.parquet", "sha256": "0" * 64}
    ]
    encoded = json.dumps(payload).encode()
    (revisions / "current.json").write_bytes(encoded)
    (revisions / "revision=7.json").write_bytes(encoded)
    _lane("silver", outcome="failed", exit_code=1)
    section = status._silver_publication_section(lake)
    body = "\n".join(section.lines)
    assert "Current manifest reference: revision=7" in body
    assert "artifact hashes were not checked" in body
    assert "data snapshot unknown" in body


def test_silver_notification_tracks_measured_failure_scope_not_occurrence_count(tmp_path):
    lake = _committed_silver(tmp_path / "lake")
    _lane("silver", outcome="failed", exit_code=1)
    _measurement("silver_failed", "silver", 2)
    _measurement("silver_window_regressions", "silver", 1)
    first = status._silver_publication_section(lake)
    _lane("silver", outcome="failed", started=NOW + timedelta(seconds=1), ended=NOW + timedelta(seconds=2))
    _measurement("silver_failed", "silver", 2, measured_at=NOW + timedelta(seconds=3))
    _measurement("silver_window_regressions", "silver", 1, measured_at=NOW + timedelta(seconds=3))
    same = status._silver_publication_section(lake)
    assert same.notification_key == first.notification_key
    _measurement("silver_failed", "silver", 20, measured_at=NOW + timedelta(seconds=4))
    changed = status._silver_publication_section(lake)
    assert changed.notification_key != first.notification_key
    assert "silver_failed=20.0" in "\n".join(changed.lines)


def test_generic_warning_identity_uses_scope_and_impact_not_run_chronology(monkeypatch):
    rows = [
        {
            "verdict": "WARN",
            "lane": "silver",
            "blocker": "validation",
            "failed_now": 2,
            "run_id": "one",
            "failed_sends": 1,
        }
    ]
    monkeypatch.setattr(ledger, "query", lambda _sql: rows)
    first = status.run_check("Example", "select 1", {})
    rows[0].update(run_id="two", failed_sends=40, running_minutes=90, started=NOW, failed_before=10)
    same = status.run_check("Example", "select 1", {})
    assert same.notification_key == first.notification_key
    rows[0]["lane"] = "equity"
    scope_changed = status.run_check("Example", "select 1", {})
    assert scope_changed.notification_key != first.notification_key
    rows[0]["failed_now"] = 3
    assert status.run_check("Example", "select 1", {}).notification_key != scope_changed.notification_key
    for label in ("Impact:", "Evidence:", "Last valid:", "Automatic handling:", "Next action:", "Clear condition:"):
        assert label in "\n".join(first.lines)


def _seed_drift(name, scope, declared_value, measured_values, unit="s"):
    _declared_or_measured(name, scope, declared_value, "declared", unit=unit)
    for index, value in enumerate(measured_values):
        _declared_or_measured(name, scope, value, "measured", run_id=f"{RUN}-{index}", unit=unit)


def _declared(name, scope, value):
    _declared_or_measured(name, scope, value, "declared")


def _measured(name, scope, value):
    _declared_or_measured(name, scope, value, "measured")


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
    for name in ("Lanes terminal", "Silver lane completed", "Lanes within budget"):
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
    assert _section("Silver lane completed").verdict is Verdict.UNKNOWN


def test_silver_blocked_is_bad():
    _run()
    _lane("silver", outcome="blocked", blocker="equity", exit_code=None)
    assert _section("Silver lane completed").verdict is Verdict.BAD


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
    _last_session("futures", NOW.date() - timedelta(days=9))
    _last_session("cmdty", NOW.date() - timedelta(days=1))
    section = _section("IB-only lanes behind")
    assert section.verdict is Verdict.WARN
    body = "\n".join(section.lines)
    assert "futures" in body and "ib_unreachable" in body


def test_an_ib_only_lane_current_is_ok():
    _run()
    _lane("futures")
    _last_session("futures", NOW.date() - timedelta(days=1))
    _last_session("cmdty", NOW.date() - timedelta(days=1))
    assert _section("IB-only lanes behind").verdict is Verdict.OK


def test_a_weekend_gap_is_not_a_backlog():
    _run()
    _lane("futures")
    _last_session("futures", NOW.date() - timedelta(days=3))
    _last_session("cmdty", NOW.date() - timedelta(days=3))
    assert _section("IB-only lanes behind").verdict is Verdict.OK


def test_an_ib_only_lane_that_never_reported_a_session_is_unknown():
    _run()
    _lane("futures")
    assert _section("IB-only lanes behind").verdict is Verdict.UNKNOWN


def test_one_missing_ib_only_lane_is_unknown_not_green():
    _last_session("futures", NOW.date())
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


def test_no_silver_measurement_is_unknown():
    _run()
    assert _section("Silver failures").verdict is Verdict.UNKNOWN


def test_one_positive_silver_measurement_warns():
    _run()
    _silver_failed(4.0, NOW)
    assert _section("Silver failures").verdict is Verdict.WARN


def test_growing_silver_failures_warn():
    _run()
    _silver_failed(4.0, NOW - timedelta(days=1))
    _silver_failed(9.0, NOW)
    section = _section("Silver failures")
    assert section.verdict is Verdict.WARN
    assert "9" in "\n".join(section.lines)


@pytest.mark.parametrize("remaining", [4.0, 9.0])
def test_remaining_silver_failures_warn_even_when_not_growing(remaining):
    _run()
    _silver_failed(9.0, NOW - timedelta(days=1))
    _silver_failed(remaining, NOW)
    assert _section("Silver failures").verdict is Verdict.WARN


def test_zero_silver_failures_is_ok():
    _run()
    _silver_failed(0.0, NOW)
    assert _section("Silver failures").verdict is Verdict.OK


def test_a_broken_check_never_takes_the_report_down(monkeypatch):
    monkeypatch.setattr(status.ledger, "query", lambda sql: (_ for _ in ()).throw(RuntimeError("boom")))
    sections = status.collect(NOW.date(), Path("/nonexistent"), Path("/nonexistent"), runner=_fake_launchctl)
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


def test_catalog_expectations_match_the_declared_sources():
    from clients.duckdb_catalog import COVERAGE_SOURCES

    assert set(status._CATALOG_LANE_FIX) == {name for name, _date_column in COVERAGE_SOURCES}


@pytest.mark.parametrize("missing", ["bronze_equity_1d", "silver_equity_1d", "bronze_rates_1d"])
def test_one_missing_catalog_view_cannot_be_hidden_by_current_other_views(monkeypatch, missing):
    headline = {name: (10, NOW.date()) for name in status._CATALOG_LANE_FIX if name != missing}
    monkeypatch.setattr(status, "_coverage_headline", lambda _db: headline)

    section = status._duckdb_section(NOW.date())

    assert section.verdict is status.Verdict.BAD
    assert missing in "\n".join(section.lines)
    assert "duckdb build" in section.fix


@pytest.mark.parametrize("entry", [(0, NOW.date()), (10, None)])
def test_empty_or_undated_catalog_view_is_incomplete(monkeypatch, entry):
    headline = {name: (10, NOW.date()) for name in status._CATALOG_LANE_FIX}
    headline["bronze_equity_1d"] = entry
    monkeypatch.setattr(status, "_coverage_headline", lambda _db: headline)

    assert status._duckdb_section(NOW.date()).verdict is status.Verdict.BAD


def test_complete_current_catalog_is_ok(monkeypatch):
    headline = {name: (10, NOW.date()) for name in status._CATALOG_LANE_FIX}
    monkeypatch.setattr(status, "_coverage_headline", lambda _db: headline)

    assert status._duckdb_section(NOW.date()).verdict is status.Verdict.OK


@pytest.mark.parametrize("target", [date(2026, 9, 5), date(2026, 9, 6), date(2026, 9, 7)])
def test_catalog_does_not_count_weekends_or_labor_day_as_missing_sessions(target):
    assert status._sessions_behind(date(2026, 9, 4), target) == 0


def _fake_launchctl(_cmd, **_kw):
    return SimpleNamespace(stdout="".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS), returncode=0)


def test_main_exits_zero_even_when_everything_is_broken(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.setattr("livewire_scripts.status.subprocess.run", _fake_launchctl)
    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _no_catalog)
    rc = main(["--run-date", NOW.date().isoformat(), "--log-dir", str(tmp_path), "--data-lake", str(tmp_path)])
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


def test_adding_a_lane_makes_it_appear_in_the_lanes_terminal_check(monkeypatch):
    """The graded surface enumerates LANE_ORDER, not its own copy of the list."""
    monkeypatch.setattr(constants, "LANE_ORDER", (*constants.LANE_ORDER, "options"))
    try:
        reloaded = importlib.reload(status)
        sql = dict(reloaded.CHECKS)["Lanes terminal"]
        assert "('options')" in sql
    finally:
        monkeypatch.undo()
        importlib.reload(status)


def test_the_ib_only_check_counts_exactly_the_ib_only_lanes():
    sql = dict(status.CHECKS)["IB-only lanes behind"]

    assert status._lane_values(constants.IB_ONLY_LANES) in sql
    assert f"count(last_session) < {len(constants.IB_ONLY_LANES)}" in sql


def test_a_lane_deferred_by_the_lake_lock_is_a_warning():
    _run()
    _all_lanes()
    _lane("corporate-actions", outcome="blocked", exit_code=None, blocker="lake_lock", elapsed_s=0.0)

    section = _section("Lanes blocked")
    assert section.verdict is status.Verdict.WARN
    assert "corporate-actions" in " ".join(section.lines)


def test_a_run_started_before_midnight_utc_resolves_from_a_host_east_of_utc():
    """`$today` is UTC; DuckDB must evaluate `date(started)` in UTC too.

    The mini runs Asia/Hong_Kong. A run started 2026-09-06 20:52:01Z is stored
    `2026-09-07 04:52:01+08:00`, and a session-local `date(started)` read it as
    the 7th while status asked for the 6th -- so `_last_run_id` returned '' and
    every run-scoped check went UNKNOWN (macmini dry run, 2026-09-06 20:52Z).
    """
    if not hasattr(time, "tzset"):
        pytest.skip("no time.tzset on this platform")
    started = datetime(2026, 9, 6, 20, 52, 1, tzinfo=UTC)
    _run(started=started, ended=started)
    _all_lanes()

    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Hong_Kong"
    time.tzset()
    try:
        sections = status.collect(
            date(2026, 9, 6),
            Path("/nonexistent"),
            Path("/nonexistent"),
            runner=_fake_launchctl,
            database=None,
        )
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()

    terminal = next(section for section in sections if section.name == "Lanes terminal")
    assert terminal.verdict is status.Verdict.OK


def test_lanes_blocked_is_unknown_when_no_run_resolved():
    """An unresolved run measured nothing; `none` would be a green lie."""
    assert _section("Lanes blocked").verdict is status.Verdict.UNKNOWN


def test_no_lane_deferred_by_the_lake_lock_is_ok_not_unknown():
    """Zero blocked lanes on a resolved run is the good case."""
    _run()
    _all_lanes()

    assert _section("Lanes blocked").verdict is status.Verdict.OK


def test_an_intraday_lane_deferred_by_the_lake_lock_is_a_warning():
    """Both scheduled jobs write the lake, so both can be the deferred one.

    Scoping the check to the daily run alone hid every intraday phase that lost
    the lock -- the exact contention this lock was added for.
    """
    _run()
    _all_lanes()
    _run(run_id=INTRADAY_RUN, job="intraday-catchup", ended=None)
    _lane("equity", run_id=INTRADAY_RUN, outcome="blocked", exit_code=None, blocker="lake_lock", elapsed_s=0.0)

    section = _section("Lanes blocked")
    assert section.verdict is status.Verdict.WARN
    assert "intraday-catchup:equity" in " ".join(section.lines)


def test_a_silver_lane_blocked_by_a_failed_prerequisite_is_not_a_lake_lock_row():
    """`blocked` is also how a failed prerequisite reads; the blocker is the discriminator."""
    _run()
    _all_lanes()
    _lane("silver", outcome="blocked", exit_code=None, blocker="equity", elapsed_s=0.0)

    assert _section("Lanes blocked").verdict is status.Verdict.OK


def test_the_declared_lake_lock_wait_is_graded_against_the_measured_p95():
    _run()
    _declared("lake_lock_wait_s", "", 2340.0)
    for lane in ("corporate-actions", "equity"):
        _measured("lake_lock_wait_s", lane, 30.0)

    section = _section("Declared constants match reality")
    assert section.verdict is status.Verdict.WARN
    assert "lake_lock_wait_s" in " ".join(section.lines)


def test_a_lake_lock_wait_near_the_declared_value_is_ok():
    _run()
    _declared("lake_lock_wait_s", "", 2340.0)
    for lane in ("corporate-actions", "equity"):
        _measured("lake_lock_wait_s", lane, 2000.0)

    assert _section("Declared constants match reality").verdict is status.Verdict.OK


def test_the_lane_budget_drift_check_still_works():
    """Widening the filter must not stop it grading the thing it was written for."""
    _run()
    _declared("lane_budget_s", "corporate-actions", 10800.0)
    _measured("lane_budget_s", "corporate-actions", 100.0)

    assert _section("Declared constants match reality").verdict is status.Verdict.WARN


def test_silver_progress_reports_the_heartbeat_and_is_unknown_without_one():
    """`rebuild-silver --full` walks hours; the ledger says how far it got.

    Graded through `status.Verdict`, not the name imported at module scope: an
    earlier test in this file reloads the module, so the top-level `Verdict` is a
    different class object by the time this runs and `is` would always fail.
    """
    _run()
    assert _section("Silver progress").verdict is status.Verdict.UNKNOWN

    _measurement("progress", "silver", 500)
    _measurement("progress_total", "silver", 13311)
    section = _section("Silver progress")
    assert section.verdict is status.Verdict.OK
    assert "symbols=500.0" in section.lines[1]
    assert "universe=13311.0" in section.lines[1]
