"""Tests for livewire_scripts/status.py — one reader, over the ledger."""

from __future__ import annotations

import collections
import hashlib
import importlib
import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from clients import constants, ledger
from livewire_scripts import notify, status
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
# An "earlier today" row that cannot cross the UTC day boundary the way
# `NOW - timedelta(hours=n)` does when the suite runs just after midnight.
EARLIER_TODAY = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
EPOCH = date(1970, 1, 1)


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("MDW_DUCKDB_PATH", str(tmp_path / "analytics.duckdb"))
    # The Disk check resolves every configured destination through env, so the
    # resolvers must land under tmp_path — otherwise tests read the operator's
    # real warehouse paths. Only the roots are created; children stay absent so
    # tests decide which destinations exist.
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
    monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))
    # Independently configured, so it must be isolated independently: an
    # inherited MDW_SILVER_DIR would point every section at the operator's real
    # silver root instead of the fixture's.
    monkeypatch.setenv("MDW_SILVER_DIR", str(tmp_path / "lake" / "silver"))
    monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("MDW_CURSOR_DIR", str(tmp_path / "cursors"))
    for name in ("ledger", "warehouse", "lake", "logs", "cursors"):
        (tmp_path / name).mkdir(exist_ok=True)
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


def _execution(script, exit_code, *, receipt=None, started=NOW):
    ledger.emit(
        "executions",
        [
            {
                "evidence_hash": None,
                "script": script,
                "attempt": 1,
                "args_json": "{}",
                "release_sha": "deadbeef",
                "started": started,
                "ended": started,
                "exit_code": exit_code,
                "receipt_json": json.dumps(receipt or {}),
                "run_id": RUN,
            }
        ],
        run_id=RUN,
    )


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
    assert (
        "revision=7" in body
        and "latest terminal lane attempt failed" in body
        and "Sustained incident: attempts=1" in body
    )
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
    assert _section("Daily update ran", now=NOW.replace(hour=5, minute=30)).verdict is Verdict.UNKNOWN


def test_daily_update_absent_after_deadline_is_bad():
    assert _section("Daily update ran", now=NOW.replace(hour=7, minute=0, second=30)).verdict is Verdict.BAD
    assert _section("Daily update ran", now=NOW.replace(hour=5, minute=30)).verdict is Verdict.UNKNOWN


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


def test_a_failed_tail_is_bad():
    _run()
    _lane("tail", outcome="failed", exit_code=2)
    assert _section("Post-success tail").verdict is Verdict.BAD


def test_post_success_tail_reads_lane_tail():
    _run()
    _lane("digest")  # the retired lane name no longer counts
    assert _section("Post-success tail").verdict is Verdict.UNKNOWN
    _lane("tail")
    assert _section("Post-success tail").verdict is Verdict.OK


def test_undelivered_notifications_reads_the_notify_script():
    _run()
    _execution("retired_mailer", 3)  # a retired script — ignored by design
    assert _section("Undelivered notifications").verdict is Verdict.OK
    _execution("notify", 1, receipt={"subject": "PAGE 2026-09-12: equity"})
    section = _section("Undelivered notifications")
    assert section.verdict is Verdict.WARN
    assert "PAGE 2026-09-12: equity" in "\n".join(section.lines)


def test_digest_sent_today_is_bad_after_its_deadline():
    # The digest waits up to 4h for today's coverage fact from its 15:45Z start,
    # so "sent" can legitimately land as late as ~19:45Z.
    before = NOW.replace(hour=19, minute=50)
    after = NOW.replace(hour=20, minute=5)
    assert _section("Digest sent today", now=before).verdict is Verdict.UNKNOWN
    assert _section("Digest sent today", now=after).verdict is Verdict.BAD
    _execution("notify", 1, receipt={"kind": "digest", "subject": "digest x"})
    assert _section("Digest sent today", now=after).verdict is Verdict.BAD
    _execution("notify", 0, receipt={"kind": "digest", "subject": "digest x"}, started=NOW + timedelta(seconds=2))
    section = _section("Digest sent today", now=after)
    assert section.verdict is Verdict.OK
    assert "digest x" in "\n".join(section.lines)


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


def test_coverage_a_zero_denominator_scope_is_unknown_not_one_hundred():
    for timeframe in ("1d", "1m", "1h", "5m", "30m"):
        _measurement("coverage_pct", timeframe, 1.0 if timeframe == "1d" else 0.99)
        _measurement("coverage_total", timeframe, 0 if timeframe == "1d" else 100)
    section = _section("Coverage")
    assert section.verdict is Verdict.UNKNOWN
    assert "1d=UNKNOWN(expected=0)" in "\n".join(section.lines)
    _measurement("coverage_pct", "1m", 0.36, measured_at=NOW + timedelta(seconds=1))
    section = _section("Coverage")
    assert section.verdict is Verdict.BAD
    assert "1d=UNKNOWN(expected=0)" in "\n".join(section.lines)


def test_coverage_ran_today_is_bad_after_the_deadline():
    # Coverage fires 15:05Z and may legitimately wait on upstream runs, so the
    # BAD deadline is 17:30Z, not the old clock-time noon.
    before = NOW.replace(hour=17, minute=0)
    after = NOW.replace(hour=17, minute=40)
    assert _section("Coverage ran today", now=before).verdict is Verdict.UNKNOWN
    assert _section("Coverage ran today", now=after).verdict is Verdict.BAD
    _measurement("coverage_scan_ok", "all", 0)
    assert _section("Coverage ran today", now=after).verdict is Verdict.WARN
    _measurement("coverage_scan_ok", "all", 1, measured_at=NOW + timedelta(seconds=1))
    assert _section("Coverage ran today", now=after).verdict is Verdict.OK


def test_coverage_skipped_reads_unknown_with_the_reason():
    # A skip is a recorded decision, not a missed deadline: UNKNOWN, with the
    # gate's reason string visible, even after the 17:30Z deadline.
    _measurement("coverage_skipped", "jobs_still_running:intraday-catchup", 1)
    section = _section("Coverage ran today", now=NOW.replace(hour=18, minute=0))
    assert section.verdict is Verdict.UNKNOWN
    assert "jobs_still_running:intraday-catchup" in "\n".join(section.lines)


def test_a_scan_row_after_a_skip_supersedes_it():
    _measurement("coverage_skipped", "session_not_due", 1)
    _measurement("coverage_scan_ok", "all", 1, measured_at=NOW + timedelta(seconds=1))
    assert _section("Coverage ran today", now=NOW.replace(hour=18, minute=0)).verdict is Verdict.OK


def test_coverage_recovery_deferred_twice_is_bad():
    assert _section("Coverage recovery").verdict is Verdict.UNKNOWN
    _measurement("coverage_recovery_deferred", "1m", 1)
    assert _section("Coverage recovery").verdict is Verdict.WARN
    _measurement("coverage_recovery_deferred", "1m", 1, measured_at=NOW + timedelta(seconds=1))
    assert _section("Coverage recovery").verdict is Verdict.BAD
    _measurement("coverage_recovery_deferred", "1m", 0, measured_at=NOW + timedelta(seconds=2))
    assert _section("Coverage recovery").verdict is Verdict.OK


def test_stale_non_equity_is_warn_never_bad():
    _measurement("stale_non_equity", "volatility", 1)
    section = _section("Stale non-equity")
    assert section.verdict is Verdict.WARN
    assert "volatility" in "\n".join(section.lines)


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
        """A lake+warehouse layout where every configured destination exists."""
        lake = tmp_path / "lake"
        warehouse = tmp_path / "warehouse"
        for path in (
            lake,
            lake / "bronze",
            lake / "catalog",
            lake / "silver",
            warehouse,
            warehouse / "data-lake" / "raw" / "massive",
        ):
            path.mkdir(parents=True, exist_ok=True)
        return lake, warehouse

    @staticmethod
    def _fake_two_volumes(monkeypatch, lake, warehouse):
        """Two physical filesystems: the lake tree vs everything internal.

        Dedup runs on st_dev, not the usage triple — the fake devices must
        differ or the two volumes collapse onto one line and the test proves
        nothing.
        """

        def fake_fs_id(path):
            return 1 if Path(path).is_relative_to(lake) else 2

        def fake_usage(path):
            if fake_fs_id(Path(path)) == 1:
                return _Usage(13_000 * _GIB, 6_400 * _GIB, 6_600 * _GIB)
            return _Usage(228 * _GIB, 214 * _GIB, 14 * _GIB)

        monkeypatch.setattr(status, "_filesystem_id", fake_fs_id)
        monkeypatch.setattr(status.shutil, "disk_usage", fake_usage)

    def test_a_full_warehouse_volume_warns_even_when_the_lake_is_empty(self, tmp_path, monkeypatch):
        lake, warehouse = self._dirs(tmp_path)
        self._fake_two_volumes(monkeypatch, lake, warehouse)
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
        # One root, everything configured under it: many logical targets, one disk.
        shared = tmp_path / "everything"
        lake = shared / "lake"
        warehouse = shared / "warehouse"
        for path in (
            lake,
            lake / "bronze",
            lake / "catalog",
            lake / "silver",
            warehouse / "data-lake" / "raw" / "massive",
            shared / "logs",
            shared / "cursors",
            shared / "ledger",
        ):
            path.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("MDW_LOG_DIR", str(shared / "logs"))
        monkeypatch.setenv("MDW_CURSOR_DIR", str(shared / "cursors"))
        monkeypatch.setenv("MDW_SILVER_DIR", str(lake / "silver"))
        monkeypatch.setenv("LW_LEDGER_ROOT", str(shared / "ledger"))
        monkeypatch.setenv("MDW_DUCKDB_PATH", str(shared / "analytics.duckdb"))
        monkeypatch.setattr(status.shutil, "disk_usage", lambda path: _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB))
        assert len([line for line in _disk_section(lake, warehouse).lines if line.startswith("Disk")]) == 1

    def test_an_unreadable_path_is_skipped_not_fatal(self, tmp_path, monkeypatch):
        lake, warehouse = self._dirs(tmp_path)

        def fake_usage(path):
            if Path(path) == lake:
                raise OSError("volume not mounted")
            return _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB)

        monkeypatch.setattr(status.shutil, "disk_usage", fake_usage)
        lines = _disk_section(lake, warehouse).lines
        measured = [line for line in lines if "GiB free" in line]
        assert len(measured) == 1
        assert "128.0 GiB" in measured[0]
        assert "[lake]" in "\n".join(lines)  # the unmounted volume stays visible as UNKNOWN

    def test_a_missing_destination_is_unknown_not_its_parents_free_space(self, tmp_path, monkeypatch):
        # raw/massive is a child symlink to an unmounted external volume — the
        # internal raw/ parent must NOT stand in for it.
        lake, warehouse = self._dirs(tmp_path)
        (warehouse / "data-lake" / "raw" / "massive").rmdir()
        (warehouse / "data-lake" / "raw" / "massive").symlink_to(tmp_path / "gone")
        monkeypatch.setattr(
            status.shutil,
            "disk_usage",
            lambda path: _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB),
        )
        section = _disk_section(lake, warehouse)
        text = "\n".join(section.lines)
        assert "[raw-massive]" in text
        assert section.verdict is Verdict.UNKNOWN  # UNKNOWN outranks OK


def test_disk_reports_the_raw_writer_destination_separately_from_the_configured_lake(tmp_path, monkeypatch):
    # The writers root at <warehouse>/data-lake regardless of MDW_DATA_LAKE;
    # when the configured lake lives elsewhere the raw volume is its own line.
    lake = tmp_path / "lake"
    warehouse = tmp_path / "warehouse"
    external = tmp_path / "external-massive"
    for path in (lake, lake / "bronze", lake / "catalog", lake / "silver", external):
        path.mkdir(parents=True, exist_ok=True)
    (warehouse / "data-lake" / "raw").mkdir(parents=True)
    (warehouse / "data-lake" / "raw" / "massive").symlink_to(external)

    def fake_fs_id(path):
        p = Path(path)
        if p == external:
            return 3
        if p.is_relative_to(lake):
            return 1
        return 2

    monkeypatch.setattr(status, "_filesystem_id", fake_fs_id)
    monkeypatch.setattr(status.shutil, "disk_usage", lambda path: _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB))
    section = _disk_section(lake, warehouse)
    text = "\n".join(section.lines)
    assert "[raw-massive]" in text and "[lake" in text
    assert section.verdict is Verdict.OK  # every destination resolved and measured


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


def test_foreign_currency_dividends_warns_with_the_remaining_count():
    _measurement("dividend_currency_mismatch", "all", 2)
    section = _section("Foreign-currency dividends")
    assert section.verdict is status.Verdict.WARN
    assert "mismatched=2.0" in " ".join(section.lines)


def test_foreign_currency_dividends_zero_is_ok():
    _measurement("dividend_currency_mismatch", "all", 0)
    assert _section("Foreign-currency dividends").verdict is status.Verdict.OK


def test_foreign_currency_dividends_never_measured_is_unknown():
    assert _section("Foreign-currency dividends").verdict is status.Verdict.UNKNOWN


def test_foreign_currency_dividends_grades_the_latest_value():
    """A repaired lake must read OK even while older WARN rows still exist."""
    _measurement("dividend_currency_mismatch", "all", 5, measured_at=EARLIER_TODAY)
    _measurement("dividend_currency_mismatch", "all", 0, measured_at=NOW)
    assert _section("Foreign-currency dividends").verdict is status.Verdict.OK


def test_foreign_currency_dividends_without_a_measurement_today_is_unknown():
    """Yesterday's green is not today's answer.

    2026-09-14: the conversion was wired to no scheduled lane, so the newest
    `dividend_currency_mismatch` row was Sunday's manual run (0) and the check
    read OK through a Monday whose Silver rebuild failed ~30 symbols on the
    currency mismatch this check exists to surface.
    """
    _measurement("dividend_currency_mismatch", "all", 0, measured_at=NOW - timedelta(days=1))
    assert _section("Foreign-currency dividends").verdict is status.Verdict.UNKNOWN


def test_foreign_currency_dividends_is_unknown_when_todays_lane_did_not_finish():
    """A lane killed at its budget after cycle one's conversion leaves cycle
    two's dividends unconverted behind an already-filed zero."""
    _measurement("dividend_currency_mismatch", "all", 0)
    _lane("corporate-actions", outcome="timeout", exit_code=124)
    section = _section("Foreign-currency dividends")
    assert section.verdict is status.Verdict.UNKNOWN
    assert "lane_outcome=timeout" in " ".join(section.lines)


def test_foreign_currency_dividends_zero_is_ok_when_todays_lane_finished():
    _measurement("dividend_currency_mismatch", "all", 0)
    _lane("corporate-actions")
    assert _section("Foreign-currency dividends").verdict is status.Verdict.OK


def test_foreign_currency_dividends_is_unknown_when_todays_conversion_errored():
    """A failing conversion files `dividend_fx_error`; the lane swallows the
    exception, so that row is the only thing standing in front of an earlier
    cycle's zero."""
    _measurement("dividend_currency_mismatch", "all", 0, measured_at=EARLIER_TODAY)
    _measurement("dividend_fx_error", "all", 1, measured_at=NOW)
    section = _section("Foreign-currency dividends")
    assert section.verdict is status.Verdict.UNKNOWN
    assert "fact=dividend_fx_error" in " ".join(section.lines)


def test_foreign_currency_dividends_recovers_when_a_later_pass_measures_again():
    _measurement("dividend_fx_error", "all", 1, measured_at=EARLIER_TODAY)
    _measurement("dividend_currency_mismatch", "all", 0, measured_at=NOW)
    assert _section("Foreign-currency dividends").verdict is status.Verdict.OK


def test_foreign_currency_dividends_ignores_a_subset_error():
    """A targeted `--tickers` repair that failed says nothing about the lake."""
    _measurement("dividend_currency_mismatch", "all", 0, measured_at=EARLIER_TODAY)
    _measurement("dividend_fx_error", "subset", 1, measured_at=NOW)
    assert _section("Foreign-currency dividends").verdict is status.Verdict.OK


def test_foreign_currency_dividends_ignores_a_subset_zero_after_a_whole_scope_zero():
    _measurement("dividend_currency_mismatch", "all", 0, measured_at=EARLIER_TODAY)
    _measurement("dividend_currency_mismatch", "subset", 0, measured_at=NOW)
    assert _section("Foreign-currency dividends").verdict is status.Verdict.OK


def test_foreign_currency_dividends_ignores_a_targeted_repairs_subset_row():
    """An afternoon `--tickers` repair does not erase the night's whole-scope WARN.

    The check grades today's newest row, so a one-symbol run filed under the
    same scope would read as the whole lake's answer.
    """
    _measurement("dividend_currency_mismatch", "all", 3, measured_at=EARLIER_TODAY)
    _measurement("dividend_currency_mismatch", "subset", 0, measured_at=NOW)
    assert _section("Foreign-currency dividends").verdict is status.Verdict.WARN


def _membership_run(verdict: str, *, started: datetime):
    run_id = f"membership-sync-{started:%Y%m%dT%H%M%S}Z"
    ledger.emit(
        "runs",
        [
            {
                "run_id": run_id,
                "job": "membership-sync",
                "host": "macmini",
                "release_sha": "deadbeef",
                "presets_sha": None,
                "registry_sha": None,
                "started": started,
                "ended": started,
                "exit_code": 0 if verdict == "OK" else 3,
                "verdict": verdict,
            }
        ],
        run_id=run_id,
    )


def _membership_section(name: str, run_date: date):
    sections = status.collect(
        run_date,
        Path("/nonexistent"),
        Path("/nonexistent"),
        runner=_fake_launchctl,
        database=None,
        now=datetime.combine(run_date, datetime.max.time(), tzinfo=UTC),
    )
    return next(section for section in sections if section.name == name)


def test_membership_sync_ran_today_is_weekday_gated():
    monday = date(2026, 9, 14)
    assert monday.isoweekday() == 1

    assert _membership_section("Membership sync ran today", monday).verdict is status.Verdict.UNKNOWN
    _membership_run("OK", started=datetime(2026, 9, 14, 1, 0, tzinfo=UTC))
    assert _membership_section("Membership sync ran today", monday).verdict is status.Verdict.OK
    _membership_run("FAILED", started=datetime(2026, 9, 14, 2, 0, tzinfo=UTC))
    assert _membership_section("Membership sync ran today", monday).verdict is status.Verdict.BAD


def test_membership_sync_ran_today_reads_ok_on_weekends():
    """The job is weekday-only: Saturday stays green, failed run or none."""
    saturday = date(2026, 9, 12)
    assert saturday.isoweekday() == 6

    assert _membership_section("Membership sync ran today", saturday).verdict is status.Verdict.OK
    _membership_run("FAILED", started=datetime(2026, 9, 12, 9, 0, tzinfo=UTC))
    assert _membership_section("Membership sync ran today", saturday).verdict is status.Verdict.OK


def test_unresolved_memberships_warns_with_per_index_counts():
    assert _section("Unresolved memberships").verdict is status.Verdict.UNKNOWN

    _measurement("membership_unresolved", "sp500", 3)
    _measurement("membership_unresolved", "djia", 0)
    section = _section("Unresolved memberships")
    assert section.verdict is status.Verdict.WARN
    detail = " ".join(section.lines)
    assert "unresolved=3" in detail and "sp500=3" in detail and "djia=0" in detail

    _measurement("membership_unresolved", "sp500", 0, measured_at=NOW + timedelta(seconds=1))
    assert _section("Unresolved memberships").verdict is status.Verdict.OK


def _reresolve_run(verdict: str, *, started: datetime):
    run_id = f"membership-reresolve-{started:%Y%m%dT%H%M%S}Z"
    ledger.emit(
        "runs",
        [
            {
                "run_id": run_id,
                "job": "membership-reresolve",
                "host": "macmini",
                "release_sha": "deadbeef",
                "presets_sha": None,
                "registry_sha": None,
                "started": started,
                "ended": started,
                "exit_code": 0 if verdict == "OK" else 1,
                "verdict": verdict,
            }
        ],
        run_id=run_id,
    )


def test_a_reresolve_run_cannot_hide_a_failed_membership_sync():
    """The reresolve pass files under its own job. Under `membership-sync` a
    later OK row would hide the night's FAILED scheduled run, because the check
    grades the latest run of the day."""
    monday = date(2026, 9, 14)
    _membership_run("FAILED", started=datetime(2026, 9, 14, 1, 0, tzinfo=UTC))
    _reresolve_run("OK", started=datetime(2026, 9, 14, 4, 0, tzinfo=UTC))

    assert _membership_section("Membership sync ran today", monday).verdict is status.Verdict.BAD


def test_the_unresolved_memberships_hint_names_commands_that_exist():
    hint = status._FIXES["Unresolved memberships"]
    assert "security-master sync" in hint
    assert "reresolve" in hint
    assert "livewire_ops.py membership --index" not in hint


def test_a_run_closed_as_abandoned_reads_warn_not_failed_and_not_unknown():
    """A run whose process died without writing its own terminal row is closed
    ABANDONED by the next start of that job. It is an interruption, not a
    failure — WARN, so the watchdog never pages for it, and never UNKNOWN,
    because there is evidence: we know exactly what happened to it.
    pm:2026-09-16-interrupted-runs-never-closed

    `status.Verdict`, not the imported `Verdict`: an earlier test in this file
    reloads the module, so the top-level name is a different enum class.
    """
    _run(verdict=ledger.ABANDONED, exit_code=None)

    assert _section("Daily update ran").verdict is status.Verdict.WARN


def test_an_abandoned_intraday_run_reads_warn_too():
    """Fix the twin: both run-level checks carry the same CASE expression."""
    _run(job="intraday-catchup", verdict=ledger.ABANDONED, exit_code=None)

    assert _section("Intraday catch-up ran").verdict is status.Verdict.WARN


def test_a_section_carries_the_single_run_its_rows_name():
    """The watchdog suppresses a BAD section whose run already paged, so a
    section has to know which run it is a statement about."""
    _run(verdict="FAILED", exit_code=1)

    assert _section("Daily update ran").run_id == RUN


def test_the_real_status_and_the_real_pager_agree_on_which_run_a_section_names():
    """The two halves of the watchdog fix, joined with nothing mocked between
    them: `status.collect` puts the run id on the section, `notify` looks that
    id up in `executions.run_id`, and the page is suppressed. Every watchdog
    test mocks `collect`, so without this one nothing proves the real check
    produces an id the real pager can match.
    pm:2026-09-16-one-failure-paged-twice
    """
    _run(job="intraday-catchup", verdict="FAILED", exit_code=1)
    section = _section("Intraday catch-up ran")
    assert section.verdict is status.Verdict.BAD
    assert section.run_id == RUN, "the check must name the run it graded"

    # nothing has paged yet, so the watchdog is the only surface that will
    assert notify.page_from_sections([section], NOW.date()) is not None

    # the lane's own page, recorded the way notify.send records every send
    # `skipped: false` matters: a send that deduped or failed must not silence
    # the watchdog, so the receipt shape is part of the contract here.
    _execution("notify", 0, receipt={"kind": "page", "subject": "PAGE: lane intraday_catchup", "skipped": False})

    assert notify.page_from_sections([section], NOW.date()) is None


def _silver_revision(data_lake: Path, revision: int, symbols: tuple = (), *, generation: str | None = None) -> dict:
    """Write immutable + current manifests whose affected scope lists `symbols`."""
    revisions = data_lake / "silver" / "revisions"
    revisions.mkdir(parents=True, exist_ok=True)
    generation = generation or f"gen-{revision}"
    payload = {
        "schema_version": 1,
        "revision": revision,
        "generation_id": generation,
        "published_at": "2026-09-02T06:00:00Z",
        "corporate_actions_as_of": "2026-09-02T05:00:00Z",
        "affected": [{"symbol": s, "earliest_date": "2026-08-01", "timeframes": ["1d"]} for s in symbols],
        "artifacts": [],
    }
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (revisions / f"revision={revision}.json").write_bytes(encoded)
    (revisions / "current.json").write_bytes(encoded)
    return {
        "ref": f"revisions/revision={revision}.json",
        "sha": hashlib.sha256(encoded).hexdigest(),
        "generation": generation,
        "revision": revision,
    }


def _faults_roots(lake: Path) -> dict:
    return {
        "data_lake_root": str(lake.resolve()),
        "silver_root": str((lake / "silver").resolve()),
    }


def _fault_evidence(lake: Path, symbol: str, *, stage="staging", run_id=RUN, fetched_at=None, **over):
    payload = (
        {
            "kind": "silver_symbol_failure",
            "schema_version": 1,
            "stage": stage,
            "symbol": symbol,
            "baseline_revision": 3,
        }
        | _faults_roots(lake)
        | over
    )
    ledger.emit(
        "evidence",
        [
            {
                "evidence_hash": f"ev-{symbol}-{stage}-{fetched_at or NOW}",
                "kind": "silver_symbol_failure",
                "subject": symbol,
                "payload_json": json.dumps(payload),
                "source_url": None,
                "fetched_at": fetched_at or NOW,
                "proposer": "rebuild-silver",
                "run_id": run_id,
            }
        ],
        run_id=run_id,
    )


_ENDED_UNSET = object()


def _silver_receipt(
    lake: Path,
    *,
    result="committed",
    manifest: dict | None = None,
    validated=(),
    failed=(),
    withheld=(),
    started=NOW,
    ended=_ENDED_UNSET,
    run_id=RUN,
    **over,
):
    receipt = {
        "kind": "silver_publication",
        "schema_version": 1,
        "result": result,
        "selected_symbols": sorted(set(validated) | set(failed) | set(withheld)),
        "staged_symbols": sorted(set(validated) | set(failed)),
        "validated_symbols": list(validated),
        "failed_symbols": list(failed),
        "withheld_symbols": list(withheld),
        "baseline_revision": 3,
    } | _faults_roots(lake)
    if manifest is not None:
        receipt |= {
            "manifest_ref": manifest["ref"],
            "manifest_sha256": manifest["sha"],
            "generation_id": manifest["generation"],
            "published_revision": manifest["revision"],
        }
    receipt |= over
    ledger.emit(
        "executions",
        [
            {
                "evidence_hash": None,
                "script": "rebuild-silver",
                "attempt": 1,
                "args_json": "{}",
                "release_sha": "deadbeef",
                "started": started,
                # _ENDED_UNSET defaults ended to started; an explicit None
                # writes a real NULL so missing-ended paths get tested.
                "ended": started if ended is _ENDED_UNSET else ended,
                "exit_code": 0 if result != "attempt_only" else 1,
                "receipt_json": json.dumps(receipt),
                "run_id": run_id,
            }
        ],
        run_id=run_id,
    )


def _faults(tmp_path):
    """A lake the fault section can root-match, and its section."""
    lake = tmp_path / "lake"
    lake.mkdir(exist_ok=True)
    return lake, status._silver_faults_section(lake)


class TestSilverSymbolFaults:
    def test_receipt_only_failure_with_an_inherited_run_id_reopens_the_issue(self, tmp_path):
        # Evidence fault F1, a verified recovery R, then a later failed receipt F2
        # that inherited F1's LW_RUN_ID. The shared run id must NOT dedupe F2:
        # it is its own observation and the issue stays unresolved with F2's
        # ended as last_seen.
        lake, _ = _faults(tmp_path)
        t1, t2, t3 = NOW, NOW + timedelta(hours=1), NOW + timedelta(hours=2)
        _fault_evidence(lake, "AAPL", run_id="run-shared", fetched_at=t1, error="parquet unreadable")
        manifest = _silver_revision(lake, 8, ("AAPL",))
        _silver_receipt(
            lake, result="noop", manifest=manifest, validated=("AAPL",), started=t2, ended=t2, run_id="run-recovery"
        )
        _silver_receipt(lake, result="committed", failed=("AAPL",), started=t3, ended=t3, run_id="run-shared")
        section = status._silver_faults_section(lake)
        text = "\n".join(section.lines)
        assert section.verdict is status.Verdict.WARN
        assert "AAPL" in text and "1 unresolved" in text
        assert f"last_seen={t3}" in text

    def test_repeated_receipt_failures_count_as_receipt_failures_not_distinct_attempts(self, tmp_path):
        lake, _ = _faults(tmp_path)
        for offset in (1, 2):
            _silver_receipt(
                lake,
                result="committed",
                failed=("BTX",),
                started=NOW + timedelta(hours=offset),
                ended=NOW + timedelta(hours=offset),
                run_id=f"run-{offset}",
            )
        section = status._silver_faults_section(lake)
        text = "\n".join(section.lines)
        assert "receipt_failures=2" in text
        assert "2 receipt-listed failures" in text
        # honest wording: evidence facts and receipt scope are separate counts
        assert "0 evidence facts" in text

    def test_verified_manifest_is_read_once_across_receipts(self, tmp_path, monkeypatch):
        lake, _ = _faults(tmp_path)
        manifest = _silver_revision(lake, 8, ("AAPL",))
        for offset in range(4):
            _silver_receipt(
                lake,
                result="noop",
                manifest=manifest,
                validated=("AAPL",),
                started=NOW + timedelta(minutes=offset),
                run_id=f"run-{offset}",
            )
        reads = []
        real_read = Path.read_bytes

        def counting(self):
            reads.append(self)
            return real_read(self)

        monkeypatch.setattr(Path, "read_bytes", counting)
        status._silver_faults_section(lake)
        assert len(reads) == 1  # one immutable manifest, four receipts

    def test_a_contradictory_receipt_never_inherits_another_receipts_verified_manifest(self, tmp_path):
        # The manifest cache keys on every validator input. A committed receipt
        # naming published_revision=9 but the revision=8 ref/sha/generation must
        # be verified on its own — it fails the ref check and cannot close AAPL.
        lake, _ = _faults(tmp_path)
        manifest = _silver_revision(lake, 8, ("AAPL",))
        _silver_receipt(lake, result="noop", manifest=manifest, validated=("AAPL",), started=NOW, run_id="run-good")
        _fault_evidence(lake, "AAPL", fetched_at=NOW + timedelta(hours=1), error="bad rows")
        _silver_receipt(
            lake,
            result="committed",
            manifest=manifest,
            published_revision=9,  # contradictory: ref still says revision=8
            validated=("AAPL",),
            started=NOW + timedelta(hours=2),
            run_id="run-lying",
        )
        section = status._silver_faults_section(lake)
        assert section.verdict is status.Verdict.WARN
        assert "1 unresolved" in "\n".join(section.lines)

    def test_strict_manifest_fields_reject_bool_and_foreign_generation(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _fault_evidence(lake, "AAPL", fetched_at=NOW, error="x")
        # bool published_revision: True is an int subclass and must not pass
        manifest = _silver_revision(lake, 8, ("AAPL",))
        _silver_receipt(
            lake,
            result="noop",
            manifest=manifest,
            published_revision=True,
            validated=("AAPL",),
            started=NOW + timedelta(hours=1),
            run_id="run-bool",
        )
        # matching ref/sha but the receipt claims a different generation
        _silver_receipt(
            lake,
            result="noop",
            manifest=manifest,
            generation_id="somebody-else",
            validated=("AAPL",),
            started=NOW + timedelta(hours=2),
            run_id="run-foreign-gen",
        )
        # manifest payload with a bool schema_version
        revisions = lake / "silver" / "revisions"
        bad = json.loads((revisions / "revision=8.json").read_text())
        bad["schema_version"] = True
        (revisions / "revision=9.json").write_text(json.dumps(bad))
        _silver_receipt(
            lake,
            result="noop",
            manifest={
                "ref": "revisions/revision=9.json",
                "sha": hashlib.sha256(json.dumps(bad).encode()).hexdigest(),
                "generation": "gen-8",
                "revision": 9,
            },
            validated=("AAPL",),
            started=NOW + timedelta(hours=3),
            run_id="run-bool-schema",
        )
        section = status._silver_faults_section(lake)
        assert "1 unresolved" in "\n".join(section.lines)

    def test_a_receipt_listing_the_symbol_as_failed_cannot_close_it(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _fault_evidence(lake, "AAPL", fetched_at=NOW, error="x")
        manifest = _silver_revision(lake, 8, ("AAPL",))
        # contradictory scope: validated AND failed — never a closer
        _silver_receipt(
            lake,
            result="committed",
            manifest=manifest,
            validated=("AAPL",),
            failed=("AAPL",),
            started=NOW + timedelta(hours=1),
        )
        section = status._silver_faults_section(lake)
        assert "1 unresolved" in "\n".join(section.lines)

    def test_attempt_only_and_unverifiable_receipts_are_unknown_not_ok(self, tmp_path):
        lake, _ = _faults(tmp_path)
        assert status._silver_faults_section(lake).verdict is status.Verdict.UNKNOWN  # nothing on record

        _silver_receipt(lake, result="attempt_only", manifest=None, run_id="run-1")
        assert status._silver_faults_section(lake).verdict is status.Verdict.UNKNOWN

        # a committed receipt whose manifest does not verify still proves nothing
        _silver_receipt(
            lake,
            result="committed",
            manifest={"ref": "revisions/revision=99.json", "sha": "0" * 64, "generation": "g", "revision": 99},
            validated=("AAPL",),
            started=NOW + timedelta(hours=1),
            run_id="run-2",
        )
        assert status._silver_faults_section(lake).verdict is status.Verdict.UNKNOWN

        # one verified noop over these roots is actual coverage proof
        manifest = _silver_revision(lake, 8, ("AAPL",))
        _silver_receipt(lake, result="noop", manifest=manifest, validated=("AAPL",), started=NOW + timedelta(hours=2))
        assert status._silver_faults_section(lake).verdict is status.Verdict.OK

    def test_legacy_unscoped_positives_stay_unknown_even_after_a_full_verified_run(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _measurement("silver_failed", "silver", 2, measured_at=NOW - timedelta(days=3))
        manifest = _silver_revision(lake, 8, ("AAPL", "BTX", "MSFT"))
        _silver_receipt(lake, result="committed", manifest=manifest, validated=("AAPL", "BTX", "MSFT"))
        section = status._silver_faults_section(lake)
        text = "\n".join(section.lines)
        # A --full receipt cannot establish WHICH symbols the unscoped counter
        # meant — the legacy positive is never auto-cleared.
        assert section.verdict is status.Verdict.UNKNOWN
        assert "legacy unscoped silver_failed=2" in text
        assert "legacy:silver_failed=2" in section.notification_key

    def test_faults_survive_a_missing_pointer_and_render_the_full_input_path(self, tmp_path):
        lake, _ = _faults(tmp_path)
        # no manifest at all — the section must still surface the symbol fault
        _fault_evidence(
            lake,
            "AAPL",
            fetched_at=NOW,
            error="hash mismatch",
            input={"path": str(lake / "raw" / "deep" / "nested" / "1d.parquet"), "sha256": "ab" * 32},
            input_date_bounds={"earliest": "2026-08-01", "latest": "2026-09-02"},
        )
        section = status._silver_faults_section(lake)
        text = "\n".join(section.lines)
        assert section.verdict is status.Verdict.WARN
        assert str(lake / "raw" / "deep" / "nested" / "1d.parquet") in text  # full path, not basename
        assert "input_bounds=2026-08-01..2026-09-02 (context, not failing sessions)" in text
        assert "scope=UNKNOWN" in text  # staging faults never invent a failing session

    def test_malformed_rows_are_counted_without_erasing_valid_faults(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _fault_evidence(lake, "AAPL", fetched_at=NOW, error="x")
        # undecodable payload, valid row
        ledger.emit(
            "evidence",
            [
                {
                    "evidence_hash": "ev-broken",
                    "kind": "silver_symbol_failure",
                    "subject": "ZZZ",
                    "payload_json": "{not json",
                    "source_url": None,
                    "fetched_at": NOW,
                    "proposer": "rebuild-silver",
                    "run_id": RUN,
                }
            ],
            run_id=RUN,
        )
        # decodable but invalid shape: bounds as a list
        _fault_evidence(lake, "ZZZ", input_date_bounds=["2026-08-01"])
        # a receipt whose receipt_json is not a receipt at all
        ledger.emit(
            "executions",
            [
                {
                    "evidence_hash": None,
                    "script": "rebuild-silver",
                    "attempt": 1,
                    "args_json": "{}",
                    "release_sha": "deadbeef",
                    "started": NOW,
                    "ended": NOW,
                    "exit_code": 1,
                    "receipt_json": "{broken",
                    "run_id": "run-broken",
                }
            ],
            run_id="run-broken",
        )
        section = status._silver_faults_section(lake)
        text = "\n".join(section.lines)
        assert "AAPL" in text
        assert "undecodable" in text
        # malformed rows must not churn the page key
        assert "ev-broken" not in section.notification_key

    def test_wrong_roots_with_the_same_revision_cannot_close_a_fault(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _fault_evidence(lake, "AAPL", fetched_at=NOW, error="x")
        manifest = _silver_revision(lake, 8, ("AAPL",))
        # another deployment's receipt for the same symbol+revision: roots gate
        # it out before it can match or close anything here
        _silver_receipt(
            tmp_path / "other-lake",
            result="committed",
            manifest=manifest,
            validated=("AAPL",),
            started=NOW + timedelta(hours=2),
            run_id="run-foreign",
        )
        section = status._silver_faults_section(lake)
        assert "1 unresolved" in "\n".join(section.lines)

    def test_a_targeted_success_clears_only_the_named_symbol(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _fault_evidence(lake, "AAPL", fetched_at=NOW, error="x")
        _fault_evidence(lake, "BTX", fetched_at=NOW, error="x")
        manifest = _silver_revision(lake, 8, ("AAPL",))
        _silver_receipt(lake, result="noop", manifest=manifest, validated=("AAPL",), started=NOW + timedelta(hours=1))
        section = status._silver_faults_section(lake)
        text = "\n".join(section.lines)
        assert "1 unresolved" in text
        assert "BTX" in text and "resolved by verified receipt" in text

    def test_a_delayed_older_success_cannot_clear_a_newer_failure(self, tmp_path):
        lake, _ = _faults(tmp_path)
        manifest = _silver_revision(lake, 8, ("AAPL",))
        # the recovery receipt STARTED before the fault was last observed
        _silver_receipt(lake, result="noop", manifest=manifest, validated=("AAPL",), started=NOW, ended=NOW)
        _fault_evidence(lake, "AAPL", fetched_at=NOW + timedelta(hours=6), error="x")
        assert "1 unresolved" in "\n".join(status._silver_faults_section(lake).lines)

    def test_the_notification_key_is_stable_across_reobservation(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _fault_evidence(lake, "AAPL", fetched_at=NOW, error="x", run_id="run-1")
        key_one = status._silver_faults_section(lake).notification_key
        _fault_evidence(lake, "AAPL", fetched_at=NOW + timedelta(days=1), error="x", run_id="run-2")
        key_two = status._silver_faults_section(lake).notification_key
        assert key_one == key_two  # same issue, new timestamp/run: no re-page

    def test_withheld_regressions_keep_their_known_scope_and_repair_guidance(self, tmp_path):
        lake, _ = _faults(tmp_path)
        _fault_evidence(
            lake,
            "AAPL",
            stage="withheld_window_regression",
            fetched_at=NOW,
            reason="window shortened",
            previous_start="2020-01-02",
            new_start="2024-06-01",
        )
        section = status._silver_faults_section(lake)
        text = "\n".join(section.lines)
        assert "scope=2020-01-02->2024-06-01" in text
        assert "--allow-window-regression adopts the shortening" in text  # review, not routine recovery
        assert "rebuild-silver --tickers" not in text.split("next:")[-1]


def _catalog_build_receipt(
    lake: Path, local_db: Path, *, result="committed", started=NOW, ended=None, run_id="duckdb-run"
):
    lake_target = lake / "catalog" / "analytics.duckdb"
    receipt = {
        "schema_version": 1,
        "kind": "catalog_publication",
        "result": result,
        "data_lake_root": str(lake.expanduser().resolve()),
        "local": {"path": str(local_db.expanduser().resolve()), "sha256": "ab" * 32},
        "lake": {
            "path": str(lake_target.expanduser().resolve()),
            "expected_sha256": "ab" * 32,
            "result": "failed" if result == "copy_failed" else "copied",
            "error": "OSError: read-only file system" if result == "copy_failed" else None,
        },
        "coverage_rows": {},
        "deployment_sha": "deadbeef",
    }
    ledger.emit(
        "executions",
        [
            {
                "evidence_hash": None,
                "script": "duckdb-build",
                "attempt": 1,
                "args_json": "{}",
                "release_sha": "deadbeef",
                "started": started,
                "ended": started if ended is None else ended,
                "exit_code": 1 if result == "copy_failed" else 0,
                "receipt_json": json.dumps(receipt),
                "run_id": run_id,
            }
        ],
        run_id=run_id,
    )


def test_a_copy_failed_build_receipt_is_bad_even_when_the_catalog_is_fresh(tmp_path, monkeypatch):
    # Grok finding: a same-root copy_failed receipt used to sit on one
    # information line while freshness graded the section OK.
    lake = tmp_path / "lake"
    lake.mkdir(exist_ok=True)
    local_db = tmp_path / "analytics.duckdb"  # MDW_DUCKDB_PATH via the root fixture
    headline = {name: (100, NOW.date()) for name in status._CATALOG_LANE_FIX}
    monkeypatch.setattr(status, "_coverage_headline", lambda _db: headline)
    _catalog_build_receipt(lake, local_db, result="copy_failed")

    section = status._duckdb_section(NOW.date())
    text = "\n".join(section.lines)
    assert section.verdict is status.Verdict.BAD
    assert "copy_failed" in text and "oldest view" in text  # freshness stays visible
    assert "retries the lake copy" in (section.fix or "")


def test_a_newer_failed_or_attempt_only_receipt_keeps_an_old_done_lane_from_reading_green(tmp_path):
    # Grok finding: lane done at T1 + standalone rebuild-silver receipt at T2
    # used to render OK with the healthy page key.
    lake = tmp_path / "lake"
    _committed_silver(lake)
    _lane("silver", started=NOW, ended=NOW)
    _silver_receipt(
        lake,
        result="committed",
        failed=("AAPL",),
        started=NOW + timedelta(hours=2),
        ended=NOW + timedelta(hours=2),
        run_id="run-late-fail",
    )
    section = status._silver_publication_section(lake)
    assert section.verdict is status.Verdict.BAD
    assert "Latest decoded rebuild receipt" in "\n".join(section.lines)
    assert section.notification_key == "silver-publication:receipt-attempt-failed"

    # a still-newer attempt_only receipt: the attempt's outcome is unknown, not
    # bad — and still not green
    _silver_receipt(
        lake,
        result="attempt_only",
        started=NOW + timedelta(hours=3),
        ended=None,
        run_id="run-later-attempt",
    )
    section = status._silver_publication_section(lake)
    assert section.verdict is status.Verdict.UNKNOWN
    assert section.notification_key == "silver-publication:receipt-attempt-unknown"


def test_an_undecodable_receipt_row_floors_a_done_lane_at_unknown(tmp_path):
    # Grok finding: a malformed newer row must not let an older decoded receipt
    # pose as the latest — UNKNOWN floor, and the label says "decoded".
    lake = tmp_path / "lake"
    _committed_silver(lake)
    _lane("silver", started=NOW, ended=NOW)
    ledger.emit(
        "executions",
        [
            {
                "evidence_hash": None,
                "script": "rebuild-silver",
                "attempt": 1,
                "args_json": "{}",
                "release_sha": "deadbeef",
                "started": NOW + timedelta(hours=1),
                "ended": NOW + timedelta(hours=1),
                "exit_code": 1,
                "receipt_json": "{broken",
                "run_id": "run-broken",
            }
        ],
        run_id="run-broken",
    )
    section = status._silver_publication_section(lake)
    text = "\n".join(section.lines)
    assert section.verdict is status.Verdict.UNKNOWN
    assert "Latest decoded rebuild receipt" in text
    assert "could not be decoded" in text
    assert section.notification_key == "silver-publication:receipts-undecodable"


def test_the_latest_receipt_is_chosen_by_completion_not_start(tmp_path, monkeypatch):
    # Lead probe o3-completion-order-probe.json: an earlier-started failure that
    # ends AFTER a later-started success is the latest fact. Ordering by
    # started put the success last and the section read OK.
    lake = tmp_path / "lake"
    _committed_silver(lake)
    _lane("silver", started=NOW, ended=NOW)
    # the slow attempt starts first, fails last
    _silver_receipt(
        lake,
        result="committed",
        failed=("AAPL",),
        started=NOW + timedelta(hours=1),
        ended=NOW + timedelta(hours=3),
        run_id="run-slow-fail",
    )
    # the quick clean noop starts second but finishes earlier — verified, real,
    # and still not the latest attempt
    manifest = _silver_revision(lake, 8, ("AAPL",))
    _silver_receipt(
        lake,
        result="noop",
        manifest=manifest,
        validated=("AAPL",),
        started=NOW + timedelta(hours=2),
        ended=NOW + timedelta(hours=2),
        run_id="run-quick-ok",
    )
    section = status._silver_publication_section(lake)
    assert section.verdict is status.Verdict.BAD
    assert "run-slow-fail" in "\n".join(section.lines)

    # same overlap for the catalog build receipt: the copy_failed build that
    # started first but finished last is the latest fact
    local_db = tmp_path / "analytics.duckdb"
    headline = {name: (100, NOW.date()) for name in status._CATALOG_LANE_FIX}
    monkeypatch.setattr(status, "_coverage_headline", lambda _db: headline)
    _catalog_build_receipt(
        lake,
        local_db,
        result="copy_failed",
        started=NOW + timedelta(hours=1),
        ended=NOW + timedelta(hours=3),
        run_id="build-slow-fail",
    )
    _catalog_build_receipt(
        lake,
        local_db,
        result="committed",
        started=NOW + timedelta(hours=2),
        ended=NOW + timedelta(hours=2),
        run_id="build-quick-ok",
    )
    section = status._duckdb_section(NOW.date())
    assert section.verdict is status.Verdict.BAD
    assert "copy_failed" in "\n".join(section.lines)
