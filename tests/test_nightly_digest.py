"""Tests for livewire_scripts.nightly_digest."""

from __future__ import annotations

import collections
import json
from datetime import UTC, date, datetime
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from livewire_scripts import nightly_digest, status
from livewire_scripts.daily_outcomes import SUMMARY_PREFIX, build_summary_line
from livewire_scripts.nightly_digest import build_digest, main


@pytest.fixture(autouse=True)
def _no_real_launchctl(monkeypatch):
    """build_digest reaches collect(), which shells out to launchctl AND opens
    the operator's real analytics.duckdb.

    Every digest assertion here would otherwise depend on which plists happen
    to be loaded on the machine running the test — green on this Mac, a
    different verdict on CI, and an unmocked subprocess either way.
    """
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


def _write_daily_log(log_dir, run, summaries):
    lines = ["=== Daily Update ==="]
    for s in summaries:
        lines.append("  AAPL: 1 bar published from Massive")
        lines.append(build_summary_line(**s))
    (log_dir / f"daily_update_{run}.log").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _daily_summary(**kw):
    base = dict(
        job="daily_update",
        asset_class="equity",
        source="massive",
        target_date="2026-07-02",
        updated=9091,
        no_trade=277,
        partial=95,
        errors=0,
        bars_inserted=9186,
        validation_issues=0,
        top_errors=[],
    )
    base.update(kw)
    return base


def _body_file_from_cmd(cmd) -> Path:
    return Path(cmd[cmd.index("--body-file") + 1])


def test_build_digest_renders_all_sections(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    run = "2026-07-02"
    _write_daily_log(
        log_dir,
        run,
        [_daily_summary(), _daily_summary(asset_class="futures", source="ib", updated=3, no_trade=0, partial=0)],
    )
    intraday = SUMMARY_PREFIX + (
        '{"job":"daily_backfill","target_date":"2026-07-02",'
        '"phases":[{"label":"daily_backfill_fred_rates","exit":0,"duration_s":4.6}],'
        '"failed":[]}'
    )
    (log_dir / f"intraday_catchup_{run}.log").write_text(intraday + "\n", encoding="utf-8")
    (log_dir / f"coverage_{run}.log").write_text(
        "2026-07-02 coverage: 1d=11840/11900 (99.50%)\n  1d missing: FOOU\n", encoding="utf-8"
    )

    out = build_digest(date(2026, 7, 2), log_dir, tmp_path)

    assert "Livewire nightly digest — 2026-07-02" in out
    assert "equity" in out and "updated=9091" in out and "no_trade=277" in out
    assert "futures" in out and "updated=3" in out
    assert "daily_backfill_fred_rates" in out and "ok" in out
    assert "2026-07-02 coverage: 1d=11840/11900 (99.50%)" in out
    assert "Disk:" in out


def test_build_digest_renders_failed_phases(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    run = "2026-07-02"
    intraday = SUMMARY_PREFIX + (
        '{"job":"daily_backfill","target_date":"2026-07-02",'
        '"phases":[{"label":"daily_backfill_fred_rates","exit":1,"duration_s":0.2}],'
        '"failed":["daily_backfill_fred_rates"]}'
    )
    (log_dir / f"intraday_catchup_{run}.log").write_text(intraday + "\n", encoding="utf-8")
    out = build_digest(date(2026, 7, 2), log_dir, tmp_path)
    assert "FAILED (exit 1)" in out
    assert "failed: daily_backfill_fred_rates" in out


def test_coverage_from_an_earlier_session_reaches_the_digest(tmp_path):
    """Coverage names its log after the session it measured, not the run date.

    Two reasons it will not match: the daily job runs at 02:00 ET the morning
    AFTER the session it ingested, and coverage now runs on its own launchd
    schedule entirely. An exact-filename lookup found neither. Production
    shape: the 2026-08-01 run ingested the 2026-07-31 session.
    """
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    _write_daily_log(log_dir, "2026-08-01", [_daily_summary(target_date="2026-07-31")])
    (log_dir / "coverage_2026-07-31.log").write_text("2026-07-31 coverage: 1d=13100/13141 (99.69%)\n", encoding="utf-8")

    out = build_digest(date(2026, 8, 1), log_dir, tmp_path)

    assert "2026-07-31 coverage: 1d=13100/13141 (99.69%)" in out
    assert "Coverage:\n  (not found)" not in out


def test_build_digest_missing_inputs_render_not_found(tmp_path):
    out = build_digest(date(2026, 7, 2), tmp_path / "empty_logs", tmp_path)
    assert isinstance(out, str)
    # outcomes, phases, silver, coverage, interior gaps
    assert out.count("(not found)") == 5
    assert "Disk:" in out


def test_disk_tripwire_warns_under_reserve(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_FLATFILE_MIN_FREE_GB", "25")
    import importlib

    from livewire_scripts import nightly_digest

    importlib.reload(nightly_digest)

    class _Usage:
        total = 200 * (1024**3)
        used = 170 * (1024**3)
        free = 30 * (1024**3)  # 30 GiB < 2*25

    monkeypatch.setattr(nightly_digest.shutil, "disk_usage", lambda p: _Usage())
    out = nightly_digest.build_digest(date(2026, 7, 2), tmp_path / "logs", tmp_path)
    assert "⚠" in out and "raw retention deferred" in out
    importlib.reload(nightly_digest)


def test_main_prints_and_no_email_by_default(tmp_path, capsys):
    rc = main(["--run-date", "2026-07-02", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)])
    assert rc == 0
    assert "Livewire nightly digest" in capsys.readouterr().out


def test_default_run_date_is_utc(tmp_path, capsys, monkeypatch):
    from livewire_scripts import nightly_digest

    class FrozenDateTime:
        @classmethod
        def now(cls, tz=None):
            if tz is UTC:
                return datetime(2026, 4, 6, 1, 0, tzinfo=UTC)
            return datetime(2026, 4, 5, 18, 0)

    monkeypatch.setattr(nightly_digest, "datetime", FrozenDateTime, raising=False)

    rc = nightly_digest.main(["--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)])

    assert rc == 0
    assert "Livewire nightly digest — 2026-04-06" in capsys.readouterr().out


def test_main_email_invokes_node_script(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    calls = []

    def fake_runner(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(args=cmd, returncode=0)

    log_dir = tmp_path / "logs"
    rc = main(
        ["--run-date", "2026-07-02", "--email", "--log-dir", str(log_dir), "--data-lake", str(tmp_path)],
        runner=fake_runner,
    )
    assert rc == 0
    assert len(calls) == 1
    cmd = calls[0]
    assert "--mode" in cmd and "digest" in cmd
    assert "--body-file" in cmd
    body_file = _body_file_from_cmd(cmd)
    assert body_file.parent == log_dir
    assert body_file.exists()
    assert "Livewire nightly digest" in body_file.read_text(encoding="utf-8")
    # Writes the marker the daily-update watchdog gates on only after send success.
    assert (log_dir / "quality_summary_2026-07-02.marker").exists()


def test_failed_send_does_not_write_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")

    def fake_runner(cmd, **kwargs):
        return CompletedProcess(args=cmd, returncode=1)

    log_dir = tmp_path / "logs"
    rc = main(
        ["--run-date", "2026-07-02", "--email", "--log-dir", str(log_dir), "--data-lake", str(tmp_path)],
        runner=fake_runner,
    )

    assert rc == 1
    assert not (log_dir / "quality_summary_2026-07-02.marker").exists()


def test_body_file_honors_log_dir_override(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    wrong_log_dir = tmp_path / "module_logs"
    custom_log_dir = tmp_path / "custom_logs"
    calls = []

    def fake_runner(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr("livewire_scripts.nightly_digest._LOG_DIR", wrong_log_dir)

    rc = main(
        ["--run-date", "2026-07-02", "--email", "--log-dir", str(custom_log_dir), "--data-lake", str(tmp_path)],
        runner=fake_runner,
    )

    assert rc == 0
    body_file = _body_file_from_cmd(calls[0])
    assert body_file.parent == custom_log_dir
    assert body_file.exists()
    assert not (wrong_log_dir / "nightly_digest_2026-07-02.txt").exists()


def _write_silver_summary(log_dir, run, **fields):
    """Append rebuild-silver's SUMMARY_JSON to the daily log, as the lane really does."""

    summary = {"revision": 3, "rebuilt": 12, "unchanged": 5, "trimmed": 2, "failed": 0, "window_regressions": 0}
    summary.update(fields)
    path = log_dir / f"daily_update_{run}.log"
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    path.write_text(existing + SUMMARY_PREFIX + json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")


def test_digest_reports_the_silver_rebuild(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    _write_silver_summary(log_dir, "2026-07-02")

    body = build_digest(date(2026, 7, 2), log_dir, tmp_path)

    assert "Silver rebuild:" in body
    assert "revision=3" in body
    assert "trimmed=2" in body
    assert "withheld" not in body  # a clean rebuild raises no window alarm


def test_digest_flags_window_regressions_loudly(tmp_path):
    """A withheld symbol means new data cost published history. Nothing else surfaces
    it — the run still exits 0, because the symbol keeps serving its old window."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    _write_silver_summary(log_dir, "2026-07-02", window_regressions=4)

    body = build_digest(date(2026, 7, 2), log_dir, tmp_path)

    assert "⚠ 4 symbol(s) withheld" in body
    assert "cost published history" in body


def test_digest_silver_section_tolerates_a_missing_rebuild(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    body = build_digest(date(2026, 7, 2), log_dir, tmp_path)

    assert "Silver rebuild:\n  (not found)" in body


def _write_quality_warnings(log_dir: Path, run: str, lines: list[str]) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    body = "=== Done equity 2026-07-27T02:00:00Z ===\n" + "\n".join(lines) + "\n"
    (log_dir / f"daily_update_{run}.log").write_text(body, encoding="utf-8")


def test_the_digest_reports_a_failed_quality_job(tmp_path):
    """coverage timed out at its 600s budget every night from 2026-07-07 to at
    least 07-27. `_spawn_post_success_quality` swallows these into a WARNING on
    purpose — they must never flip a successful run to failure — but nothing
    counted them, so nobody found out for three weeks."""
    log_dir = tmp_path / "logs"
    _write_quality_warnings(
        log_dir,
        "2026-07-27",
        ["WARNING: coverage report failed: Command '[...]' timed out after 600 seconds"],
    )

    out = build_digest(date(2026, 7, 27), log_dir, tmp_path)

    assert "Quality jobs: 1 FAILED" in out
    assert "coverage report" in out
    assert "timed out after 600 seconds" in out


def test_the_digest_says_so_when_every_quality_job_passed(tmp_path):
    log_dir = tmp_path / "logs"
    _write_quality_warnings(log_dir, "2026-07-27", [])

    assert "Quality jobs: all green" in build_digest(date(2026, 7, 27), log_dir, tmp_path)


def test_one_job_failing_on_every_pass_is_reported_once(tmp_path):
    log_dir = tmp_path / "logs"
    _write_quality_warnings(log_dir, "2026-07-27", ["WARNING: coverage report failed: boom"] * 3)

    out = build_digest(date(2026, 7, 27), log_dir, tmp_path)

    assert "Quality jobs: 1 FAILED" in out
    assert out.count("coverage report") == 1


def test_several_failed_quality_jobs_are_all_reported(tmp_path):
    log_dir = tmp_path / "logs"
    _write_quality_warnings(
        log_dir,
        "2026-07-27",
        [
            "WARNING: coverage report failed: timed out after 600 seconds",
            "WARNING: nightly digest failed: exit_code=1",
        ],
    )

    out = build_digest(date(2026, 7, 27), log_dir, tmp_path)

    assert "Quality jobs: 2 FAILED" in out
    assert "nightly digest" in out


def test_an_unrelated_warning_is_not_a_quality_job(tmp_path):
    """The alert-delivery WARNING uses the same 'WARNING: ... ' prefix but is
    not a post-success quality job."""
    log_dir = tmp_path / "logs"
    _write_quality_warnings(log_dir, "2026-07-27", ["WARNING: some other thing happened"])

    assert "Quality jobs: all green" in build_digest(date(2026, 7, 27), log_dir, tmp_path)


_Usage = collections.namedtuple("_Usage", "total used free")
_GIB = 1024**3


class TestTheDigestSurvivesDegenerateInputs:
    """build_digest promises never to raise on a missing input.

    A section that throws takes the whole nightly email with it — the one
    artifact a human actually reads. The per-section cases now live in
    tests/test_status.py, where the sections themselves do.
    """

    def test_build_digest_never_raises_on_a_degenerate_filesystem(self, tmp_path, monkeypatch):
        monkeypatch.setattr(nightly_digest.shutil, "disk_usage", lambda path: _Usage(0, 0, 0))
        out = build_digest(date(2026, 8, 9), tmp_path / "logs", tmp_path)
        assert "Livewire nightly digest" in out
