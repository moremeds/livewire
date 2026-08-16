"""Tests for the graded status surface."""

from __future__ import annotations

import collections
import inspect
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from livewire_scripts import status
from livewire_scripts.daily_outcomes import SUMMARY_PREFIX
from livewire_scripts.status import (
    _LAUNCHD_JOBS,
    Section,
    Verdict,
    _coverage_section,
    _disk_section,
    _duckdb_section,
    _launchd_section,
    _outcomes_section,
    _phases_section,
    _quality_jobs_section,
    _silver_section,
    _undelivered_section,
    collect,
    main,
    render,
)

_SILVER = (
    'SUMMARY_JSON {"revision":25,"rebuilt":2,"unchanged":13076,"trimmed":254,"failed":233,"window_regressions":39}'
)
_EQUITY_OK = (
    'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"ib",'
    '"target_date":"2026-08-07","updated":13000,"no_trade":277,"partial":0,'
    '"errors":0,"bars_inserted":13000,"validation_issues":0,"top_errors":[]}'
)


def test_a_missing_run_log_is_bad_not_ok(tmp_path: Path) -> None:
    """The defect being fixed: '(not found)' used to render like a healthy line.
    A log that does not exist means the run appears never to have happened."""
    section = _outcomes_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.BAD
    assert section.fix is not None


def test_a_log_with_no_summary_is_unknown(tmp_path: Path) -> None:
    """Distinct from the above: the job ran and wrote something, but produced
    no machine-readable outcome. Cannot measure is not the same as did not run."""
    (tmp_path / "daily_update_2026-08-10.log").write_text("starting...\n", encoding="utf-8")
    section = _outcomes_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.UNKNOWN
    assert section.verdict is not Verdict.OK


def test_outcomes_with_no_errors_is_ok(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(_EQUITY_OK, encoding="utf-8")
    assert _outcomes_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_a_total_wipeout_is_bad_and_one_flaky_symbol_is_only_warn(tmp_path: Path) -> None:
    """updated=0 with 13,311 errors must not render at the same severity as one
    bad warrant. resolve_exit_code already encodes the measured rule."""
    wipeout = (
        'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"ib",'
        '"target_date":"2026-08-07","updated":0,"no_trade":0,"partial":0,'
        '"errors":13311,"bars_inserted":0,"validation_issues":0,"top_errors":[]}'
    )
    (tmp_path / "daily_update_2026-08-10.log").write_text(wipeout, encoding="utf-8")
    assert _outcomes_section("2026-08-10", tmp_path).verdict is Verdict.BAD

    one_bad = (
        'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"ib",'
        '"target_date":"2026-08-07","updated":13000,"no_trade":277,"partial":0,'
        '"errors":1,"bars_inserted":13000,"validation_issues":0,"top_errors":[]}'
    )
    (tmp_path / "daily_update_2026-08-10.log").write_text(one_bad, encoding="utf-8")
    assert _outcomes_section("2026-08-10", tmp_path).verdict is Verdict.WARN


def test_every_non_ok_section_carries_a_runnable_fix(tmp_path: Path) -> None:
    """Pain point 3. A fix with an unsubstituted <placeholder> is not a fix."""
    for section in collect(
        date(2026, 8, 10), tmp_path, tmp_path, runner=_fake_launchctl, database=tmp_path / "absent.duckdb"
    ):
        if section.verdict is Verdict.OK:
            continue
        assert section.fix, f"{section.name} is {section.verdict} with no fix"
        assert "<" not in section.fix, f"{section.name} fix has an unsubstituted placeholder"


def test_a_failed_phase_is_bad_and_a_degraded_phase_is_warn(tmp_path: Path) -> None:
    failed = (
        'SUMMARY_JSON {"job":"daily_backfill","phases":[{"label":"equity","exit":1,'
        '"duration_s":9}],"failed":["equity"],"degraded":[]}'
    )
    (tmp_path / "intraday_catchup_2026-08-10.log").write_text(failed, encoding="utf-8")
    assert _phases_section("2026-08-10", tmp_path).verdict is Verdict.BAD

    degraded = (
        'SUMMARY_JSON {"job":"daily_backfill","phases":[{"label":"futures","exit":86,'
        '"duration_s":9}],"failed":[],"degraded":["futures"]}'
    )
    (tmp_path / "intraday_catchup_2026-08-10.log").write_text(degraded, encoding="utf-8")
    assert _phases_section("2026-08-10", tmp_path).verdict is Verdict.WARN


def test_a_missing_phases_log_is_bad_and_a_malformed_one_is_unknown(tmp_path: Path) -> None:
    assert _phases_section("2026-08-10", tmp_path).verdict is Verdict.BAD
    (tmp_path / "intraday_catchup_2026-08-10.log").write_text("starting...\n", encoding="utf-8")
    assert _phases_section("2026-08-10", tmp_path).verdict is Verdict.UNKNOWN


def test_all_phases_green_is_ok(tmp_path: Path) -> None:
    ok = (
        'SUMMARY_JSON {"job":"daily_backfill","phases":[{"label":"equity","exit":0,'
        '"duration_s":9}],"failed":[],"degraded":[]}'
    )
    (tmp_path / "intraday_catchup_2026-08-10.log").write_text(ok, encoding="utf-8")
    assert _phases_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_withheld_windows_are_a_warning(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER, encoding="utf-8")
    section = _silver_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.WARN
    assert any("39" in line for line in section.lines)


def test_coverage_below_the_threshold_is_bad(tmp_path: Path) -> None:
    """The real 2026-08-07 line: warehouse-wide zero, previously rendered plain."""
    (tmp_path / "coverage_2026-08-10.log").write_text(
        "2026-08-10 coverage: 1d=0/13311 (0.00%) 1m=0/14613 (0.00%) "
        "1h=0/14613 (0.00%) 5m=0/14613 (0.00%) 30m=0/14613 (0.00%)\n",
        encoding="utf-8",
    )
    section = _coverage_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.BAD
    assert section.fix is not None


def test_coverage_above_the_threshold_is_ok(tmp_path: Path) -> None:
    (tmp_path / "coverage_2026-08-10.log").write_text(
        "2026-08-10 coverage: 1d=13100/13141 (99.69%) 1m=14000/14100 (99.29%) "
        "1h=14000/14100 (99.29%) 5m=14000/14100 (99.29%) 30m=14000/14100 (99.29%)\n",
        encoding="utf-8",
    )
    assert _coverage_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_a_stale_coverage_log_is_bad_even_when_the_numbers_are_green(tmp_path: Path) -> None:
    """A dead detector is worse than a red one: it reads green forever."""
    (tmp_path / "coverage_2026-07-01.log").write_text(
        "2026-07-01 coverage: 1d=13100/13141 (99.69%) 1m=14000/14100 (99.29%) "
        "1h=14000/14100 (99.29%) 5m=14000/14100 (99.29%) 30m=14000/14100 (99.29%)\n",
        encoding="utf-8",
    )
    assert _coverage_section("2026-08-10", tmp_path).verdict is Verdict.BAD


def test_a_timeframe_with_no_files_at_all_is_not_a_gap(tmp_path: Path) -> None:
    """total == 0 is ratio 1.0, matching CoverageResult.ratio. An asset class
    with no files is not a coverage failure, and dividing by it would raise."""
    (tmp_path / "coverage_2026-08-10.log").write_text(
        "2026-08-10 coverage: 1d=13100/13141 (99.69%) 1m=0/0 (0.00%)\n", encoding="utf-8"
    )
    assert _coverage_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_disk_below_the_reserve_is_bad(tmp_path: Path, monkeypatch) -> None:
    class _FullVolume:
        total = 100 * 1024**3
        used = 96 * 1024**3
        free = 4 * 1024**3

    monkeypatch.setattr("livewire_scripts.status.shutil.disk_usage", lambda _p: _FullVolume())
    assert _disk_section(tmp_path).verdict is Verdict.BAD


def test_disk_between_one_and_two_reserves_is_only_a_warning(tmp_path: Path, monkeypatch) -> None:
    class _TightVolume:
        total = 228 * 1024**3
        used = 188 * 1024**3
        free = 40 * 1024**3

    monkeypatch.setattr("livewire_scripts.status.shutil.disk_usage", lambda _p: _TightVolume())
    assert _disk_section(tmp_path).verdict is Verdict.WARN


def test_quality_jobs_all_green_is_ok(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text("nothing wrong\n", encoding="utf-8")
    assert _quality_jobs_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_a_failed_quality_job_is_a_warning(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(
        "WARNING: coverage failed: timed out after 1800 seconds\n", encoding="utf-8"
    )
    assert _quality_jobs_section("2026-08-10", tmp_path).verdict is Verdict.WARN


def test_section_is_frozen() -> None:
    section = Section(name="x", verdict=Verdict.OK, lines=["x"])
    assert section.fix is None


def test_unknown_outranks_ok_so_a_run_verdict_can_never_be_green_on_a_gap() -> None:
    """The ordering is the mechanism, not the documentation. max() over a run
    of sections must not report OK when one of them could not measure."""
    assert max(Verdict.OK, Verdict.UNKNOWN) is Verdict.UNKNOWN
    assert max(Verdict.OK, Verdict.UNKNOWN, Verdict.WARN, Verdict.BAD) is Verdict.BAD


def test_collect_returns_a_section_per_check(tmp_path: Path) -> None:
    sections = collect(
        date(2026, 8, 10), tmp_path, tmp_path, runner=_fake_launchctl, database=tmp_path / "absent.duckdb"
    )
    assert len(sections) == 9
    assert all(isinstance(s, Section) for s in sections)


def test_collect_never_raises_when_a_check_explodes(tmp_path: Path, monkeypatch) -> None:
    def _boom(*_args, **_kwargs):
        raise RuntimeError("footer read exploded")

    monkeypatch.setattr("livewire_scripts.status._disk_section", _boom)
    sections = collect(
        date(2026, 8, 10), tmp_path, tmp_path, runner=_fake_launchctl, database=tmp_path / "absent.duckdb"
    )
    disk = [s for s in sections if s.name == "Disk"]
    assert len(disk) == 1
    assert disk[0].verdict is Verdict.UNKNOWN
    assert "footer read exploded" in "\n".join(disk[0].lines)


def test_the_digest_and_the_terminal_see_the_same_checks(tmp_path: Path) -> None:
    """The defect this replaces: build_digest had its own hard-coded list, so
    every check added later reached the terminal and never the email."""
    from livewire_scripts import nightly_digest

    source = inspect.getsource(nightly_digest.build_digest)
    assert "collect(" in source
    for name in ("_outcomes_section", "_coverage_section", "_disk_section"):
        assert name not in source, "build_digest must not enumerate sections itself"


class TestTheDigestFindsCoverageOnAnySchedule:
    """Coverage runs on its own schedule now, so its log will not match the run date.

    Requiring an exact filename match would make the digest report
    "(not found)" forever — the same silence that hid the dead detector for
    four weeks. Read the newest log and print the date it actually measured.
    """

    def test_a_coverage_log_from_another_date_is_found(self, tmp_path):
        (tmp_path / "coverage_2026-08-06.log").write_text(
            "2026-08-06 coverage: 1d=13265/13270 (99.96%)\n", encoding="utf-8"
        )
        (tmp_path / "coverage_2026-08-07.log").write_text(
            "2026-08-07 coverage: 1d=13300/13311 (99.92%)\n", encoding="utf-8"
        )

        lines = _coverage_section("2026-08-09", tmp_path).lines

        assert any("2026-08-07" in line for line in lines), "the newest log wins"
        assert not any("2026-08-06" in line for line in lines)
        assert "(not found)" not in "".join(lines)

    def test_no_coverage_logs_at_all_still_says_not_found(self, tmp_path):
        assert "  (not found)" in _coverage_section("2026-08-09", tmp_path).lines

    def test_an_empty_coverage_log_is_skipped_for_the_next_newest(self, tmp_path):
        """A crashed run leaves a stub; it must not mask a real measurement."""
        (tmp_path / "coverage_2026-08-06.log").write_text(
            "2026-08-06 coverage: 1d=13265/13270 (99.96%)\n", encoding="utf-8"
        )
        (tmp_path / "coverage_2026-08-07.log").write_text("", encoding="utf-8")
        lines = _coverage_section("2026-08-09", tmp_path).lines
        assert any("2026-08-06" in line for line in lines)

    def test_only_an_empty_log_is_not_found(self, tmp_path):
        (tmp_path / "coverage_2026-08-07.log").write_text("", encoding="utf-8")
        assert "  (not found)" in _coverage_section("2026-08-09", tmp_path).lines

    def test_a_recent_log_does_not_warn(self, tmp_path):
        (tmp_path / "coverage_2026-08-08.log").write_text(
            "2026-08-08 coverage: 1d=13300/13311 (99.92%)\n", encoding="utf-8"
        )
        assert not any("⚠" in line for line in _coverage_section("2026-08-09", tmp_path).lines)

    def test_a_stale_log_warns_that_the_job_may_be_dead(self, tmp_path):
        # Decoupling the schedules must not buy a silent detector back. If the
        # coverage job stops firing, the newest log stops advancing and the
        # digest would otherwise print a reassuring line indefinitely.
        (tmp_path / "coverage_2026-06-17.log").write_text(
            "2026-06-17 coverage: 1d=13100/13141 (99.69%)\n", encoding="utf-8"
        )
        lines = _coverage_section("2026-08-09", tmp_path).lines
        assert any("⚠" in line and "coverage job" in line for line in lines)
        assert any("2026-06-17" in line for line in lines), "still shows what it measured"

    def test_an_unparseable_filename_does_not_raise(self, tmp_path):
        (tmp_path / "coverage_rotated.log").write_text("something\n", encoding="utf-8")
        section = _coverage_section("2026-08-09", tmp_path)
        assert "  something" in section.lines
        assert not any("⚠" in line for line in section.lines)
        assert section.verdict is Verdict.UNKNOWN


_Usage = collections.namedtuple("_Usage", "total used free")
_GIB = 1024**3


class TestTheDiskCheckWatchesBothVolumes:
    """One symlink was enough to silently swap the monitored volume.

    data-lake points at an external 13 TiB volume, so the nightly line read
    "6752.4 GiB free (48% used)" while the volume that actually holds releases,
    logs and the venv was under its own reserve with nothing reporting it.
    """

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

        assert "6600.0 GiB" in text
        assert "14.0 GiB" in text
        assert "⚠" in text, "the warehouse volume is under reserve and must warn"
        assert section.verdict is Verdict.BAD, "14 GiB is under the 25 GiB reserve"

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
        monkeypatch.setattr(
            status.shutil,
            "disk_usage",
            lambda path: _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB),
        )
        lines = _disk_section(shared, shared).lines
        assert len([ln for ln in lines if ln.startswith("Disk")]) == 1

    def test_an_unreadable_path_is_skipped_not_fatal(self, tmp_path, monkeypatch):
        lake, warehouse = self._dirs(tmp_path)

        def fake_usage(path):
            if Path(path) == lake:
                raise OSError("volume not mounted")
            return _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB)

        monkeypatch.setattr(status.shutil, "disk_usage", fake_usage)
        lines = _disk_section(lake, warehouse).lines
        assert len([ln for ln in lines if ln.startswith("Disk")]) == 1
        assert "128.0 GiB" in lines[0]


class TestTheSectionsSurviveDegenerateInputs:
    """collect() promises never to raise on a missing input.

    A section that throws takes the whole nightly email with it — the one
    artifact a human actually reads.
    """

    def test_a_coverage_log_with_a_blank_first_line_is_skipped(self, tmp_path):
        # A partially-flushed write. Printing "  " would read as a healthy
        # coverage line while carrying no measurement at all.
        (tmp_path / "coverage_2026-08-06.log").write_text(
            "2026-08-06 coverage: 1d=13265/13270 (99.96%)\n", encoding="utf-8"
        )
        (tmp_path / "coverage_2026-08-08.log").write_text("\n\n", encoding="utf-8")

        lines = _coverage_section("2026-08-09", tmp_path).lines

        assert not any(line.strip() == "" for line in lines[1:]), "no blank measurement line"
        assert any("2026-08-06" in line for line in lines), "falls through to the real one"

    def test_a_leading_blank_line_does_not_hide_the_measurement(self, tmp_path):
        (tmp_path / "coverage_2026-08-08.log").write_text(
            "\n2026-08-08 coverage: 1d=13300/13311 (99.92%)\n", encoding="utf-8"
        )
        lines = _coverage_section("2026-08-09", tmp_path).lines
        assert any("13300/13311" in line for line in lines)

    def test_a_zero_total_filesystem_does_not_raise(self, tmp_path, monkeypatch):
        monkeypatch.setattr(status.shutil, "disk_usage", lambda path: _Usage(0, 0, 0))
        section = _disk_section(tmp_path, tmp_path)
        assert section.lines == ["Disk: (unavailable)"]
        assert section.verdict is Verdict.UNKNOWN

    def test_one_usable_volume_still_reports_when_the_other_is_degenerate(self, tmp_path, monkeypatch):
        lake = tmp_path / "lake"
        warehouse = tmp_path / "warehouse"
        lake.mkdir()
        warehouse.mkdir()

        def fake_usage(path):
            if Path(path) == lake:
                return _Usage(0, 0, 0)
            return _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB)

        monkeypatch.setattr(status.shutil, "disk_usage", fake_usage)
        assert _disk_section(lake, warehouse).lines == ["Disk: 128.0 GiB free (44% used)"]


class TestTheDigestDistinguishesDegradedFromFailed:
    """A `degraded` field nobody renders changes nothing an operator sees.

    The orchestrator can return 0 while the nightly email still reads
    `FAILED (exit 86)` — the exit code and the only human-facing artifact
    disagreeing about the same run.
    """

    @staticmethod
    def _write(tmp_path, summary):
        (tmp_path / "intraday_catchup_2026-08-08.log").write_text(
            SUMMARY_PREFIX + json.dumps(summary) + "\n", encoding="utf-8"
        )

    def test_an_ib_phase_at_86_reads_degraded_not_failed(self, tmp_path):
        self._write(
            tmp_path,
            {
                "job": "daily_backfill",
                "target_date": "2026-08-07",
                "phases": [
                    {"label": "daily_backfill_equity_day_aggs", "exit": 0, "duration_s": 41.0},
                    {
                        "label": "daily_backfill_intraday_30m_volatility",
                        "exit": 86,
                        "duration_s": 0.2,
                    },
                ],
                "failed": [],
                "degraded": ["daily_backfill_intraday_30m_volatility"],
            },
        )

        text = "\n".join(_phases_section("2026-08-08", tmp_path).lines)

        assert "DEGRADED (IB down)" in text
        assert "FAILED" not in text, "a Gateway outage must not read as a failure"

    def test_a_real_failure_still_reads_failed(self, tmp_path):
        self._write(
            tmp_path,
            {
                "job": "daily_backfill",
                "target_date": "2026-08-07",
                "phases": [{"label": "daily_backfill_equity_day_aggs", "exit": 1, "duration_s": 3.0}],
                "failed": ["daily_backfill_equity_day_aggs"],
                "degraded": [],
            },
        )
        assert "FAILED (exit 1)" in "\n".join(_phases_section("2026-08-08", tmp_path).lines)

    def test_a_summary_without_the_field_is_unchanged(self, tmp_path):
        """Old logs predate `degraded` and must still render exactly as before."""
        self._write(
            tmp_path,
            {
                "job": "daily_backfill",
                "target_date": "2026-08-07",
                "phases": [{"label": "daily_backfill_equity_day_aggs", "exit": 2, "duration_s": 3.0}],
                "failed": ["daily_backfill_equity_day_aggs"],
            },
        )
        assert "FAILED (exit 2)" in "\n".join(_phases_section("2026-08-08", tmp_path).lines)


def test_render_shows_the_fix_for_anything_not_ok() -> None:
    out = render([Section(name="Coverage", verdict=Verdict.BAD, lines=["Coverage:"], fix="run me")])
    assert "run me" in out
    assert "BAD" in out


def test_render_omits_the_fix_when_ok() -> None:
    out = render([Section(name="Disk", verdict=Verdict.OK, lines=["Disk: fine"], fix="run me")])
    assert "run me" not in out


def test_render_names_a_section_that_produced_no_lines() -> None:
    out = render([Section(name="Coverage", verdict=Verdict.UNKNOWN)])
    assert "Coverage" in out


def test_render_survives_markup_in_log_derived_text(capsys) -> None:
    """Measured: a log line containing "[/]" raises MarkupError and takes the
    whole command down; "[bold red]" is silently eaten as a style."""
    from rich.console import Console

    hostile = Section(
        name="Quality jobs",
        verdict=Verdict.WARN,
        lines=["Quality jobs: 1 FAILED", "  coverage failed: timed out [/] after [bold red]1800[/bold red]s"],
    )
    Console().print(render([hostile]))
    out = capsys.readouterr().out
    assert "[/]" in out
    assert "1800" in out


def test_main_exits_zero_even_when_everything_is_broken(tmp_path: Path, capsys, monkeypatch) -> None:
    """A nonzero exit invites someone to schedule this, and every stale
    launchctl red would then page."""
    # main() builds its own collect() arguments, so it cannot be handed a fake
    # runner. Without this the unit test shells out to the operator's real
    # launchctl and its verdict depends on the machine it runs on.
    monkeypatch.setattr("livewire_scripts.status.subprocess.run", _fake_launchctl)
    # Same reason for the catalog: main() cannot be handed a database= either,
    # so without this the unit test opens the operator's real analytics.duckdb.
    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _no_catalog)
    rc = main(["--run-date", "2026-08-10", "--log-dir", str(tmp_path), "--data-lake", str(tmp_path)])
    assert rc == 0
    assert "Livewire status" in capsys.readouterr().out


def test_a_fix_command_survives_a_narrow_terminal_in_one_piece(tmp_path: Path, capsys, monkeypatch) -> None:
    """The fixes exist to be COPIED. rich's default word-wrap inserts real
    newlines at the console width, so a long command came back as two lines and
    pasted as two commands — pain point 3 surviving its own cure."""
    # 101 characters — comfortably past the 80-column default rich falls back to
    # when stdout is not a TTY, which is exactly the case under pytest capture.
    monkeypatch.setattr("livewire_scripts.status.subprocess.run", _fake_launchctl)
    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _no_catalog)
    assert len(status._SILVER_FIX) > 80
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER, encoding="utf-8")
    main(["--run-date", "2026-08-10", "--log-dir", str(tmp_path), "--data-lake", str(tmp_path)])
    out = capsys.readouterr().out
    assert status._SILVER_FIX in out


def _no_catalog(_db):
    raise FileNotFoundError("analytics.duckdb")


def _fake_launchctl(_cmd, **_kw):
    return SimpleNamespace(stdout="".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS), returncode=0)


def test_a_job_that_is_not_loaded_is_bad() -> None:
    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout="-\t0\tcom.livewire.daily-update\n", returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.BAD
    assert "com.livewire.coverage" in "\n".join(section.lines)


def test_a_nonzero_exit_is_capped_at_warn() -> None:
    """launchctl reports the LAST exit with no timestamp. Today's watchdog=1 is
    residue from a run that predates the fix already in production. Calling that
    BAD is the fastest way to make the whole surface ignorable."""
    stdout = "".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS)
    stdout = stdout.replace("-\t0\tcom.livewire.intraday-catchup", "-\t86\tcom.livewire.intraday-catchup")

    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout=stdout, returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.WARN
    assert "no timestamp" in "\n".join(section.lines)


def test_all_jobs_green_is_ok() -> None:
    assert _launchd_section(runner=_fake_launchctl).verdict is Verdict.OK


def test_launchctl_missing_is_unknown() -> None:
    def _runner(_cmd, **_kw):
        raise FileNotFoundError("launchctl")

    assert _launchd_section(runner=_runner).verdict is Verdict.UNKNOWN


def test_an_empty_or_absent_queue_is_ok(tmp_path: Path) -> None:
    assert _undelivered_section(tmp_path).verdict is Verdict.OK
    (tmp_path / "quality_alerts_undelivered").mkdir()
    assert _undelivered_section(tmp_path).verdict is Verdict.OK


def test_any_undelivered_alert_is_a_warning(tmp_path: Path) -> None:
    """4,408 of these were on disk, newest 2026-08-02, and appeared in no
    report anywhere — the channel that would report it was the broken one."""
    queue = tmp_path / "quality_alerts_undelivered"
    queue.mkdir()
    (queue / "2026-08-02T05-54-08Z_ib_CL_202612.html").write_text("<p>x</p>", encoding="utf-8")
    section = _undelivered_section(tmp_path)
    assert section.verdict is Verdict.WARN
    assert "1" in "\n".join(section.lines)
    assert section.fix is not None


def test_the_scheduled_job_queue_is_counted_too(tmp_path: Path) -> None:
    """The repo keeps TWO queues, deliberately and separately. The one this
    used to omit is the JOB FAILURE page — the more serious of the pair."""
    queue = tmp_path / "alerts_undelivered"
    queue.mkdir()
    (queue / "2026-08-08T06-00-00Z_daily_update.html").write_text("<p>x</p>", encoding="utf-8")
    section = _undelivered_section(tmp_path)
    assert section.verdict is Verdict.WARN
    assert "alerts_undelivered" in "\n".join(section.lines)


def test_the_quality_queue_honours_its_env_knob(tmp_path: Path, monkeypatch) -> None:
    """MDW_UNDELIVERED_DIR relocates the quality queue; the section must follow
    it rather than assuming the default sits under log_dir."""
    elsewhere = tmp_path / "somewhere-else"
    elsewhere.mkdir()
    (elsewhere / "2026-08-02T05-54-08Z_ib_CL_202612.html").write_text("<p>x</p>", encoding="utf-8")
    monkeypatch.setenv("MDW_UNDELIVERED_DIR", str(elsewhere))
    section = _undelivered_section(tmp_path / "logs")
    assert section.verdict is Verdict.WARN
    assert "somewhere-else" in "\n".join(section.lines)


def test_duckdb_check_is_unknown_when_duckdb_is_not_installed(monkeypatch) -> None:
    """~/market-warehouse/.venv genuinely has no duckdb. A status command that
    cannot start in one of the three real environments is worthless.

    Patches the `_coverage_headline` seam rather than `builtins.__import__` —
    replacing __import__ affects every import for the duration of the test,
    including pytest's own, and a status check is not worth that blast radius.
    """

    def _no_duckdb(_db):
        raise ImportError("No module named 'duckdb'")

    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _no_duckdb)
    section = _duckdb_section(date(2026, 8, 10))
    assert section.verdict is Verdict.UNKNOWN
    assert "duckdb" in "\n".join(section.lines).lower()


def test_duckdb_check_is_bad_when_the_table_lags_by_more_than_three_sessions(monkeypatch) -> None:
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {"bronze_equity_1d": (13311, date(2026, 7, 1))},
    )
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.BAD


def test_duckdb_check_is_ok_when_current(monkeypatch) -> None:
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {"bronze_equity_1d": (13311, date(2026, 8, 10))},
    )
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.OK


def test_duckdb_grades_the_oldest_view_not_the_freshest(monkeypatch) -> None:
    """max(dates) would let one current view green the whole check while
    bronze_equity_1d sat frozen — a fact nobody grades, one level down."""
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {
            "bronze_equity_1d": (13311, date(2026, 7, 1)),
            "silver_equity_1d": (13076, date(2026, 8, 10)),
        },
    )
    section = _duckdb_section(date(2026, 8, 10))
    assert section.verdict is Verdict.BAD
    assert "bronze_equity_1d" in section.lines[1], "the laggard is named in the headline detail"


def test_duckdb_check_is_unknown_when_never_built(monkeypatch) -> None:
    def _absent(_db):
        raise FileNotFoundError("analytics.duckdb")

    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _absent)
    section = _duckdb_section(date(2026, 8, 10))
    assert section.verdict is Verdict.UNKNOWN
    assert section.fix is not None


def test_duckdb_check_is_unknown_when_the_table_has_no_dated_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {"bronze_equity_1d": (0, None)},
    )
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.UNKNOWN


def test_duckdb_one_session_behind_is_still_ok(monkeypatch) -> None:
    """Friday measured against Monday is one session behind but three calendar
    days — a calendar-day rule would flag every Monday morning."""
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {"bronze_equity_1d": (13311, date(2026, 8, 7))},
    )
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.OK


_SILVER_CLEAN = (
    'SUMMARY_JSON {"revision":25,"rebuilt":2,"unchanged":13076,"trimmed":254,"failed":233,"window_regressions":0}'
)
_SILVER_WORSE = (
    'SUMMARY_JSON {"revision":26,"rebuilt":2,"unchanged":13076,"trimmed":254,"failed":301,"window_regressions":0}'
)


def test_silver_failed_rising_is_a_warning(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-09.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER_WORSE, encoding="utf-8")
    section = _silver_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.WARN
    assert "+68" in "\n".join(section.lines)


def test_silver_failed_flat_is_ok_and_still_prints_the_absolute(tmp_path: Path) -> None:
    """failed=233 is not graded on an absolute line — there is no measured
    basis for one, and inventing a threshold would be fabrication."""
    (tmp_path / "daily_update_2026-08-09.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    section = _silver_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.OK
    assert "failed=233" in "\n".join(section.lines)


def test_silver_without_a_baseline_is_unknown(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    assert _silver_section("2026-08-10", tmp_path).verdict is Verdict.UNKNOWN


def test_the_watchdog_log_is_not_mistaken_for_a_silver_baseline(tmp_path: Path) -> None:
    """`daily_update_*.log` also matches `daily_update_watchdog_<date>.log`,
    a different job's log. It sorts FIRST under reverse=True and only fell out
    of a `>=` string comparison because "w" happens to exceed "2"."""
    (tmp_path / "daily_update_watchdog_2026-08-11.log").write_text(_SILVER_WORSE, encoding="utf-8")
    (tmp_path / "daily_update_2026-08-09.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    assert _silver_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_a_missing_baseline_never_downgrades_a_known_regression(tmp_path: Path) -> None:
    """UNKNOWN ranks BELOW WARN. A baseline we cannot find makes the DELTA
    unmeasurable — it does not un-measure the 39 withheld symbols."""
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER, encoding="utf-8")
    assert _silver_section("2026-08-10", tmp_path).verdict is Verdict.WARN


def test_duckdb_two_sessions_behind_is_a_warning(monkeypatch) -> None:
    """The state the real warehouse was in on 2026-08-10: bronze_cmdty_1d at
    2026-08-05, three sessions back. Behind but not yet dead."""
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {"bronze_equity_1d": (13311, date(2026, 8, 5))},
    )
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.WARN


def test_a_coverage_line_that_carries_no_timeframes_is_unknown(tmp_path: Path) -> None:
    """A dated log whose measurement does not parse. Reporting OK would be the
    dead-detector shape with an extra step."""
    (tmp_path / "coverage_2026-08-10.log").write_text("coverage run aborted\n", encoding="utf-8")
    section = _coverage_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.UNKNOWN
    assert "did not parse" in (section.fix or "")


def test_an_unparseable_run_date_yields_no_silver_baseline(tmp_path: Path) -> None:
    assert status._previous_silver_summary("not-a-date", tmp_path) is None


def test_an_unreadable_alert_queue_is_unknown_not_ok(tmp_path: Path, monkeypatch) -> None:
    """A queue we cannot read holds an unknown number of undelivered pages."""
    queue = tmp_path / "quality_alerts_undelivered"
    queue.mkdir()
    real_iterdir = Path.iterdir

    def _denied(self):
        if self == queue:
            raise PermissionError("Operation not permitted")
        return real_iterdir(self)

    monkeypatch.setattr(Path, "iterdir", _denied)
    section = _undelivered_section(tmp_path)
    assert section.verdict is Verdict.UNKNOWN
    assert "unreadable" in "\n".join(section.lines)


def test_an_uninstalled_plist_says_render_it_not_launchctl_load(tmp_path: Path, monkeypatch) -> None:
    """The repo ships `.plist.example` templates. `launchctl load` on a label
    with no rendered plist fails with a message that explains nothing."""
    monkeypatch.setattr(Path, "home", classmethod(lambda _cls: tmp_path))

    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout="-\t0\tcom.livewire.daily-update\n", returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.BAD
    assert "render the plist first" in (section.fix or "")


def test_an_installed_but_unloaded_plist_says_launchctl_load(tmp_path: Path, monkeypatch) -> None:
    agents = tmp_path / "Library/LaunchAgents"
    agents.mkdir(parents=True)
    for label in _LAUNCHD_JOBS:
        (agents / f"{label}.plist").write_text("<plist/>", encoding="utf-8")
    monkeypatch.setattr(Path, "home", classmethod(lambda _cls: tmp_path))

    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout="-\t0\tcom.livewire.daily-update\n", returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.BAD
    assert (section.fix or "").startswith("launchctl load ")
    # EVERY missing job, not just the first: an operator who runs the printed
    # command and sees the section still red learns to distrust the surface.
    for label in _LAUNCHD_JOBS[1:]:
        assert label in (section.fix or "")
