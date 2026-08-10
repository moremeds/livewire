"""Tests for the graded status surface."""

from __future__ import annotations

import collections
import inspect
import json
from datetime import date
from pathlib import Path

from livewire_scripts import status
from livewire_scripts.daily_outcomes import SUMMARY_PREFIX
from livewire_scripts.status import (
    Section,
    Verdict,
    _coverage_section,
    _disk_section,
    _outcomes_section,
    _phases_section,
    _quality_jobs_section,
    _silver_section,
    collect,
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
    for section in collect(date(2026, 8, 10), tmp_path, tmp_path):
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
    sections = collect(date(2026, 8, 10), tmp_path, tmp_path)
    assert len(sections) == 6
    assert all(isinstance(s, Section) for s in sections)


def test_collect_never_raises_when_a_check_explodes(tmp_path: Path, monkeypatch) -> None:
    def _boom(*_args, **_kwargs):
        raise RuntimeError("footer read exploded")

    monkeypatch.setattr("livewire_scripts.status._disk_section", _boom)
    sections = collect(date(2026, 8, 10), tmp_path, tmp_path)
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
