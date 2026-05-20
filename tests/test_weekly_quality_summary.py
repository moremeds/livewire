"""Tests for scripts/weekly_quality_summary.py."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from livewire_scripts.weekly_quality_summary import (
    CoverageEntry,
    _iso_week_start,
    detect_churn,
    detect_persistent_gaps,
    load_week,
    main,
    parse_coverage_log,
    render_markdown,
    write_summary,
)


def _write_log(log_dir: Path, day: date, header: str, missing_lines: list[str] = ()) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"coverage_{day:%Y-%m-%d}.log"
    path.write_text(header + "\n" + "\n".join(missing_lines) + ("\n" if missing_lines else ""))
    return path


def _spec_header(d: date, *, present_5m: int = 1166, total: int = 1166) -> str:
    return (
        f"{d} coverage: 1d={total}/{total} (100.00%) "
        f"1m={total}/{total} (100.00%) "
        f"1h={total}/{total} (100.00%) "
        f"5m={present_5m}/{total} ({present_5m / total:.2%})"
    )


# ── parse_coverage_log ───────────────────────────────────────────────────────


class TestParseCoverageLog:
    def test_parses_clean_header(self, tmp_path):
        path = _write_log(tmp_path, date(2026, 4, 6), _spec_header(date(2026, 4, 6)))
        entry = parse_coverage_log(path)
        assert entry is not None
        assert entry.day == date(2026, 4, 6)
        assert entry.totals["1d"] == (1166, 1166)
        assert entry.totals["1m"] == (1166, 1166)
        assert entry.totals["5m"] == (1166, 1166)

    def test_parses_missing_block_with_total_suffix(self, tmp_path):
        d = date(2026, 4, 6)
        path = _write_log(
            tmp_path, d, _spec_header(d, present_5m=1158),
            ["  5m missing: NEWA, RECENT_IPO, LOWLIQ_1, ... (8 total)"],
        )
        entry = parse_coverage_log(path)
        assert entry is not None
        assert entry.missing["5m"] == ["NEWA", "RECENT_IPO", "LOWLIQ_1"]

    def test_parses_short_missing_block_without_suffix(self, tmp_path):
        d = date(2026, 4, 6)
        path = _write_log(
            tmp_path, d, _spec_header(d, present_5m=1164),
            ["  5m missing: NEWA, RECENT_IPO"],
        )
        entry = parse_coverage_log(path)
        assert entry.missing["5m"] == ["NEWA", "RECENT_IPO"]

    def test_missing_file_returns_none(self, tmp_path):
        assert parse_coverage_log(tmp_path / "nope.log") is None

    def test_unparseable_file_returns_none(self, tmp_path):
        path = tmp_path / "coverage_2026-04-06.log"
        path.write_text("garbage line\nanother garbage\n")
        assert parse_coverage_log(path) is None


# ── load_week ────────────────────────────────────────────────────────────────


class TestLoadWeek:
    def test_loads_seven_consecutive_days(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.weekly_quality_summary._LOG_DIR", tmp_path)
        start = date(2026, 3, 30)  # Monday
        for i in range(7):
            d = start + __import__("datetime").timedelta(days=i)
            _write_log(tmp_path, d, _spec_header(d))
        entries = load_week(start)
        assert len(entries) == 7

    def test_skips_missing_days(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.weekly_quality_summary._LOG_DIR", tmp_path)
        start = date(2026, 3, 30)
        for offset in (0, 2, 4):  # only 3 of 7 days have logs
            d = start + __import__("datetime").timedelta(days=offset)
            _write_log(tmp_path, d, _spec_header(d))
        entries = load_week(start)
        assert len(entries) == 3


# ── detect_persistent_gaps ───────────────────────────────────────────────────


class TestDetectPersistentGaps:
    def test_three_consecutive_flagged(self):
        entries = [
            CoverageEntry(day=date(2026, 3, 30), missing={"5m": ["LOWLIQ"]}),
            CoverageEntry(day=date(2026, 3, 31), missing={"5m": ["LOWLIQ"]}),
            CoverageEntry(day=date(2026, 4, 1), missing={"5m": ["LOWLIQ"]}),
        ]
        gaps = detect_persistent_gaps(entries)
        assert gaps == {"LOWLIQ": {"5m": 3}}

    def test_two_consecutive_not_flagged(self):
        entries = [
            CoverageEntry(day=date(2026, 3, 30), missing={"5m": ["X"]}),
            CoverageEntry(day=date(2026, 3, 31), missing={"5m": ["X"]}),
            CoverageEntry(day=date(2026, 4, 1), missing={}),
        ]
        assert detect_persistent_gaps(entries) == {}

    def test_streak_breaks_and_resumes(self):
        # 2 missing → present → 3 missing → max streak should be 3
        entries = [
            CoverageEntry(day=date(2026, 3, 30), missing={"5m": ["X"]}),
            CoverageEntry(day=date(2026, 3, 31), missing={"5m": ["X"]}),
            CoverageEntry(day=date(2026, 4, 1), missing={}),
            CoverageEntry(day=date(2026, 4, 2), missing={"5m": ["X"]}),
            CoverageEntry(day=date(2026, 4, 3), missing={"5m": ["X"]}),
            CoverageEntry(day=date(2026, 4, 4), missing={"5m": ["X"]}),
        ]
        gaps = detect_persistent_gaps(entries)
        assert gaps == {"X": {"5m": 3}}

    def test_empty_input(self):
        assert detect_persistent_gaps([]) == {}


# ── detect_churn ─────────────────────────────────────────────────────────────


class TestDetectChurn:
    def test_universe_growth_recorded(self):
        entries = [
            CoverageEntry(day=date(2026, 3, 30), totals={"1d": (1000, 1000)}),
            CoverageEntry(day=date(2026, 4, 5), totals={"1d": (1003, 1003)}),
        ]
        added, removed = detect_churn(entries)
        assert any("3 new" in a for a in added)
        assert removed == []

    def test_persistent_gap_marks_removal_candidate(self):
        entries = [
            CoverageEntry(day=date(2026, 3, 30), totals={"1d": (10, 10)}, missing={"5m": ["OLDX"]}),
            CoverageEntry(day=date(2026, 3, 31), totals={"1d": (10, 10)}, missing={"5m": ["OLDX"]}),
            CoverageEntry(day=date(2026, 4, 1), totals={"1d": (10, 10)}, missing={"5m": ["OLDX"]}),
        ]
        added, removed = detect_churn(entries)
        assert "OLDX" in removed

    def test_too_few_entries(self):
        assert detect_churn([]) == ([], [])
        assert detect_churn([CoverageEntry(day=date(2026, 3, 30))]) == ([], [])


# ── render_markdown ──────────────────────────────────────────────────────────


class TestRenderMarkdown:
    def test_renders_clean_week(self):
        entries = [
            CoverageEntry(day=date(2026, 3, 30), totals={
                "1d": (10, 10), "1m": (10, 10), "1h": (10, 10), "5m": (10, 10),
            }),
            CoverageEntry(day=date(2026, 3, 31), totals={
                "1d": (10, 10), "1m": (10, 10), "1h": (10, 10), "5m": (10, 10),
            }),
        ]
        md = render_markdown("Week 14 of 2026", entries)
        assert "# Weekly Quality Report — Week 14 of 2026" in md
        assert "## Coverage trend" in md
        assert "| 1m" in md
        assert "2026-03-30" in md
        assert "No churn detected" in md
        assert "None — every symbol recovered" in md

    def test_renders_persistent_gaps_section(self):
        entries = [
            CoverageEntry(day=date(2026, 3, 30), totals={"1d": (10, 10), "1m": (10, 10), "1h": (10, 10), "5m": (10, 10)}, missing={"5m": ["LOWLIQ"]}),
            CoverageEntry(day=date(2026, 3, 31), totals={"1d": (10, 10), "1m": (10, 10), "1h": (10, 10), "5m": (10, 10)}, missing={"5m": ["LOWLIQ"]}),
            CoverageEntry(day=date(2026, 4, 1), totals={"1d": (10, 10), "1m": (10, 10), "1h": (10, 10), "5m": (10, 10)}, missing={"5m": ["LOWLIQ"]}),
        ]
        md = render_markdown("Week 14 of 2026", entries)
        assert "LOWLIQ (5m): missing 3 of last 3 days" in md

    def test_renders_added_section(self):
        # Universe grew from 10 → 12 → triggers the "Added" line
        entries = [
            CoverageEntry(day=date(2026, 3, 30), totals={
                "1d": (10, 10), "1m": (10, 10), "1h": (10, 10), "5m": (10, 10),
            }),
            CoverageEntry(day=date(2026, 4, 5), totals={
                "1d": (12, 12), "1m": (12, 12), "1h": (12, 12), "5m": (12, 12),
            }),
        ]
        md = render_markdown("Week 14 of 2026", entries)
        assert "Added (1)" in md
        assert "2 new symbols" in md

    def test_empty_entries(self):
        md = render_markdown("Week 14 of 2026", [])
        assert "No coverage logs found" in md


# ── write_summary ────────────────────────────────────────────────────────────


class TestWriteSummary:
    def test_writes_to_correct_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.weekly_quality_summary._LOG_DIR", tmp_path)
        path = write_summary("# hello", (2026, 14))
        assert path.name == "quality_weekly_2026-14.md"
        assert path.read_text() == "# hello"


# ── _iso_week_start ──────────────────────────────────────────────────────────


def test_iso_week_start_monday():
    # ISO week 14 of 2026 starts Monday 2026-03-30
    assert _iso_week_start(2026, 14) == date(2026, 3, 30)


# ── main() ───────────────────────────────────────────────────────────────────


class TestMain:
    def test_skips_non_sunday_without_force(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.weekly_quality_summary._LOG_DIR", tmp_path)
        with patch("livewire_scripts.weekly_quality_summary.date") as mock_date:
            # Wednesday 2026-04-08
            mock_date.today.return_value = date(2026, 4, 8)
            mock_date.fromisocalendar = date.fromisocalendar
            mock_date.fromisoformat = date.fromisoformat
            with patch.object(sys, "argv", ["weekly_quality_summary.py"]):
                main()
        # No file created
        assert list(tmp_path.glob("quality_weekly_*.md")) == []

    def test_force_runs_any_day(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.weekly_quality_summary._LOG_DIR", tmp_path)
        # Seed one log so the report has content
        _write_log(tmp_path, date(2026, 3, 30), _spec_header(date(2026, 3, 30)))
        with patch("livewire_scripts.weekly_quality_summary.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 8)  # Wed
            mock_date.fromisocalendar = date.fromisocalendar
            mock_date.fromisoformat = date.fromisoformat
            with patch.object(sys, "argv", ["weekly_quality_summary.py", "--force"]):
                main()
        files = list(tmp_path.glob("quality_weekly_*.md"))
        assert len(files) == 1

    def test_explicit_week_argument(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.weekly_quality_summary._LOG_DIR", tmp_path)
        _write_log(tmp_path, date(2026, 3, 30), _spec_header(date(2026, 3, 30)))
        with patch("livewire_scripts.weekly_quality_summary.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 5)  # Sunday
            mock_date.fromisocalendar = date.fromisocalendar
            mock_date.fromisoformat = date.fromisoformat
            with patch.object(
                sys, "argv",
                ["weekly_quality_summary.py", "--week", "2026-14"],
            ):
                main()
        path = tmp_path / "quality_weekly_2026-14.md"
        assert path.exists()
        assert "Week 14 of 2026" in path.read_text()

    def test_sunday_default_runs(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.weekly_quality_summary._LOG_DIR", tmp_path)
        with patch("livewire_scripts.weekly_quality_summary.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 5)  # Sunday
            mock_date.fromisocalendar = date.fromisocalendar
            mock_date.fromisoformat = date.fromisoformat
            with patch.object(sys, "argv", ["weekly_quality_summary.py"]):
                main()
        files = list(tmp_path.glob("quality_weekly_*.md"))
        assert len(files) == 1
