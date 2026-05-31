"""Tests for livewire_scripts/check_gaps.py."""

from __future__ import annotations

from datetime import date

from livewire_scripts.check_gaps import GapReport, compute_gaps


class TestComputeGaps:
    def test_no_gaps(self):
        bronze_dates = {
            date(2026, 5, 27),
            date(2026, 5, 28),
            date(2026, 5, 29),
        }
        report = compute_gaps(
            "AAPL", "2026-05-27", bronze_dates, as_of=date(2026, 5, 29)
        )
        assert report.gap_count == 0
        assert report.complete is True

    def test_with_gaps(self):
        bronze_dates = {date(2026, 5, 27), date(2026, 5, 29)}
        report = compute_gaps(
            "AAPL", "2026-05-27", bronze_dates, as_of=date(2026, 5, 29)
        )
        assert report.gap_count == 1
        assert date(2026, 5, 28) in report.missing_dates

    def test_no_earliest_returns_unknown(self):
        report = compute_gaps("AAPL", None, set(), as_of=date(2026, 5, 29))
        assert report.complete is False
        assert report.earliest_available is None

    def test_skips_weekends(self):
        bronze_dates = {date(2026, 5, 29)}
        report = compute_gaps(
            "AAPL", "2026-05-29", bronze_dates, as_of=date(2026, 5, 31)
        )
        assert report.gap_count == 0

    def test_empty_bronze_with_earliest(self):
        report = compute_gaps("AAPL", "2026-05-29", set(), as_of=date(2026, 5, 29))
        assert report.gap_count == 1
        assert report.complete is False
        assert report.bronze_start is None

    def test_full_week(self):
        bronze_dates = {
            date(2026, 5, 18),
            date(2026, 5, 19),
            date(2026, 5, 20),
            date(2026, 5, 21),
            date(2026, 5, 22),
        }
        report = compute_gaps(
            "AAPL", "2026-05-18", bronze_dates, as_of=date(2026, 5, 22)
        )
        assert report.complete is True
        assert report.bronze_count == 5
        assert report.expected_count == 5

    def test_report_fields(self):
        bronze_dates = {date(2026, 5, 27), date(2026, 5, 29)}
        report = compute_gaps(
            "AAPL", "2026-05-27", bronze_dates, as_of=date(2026, 5, 29)
        )
        assert report.ticker == "AAPL"
        assert report.earliest_available == "2026-05-27"
        assert report.bronze_start == "2026-05-27"
        assert report.bronze_end == "2026-05-29"
        assert report.bronze_count == 2
