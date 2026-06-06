"""Tests for livewire_scripts/check_gaps.py."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

from livewire_scripts.check_gaps import GapReport, compute_gaps, main


class TestComputeGaps:
    def test_no_gaps(self):
        bronze_dates = {
            date(2026, 5, 27),
            date(2026, 5, 28),
            date(2026, 5, 29),
        }
        report = compute_gaps("AAPL", "2026-05-27", bronze_dates, as_of=date(2026, 5, 29))
        assert report.gap_count == 0
        assert report.complete is True

    def test_with_gaps(self):
        bronze_dates = {date(2026, 5, 27), date(2026, 5, 29)}
        report = compute_gaps("AAPL", "2026-05-27", bronze_dates, as_of=date(2026, 5, 29))
        assert report.gap_count == 1
        assert date(2026, 5, 28) in report.missing_dates

    def test_no_earliest_returns_unknown(self):
        report = compute_gaps("AAPL", None, set(), as_of=date(2026, 5, 29))
        assert report.complete is False
        assert report.earliest_available is None

    def test_skips_weekends(self):
        bronze_dates = {date(2026, 5, 29)}
        report = compute_gaps("AAPL", "2026-05-29", bronze_dates, as_of=date(2026, 5, 31))
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
        report = compute_gaps("AAPL", "2026-05-18", bronze_dates, as_of=date(2026, 5, 22))
        assert report.complete is True
        assert report.bronze_count == 5
        assert report.expected_count == 5

    def test_report_fields(self):
        bronze_dates = {date(2026, 5, 27), date(2026, 5, 29)}
        report = compute_gaps("AAPL", "2026-05-27", bronze_dates, as_of=date(2026, 5, 29))
        assert report.ticker == "AAPL"
        assert report.earliest_available == "2026-05-27"
        assert report.bronze_start == "2026-05-27"
        assert report.bronze_end == "2026-05-29"
        assert report.bronze_count == 2

    def test_no_earliest_with_bronze_dates(self):
        bronze_dates = {date(2026, 5, 27)}
        report = compute_gaps("AAPL", None, bronze_dates, as_of=date(2026, 5, 29))
        assert report.bronze_start == "2026-05-27"
        assert report.bronze_end == "2026-05-27"
        assert report.complete is False


class TestMain:
    def _setup(self, tmp_path, monkeypatch):
        warehouse = tmp_path / "warehouse"
        warehouse.mkdir()
        bronze_dir = warehouse / "data-lake" / "bronze" / "asset_class=equity" / "symbol=AAPL"
        bronze_dir.mkdir(parents=True)
        monkeypatch.setattr("livewire_scripts.check_gaps._WAREHOUSE_DIR", warehouse)
        return warehouse

    def test_main_no_data(self, tmp_path, monkeypatch):
        warehouse = self._setup(tmp_path, monkeypatch)
        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {}
        with patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze):
            main([])

    def test_main_with_complete_ticker(self, tmp_path, monkeypatch):
        warehouse = self._setup(tmp_path, monkeypatch)
        from clients.tag_registry import TagRegistry

        reg = TagRegistry(warehouse / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_earliest("AAPL", "2026-05-27", source="ib")
        reg.save()

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 27), date(2026, 5, 28), date(2026, 5, 29)],
        }
        with patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze):
            main([])

    def test_main_with_gaps(self, tmp_path, monkeypatch):
        warehouse = self._setup(tmp_path, monkeypatch)
        from clients.tag_registry import TagRegistry

        reg = TagRegistry(warehouse / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_earliest("AAPL", "2026-05-27", source="ib")
        reg.save()

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 27), date(2026, 5, 29)],
        }
        with patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze):
            main(["--show-gaps"])

    def test_main_incomplete_only(self, tmp_path, monkeypatch):
        warehouse = self._setup(tmp_path, monkeypatch)
        from clients.tag_registry import TagRegistry

        reg = TagRegistry(warehouse / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_earliest("AAPL", "2026-05-27", source="ib")
        reg.save()

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 27), date(2026, 5, 28), date(2026, 5, 29)],
        }
        with patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze):
            main(["--incomplete-only"])

    def test_main_with_preset(self, tmp_path, monkeypatch):
        warehouse = self._setup(tmp_path, monkeypatch)
        preset_path = tmp_path / "test_preset.json"
        preset_path.write_text(json.dumps({"tickers": ["AAPL"]}))

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 29)],
            "MSFT": [date(2026, 5, 29)],
        }
        with patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze):
            main(["--preset", str(preset_path)])

    def test_main_unknown_ticker(self, tmp_path, monkeypatch):
        """Ticker in bronze but not in registry — no earliest bounds."""
        warehouse = self._setup(tmp_path, monkeypatch)
        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 29)],
        }
        with patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze):
            main([])

    def test_main_complete_status_and_incomplete_filter(self, tmp_path, monkeypatch):
        self._setup(tmp_path, monkeypatch)
        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {"AAPL": [date(2026, 5, 29)]}
        complete = GapReport("AAPL", "2026-05-29", "2026-05-29", "2026-05-29", 1, 1, 0, complete=True)
        with (
            patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze),
            patch("livewire_scripts.check_gaps.compute_gaps", return_value=complete),
        ):
            main([])
            main(["--incomplete-only"])

    def test_main_show_gaps_many(self, tmp_path, monkeypatch):
        """Test show-gaps with more than 20 missing dates (triggers truncation)."""
        warehouse = self._setup(tmp_path, monkeypatch)
        from clients.tag_registry import TagRegistry

        reg = TagRegistry(warehouse / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_earliest("AAPL", "2026-01-01", source="ib")
        reg.save()

        mock_bronze = MagicMock()
        mock_bronze.get_trade_dates_by_symbol.return_value = {
            "AAPL": [date(2026, 5, 29)],
        }
        with patch("livewire_scripts.check_gaps.BronzeClient", return_value=mock_bronze):
            main(["--show-gaps"])
