"""Tests for livewire_scripts/ingest_flatfiles.py — flat file ingestion orchestrator."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from livewire_scripts.ingest_flatfiles import (
    DERIVED_TIMEFRAMES,
    ingest_date,
    ingest_range,
    main,
)


def _make_rows(ticker: str, n: int = 60) -> list[dict]:
    """Build n 1m rows starting at 14:00 UTC (10:00 ET) on 2026-05-28."""
    from datetime import timedelta

    base = datetime(2026, 5, 28, 14, 0, tzinfo=timezone.utc)
    return [
        {
            "bar_timestamp": base + timedelta(minutes=i),
            "symbol_id": 0,
            "open": 100.0 + i,
            "high": 101.0 + i,
            "low": 99.0 + i,
            "close": 100.5 + i,
            "volume": 1000,
        }
        for i in range(n)
    ]


class TestDerivedTimeframes:
    def test_includes_expected(self):
        assert "5m" in DERIVED_TIMEFRAMES
        assert "30m" in DERIVED_TIMEFRAMES
        assert "1h" in DERIVED_TIMEFRAMES


class TestIngestDate:
    def test_writes_1m_and_derived_parquet(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        rows = _make_rows("AAPL", n=60)

        mock_client = MagicMock()
        mock_client.download_date.return_value = {"AAPL": rows}

        stats = ingest_date(
            mock_client,
            date(2026, 5, 28),
            target_tickers={"AAPL"},
            bronze_dir=bronze_dir,
        )
        assert stats["tickers_written"] == 1
        assert stats["bars_1m"] == 60
        assert (bronze_dir / "symbol=AAPL" / "1m.parquet").exists()
        assert (bronze_dir / "symbol=AAPL" / "5m.parquet").exists()
        assert (bronze_dir / "symbol=AAPL" / "30m.parquet").exists()
        assert (bronze_dir / "symbol=AAPL" / "1h.parquet").exists()

    def test_skips_empty_response(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_client = MagicMock()
        mock_client.download_date.return_value = {}

        stats = ingest_date(
            mock_client,
            date(2026, 5, 28),
            target_tickers={"AAPL"},
            bronze_dir=bronze_dir,
        )
        assert stats["tickers_written"] == 0

    def test_skips_empty_rows_for_ticker(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_client = MagicMock()
        mock_client.download_date.return_value = {"AAPL": []}

        stats = ingest_date(
            mock_client,
            date(2026, 5, 28),
            target_tickers={"AAPL"},
            bronze_dir=bronze_dir,
        )
        assert stats["tickers_written"] == 0


class TestIngestRange:
    def test_processes_multiple_dates(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_client = MagicMock()
        mock_client.download_date.return_value = {"AAPL": _make_rows("AAPL", n=10)}

        stats = ingest_range(
            mock_client,
            start=date(2026, 5, 28),
            end=date(2026, 5, 29),
            target_tickers={"AAPL"},
            bronze_dir=bronze_dir,
        )
        assert stats["dates_processed"] >= 1

    def test_empty_download_returns_zero_stats(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        mock_client = MagicMock()
        mock_client.download_date.return_value = {}

        stats = ingest_range(
            mock_client,
            start=date(2026, 5, 28),
            end=date(2026, 5, 29),
            target_tickers={"AAPL"},
            bronze_dir=bronze_dir,
        )
        assert stats["dates_processed"] == 1
        assert stats["total_bars_1m"] == 0


class TestMain:
    def test_dry_run_with_preset(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))

        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"name": "test", "tickers": ["AAPL"]}))

        rc = main(["--preset", str(preset), "--years", "1", "--dry-run"])
        assert rc == 0

    def test_dry_run_with_tickers(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))

        rc = main(["--tickers", "AAPL", "MSFT", "--years", "1", "--dry-run"])
        assert rc == 0

    def test_live_run_with_preset(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
        (tmp_path / "data-lake" / "bronze" / "asset_class=equity").mkdir(parents=True)

        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"name": "test", "tickers": ["AAPL"]}))

        mock_client = MagicMock()
        mock_client.download_date.return_value = {}
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch(
            "livewire_scripts.ingest_flatfiles.MassiveFlatfileClient",
            return_value=mock_client,
        ):
            rc = main(["--preset", str(preset), "--years", "1"])
        assert rc == 0
