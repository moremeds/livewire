"""Tests for clients/massive_flatfile_client.py — Polygon S3 flat file client."""

from __future__ import annotations

import csv
import gzip
import io
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from clients.massive_flatfile_client import (
    S3_BUCKET,
    S3_ENDPOINT,
    MassiveFlatfileClient,
    _s3_key_for_date,
    parse_flatfile_csv,
    trading_dates_between,
)


def _make_csv_gz(rows: list[dict]) -> bytes:
    """Build a gzipped CSV bytes object matching Polygon minute agg format."""
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "ticker",
            "volume",
            "open",
            "close",
            "high",
            "low",
            "window_start",
            "transactions",
        ],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return gzip.compress(buf.getvalue().encode("utf-8"))


class TestS3KeyForDate:
    def test_formats_correctly(self):
        d = date(2026, 3, 15)
        key = _s3_key_for_date(d)
        assert key == "us_stocks_sip/minute_aggs_v1/2026/03/2026-03-15.csv.gz"


class TestTradingDatesBetween:
    def test_excludes_weekends(self):
        # 2026-05-25 is Monday, 2026-05-31 is Sunday
        dates = trading_dates_between(date(2026, 5, 25), date(2026, 5, 31))
        assert date(2026, 5, 25) in dates
        assert date(2026, 5, 29) in dates
        assert date(2026, 5, 30) not in dates
        assert date(2026, 5, 31) not in dates

    def test_empty_range(self):
        assert trading_dates_between(date(2026, 5, 30), date(2026, 5, 30)) == []


class TestParseFlatfileCsv:
    def test_filters_to_target_tickers(self):
        csv_gz = _make_csv_gz(
            [
                {
                    "ticker": "AAPL",
                    "volume": "1000",
                    "open": "150.0",
                    "close": "151.0",
                    "high": "152.0",
                    "low": "149.0",
                    "window_start": "1748440200000000000",
                    "transactions": "50",
                },
                {
                    "ticker": "MSFT",
                    "volume": "2000",
                    "open": "300.0",
                    "close": "301.0",
                    "high": "302.0",
                    "low": "299.0",
                    "window_start": "1748440200000000000",
                    "transactions": "30",
                },
                {
                    "ticker": "GOOG",
                    "volume": "500",
                    "open": "170.0",
                    "close": "171.0",
                    "high": "172.0",
                    "low": "169.0",
                    "window_start": "1748440200000000000",
                    "transactions": "10",
                },
            ]
        )
        result = parse_flatfile_csv(csv_gz, target_tickers={"AAPL", "MSFT"})
        assert set(result.keys()) == {"AAPL", "MSFT"}
        assert len(result["AAPL"]) == 1
        assert result["AAPL"][0]["open"] == 150.0
        assert result["AAPL"][0]["volume"] == 1000

    def test_returns_empty_for_no_matches(self):
        csv_gz = _make_csv_gz(
            [
                {
                    "ticker": "GOOG",
                    "volume": "500",
                    "open": "170.0",
                    "close": "171.0",
                    "high": "172.0",
                    "low": "169.0",
                    "window_start": "1748440200000000000",
                    "transactions": "10",
                },
            ]
        )
        result = parse_flatfile_csv(csv_gz, target_tickers={"AAPL"})
        assert result == {}

    def test_all_tickers_when_no_filter(self):
        csv_gz = _make_csv_gz(
            [
                {
                    "ticker": "AAPL",
                    "volume": "1000",
                    "open": "150.0",
                    "close": "151.0",
                    "high": "152.0",
                    "low": "149.0",
                    "window_start": "1748440200000000000",
                    "transactions": "50",
                },
            ]
        )
        result = parse_flatfile_csv(csv_gz, target_tickers=None)
        assert "AAPL" in result

    def test_bar_timestamp_is_utc(self):
        csv_gz = _make_csv_gz(
            [
                {
                    "ticker": "AAPL",
                    "volume": "100",
                    "open": "150.0",
                    "close": "151.0",
                    "high": "152.0",
                    "low": "149.0",
                    "window_start": "1748440200000000000",
                    "transactions": "5",
                },
            ]
        )
        result = parse_flatfile_csv(csv_gz, target_tickers={"AAPL"})
        ts = result["AAPL"][0]["bar_timestamp"]
        assert ts.tzinfo == timezone.utc


class TestMassiveFlatfileClient:
    def test_injected_s3_client(self):
        mock_s3 = MagicMock()
        client = MassiveFlatfileClient(_s3_client=mock_s3)
        assert client._s3 is mock_s3
        client.close()

    def test_download_date_writes_temp_and_deletes(self, monkeypatch):
        """Verify download_date uses a temp file and cleans it up after parsing."""
        csv_gz = _make_csv_gz(
            [
                {
                    "ticker": "AAPL",
                    "volume": "1000",
                    "open": "150.0",
                    "close": "151.0",
                    "high": "152.0",
                    "low": "149.0",
                    "window_start": "1748440200000000000",
                    "transactions": "50",
                },
            ]
        )

        created_temps: list[Path] = []

        def fake_download_fileobj(bucket, key, fh):
            fh.write(csv_gz)

        mock_s3 = MagicMock()
        mock_s3.download_fileobj.side_effect = fake_download_fileobj

        import clients.massive_flatfile_client as mod

        orig_mkstemp = mod.tempfile.mkstemp

        def tracking_mkstemp(**kwargs):
            fd, name = orig_mkstemp(**kwargs)
            created_temps.append(Path(name))
            return fd, name

        monkeypatch.setattr(mod.tempfile, "mkstemp", tracking_mkstemp)

        client = MassiveFlatfileClient(_s3_client=mock_s3)
        result = client.download_date(date(2026, 5, 28), target_tickers={"AAPL"})
        client.close()

        assert "AAPL" in result
        assert len(result["AAPL"]) == 1
        # Temp file was created and then deleted
        assert len(created_temps) == 1
        assert not created_temps[0].exists()

    def test_download_date_deletes_on_parse_error(self, monkeypatch):
        """Temp file is deleted even when parsing fails."""

        def fake_download_fileobj(bucket, key, fh):
            fh.write(b"not valid gzip data")

        mock_s3 = MagicMock()
        mock_s3.download_fileobj.side_effect = fake_download_fileobj

        created_temps: list[Path] = []
        import clients.massive_flatfile_client as mod

        orig_mkstemp = mod.tempfile.mkstemp

        def tracking_mkstemp(**kwargs):
            fd, name = orig_mkstemp(**kwargs)
            created_temps.append(Path(name))
            return fd, name

        monkeypatch.setattr(mod.tempfile, "mkstemp", tracking_mkstemp)

        client = MassiveFlatfileClient(_s3_client=mock_s3)
        with pytest.raises(Exception):
            client.download_date(date(2026, 5, 28), target_tickers={"AAPL"})
        client.close()

        assert len(created_temps) == 1
        assert not created_temps[0].exists()

    def test_context_manager(self):
        mock_s3 = MagicMock()
        with MassiveFlatfileClient(_s3_client=mock_s3) as client:
            assert client is not None
