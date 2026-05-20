"""Tests for clients/intraday_bronze_client.py."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pyarrow as pa
import pytest

from clients.intraday_bronze_client import (
    INTRADAY_TIMEFRAMES,
    INTRADAY_PARQUET_FILENAME,
    IntradayBronzeClient,
)


_UTC = timezone.utc
_ET = ZoneInfo("America/New_York")


class TestConstants:
    def test_timeframes_include_1m_1h_and_5m(self):
        assert INTRADAY_TIMEFRAMES == ("1m", "1h", "5m")

    def test_filenames_match_timeframes(self):
        assert INTRADAY_PARQUET_FILENAME == {
            "1m": "1m.parquet",
            "1h": "1h.parquet",
            "5m": "5m.parquet",
        }


class TestConstructor:
    def test_invalid_timeframe_raises(self, tmp_path):
        with pytest.raises(ValueError, match="unsupported timeframe"):
            IntradayBronzeClient(bronze_dir=tmp_path, timeframe="15m")

    def test_valid_timeframes_accepted(self, tmp_path):
        for tf in ("1m", "1h", "5m"):
            with IntradayBronzeClient(bronze_dir=tmp_path, timeframe=tf) as client:
                assert client.timeframe == tf

    def test_path_uses_timeframe_filename(self, tmp_path):
        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="1h") as client:
            path = client._symbol_path("AAPL")
            assert path.name == "1h.parquet"

    def test_default_bronze_dir(self):
        # Just ensure constructor doesn't crash with no bronze_dir
        client = IntradayBronzeClient(timeframe="5m")
        assert client.timeframe == "5m"
        client.close()

    def test_bronze_dir_property(self, tmp_path):
        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="1h") as client:
            assert client.bronze_dir == tmp_path


class TestRowNormalization:
    def test_naive_timestamp_rejected(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        rows = [{
            "bar_timestamp": datetime(2026, 4, 6, 13, 30),  # naive!
            "symbol_id": 1,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "volume": 100,
        }]
        with pytest.raises(ValueError, match="must be tz-aware"):
            client.replace_ticker_rows("AAPL", rows)
        client.close()

    def test_non_datetime_rejected(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        rows = [{
            "bar_timestamp": "2026-04-06T13:30:00",  # string, not datetime
            "symbol_id": 1,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "volume": 100,
        }]
        with pytest.raises(ValueError, match="must be a datetime"):
            client.replace_ticker_rows("AAPL", rows)
        client.close()

    def test_et_timestamp_normalized_to_utc(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        et_ts = datetime(2026, 4, 6, 9, 30, tzinfo=_ET)
        rows = [{
            "bar_timestamp": et_ts,
            "symbol_id": 1,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "volume": 100,
        }]
        client.replace_ticker_rows("AAPL", rows)

        out_path = tmp_path / "symbol=AAPL" / "5m.parquet"
        table = pq.read_table(out_path)
        stored = table.column("bar_timestamp")[0].as_py()
        # 9:30 ET on 2026-04-06 (EDT = UTC-4) = 13:30 UTC
        assert stored == et_ts.astimezone(_UTC)
        client.close()

    def test_replace_then_read_roundtrip(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        ts2 = datetime(2026, 4, 6, 13, 35, tzinfo=_UTC)
        rows = [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
            {"bar_timestamp": ts2, "symbol_id": 1, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 200},
        ]
        n = client.replace_ticker_rows("AAPL", rows)
        assert n == 2

        loaded = client.read_symbol_rows("AAPL")
        assert len(loaded) == 2
        assert loaded[0]["bar_timestamp"] == ts1
        assert loaded[1]["bar_timestamp"] == ts2
        client.close()

    def test_replace_empty_rows_raises(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        with pytest.raises(ValueError, match="cannot publish an empty"):
            client.replace_ticker_rows("AAPL", [])
        client.close()

    def test_merge_dedups_by_timestamp(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        ts2 = datetime(2026, 4, 6, 13, 35, tzinfo=_UTC)
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        # Merge with overlap + new
        n = client.merge_ticker_rows("AAPL", [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 9.0, "high": 9.0, "low": 9.0, "close": 9.0, "volume": 999},
            {"bar_timestamp": ts2, "symbol_id": 1, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 200},
        ])
        assert n == 1  # 1 new (ts2), 1 overwritten (ts1)
        loaded = client.read_symbol_rows("AAPL")
        assert len(loaded) == 2
        # ts1 was overwritten
        assert loaded[0]["volume"] == 999
        client.close()

    def test_merge_can_skip_existing_timestamp_overwrites(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        ts2 = datetime(2026, 4, 6, 13, 35, tzinfo=_UTC)
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])

        n = client.merge_ticker_rows(
            "AAPL",
            [
                {"bar_timestamp": ts1, "symbol_id": 1, "open": 9.0, "high": 9.0, "low": 9.0, "close": 9.0, "volume": 999},
                {"bar_timestamp": ts2, "symbol_id": 1, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 200},
            ],
            overwrite_existing=False,
        )

        assert n == 1
        loaded = client.read_symbol_rows("AAPL")
        assert len(loaded) == 2
        assert loaded[0]["volume"] == 100
        assert loaded[1]["volume"] == 200
        client.close()

    def test_merge_skips_publish_when_recent_rows_already_exist(self, tmp_path, monkeypatch):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        published = False

        def fail_publish(*_args):
            nonlocal published
            published = True
            raise AssertionError("should not publish duplicate-only recent merge")

        monkeypatch.setattr(client, "_publish", fail_publish)

        n = client.merge_ticker_rows(
            "AAPL",
            [{"bar_timestamp": ts1, "symbol_id": 1, "open": 9.0, "high": 9.0, "low": 9.0, "close": 9.0, "volume": 999}],
            overwrite_existing=False,
        )

        assert n == 0
        assert published is False
        client.close()

    def test_merge_without_overwrite_can_create_new_symbol(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)

        n = client.merge_ticker_rows(
            "AAPL",
            [{"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100}],
            overwrite_existing=False,
        )

        assert n == 1
        assert len(client.read_symbol_rows("AAPL")) == 1
        client.close()

    def test_read_symbol_timestamps_normalizes_naive_values(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        path = tmp_path / "symbol=AAPL" / "5m.parquet"
        path.parent.mkdir(parents=True)
        naive = datetime(2026, 4, 6, 13, 30)
        table = pa.table({"bar_timestamp": [naive]})
        pq.write_table(table, path)

        assert client._read_symbol_timestamps("AAPL") == {naive.replace(tzinfo=_UTC)}
        client.close()

    def test_merge_empty_rows_returns_zero(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        n = client.merge_ticker_rows("AAPL", [])
        assert n == 0
        client.close()


class TestDiscovery:
    def test_get_existing_symbols_finds_intraday_files(self, tmp_path):
        # Create AAPL with 5m, MSFT with 1h, NVDA with 5m
        for sym, tf in [("AAPL", "5m"), ("MSFT", "1h"), ("NVDA", "5m")]:
            client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe=tf)
            client.replace_ticker_rows(sym, [
                {
                    "bar_timestamp": datetime(2026, 4, 6, 13, 30, tzinfo=_UTC),
                    "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5,
                    "close": 1.5, "volume": 100,
                },
            ])
            client.close()

        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m") as client:
            assert client.get_existing_symbols() == {"AAPL", "NVDA"}
        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="1h") as client:
            assert client.get_existing_symbols() == {"MSFT"}

    def test_get_existing_symbols_empty_dir(self, tmp_path):
        with IntradayBronzeClient(bronze_dir=tmp_path / "nonexistent", timeframe="5m") as client:
            assert client.get_existing_symbols() == set()

    def test_get_latest_timestamps(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        ts2 = datetime(2026, 4, 6, 13, 35, tzinfo=_UTC)
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
            {"bar_timestamp": ts2, "symbol_id": 1, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 200},
        ])
        result = client.get_latest_timestamps()
        # The result should map AAPL to ts2
        assert "AAPL" in result
        # Parquet readers may return tz-naive or tz-aware values; compare on epoch.
        latest = result["AAPL"]
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=_UTC)
        assert latest == ts2
        client.close()

    def test_get_latest_timestamps_empty(self, tmp_path):
        with IntradayBronzeClient(bronze_dir=tmp_path / "empty", timeframe="5m") as client:
            assert client.get_latest_timestamps() == {}

    def test_read_symbol_rows_missing_returns_empty(self, tmp_path):
        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m") as client:
            assert client.read_symbol_rows("NOSUCH") == []

    def test_get_symbol_id_for_existing(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": datetime(2026, 4, 6, 13, 30, tzinfo=_UTC),
             "symbol_id": 42, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        assert client.get_symbol_id("AAPL") == 42
        client.close()

    def test_get_symbol_id_for_new_uses_stable_hash(self, tmp_path):
        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m") as client:
            sid = client.get_symbol_id("NEWSTUFF")
            assert isinstance(sid, int)
            assert sid > 0
