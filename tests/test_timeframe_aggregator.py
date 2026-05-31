"""Tests for clients/timeframe_aggregator.py — lossless OHLCV rollup."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from clients.timeframe_aggregator import VALID_ROLLUPS, aggregate_bars

_BASE = datetime(2026, 5, 28, 13, 30, tzinfo=timezone.utc)  # 09:30 ET in UTC


def _bar(minute_offset: int, o: float, h: float, l: float, c: float, v: int) -> dict:
    """Build a 1m bar dict at 2026-05-28 09:30 ET + minute_offset in UTC."""
    ts = _BASE + timedelta(minutes=minute_offset)
    return {
        "bar_timestamp": ts,
        "symbol_id": 12345,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
    }


class TestValidRollups:
    def test_supported_pairs(self):
        assert ("1m", "5m") in VALID_ROLLUPS
        assert ("1m", "30m") in VALID_ROLLUPS
        assert ("1m", "1h") in VALID_ROLLUPS
        assert ("30m", "1h") in VALID_ROLLUPS

    def test_unsupported_pair_raises(self):
        with pytest.raises(ValueError, match="unsupported"):
            aggregate_bars([], source_tf="5m", target_tf="1h")


class TestAggregate1mTo5m:
    def test_five_bars_produce_one(self):
        bars = [
            _bar(0, 100.0, 102.0, 99.0, 101.0, 1000),
            _bar(1, 101.0, 103.0, 100.0, 102.0, 2000),
            _bar(2, 102.0, 104.0, 101.0, 103.0, 1500),
            _bar(3, 103.0, 105.0, 99.5, 104.0, 1800),
            _bar(4, 104.0, 106.0, 103.0, 105.0, 2200),
        ]
        result = aggregate_bars(bars, source_tf="1m", target_tf="5m")
        assert len(result) == 1
        agg = result[0]
        assert agg["open"] == 100.0
        assert agg["high"] == 106.0
        assert agg["low"] == 99.0
        assert agg["close"] == 105.0
        assert agg["volume"] == 8500
        assert agg["symbol_id"] == 12345
        assert agg["bar_timestamp"].minute == 30

    def test_partial_window_at_end(self):
        bars = [_bar(i, 100.0, 101.0, 99.0, 100.5, 100) for i in range(7)]
        result = aggregate_bars(bars, source_tf="1m", target_tf="5m")
        assert len(result) == 2
        assert result[0]["volume"] == 500
        assert result[1]["volume"] == 200

    def test_empty_input(self):
        assert aggregate_bars([], source_tf="1m", target_tf="5m") == []


class TestAggregate1mTo30m:
    def test_thirty_bars_produce_one(self):
        bars = [_bar(i, 100.0 + i, 110.0 + i, 90.0, 100.0 + i, 100) for i in range(30)]
        result = aggregate_bars(bars, source_tf="1m", target_tf="30m")
        assert len(result) == 1
        assert result[0]["open"] == 100.0
        assert result[0]["close"] == 129.0
        assert result[0]["volume"] == 3000


class TestAggregate1mTo1h:
    def test_sixty_bars_produce_one(self):
        base = datetime(2026, 5, 28, 14, 0, tzinfo=timezone.utc)
        bars = [
            {
                "bar_timestamp": base + timedelta(minutes=i),
                "symbol_id": 12345,
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 100.0,
                "volume": 100,
            }
            for i in range(60)
        ]
        result = aggregate_bars(bars, source_tf="1m", target_tf="1h")
        assert len(result) == 1
        assert result[0]["volume"] == 6000
        assert result[0]["bar_timestamp"].minute == 0


class TestAggregate30mTo1h:
    def test_two_30m_bars_produce_one_1h(self):
        ts1 = datetime(2026, 5, 28, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
        ts2 = datetime(2026, 5, 28, 14, 30, tzinfo=timezone.utc)  # 10:30 ET
        bars = [
            {
                "bar_timestamp": ts1,
                "symbol_id": 1,
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 103.0,
                "volume": 5000,
            },
            {
                "bar_timestamp": ts2,
                "symbol_id": 1,
                "open": 103.0,
                "high": 107.0,
                "low": 101.0,
                "close": 106.0,
                "volume": 3000,
            },
        ]
        result = aggregate_bars(bars, source_tf="30m", target_tf="1h")
        assert len(result) == 1
        assert result[0]["open"] == 100.0
        assert result[0]["high"] == 107.0
        assert result[0]["low"] == 98.0
        assert result[0]["close"] == 106.0
        assert result[0]["volume"] == 8000
        assert result[0]["bar_timestamp"] == ts1  # window start = top of hour
