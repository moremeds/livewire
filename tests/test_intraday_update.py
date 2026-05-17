"""Tests for scripts/intraday_update.py."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from scripts.intraday_update import (
    SessionState,
    classify_session_state,
    expected_last_bar_utc,
)


_UTC = timezone.utc
_ET = ZoneInfo("America/New_York")


class TestExpectedLastBarUtc:
    def test_normal_day_5m(self):
        # Tue Apr 7 2026, 5m bars, last bar = 15:55 ET
        d = date(2026, 4, 7)
        result = expected_last_bar_utc(d, timeframe="5m")
        expected_et = datetime(2026, 4, 7, 15, 55, tzinfo=_ET)
        assert result == expected_et.astimezone(_UTC)

    def test_normal_day_1h(self):
        # Tue Apr 7 2026, 1h bars, last bar = 15:30 ET
        d = date(2026, 4, 7)
        result = expected_last_bar_utc(d, timeframe="1h")
        expected_et = datetime(2026, 4, 7, 15, 30, tzinfo=_ET)
        assert result == expected_et.astimezone(_UTC)

    def test_early_close_day_5m(self):
        # Day after Thanksgiving 2025 (Nov 28), close 13:00 ET, last 5m bar = 12:55 ET
        d = date(2025, 11, 28)
        result = expected_last_bar_utc(d, timeframe="5m")
        expected_et = datetime(2025, 11, 28, 12, 55, tzinfo=_ET)
        assert result == expected_et.astimezone(_UTC)

    def test_invalid_timeframe_raises(self):
        with pytest.raises(ValueError, match="unsupported"):
            expected_last_bar_utc(date(2026, 4, 7), timeframe="2m")


class TestClassifySessionState:
    def _now(self, et_str: str) -> datetime:
        # Helper: parse "YYYY-MM-DD HH:MM" → ET datetime → UTC
        dt = datetime.strptime(et_str, "%Y-%m-%d %H:%M").replace(tzinfo=_ET)
        return dt.astimezone(_UTC)

    def test_complete_session_with_full_bars(self):
        # Now: Wed Apr 8 2026 09:00 ET (next day, before market open)
        # Latest stored: Tue Apr 7 2026 15:55 ET (last bar of prior day)
        now = self._now("2026-04-08 09:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 7), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.COMPLETE

    def test_in_progress_after_close(self):
        # Now: Tue Apr 7 2026 16:30 ET (after close)
        # Latest stored: Tue Apr 7 2026 14:00 ET (gap to fill)
        now = self._now("2026-04-07 16:30")
        latest_stored = datetime(2026, 4, 7, 14, 0, tzinfo=_ET).astimezone(_UTC)
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.IN_PROGRESS

    def test_live_during_session(self):
        # Now: Tue Apr 7 2026 11:00 ET (mid-session)
        # Latest stored: Tue Apr 7 2026 10:30 ET
        now = self._now("2026-04-07 11:00")
        latest_stored = datetime(2026, 4, 7, 10, 30, tzinfo=_ET).astimezone(_UTC)
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.LIVE

    def test_tail_gap_no_today_data(self):
        # Now: Tue Apr 7 2026 10:00 ET (mid-session)
        # Latest stored: Mon Apr 6 2026 15:55 ET (no bars from today yet)
        now = self._now("2026-04-07 10:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 6), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.TAIL_GAP

    def test_historical_multiple_days_behind(self):
        # Now: Thu Apr 9 2026 09:00 ET
        # Latest stored: Mon Apr 6 2026 15:55 ET
        now = self._now("2026-04-09 09:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 6), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.HISTORICAL

    def test_weekend_walks_back_to_friday(self):
        # Now: Sat Apr 11 2026 10:00 ET (weekend)
        # Latest stored: Fri Apr 10 2026 15:55 ET
        now = self._now("2026-04-11 10:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 10), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.COMPLETE


class TestClassifySessionStateAdditional:
    """Additional tests to cover remaining branches."""

    def _now(self, et_str: str) -> datetime:
        dt = datetime.strptime(et_str, "%Y-%m-%d %H:%M").replace(tzinfo=_ET)
        return dt.astimezone(_UTC)

    def test_historical_after_session_close(self):
        # Now: Fri Apr 10 2026 17:00 ET (session closed)
        # Latest stored: Mon Apr 6 2026 15:55 ET (multiple days behind)
        now = self._now("2026-04-10 17:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 6), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.HISTORICAL

    def test_in_progress_pre_market(self):
        # Now: Wed Apr 8 2026 09:00 ET (pre-market)
        # Latest stored: Tue Apr 7 2026 14:00 ET (partial, not the last bar)
        now = self._now("2026-04-08 09:00")
        latest_stored = datetime(2026, 4, 7, 14, 0, tzinfo=_ET).astimezone(_UTC)
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.IN_PROGRESS

    def test_non_trading_day_walks_back_multiple_days(self):
        # Sunday Apr 12 2026 10:00 ET — should walk back past Saturday to Friday
        now = self._now("2026-04-12 10:00")
        # Fri Apr 10 15:55 ET — complete
        latest_stored = expected_last_bar_utc(date(2026, 4, 10), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.COMPLETE

    def test_pre_market_walks_back_over_weekend(self):
        # Monday Apr 13 2026 09:00 ET — pre-market should evaluate Friday's close
        now = self._now("2026-04-13 09:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 10), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.COMPLETE


class TestMain:
    def test_dry_run_classifies_states_without_fetching(self, tmp_path, monkeypatch):
        from clients.intraday_bronze_client import IntradayBronzeClient

        bronze = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze.mkdir(parents=True)

        # Seed AAPL with one 5m bar from a recent trading day at 15:55 ET
        # Use a known weekday: Mon Apr 6 2026
        et_ts = datetime(2026, 4, 6, 15, 55, tzinfo=_ET)
        client = IntradayBronzeClient(bronze_dir=bronze, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": et_ts.astimezone(_UTC), "symbol_id": 1,
             "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        client.close()

        monkeypatch.setattr("scripts.intraday_update._DATA_LAKE", tmp_path / "data-lake")

        with patch("sys.argv", ["intraday_update.py", "--dry-run", "--force", "--timeframe", "5m"]):
            from scripts.intraday_update import main
            main()
        # No exceptions = pass

    def test_not_trading_day_exits_without_force(self, tmp_path, monkeypatch):
        monkeypatch.setattr("scripts.intraday_update._DATA_LAKE", tmp_path / "data-lake")
        # Patch is_trading_day to return False
        with patch("scripts.intraday_update.is_trading_day", return_value=False):
            with patch("sys.argv", ["intraday_update.py"]):
                from scripts.intraday_update import main
                main()
        # No exceptions, just early return

    def test_main_no_dry_run_runs_summary(self, tmp_path, monkeypatch):
        """Cover the non-dry-run completion path (line 185)."""
        from clients.intraday_bronze_client import IntradayBronzeClient

        bronze = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze.mkdir(parents=True)

        # Seed AAPL with bars
        et_ts = datetime(2026, 4, 6, 15, 55, tzinfo=_ET)
        client = IntradayBronzeClient(bronze_dir=bronze, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": et_ts.astimezone(_UTC), "symbol_id": 1,
             "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        client.close()

        monkeypatch.setattr("scripts.intraday_update._DATA_LAKE", tmp_path / "data-lake")

        with patch("sys.argv", ["intraday_update.py", "--force", "--timeframe", "5m"]):
            from scripts.intraday_update import main
            main()
        # No exceptions = pass

    def test_main_symbol_missing_from_latest_ts(self, tmp_path, monkeypatch):
        """Cover the branch where latest_ts.get(sym) is None (lines 171-172)."""
        from clients.intraday_bronze_client import IntradayBronzeClient
        from unittest.mock import MagicMock

        bronze = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze.mkdir(parents=True)

        # Seed AAPL with bars so get_existing_symbols returns it
        et_ts = datetime(2026, 4, 6, 15, 55, tzinfo=_ET)
        client = IntradayBronzeClient(bronze_dir=bronze, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": et_ts.astimezone(_UTC), "symbol_id": 1,
             "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        client.close()

        monkeypatch.setattr("scripts.intraday_update._DATA_LAKE", tmp_path / "data-lake")

        # Patch get_latest_timestamps to return empty dict (symbol in existing but not in ts map)
        with patch.object(IntradayBronzeClient, "get_latest_timestamps", return_value={}):
            with patch("sys.argv", ["intraday_update.py", "--dry-run", "--force", "--timeframe", "5m"]):
                from scripts.intraday_update import main
                main()

    def test_main_naive_timestamp_gets_utc(self, tmp_path, monkeypatch):
        """Cover the naive timestamp path (line 175)."""
        from clients.intraday_bronze_client import IntradayBronzeClient

        bronze = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze.mkdir(parents=True)

        # Seed AAPL
        et_ts = datetime(2026, 4, 6, 15, 55, tzinfo=_ET)
        client = IntradayBronzeClient(bronze_dir=bronze, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": et_ts.astimezone(_UTC), "symbol_id": 1,
             "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        client.close()

        monkeypatch.setattr("scripts.intraday_update._DATA_LAKE", tmp_path / "data-lake")

        # Return a naive datetime for AAPL
        naive_ts = datetime(2026, 4, 6, 19, 55)  # naive UTC equivalent
        with patch.object(IntradayBronzeClient, "get_latest_timestamps", return_value={"AAPL": naive_ts}):
            with patch("sys.argv", ["intraday_update.py", "--dry-run", "--force", "--timeframe", "5m"]):
                from scripts.intraday_update import main
                main()
