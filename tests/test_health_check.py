"""Tests for scripts/health_check.py — core gap detection functions and main entry point."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.bronze_client import BronzeClient
from livewire_scripts.health_check import (
    _resolve_bronze_dir,
    _send_alert,
    compute_range_duration,
    find_interior_gaps,
    find_intraday_gaps,
    generate_expected_intraday_timestamps,
    get_all_trade_dates,
    group_contiguous_dates,
    main,
    repair_intraday_window,
    report_intraday_health,
)

# ── Helpers ────────────────────────────────────────────────────────────────────

_EQUITY_SCHEMA = pa.schema(
    [
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
    ]
)


def _write_parquet(bronze_dir, symbol: str, dates: list[date]) -> None:
    """Write a minimal equity parquet file for *symbol* under *bronze_dir*."""
    sym_dir = bronze_dir / f"symbol={symbol}"
    sym_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "trade_date": d,
            "symbol_id": 1,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "adj_close": 1.5,
            "volume": 1000,
        }
        for d in dates
    ]
    table = pa.Table.from_pylist(rows, schema=_EQUITY_SCHEMA)
    pq.write_table(table, sym_dir / "1d.parquet", compression="snappy")


# ── find_interior_gaps ─────────────────────────────────────────────────────────


class TestFindInteriorGaps:
    def test_empty_list_returns_empty(self):
        assert find_interior_gaps([]) == []

    def test_single_date_returns_empty(self):
        assert find_interior_gaps([date(2026, 1, 5)]) == []

    def test_full_week_no_gaps(self):
        # Mon-Fri Jan 5-9 2026 — all trading days, no gaps
        week = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8), date(2026, 1, 9)]
        assert find_interior_gaps(week) == []

    def test_missing_wednesday_detected(self):
        # Mon Jan 5, Tue Jan 6, Thu Jan 8, Fri Jan 9 — Wed Jan 7 is missing
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8), date(2026, 1, 9)]
        assert find_interior_gaps(dates) == [date(2026, 1, 7)]

    def test_weekend_not_a_gap(self):
        # Fri Jan 9 → Mon Jan 12 — Sat/Sun are not trading days
        dates = [date(2026, 1, 9), date(2026, 1, 12)]
        assert find_interior_gaps(dates) == []

    def test_nyse_holiday_mlk_day_not_a_gap(self):
        # MLK Day 2026 = Mon Jan 19; Fri Jan 16 → Tue Jan 20 has no gap
        dates = [date(2026, 1, 16), date(2026, 1, 20)]
        assert find_interior_gaps(dates) == []

    def test_futures_missing_tuesday_is_a_gap(self):
        # Mon Jan 5, Wed Jan 7 — Tue Jan 6 is a weekday, so it's a gap for futures
        dates = [date(2026, 1, 5), date(2026, 1, 7)]
        gaps = find_interior_gaps(dates, asset_class="futures")
        assert gaps == [date(2026, 1, 6)]

    def test_futures_weekend_not_a_gap(self):
        # Fri Jan 9 → Mon Jan 12 — weekends are still not expected for futures
        dates = [date(2026, 1, 9), date(2026, 1, 12)]
        assert find_interior_gaps(dates, asset_class="futures") == []

    def test_futures_mlk_day_is_a_gap(self):
        # For futures, MLK Day is NOT a holiday — CME trades some holidays
        # MLK Day 2026 = Mon Jan 19 (weekday) → should be flagged as gap
        dates = [date(2026, 1, 16), date(2026, 1, 20)]
        gaps = find_interior_gaps(dates, asset_class="futures")
        assert date(2026, 1, 19) in gaps


# ── group_contiguous_dates ─────────────────────────────────────────────────────


class TestGroupContiguousDates:
    def test_empty_returns_empty(self):
        assert group_contiguous_dates([]) == []

    def test_single_date(self):
        d = date(2026, 1, 5)
        assert group_contiguous_dates([d]) == [(d, d)]

    def test_three_contiguous_dates(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        assert group_contiguous_dates(dates) == [(date(2026, 1, 5), date(2026, 1, 7))]

    def test_two_separate_ranges(self):
        # Jan 5-6 and Jan 8-9 (gap at Jan 7)
        dates = [
            date(2026, 1, 5), date(2026, 1, 6),
            date(2026, 1, 8), date(2026, 1, 9),
        ]
        result = group_contiguous_dates(dates)
        assert result == [
            (date(2026, 1, 5), date(2026, 1, 6)),
            (date(2026, 1, 8), date(2026, 1, 9)),
        ]

    def test_non_contiguous_single_dates(self):
        dates = [date(2026, 1, 5), date(2026, 1, 7), date(2026, 1, 9)]
        result = group_contiguous_dates(dates)
        assert result == [
            (date(2026, 1, 5), date(2026, 1, 5)),
            (date(2026, 1, 7), date(2026, 1, 7)),
            (date(2026, 1, 9), date(2026, 1, 9)),
        ]


# ── compute_range_duration ─────────────────────────────────────────────────────


class TestComputeRangeDuration:
    def test_same_day_returns_one_d(self):
        d = date(2026, 1, 5)
        assert compute_range_duration(d, d) == "1 D"

    def test_end_before_start_returns_one_d(self):
        assert compute_range_duration(date(2026, 1, 10), date(2026, 1, 5)) == "1 D"

    def test_short_range_four_cal_days(self):
        # 4 cal days + 2 buffer = 6
        start = date(2026, 1, 5)
        end = date(2026, 1, 9)
        assert compute_range_duration(start, end) == "6 D"

    def test_about_two_months(self):
        # ~60 cal days + 2 = 62 D
        start = date(2026, 1, 5)
        end = date(2026, 3, 6)  # 60 cal days later
        result = compute_range_duration(start, end)
        assert result.endswith(" D")
        days = int(result.split()[0])
        assert 60 <= days <= 70

    def test_exactly_180_days_with_buffer(self):
        # 178 cal days + 2 = 180 → still "180 D"
        start = date(2026, 1, 1)
        end = date(2026, 6, 28)  # 178 days later → +2 = 180
        result = compute_range_duration(start, end)
        assert result.endswith(" D")

    def test_over_180_cal_days(self):
        # 200 cal days → "1 Y"
        start = date(2026, 1, 1)
        end = date(2026, 7, 20)  # well over 180 days
        assert compute_range_duration(start, end) == "1 Y"

    def test_over_one_year(self):
        # 400 cal days → "2 Y"
        start = date(2025, 1, 1)
        end = date(2026, 2, 5)  # ~400 days
        assert compute_range_duration(start, end) == "2 Y"


# ── get_all_trade_dates ────────────────────────────────────────────────────────


class TestGetAllTradeDates:
    @pytest.mark.integration
    def test_empty_bronze_returns_empty_dict(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert result == {}
        finally:
            client.close()

    @pytest.mark.integration
    def test_reads_known_dates_for_one_symbol(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        known_dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        _write_parquet(bronze_dir, "AAPL", known_dates)

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert "AAPL" in result
            assert result["AAPL"] == known_dates
        finally:
            client.close()

    @pytest.mark.integration
    def test_reads_multiple_symbols(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        aapl_dates = [date(2026, 1, 5), date(2026, 1, 6)]
        msft_dates = [date(2026, 1, 5), date(2026, 1, 7)]
        _write_parquet(bronze_dir, "AAPL", aapl_dates)
        _write_parquet(bronze_dir, "MSFT", msft_dates)

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert result["AAPL"] == aapl_dates
            assert result["MSFT"] == msft_dates
        finally:
            client.close()

    @pytest.mark.integration
    def test_dates_are_sorted_ascending(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        dates = [date(2026, 1, 7), date(2026, 1, 5), date(2026, 1, 6)]
        _write_parquet(bronze_dir, "NVDA", sorted(dates))  # write sorted (parquet requirement)

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert result["NVDA"] == sorted(dates)
        finally:
            client.close()

    @pytest.mark.integration
    def test_string_trade_date_is_handled(self, tmp_path):
        """Cover the defensive str-conversion branch when DuckDB returns strings."""
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        _write_parquet(bronze_dir, "TSLA", [date(2026, 1, 5)])

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            # Patch _query to return trade_date as a string rather than a date object
            original_query = client._query

            def _patched_query(sql, params=None):
                rows = original_query(sql, params)
                return [
                    {**row, "trade_date": str(row["trade_date"])}
                    for row in rows
                ]

            with patch.object(client, "_query", side_effect=_patched_query):
                result = get_all_trade_dates(client)
            assert result["TSLA"] == [date(2026, 1, 5)]
        finally:
            client.close()


# ── _resolve_bronze_dir ────────────────────────────────────────────────────────


class TestResolvedBronzeDir:
    def test_equity_path(self):
        result = _resolve_bronze_dir("equity")
        assert str(result).endswith("asset_class=equity")

    def test_futures_path(self):
        result = _resolve_bronze_dir("futures")
        assert str(result).endswith("asset_class=futures")

    def test_volatility_path(self):
        result = _resolve_bronze_dir("volatility")
        assert str(result).endswith("asset_class=volatility")


# ── _send_alert ────────────────────────────────────────────────────────────────


class TestSendAlert:
    def test_calls_subprocess_run_with_correct_args(self, tmp_path):
        log_path = tmp_path / "health_check_2026-04-05.log"
        log_path.touch()

        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            _send_alert("2026-04-05", "equity", 20, 15, log_path)

        assert mock_run.called
        cmd = mock_run.call_args[0][0]
        assert "livewire_ops.py" in cmd[1]
        assert cmd[2] == "send-alert"
        assert "--run-date" in cmd
        assert "2026-04-05" in cmd
        assert "--log-file" in cmd
        assert str(log_path) in cmd
        assert "--error-summary" in cmd
        assert "--job-name" in cmd
        assert "health_check" in cmd

    def test_error_summary_contains_asset_class_and_counts(self, tmp_path):
        log_path = tmp_path / "health_check.log"
        log_path.touch()

        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            _send_alert("2026-04-05", "futures", 5, 3, log_path)

        cmd = mock_run.call_args[0][0]
        summary_idx = cmd.index("--error-summary") + 1
        summary = cmd[summary_idx]
        assert "futures" in summary
        assert "5" in summary
        assert "3" in summary


# ── main() ────────────────────────────────────────────────────────────────────


class TestMain:
    def test_not_trading_day_without_force_exits(self):
        """Non-trading day without --force should print warning and return."""
        # Patch is_trading_day to always return False so the check triggers regardless of real date
        with patch("livewire_scripts.health_check.is_trading_day", return_value=False):
            with patch("sys.argv", ["health_check.py", "--asset-class", "equity"]):
                with patch("livewire_scripts.health_check.BronzeClient") as mock_bronze_cls:
                    main()
                    # BronzeClient should not have been opened
                    mock_bronze_cls.assert_not_called()

    def test_dry_run_reports_but_does_not_backfill(self, tmp_path):
        """Dry-run should detect gaps but not connect to IB."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        # Jan 5 (Mon), Jan 6 (Tue), Jan 8 (Thu) — missing Jan 7 (Wed)
        _write_parquet(bronze_dir, "AAPL", [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8)])

        # Use --force so trading-day check is skipped and date.today() doesn't matter
        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("sys.argv", ["health_check.py", "--dry-run", "--force"]):
                with patch("clients.ib_client.IBClient") as mock_ib_cls:
                    main()
                    # IB should NOT have been used in dry-run mode
                    mock_ib_cls.assert_not_called()

    def test_no_gaps_prints_green(self, tmp_path):
        """When no interior gaps exist, main should return cleanly without IB."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        # Full week Mon-Fri, no gaps
        _write_parquet(bronze_dir, "AAPL", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
            date(2026, 1, 8), date(2026, 1, 9),
        ])

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("sys.argv", ["health_check.py", "--force"]):
                with patch("clients.ib_client.IBClient") as mock_ib_cls:
                    main()
                    mock_ib_cls.assert_not_called()

    def test_no_tickers_in_bronze_returns_early(self, tmp_path):
        """Empty bronze directory should print warning and return."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("sys.argv", ["health_check.py", "--force"]):
                with patch("clients.ib_client.IBClient") as mock_ib_cls:
                    main()
                    mock_ib_cls.assert_not_called()

    def test_volatility_backfill_not_implemented(self, tmp_path):
        """Volatility asset class should print 'not yet implemented' and return."""
        bronze_dir = tmp_path / "bronze" / "asset_class=volatility"
        bronze_dir.mkdir(parents=True)

        # VIX with a gap: Jan 5, Jan 6, Jan 8 (missing Jan 7)
        _write_parquet(bronze_dir, "VIX", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8),
        ])

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("sys.argv", ["health_check.py", "--force", "--asset-class", "volatility"]):
                with patch("clients.ib_client.IBClient") as mock_ib_cls:
                    main()
                    # IB should NOT have been connected for volatility
                    mock_ib_cls.assert_not_called()

    def test_backfill_with_mocked_ib(self, tmp_path):
        """Full backfill path with mocked IB — should repair gaps and not send alert below threshold."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        # AAPL with gap on Jan 7 (Wed)
        _write_parquet(bronze_dir, "AAPL", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8),
        ])

        # Build a fake IB bar for the missing date
        fake_bar = SimpleNamespace(
            date=date(2026, 1, 7),
            open=150.0,
            high=152.0,
            low=149.0,
            close=151.0,
            volume=1_000_000,
        )

        mock_ib_instance = MagicMock()
        mock_ib_instance.ib.run.return_value = [fake_bar]
        mock_ib_instance.get_historical_data_async = MagicMock(return_value=MagicMock())

        mock_ib_cm = MagicMock()
        mock_ib_cm.__enter__ = MagicMock(return_value=mock_ib_instance)
        mock_ib_cm.__exit__ = MagicMock(return_value=False)

        warehouse_dir = tmp_path

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("livewire_scripts.health_check._WAREHOUSE_DIR", warehouse_dir):
                with patch("sys.argv", [
                    "health_check.py", "--force", "--alert-threshold", "100",
                ]):
                    with patch("livewire_scripts.health_check.subprocess.run") as mock_subprocess:
                        with patch("clients.ib_client.IBClient", return_value=mock_ib_cm) as mock_ib_cls:
                            with patch("livewire_scripts.daily_update.fetch_fallback_bars", return_value=([], [])):
                                main()
                                # IB connection should have been attempted
                                mock_ib_cls.assert_called_once()
                                # No alert since threshold is 100 and we repaired < 100
                                mock_subprocess.assert_not_called()

    def test_alert_sent_when_threshold_exceeded(self, tmp_path):
        """Alert email should be sent when repaired gaps meet or exceed threshold."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        # AAPL with a gap on Jan 7
        _write_parquet(bronze_dir, "AAPL", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8),
        ])

        fake_bar = SimpleNamespace(
            date=date(2026, 1, 7),
            open=150.0,
            high=152.0,
            low=149.0,
            close=151.0,
            volume=1_000_000,
        )

        mock_ib_instance = MagicMock()
        mock_ib_instance.ib.run.return_value = [fake_bar]
        mock_ib_instance.get_historical_data_async = MagicMock(return_value=MagicMock())

        mock_ib_cm = MagicMock()
        mock_ib_cm.__enter__ = MagicMock(return_value=mock_ib_instance)
        mock_ib_cm.__exit__ = MagicMock(return_value=False)

        warehouse_dir = tmp_path

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("livewire_scripts.health_check._WAREHOUSE_DIR", warehouse_dir):
                with patch("sys.argv", [
                    "health_check.py", "--force", "--alert-threshold", "1",
                ]):
                    with patch("livewire_scripts.health_check.subprocess.run") as mock_subprocess:
                        with patch("clients.ib_client.IBClient", return_value=mock_ib_cm):
                            with patch("livewire_scripts.daily_update.fetch_fallback_bars", return_value=([], [])):
                                main()
                                # Alert should be sent since threshold=1 and we repaired >=1
                                mock_subprocess.assert_called_once()
                                cmd = mock_subprocess.call_args[0][0]
                                assert "health_check" in cmd

    def test_backfill_ib_exception_handled(self, tmp_path):
        """IB exceptions during historical data fetch should be caught and bars treated as empty."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        _write_parquet(bronze_dir, "AAPL", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8),
        ])

        mock_ib_instance = MagicMock()
        # First call to ib.run() is ib.connect() — returns None (success)
        # Second call is qualifyContractsAsync — returns None
        # Third call is get_historical_data_async — raises exception
        mock_ib_instance.ib.run.side_effect = [None, None, RuntimeError("fetch failed")]
        mock_ib_instance.get_historical_data_async = MagicMock(return_value=MagicMock())

        mock_ib_cm = MagicMock()
        mock_ib_cm.__enter__ = MagicMock(return_value=mock_ib_instance)
        mock_ib_cm.__exit__ = MagicMock(return_value=False)

        warehouse_dir = tmp_path

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("livewire_scripts.health_check._WAREHOUSE_DIR", warehouse_dir):
                with patch("sys.argv", ["health_check.py", "--force", "--alert-threshold", "100"]):
                    with patch("livewire_scripts.health_check.subprocess.run"):
                        with patch("clients.ib_client.IBClient", return_value=mock_ib_cm):
                            with patch("livewire_scripts.daily_update.fetch_fallback_bars", return_value=([], [])):
                                # Should not raise — exception is caught internally
                                main()

    def test_backfill_ib_returns_empty_bars(self, tmp_path):
        """When IB returns no bars, the symbol is skipped gracefully."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        _write_parquet(bronze_dir, "AAPL", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8),
        ])

        mock_ib_instance = MagicMock()
        # Return empty list from ib.run (no bars)
        mock_ib_instance.ib.run.return_value = []
        mock_ib_instance.get_historical_data_async = MagicMock(return_value=MagicMock())

        mock_ib_cm = MagicMock()
        mock_ib_cm.__enter__ = MagicMock(return_value=mock_ib_instance)
        mock_ib_cm.__exit__ = MagicMock(return_value=False)

        warehouse_dir = tmp_path

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("livewire_scripts.health_check._WAREHOUSE_DIR", warehouse_dir):
                with patch("sys.argv", ["health_check.py", "--force", "--alert-threshold", "100"]):
                    with patch("livewire_scripts.health_check.subprocess.run"):
                        with patch("clients.ib_client.IBClient", return_value=mock_ib_cm):
                            with patch("livewire_scripts.daily_update.fetch_fallback_bars", return_value=([], [])):
                                main()  # Should complete without error

    def test_backfill_validate_bars_issues_logged(self, tmp_path):
        """validate_bars issues should be logged as warnings."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        _write_parquet(bronze_dir, "AAPL", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8),
        ])

        # A bar with high < low (invalid) so validate_bars returns an issue
        bad_bar = SimpleNamespace(
            date=date(2026, 1, 7),
            open=150.0,
            high=148.0,  # high < low → invalid
            low=149.0,
            close=150.0,
            volume=1_000_000,
        )

        mock_ib_instance = MagicMock()
        mock_ib_instance.ib.run.return_value = [bad_bar]
        mock_ib_instance.get_historical_data_async = MagicMock(return_value=MagicMock())

        mock_ib_cm = MagicMock()
        mock_ib_cm.__enter__ = MagicMock(return_value=mock_ib_instance)
        mock_ib_cm.__exit__ = MagicMock(return_value=False)

        warehouse_dir = tmp_path

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("livewire_scripts.health_check._WAREHOUSE_DIR", warehouse_dir):
                with patch("sys.argv", ["health_check.py", "--force", "--alert-threshold", "100"]):
                    with patch("livewire_scripts.health_check.subprocess.run"):
                        with patch("clients.ib_client.IBClient", return_value=mock_ib_cm):
                            with patch("livewire_scripts.health_check.log") as mock_log:
                                with patch("livewire_scripts.daily_update.fetch_fallback_bars", return_value=([], [])):
                                    main()
                                    # log.warning should have been called for the bad bar issue
                                    mock_log.warning.assert_called()

    def test_backfill_futures_path(self, tmp_path):
        """Backfill for futures should use bars_to_futures_rows and contract structure."""
        bronze_dir = tmp_path / "bronze" / "asset_class=futures"
        bronze_dir.mkdir(parents=True)

        # Use futures parquet schema
        futures_schema = pa.schema([
            ("trade_date", pa.date32()),
            ("contract_id", pa.int64()),
            ("root_symbol", pa.string()),
            ("expiry_date", pa.date32()),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("settlement", pa.float64()),
            ("volume", pa.int64()),
            ("open_interest", pa.int64()),
        ])
        sym_dir = bronze_dir / "symbol=ES_202506"
        sym_dir.mkdir(parents=True)
        rows = [
            {
                "trade_date": date(2026, 1, 5),
                "contract_id": 1,
                "root_symbol": "ES",
                "expiry_date": date(2026, 6, 1),
                "open": 5000.0, "high": 5010.0, "low": 4990.0, "close": 5005.0,
                "settlement": 5005.0, "volume": 100000, "open_interest": 0,
            },
            {
                "trade_date": date(2026, 1, 7),  # gap: missing Jan 6
                "contract_id": 1,
                "root_symbol": "ES",
                "expiry_date": date(2026, 6, 1),
                "open": 5010.0, "high": 5020.0, "low": 5000.0, "close": 5015.0,
                "settlement": 5015.0, "volume": 110000, "open_interest": 0,
            },
        ]
        table = pa.Table.from_pylist(rows, schema=futures_schema)
        pq.write_table(table, sym_dir / "1d.parquet", compression="snappy")

        fake_bar = SimpleNamespace(
            date=date(2026, 1, 6),
            open=5001.0,
            high=5011.0,
            low=4991.0,
            close=5006.0,
            volume=105000,
        )

        mock_ib_instance = MagicMock()
        mock_ib_instance.ib.run.return_value = [fake_bar]
        mock_ib_instance.get_historical_data_async = MagicMock(return_value=MagicMock())

        mock_ib_cm = MagicMock()
        mock_ib_cm.__enter__ = MagicMock(return_value=mock_ib_instance)
        mock_ib_cm.__exit__ = MagicMock(return_value=False)

        warehouse_dir = tmp_path

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("livewire_scripts.health_check._WAREHOUSE_DIR", warehouse_dir):
                with patch("sys.argv", [
                    "health_check.py", "--force", "--asset-class", "futures",
                    "--alert-threshold", "100",
                ]):
                    with patch("livewire_scripts.health_check.subprocess.run"):
                        with patch("clients.ib_client.IBClient", return_value=mock_ib_cm):
                            main()  # Should complete without error

    def test_backfill_equity_fallback_succeeds(self, tmp_path):
        """When IB returns empty bars but fallback succeeds, fallback bars are merged."""
        bronze_dir = tmp_path / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        _write_parquet(bronze_dir, "AAPL", [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8),
        ])

        # IB returns empty bars — so gap remains unresolved after IB
        mock_ib_instance = MagicMock()
        mock_ib_instance.ib.run.return_value = []
        mock_ib_instance.get_historical_data_async = MagicMock(return_value=MagicMock())

        mock_ib_cm = MagicMock()
        mock_ib_cm.__enter__ = MagicMock(return_value=mock_ib_instance)
        mock_ib_cm.__exit__ = MagicMock(return_value=False)

        # Fallback returns a bar for the missing date
        fallback_bar = SimpleNamespace(
            date=date(2026, 1, 7),
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )

        warehouse_dir = tmp_path

        with patch("livewire_scripts.health_check._resolve_bronze_dir", return_value=bronze_dir):
            with patch("livewire_scripts.health_check._WAREHOUSE_DIR", warehouse_dir):
                with patch("sys.argv", [
                    "health_check.py", "--force", "--alert-threshold", "100",
                ]):
                    with patch("livewire_scripts.health_check.subprocess.run"):
                        with patch("clients.ib_client.IBClient", return_value=mock_ib_cm):
                            with patch("livewire_scripts.daily_update.fetch_fallback_bars", return_value=([fallback_bar], ["fallback"])):
                                with patch("clients.daily_bar_fallback.DailyBarFallbackClient"):
                                    main()  # Should complete without error


# ── Intraday helpers ──────────────────────────────────────────────────────────

from datetime import datetime, time, timezone  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

import pyarrow as pa  # noqa: E402,F811
import pyarrow.parquet as pq  # noqa: E402,F811

_INTRADAY_SCHEMA = pa.schema(
    [
        ("bar_timestamp", pa.timestamp("us", tz="UTC")),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
    ]
)
_ET = ZoneInfo("America/New_York")


def _et_to_utc(d: date, h: int, m: int) -> datetime:
    return datetime(d.year, d.month, d.day, h, m, tzinfo=_ET).astimezone(timezone.utc)


def _write_intraday_parquet(
    bronze_dir: Path,
    symbol: str,
    timeframe: str,
    timestamps: list[datetime],
) -> None:
    sym_dir = bronze_dir / f"symbol={symbol}"
    sym_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "bar_timestamp": ts.astimezone(timezone.utc),
            "symbol_id": 1,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1000,
        }
        for ts in sorted(timestamps)
    ]
    table = pa.Table.from_pylist(rows, schema=_INTRADAY_SCHEMA)
    fname = "1h.parquet" if timeframe == "1h" else "5m.parquet"
    pq.write_table(table, sym_dir / fname, compression="snappy")


# ── generate_expected_intraday_timestamps ─────────────────────────────────────


class TestGenerateExpectedIntradayTimestamps:
    def test_5m_full_day_count(self):
        d = date(2026, 4, 6)  # Monday, no holiday
        ts = generate_expected_intraday_timestamps([d], "5m")
        # 9:30 → 15:55 inclusive = (16:00 - 9:30)/5 = 78 bars
        assert len(ts) == 78

    def test_1h_full_day_count(self):
        d = date(2026, 4, 6)
        ts = generate_expected_intraday_timestamps([d], "1h")
        # IB returns: 9:30 (partial open), 10:00, 11:00, 12:00, 13:00, 14:00, 15:00
        # 15:00 covers 15:00-16:00 (last full bar). 7 bars per session.
        assert len(ts) == 7
        et_minutes = sorted(t.astimezone(_ET).strftime("%H:%M") for t in ts)
        assert et_minutes == [
            "09:30", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00",
        ]

    def test_5m_early_close(self):
        # 2026-11-27 is Black Friday (early close at 13:00 ET)
        d = date(2026, 11, 27)
        ts = generate_expected_intraday_timestamps([d], "5m")
        # 9:30 → 12:55 inclusive = (13:00 - 9:30)/5 = 42 bars
        assert len(ts) == 42

    def test_skips_holidays(self):
        # 2026-12-25 is Christmas (NYSE closed)
        d = date(2026, 12, 25)
        ts = generate_expected_intraday_timestamps([d], "5m")
        assert ts == set()

    def test_invalid_timeframe_raises(self):
        with pytest.raises(ValueError):
            generate_expected_intraday_timestamps([date(2026, 4, 6)], "1d")

    def test_empty_input(self):
        assert generate_expected_intraday_timestamps([], "5m") == set()


# ── find_intraday_gaps ────────────────────────────────────────────────────────


class TestFindIntradayGaps:
    def test_clean_series_no_gaps(self):
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        missing, halts = find_intraday_gaps(full, "5m")
        assert missing == []
        assert halts == []

    def test_too_few_bars_returns_empty(self):
        ts = [_et_to_utc(date(2026, 4, 6), 9, 30)]
        assert find_intraday_gaps(ts, "5m") == ([], [])

    def test_interior_missing_detected(self):
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        # Drop one bar in the middle
        dropped = full.pop(20)
        missing, halts = find_intraday_gaps(full, "5m")
        assert dropped in missing
        # Single 5m gap is < 30 min → flagged as halt
        assert len(halts) == 1

    def test_short_run_flagged_as_halt(self):
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        # Drop 3 consecutive bars (15 min) → halt
        keep = [t for i, t in enumerate(full) if i not in (30, 31, 32)]
        missing, halts = find_intraday_gaps(keep, "5m")
        assert len(missing) == 3
        assert len(halts) == 1

    def test_two_separate_runs_each_classified(self):
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        # Drop two separate single-bar gaps with bars in between → exercises
        # the non-contiguous-run branch in find_intraday_gaps
        keep = [t for i, t in enumerate(full) if i not in (10, 40)]
        missing, halts = find_intraday_gaps(keep, "5m")
        assert len(missing) == 2
        assert len(halts) == 2

    def test_long_run_not_halt(self):
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        # Drop 6 consecutive bars (30 min) → NOT a halt
        keep = [t for i, t in enumerate(full) if not (30 <= i < 36)]
        missing, halts = find_intraday_gaps(keep, "5m")
        assert len(missing) == 6
        assert halts == []


# ── report_intraday_health ────────────────────────────────────────────────────


class TestReportIntradayHealth:
    def test_clean_symbol_no_gaps(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full)
        summary = report_intraday_health("5m", bronze_dir)
        assert summary["symbols_scanned"] == 1
        assert summary["symbols_with_gaps"] == 0

    def test_symbol_with_interior_gap(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        full.pop(40)
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full)
        summary = report_intraday_health("5m", bronze_dir)
        assert summary["symbols_with_gaps"] == 1
        assert summary["total_missing"] == 1
        assert "AAPL" in summary["by_symbol"]

    def test_symbol_filter(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full[:-2])
        _write_intraday_parquet(bronze_dir, "MSFT", "5m", full[:-2])
        summary = report_intraday_health("5m", bronze_dir, symbol_filter="AAPL")
        assert summary["symbols_scanned"] == 1
        assert "AAPL" in summary["by_symbol"]
        assert "MSFT" not in summary["by_symbol"]

    def test_empty_bronze(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        summary = report_intraday_health("5m", bronze_dir)
        assert summary["symbols_scanned"] == 0


# ── repair_intraday_window ────────────────────────────────────────────────────


class TestRepairIntradayWindow:
    def test_invokes_subprocess_with_expected_args(self):
        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            mock_run.return_value = SimpleNamespace(returncode=0)
            rc = repair_intraday_window(
                symbol="AAPL",
                timeframe="5m",
                since=date(2026, 4, 1),
                host="127.0.0.1",
                port=4001,
            )
            assert rc == 0
            cmd = mock_run.call_args[0][0]
            assert "livewire_ingest.py" in cmd[1]
            assert "--tickers" in cmd and "AAPL" in cmd
            assert "--timeframe" in cmd and "5m" in cmd
            # repair_intraday_window converts --since to --years
            assert "--years" in cmd

    def test_propagates_nonzero_returncode(self):
        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            mock_run.return_value = SimpleNamespace(returncode=2)
            rc = repair_intraday_window("AAPL", "5m", date(2026, 4, 1), "h", 4001)
            assert rc == 2


# ── main() intraday branch ────────────────────────────────────────────────────


class TestMainIntradayBranch:
    def test_intraday_requires_timeframe(self):
        with patch.object(sys, "argv", ["health_check.py", "--intraday"]):
            with pytest.raises(SystemExit):
                main()

    def test_default_intraday_no_subprocess(self, tmp_path, monkeypatch):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full)
        monkeypatch.setattr(
            "livewire_scripts.health_check._resolve_bronze_dir", lambda ac: bronze_dir
        )
        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            with patch.object(
                sys, "argv",
                ["health_check.py", "--intraday", "--timeframe", "5m", "--force"],
            ):
                main()
        assert mock_run.call_count == 0

    def test_full_scope_invokes_repair_subprocess(self, tmp_path, monkeypatch):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full)
        monkeypatch.setattr(
            "livewire_scripts.health_check._resolve_bronze_dir", lambda ac: bronze_dir
        )
        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            mock_run.return_value = SimpleNamespace(returncode=0)
            with patch.object(
                sys, "argv",
                [
                    "health_check.py", "--intraday", "--timeframe", "5m",
                    "--symbol", "AAPL", "--since", "2026-04-01", "--force",
                ],
            ):
                main()
        assert mock_run.call_count == 1
        cmd = mock_run.call_args[0][0]
        assert "AAPL" in cmd

    def test_partial_scope_symbol_only_no_repair(self, tmp_path, monkeypatch):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full)
        monkeypatch.setattr(
            "livewire_scripts.health_check._resolve_bronze_dir", lambda ac: bronze_dir
        )
        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            with patch.object(
                sys, "argv",
                [
                    "health_check.py", "--intraday", "--timeframe", "5m",
                    "--symbol", "AAPL", "--force",
                ],
            ):
                main()
        assert mock_run.call_count == 0

    def test_dry_run_skips_repair_even_with_full_scope(self, tmp_path, monkeypatch):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full)
        monkeypatch.setattr(
            "livewire_scripts.health_check._resolve_bronze_dir", lambda ac: bronze_dir
        )
        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            with patch.object(
                sys, "argv",
                [
                    "health_check.py", "--intraday", "--timeframe", "5m",
                    "--symbol", "AAPL", "--since", "2026-04-01",
                    "--dry-run", "--force",
                ],
            ):
                main()
        assert mock_run.call_count == 0

    def test_repair_nonzero_logged(self, tmp_path, monkeypatch):
        bronze_dir = tmp_path / "bronze"
        d = date(2026, 4, 6)
        full = sorted(generate_expected_intraday_timestamps([d], "5m"))
        _write_intraday_parquet(bronze_dir, "AAPL", "5m", full)
        monkeypatch.setattr(
            "livewire_scripts.health_check._resolve_bronze_dir", lambda ac: bronze_dir
        )
        with patch("livewire_scripts.health_check.subprocess.run") as mock_run:
            mock_run.return_value = SimpleNamespace(returncode=3)
            with patch.object(
                sys, "argv",
                [
                    "health_check.py", "--intraday", "--timeframe", "5m",
                    "--symbol", "AAPL", "--since", "2026-04-01", "--force",
                ],
            ):
                main()
        assert mock_run.call_count == 1
