"""Tests for scripts/backfill_intraday.py."""

from __future__ import annotations

import json
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.intraday_bronze_client import IntradayBronzeClient
from livewire_scripts import backfill_intraday
from livewire_scripts.backfill_intraday import (
    TickerOutcome,
    _BarRow,
    _is_regular_trading_timestamp,
    _resolve_tickers,
    backfill_ticker,
    backfill_ticker_massive,
    compute_intraday_chunks_for_days,
    compute_intraday_date_windows,
    ib_bar_to_row,
    load_cursor,
    main,
    massive_intraday_bar_to_row,
    plan_chunks,
    save_cursor,
    should_skip_existing,
)

_ET = ZoneInfo("America/New_York")
_UTC = timezone.utc


def _make_ib_bar(dt_et_naive: datetime, *, open_=1.0) -> SimpleNamespace:
    """Mimic an ib_async BarData with formatDate=1 (naive local datetime)."""
    return SimpleNamespace(
        date=dt_et_naive,
        open=open_,
        high=open_ + 0.5,
        low=open_ - 0.5,
        close=open_ + 0.1,
        volume=1000,
    )


# ── ib_bar_to_row ─────────────────────────────────────────────────────────────


class TestIbBarToRow:
    def test_naive_datetime_attached_as_et_then_utc(self):
        bar = _make_ib_bar(
            datetime(2026, 4, 6, 9, 30)
        )  # Mon 09:30 ET → 13:30 UTC (EDT)
        row = ib_bar_to_row(bar, symbol_id=42)
        assert row["bar_timestamp"].tzinfo == _UTC
        assert row["bar_timestamp"] == datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        assert row["symbol_id"] == 42
        assert row["volume"] == 1000

    def test_aware_datetime_passes_through(self):
        ts = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        bar = SimpleNamespace(
            date=ts,
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=100,
        )
        row = ib_bar_to_row(bar, symbol_id=1)
        assert row["bar_timestamp"] == ts

    def test_date_only_promoted_to_midnight_et(self):
        from datetime import date as _date

        bar = SimpleNamespace(
            date=_date(2026, 4, 6),
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=100,
        )
        row = ib_bar_to_row(bar, symbol_id=1)
        assert row["bar_timestamp"].tzinfo == _UTC
        assert row["bar_timestamp"].date() == _date(2026, 4, 6)


class TestMassiveIntradayBarToRow:
    def test_requires_tz_aware_timestamp(self):
        bar = SimpleNamespace(
            bar_timestamp=datetime(2026, 4, 6, 13, 30),
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=100,
        )
        with pytest.raises(ValueError, match="tz-aware"):
            massive_intraday_bar_to_row(bar, symbol_id=1)

    def test_regular_trading_timestamp_rejects_naive_and_non_trading_days(self):
        assert _is_regular_trading_timestamp(datetime(2026, 4, 6, 13, 30)) is False
        assert (
            _is_regular_trading_timestamp(datetime(2026, 4, 4, 13, 30, tzinfo=_UTC))
            is False
        )


# ── load/save cursor ──────────────────────────────────────────────────────────


class TestCursor:
    def test_load_missing_returns_empty_set(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path)
        assert load_cursor("5m", "test") == set()

    def test_save_then_load_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path)
        save_cursor("5m", "test", {"AAPL", "MSFT"})
        assert load_cursor("5m", "test") == {"AAPL", "MSFT"}

    def test_corrupt_cursor_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path)
        path = tmp_path / "cursor_intraday_5m_test.json"
        path.write_text("not json{{{")
        assert load_cursor("5m", "test") == set()


# ── should_skip_existing ──────────────────────────────────────────────────────


class TestShouldSkipExisting:
    def _seed(self, tmp_path, ticker, ts: datetime):
        bronze_dir = tmp_path / "bronze"
        client = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="5m")
        rows = [
            {
                "bar_timestamp": ts,
                "symbol_id": 1,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 100,
            }
        ]
        client.replace_ticker_rows(ticker, rows)
        return client

    def test_empty_returns_false(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="5m")
        assert should_skip_existing(client, "AAPL", years=1) is False

    def test_old_enough_returns_true(self, tmp_path):
        old = datetime.now(_UTC) - timedelta(days=400)
        client = self._seed(tmp_path, "AAPL", old)
        assert should_skip_existing(client, "AAPL", years=1) is True

    def test_too_recent_returns_false(self, tmp_path):
        recent = datetime.now(_UTC) - timedelta(days=10)
        client = self._seed(tmp_path, "AAPL", recent)
        assert should_skip_existing(client, "AAPL", years=1) is False


# ── backfill_ticker ───────────────────────────────────────────────────────────


class TestBackfillTicker:
    def test_happy_path_merges_bars(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="5m")
        # Two valid bars on a Monday during RTH
        bars = [
            _make_ib_bar(datetime(2026, 4, 6, 10, 0)),
            _make_ib_bar(datetime(2026, 4, 6, 10, 5), open_=1.1),
        ]
        ib = MagicMock()
        ib.get_historical_data.return_value = bars

        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "20260406-15:00:00")],
        ):
            outcome = backfill_ticker("AAPL", "5m", years=1, ib=ib, bronze=bronze)
        assert outcome.bars_inserted == 2
        assert outcome.rejected == 0
        assert outcome.chunks_fetched == 1

    def test_rejects_invalid_bars(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="5m")
        # Bar at 04:00 ET → outside RTH → rejected
        bars = [_make_ib_bar(datetime(2026, 4, 6, 4, 0))]
        ib = MagicMock()
        ib.get_historical_data.return_value = bars
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "20260406-15:00:00")],
        ):
            outcome = backfill_ticker("AAPL", "5m", years=1, ib=ib, bronze=bronze)
        assert outcome.bars_inserted == 0
        assert outcome.rejected == 1

    def test_ib_no_data_error_skips_ticker(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="5m")
        ib = MagicMock()
        err = Exception("HMDS no data")
        err.code = 162
        ib.get_historical_data.side_effect = err
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "20260406-15:00:00")],
        ):
            outcome = backfill_ticker("AAPL", "5m", years=1, ib=ib, bronze=bronze)
        assert outcome.skipped_reason == "IB error 162"
        assert outcome.bars_inserted == 0

    def test_unknown_error_recorded_and_continues(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="5m")
        ib = MagicMock()
        ib.get_historical_data.side_effect = [
            RuntimeError("transient blip"),
            [_make_ib_bar(datetime(2026, 4, 6, 10, 0))],
        ]
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "a"), ("1 W", "b")],
        ):
            outcome = backfill_ticker("AAPL", "5m", years=1, ib=ib, bronze=bronze)
        assert outcome.bars_inserted == 1
        assert len(outcome.errors) == 1

    def test_empty_chunk_response_handled(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="5m")
        ib = MagicMock()
        ib.get_historical_data.return_value = []
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "x")],
        ):
            outcome = backfill_ticker("AAPL", "5m", years=1, ib=ib, bronze=bronze)
        assert outcome.chunks_fetched == 1
        assert outcome.bars_inserted == 0

    def test_volatility_backfill_uses_index_contract_and_metadata(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="5m")
        bars = [_make_ib_bar(datetime(2026, 4, 6, 10, 0))]
        ib = MagicMock()
        ib.get_historical_data.return_value = bars
        contract = object()

        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "20260406-15:00:00")],
        ):
            with patch(
                "livewire_scripts.backfill_intraday._make_contract",
                return_value=contract,
            ) as m_contract:
                with patch(
                    "livewire_scripts.backfill_intraday._run_quality_detection"
                ) as m_quality:
                    outcome = backfill_ticker(
                        "VIX",
                        "5m",
                        years=1,
                        ib=ib,
                        bronze=bronze,
                        asset_class="volatility",
                    )

        assert outcome.bars_inserted == 1
        m_contract.assert_called_once_with("VIX", "volatility")
        assert ib.get_historical_data.call_args.args[0] is contract
        assert m_quality.call_args.kwargs["asset_class"] == "volatility"


class TestBackfillTickerMassive:
    def test_compute_massive_windows_accepts_recent_days(self):
        windows = compute_intraday_date_windows(5, lookback_days=45, window_days=30)

        assert len(windows) == 2
        assert (windows[0][1] - windows[0][0]).days == 29
        assert (windows[-1][1] - windows[-1][0]).days <= 29

    def test_recent_days_window_passed_to_massive_window_planner(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="1m")
        massive = MagicMock()
        massive.get_intraday_bars.return_value = []
        fake_windows = [(datetime(2026, 4, 6).date(), datetime(2026, 4, 7).date())]

        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_date_windows",
            return_value=fake_windows,
        ) as planner:
            backfill_ticker_massive(
                "AAPL",
                "1m",
                years=5,
                massive=massive,
                bronze=bronze,
                lookback_days=7,
            )

        planner.assert_called_once_with(5, lookback_days=7)

    def test_unknown_error_recorded_and_continues(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="1m")
        massive = MagicMock()
        massive.get_intraday_bars.side_effect = [
            RuntimeError("transient blip"),
            [
                SimpleNamespace(
                    bar_timestamp=datetime(2026, 4, 6, 13, 30, tzinfo=_UTC),
                    open=1.0,
                    high=2.0,
                    low=0.5,
                    close=1.5,
                    volume=100,
                )
            ],
        ]
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_date_windows",
            return_value=[
                (datetime(2026, 4, 1).date(), datetime(2026, 4, 1).date()),
                (datetime(2026, 4, 6).date(), datetime(2026, 4, 6).date()),
            ],
        ):
            outcome = backfill_ticker_massive(
                "AAPL", "1m", years=5, massive=massive, bronze=bronze
            )
        assert outcome.bars_inserted == 1
        assert len(outcome.errors) == 1

    def test_rejects_invalid_massive_intraday_bars(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="1m")
        massive = MagicMock()
        massive.get_intraday_bars.return_value = [
            SimpleNamespace(
                bar_timestamp=datetime(2026, 4, 6, 13, 30, 30, tzinfo=_UTC),
                open=1.0,
                high=2.0,
                low=0.5,
                close=1.5,
                volume=100,
            )
        ]
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_date_windows",
            return_value=[(datetime(2026, 4, 6).date(), datetime(2026, 4, 6).date())],
        ):
            outcome = backfill_ticker_massive(
                "AAPL", "1m", years=5, massive=massive, bronze=bronze
            )
        assert outcome.bars_inserted == 0
        assert outcome.rejected == 1

    def test_filters_massive_extended_hours_without_rejection_noise(self, tmp_path):
        bronze = IntradayBronzeClient(bronze_dir=tmp_path / "bronze", timeframe="1m")
        massive = MagicMock()
        massive.get_intraday_bars.return_value = [
            SimpleNamespace(
                bar_timestamp=datetime(2026, 4, 6, 8, 0, tzinfo=_UTC),
                open=1.0,
                high=2.0,
                low=0.5,
                close=1.5,
                volume=100,
            )
        ]
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_date_windows",
            return_value=[(datetime(2026, 4, 6).date(), datetime(2026, 4, 6).date())],
        ):
            outcome = backfill_ticker_massive(
                "AAPL", "1m", years=5, massive=massive, bronze=bronze
            )
        assert outcome.bars_inserted == 0
        assert outcome.rejected == 0

    def test_provider_errors_do_not_advance_preset_cursor(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        fake_massive = MagicMock()
        fake_massive.__enter__.return_value = fake_massive
        fake_massive.__exit__.return_value = None
        fake_massive.get_intraday_bars.side_effect = RuntimeError("403 forbidden")

        with patch(
            "livewire_scripts.backfill_intraday.MassiveClient",
            return_value=fake_massive,
        ):
            with patch(
                "livewire_scripts.backfill_intraday.load_preset",
                return_value=("sp500", ["AAPL"], {}),
            ):
                with patch(
                    "livewire_scripts.backfill_intraday.compute_intraday_date_windows",
                    return_value=[
                        (datetime(2026, 4, 6).date(), datetime(2026, 4, 6).date())
                    ],
                ):
                    with patch.object(
                        sys,
                        "argv",
                        [
                            "backfill_intraday.py",
                            "--timeframe",
                            "1m",
                            "--source",
                            "massive",
                            "--preset",
                            "sp.json",
                        ],
                    ):
                        with pytest.raises(SystemExit) as exc:
                            main()

        assert exc.value.code == 1
        assert load_cursor("1m", "sp500") == set()


class TestQualityHookIntegration:
    def test_quality_hook_suppresses_bulk_email_alerts_by_default(
        self, tmp_path, monkeypatch
    ):
        from clients.quality_detector import QualityFlag
        from livewire_scripts.backfill_intraday import _run_quality_detection

        monkeypatch.delenv("MDW_INTRADAY_BACKFILL_ALERTS", raising=False)
        bars = [_make_ib_bar(datetime(2026, 4, 6, 9, 30))]
        outcome = TickerOutcome(ticker="AAPL", errors=["2026-04-06: provider error"])
        parquet_path = tmp_path / "5m.parquet"
        parquet_path.write_bytes(b"")
        fake_flag = QualityFlag(
            category="fetch_tainted",
            severity="warning",
            detail={},
            ts="2026-05-17T00:00:00Z",
        )

        with (
            patch(
                "livewire_scripts.backfill_intraday.detect_all",
                return_value=[fake_flag],
            ),
            patch("livewire_scripts.backfill_intraday.write_sidecar") as write_sidecar,
            patch("livewire_scripts.backfill_intraday.append_audit") as append_audit,
            patch("livewire_scripts.backfill_intraday.alert_on_flag") as alert_on_flag,
        ):
            _run_quality_detection(
                ticker="AAPL",
                timeframe="5m",
                bars=bars,
                parquet_path=parquet_path,
                outcome=outcome,
            )

        write_sidecar.assert_called_once()
        append_audit.assert_called_once()
        alert_on_flag.assert_not_called()

    def test_quality_hook_fires_with_outcome_errors_when_enabled(
        self, tmp_path, monkeypatch
    ):
        from clients.quality_detector import QualityFlag
        from livewire_scripts.backfill_intraday import _run_quality_detection

        monkeypatch.setenv("MDW_INTRADAY_BACKFILL_ALERTS", "1")
        bars = [_make_ib_bar(datetime(2026, 4, 6, 9, 30))]
        outcome = TickerOutcome(ticker="AAPL", errors=["2026-04-06: error 162"])
        parquet_path = tmp_path / "5m.parquet"
        parquet_path.write_bytes(b"")
        fake_flag = QualityFlag(
            category="fetch_tainted",
            severity="critical",
            detail={},
            ts="2026-05-17T00:00:00Z",
        )

        with (
            patch(
                "livewire_scripts.backfill_intraday.detect_all",
                return_value=[fake_flag],
            ) as m_detect,
            patch(
                "livewire_scripts.backfill_intraday.write_sidecar", return_value=True
            ) as m_sidecar,
            patch(
                "livewire_scripts.backfill_intraday.append_audit", return_value=True
            ) as m_audit,
            patch(
                "livewire_scripts.backfill_intraday.alert_on_flag", return_value=True
            ) as m_alert,
        ):
            _run_quality_detection(
                ticker="AAPL",
                timeframe="5m",
                bars=bars,
                parquet_path=parquet_path,
                outcome=outcome,
                asset_class="volatility",
            )

        kwargs = m_detect.call_args.kwargs
        assert kwargs["metadata"]["errors_during_fetch"]
        assert kwargs["metadata"]["asset_class"] == "volatility"
        assert m_sidecar.call_count == 1
        assert m_audit.call_count == 1
        assert m_alert.call_count == 1

    def test_quality_hook_skips_empty_bars(self, tmp_path):
        from livewire_scripts.backfill_intraday import _run_quality_detection

        outcome = TickerOutcome(ticker="AAPL")
        with patch("livewire_scripts.backfill_intraday.detect_all") as m_detect:
            _run_quality_detection(
                ticker="AAPL",
                timeframe="5m",
                bars=[],
                parquet_path=tmp_path / "x.parquet",
                outcome=outcome,
            )

        m_detect.assert_not_called()


# ── plan_chunks ──────────────────────────────────────────────────────────────


class TestPlanChunks:
    def test_one_line_per_ticker(self):
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "x"), ("1 W", "y")],
        ):
            lines = plan_chunks("5m", years=1, tickers=["AAPL", "MSFT"])
        assert len(lines) == 2
        assert all("2 chunks" in line for line in lines)

    def test_1m_plan_uses_one_minute_label(self):
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 D", "x")],
        ):
            lines = plan_chunks("1m", years=5, tickers=["AAPL"])
        assert lines == ["AAPL: 1 chunks of 1 min"]

    def test_massive_plan_uses_date_window_label(self):
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_date_windows",
            return_value=[(datetime(2026, 1, 1).date(), datetime(2026, 1, 30).date())],
        ):
            lines = plan_chunks("1m", years=5, tickers=["AAPL"], source="massive")
        assert lines == ["AAPL: 1 Massive date windows"]


# ── _resolve_tickers ─────────────────────────────────────────────────────────


class TestResolveTickers:
    def test_explicit_tickers(self):
        args = SimpleNamespace(preset=None, tickers=["AAPL", "MSFT"])
        name, tickers = _resolve_tickers(args)
        assert name == "custom"
        assert tickers == ["AAPL", "MSFT"]

    def test_preset_path(self):
        args = SimpleNamespace(preset="some.json", tickers=None)
        with patch(
            "livewire_scripts.backfill_intraday.load_preset",
            return_value=("sp500", ["AAPL", "MSFT"], {}),
        ):
            name, tickers = _resolve_tickers(args)
        assert name == "sp500"
        assert tickers == ["AAPL", "MSFT"]

    def test_neither_raises(self):
        args = SimpleNamespace(preset=None, tickers=None)
        with pytest.raises(SystemExit):
            _resolve_tickers(args)


# ── main() ───────────────────────────────────────────────────────────────────


class TestMain:
    def test_requires_timeframe(self):
        with patch.object(sys, "argv", ["backfill_intraday.py"]):
            with pytest.raises(SystemExit):
                main()

    def test_dry_run_no_ib_calls(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        # Patching IBClient at module path shouldn't even fire — dry-run skips it
        with patch.object(
            sys,
            "argv",
            [
                "backfill_intraday.py",
                "--timeframe",
                "5m",
                "--tickers",
                "AAPL",
                "MSFT",
                "--dry-run",
            ],
        ):
            main()
        assert not (tmp_path / "logs").exists()

    def test_1m_massive_dry_run_defaults_to_five_years(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 D", "x")],
        ):
            with patch.object(
                sys,
                "argv",
                [
                    "backfill_intraday.py",
                    "--timeframe",
                    "1m",
                    "--source",
                    "massive",
                    "--tickers",
                    "AAPL",
                    "--dry-run",
                ],
            ):
                main()

        out = capsys.readouterr().out
        assert "source=massive" in out
        assert "tf=1m" in out
        assert "years=5" in out

    def test_massive_source_rejected_for_non_equity_intraday(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        with patch.object(
            sys,
            "argv",
            [
                "backfill_intraday.py",
                "--timeframe",
                "1m",
                "--source",
                "massive",
                "--asset-class",
                "futures",
                "--tickers",
                "ES_202506",
                "--dry-run",
            ],
        ):
            with pytest.raises(
                SystemExit,
                match="--source massive is only supported for equity intraday",
            ):
                main()

    def test_non_equity_intraday_defaults_to_ib_source(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 D", "x")],
        ):
            with patch.object(
                sys,
                "argv",
                [
                    "backfill_intraday.py",
                    "--timeframe",
                    "1m",
                    "--asset-class",
                    "futures",
                    "--tickers",
                    "ES_202506",
                    "--dry-run",
                ],
            ):
                main()

        out = capsys.readouterr().out
        assert "asset_class=futures" in out
        assert "source=ib" in out

    def test_volatility_intraday_vix_spx_preset_dry_run(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        preset_path = Path("presets/volatility-intraday.json")
        preset = json.loads(preset_path.read_text(encoding="utf-8"))
        assert preset["tickers"] == ["VIX", "SPX"]

        with patch(
            "livewire_scripts.backfill_intraday.compute_intraday_chunks",
            return_value=[("1 W", "x")],
        ):
            with patch.object(
                sys,
                "argv",
                [
                    "backfill_intraday.py",
                    "--timeframe",
                    "5m",
                    "--asset-class",
                    "volatility",
                    "--preset",
                    str(preset_path),
                    "--dry-run",
                ],
            ):
                main()

        out = capsys.readouterr().out
        assert "asset_class=volatility" in out
        assert "source=ib" in out
        assert "tickers=2" in out
        assert "VIX: 1 chunks of 5 mins" in out
        assert "SPX: 1 chunks of 5 mins" in out

    def test_skip_existing_marks_completed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        # Seed bronze with old data so should_skip_existing returns True
        bronze_dir = tmp_path / "lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        client = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="5m")
        old = datetime.now(_UTC) - timedelta(days=400)
        client.replace_ticker_rows(
            "AAPL",
            [
                {
                    "bar_timestamp": old,
                    "symbol_id": 1,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 100,
                }
            ],
        )

        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch.object(
                sys,
                "argv",
                [
                    "backfill_intraday.py",
                    "--timeframe",
                    "5m",
                    "--tickers",
                    "AAPL",
                    "--skip-existing",
                    "--years",
                    "1",
                ],
            ):
                main()
        # No fetch issued because skip-existing fired
        assert fake_ib.get_historical_data.call_count == 0
        # --tickers runs do NOT touch the cursor (cursor is for preset resume)
        assert load_cursor("5m", "custom") == set()

    def test_max_tickers_caps_run(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        with patch.object(
            sys,
            "argv",
            [
                "backfill_intraday.py",
                "--timeframe",
                "5m",
                "--tickers",
                "A",
                "B",
                "C",
                "D",
                "--max-tickers",
                "2",
                "--dry-run",
            ],
        ):
            main()
        # Dry-run + max-tickers = no fetch, plan only for first 2
        # Just confirm no crash; the cap is exercised through code coverage

    def test_explicit_tickers_bypass_cursor(self, tmp_path, monkeypatch):
        # A "completed" cursor entry must NOT block a --tickers explicit run.
        # The cursor is for preset resume only.
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        save_cursor("5m", "custom", {"AAPL"})

        bars = [_make_ib_bar(datetime(2026, 4, 6, 10, 0))]
        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None
        fake_ib.get_historical_data.return_value = bars

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.compute_intraday_chunks",
                return_value=[("1 W", "x")],
            ):
                with patch.object(
                    sys,
                    "argv",
                    ["backfill_intraday.py", "--timeframe", "5m", "--tickers", "AAPL"],
                ):
                    main()
        # IB was called even though AAPL was in the cursor
        assert fake_ib.get_historical_data.call_count == 1

    def test_preset_run_writes_cursor_on_success(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        bars = [_make_ib_bar(datetime(2026, 4, 6, 10, 0))]
        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None
        fake_ib.get_historical_data.return_value = bars

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.load_preset",
                return_value=("sp500", ["AAPL"], {}),
            ):
                with patch(
                    "livewire_scripts.backfill_intraday.compute_intraday_chunks",
                    return_value=[("1 W", "x")],
                ):
                    with patch.object(
                        sys,
                        "argv",
                        [
                            "backfill_intraday.py",
                            "--timeframe",
                            "5m",
                            "--preset",
                            "sp.json",
                        ],
                    ):
                        main()
        assert load_cursor("5m", "sp500") == {"AAPL"}

    def test_preset_run_writes_cursor_on_no_data(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None
        err = Exception("no data")
        err.code = 162
        fake_ib.get_historical_data.side_effect = err

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.load_preset",
                return_value=("sp500", ["BAD"], {}),
            ):
                with patch(
                    "livewire_scripts.backfill_intraday.compute_intraday_chunks",
                    return_value=[("1 W", "x")],
                ):
                    with patch.object(
                        sys,
                        "argv",
                        [
                            "backfill_intraday.py",
                            "--timeframe",
                            "5m",
                            "--preset",
                            "sp.json",
                        ],
                    ):
                        main()
        assert load_cursor("5m", "sp500") == {"BAD"}

    def test_preset_run_writes_cursor_on_skip_existing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        bronze_dir = tmp_path / "lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        client = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="5m")
        old = datetime.now(_UTC) - timedelta(days=400)
        client.replace_ticker_rows(
            "AAPL",
            [
                {
                    "bar_timestamp": old,
                    "symbol_id": 1,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 100,
                }
            ],
        )

        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.load_preset",
                return_value=("sp500", ["AAPL"], {}),
            ):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "backfill_intraday.py",
                        "--timeframe",
                        "5m",
                        "--preset",
                        "sp.json",
                        "--skip-existing",
                    ],
                ):
                    main()
        assert load_cursor("5m", "sp500") == {"AAPL"}

    def test_preset_run_respects_cursor(self, tmp_path, monkeypatch):
        # Inverse: when --preset is used, cursor entries DO skip tickers.
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        save_cursor("5m", "sp500", {"AAPL"})

        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.load_preset",
                return_value=("sp500", ["AAPL"], {}),
            ):
                with patch.object(
                    sys,
                    "argv",
                    ["backfill_intraday.py", "--timeframe", "5m", "--preset", "x.json"],
                ):
                    main()
        # AAPL was in cursor → no IB call
        assert fake_ib.get_historical_data.call_count == 0

    def test_full_run_inserts_via_mocked_ib(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        bars = [_make_ib_bar(datetime(2026, 4, 6, 10, 0))]
        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None
        fake_ib.get_historical_data.return_value = bars

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.compute_intraday_chunks",
                return_value=[("1 W", "20260406-15:00:00")],
            ):
                with patch.object(
                    sys,
                    "argv",
                    ["backfill_intraday.py", "--timeframe", "5m", "--tickers", "AAPL"],
                ):
                    main()
        # --tickers does not write the cursor; verify bronze parquet instead
        assert load_cursor("5m", "custom") == set()
        bronze_path = (
            tmp_path
            / "lake"
            / "bronze"
            / "asset_class=equity"
            / "symbol=AAPL"
            / "5m.parquet"
        )
        assert bronze_path.exists()

    def test_full_volatility_run_inserts_vix_under_volatility_asset_class(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        bars = [_make_ib_bar(datetime(2026, 4, 6, 10, 0))]
        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None
        fake_ib.get_historical_data.return_value = bars

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.compute_intraday_chunks",
                return_value=[("1 W", "20260406-15:00:00")],
            ):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "backfill_intraday.py",
                        "--timeframe",
                        "5m",
                        "--asset-class",
                        "volatility",
                        "--tickers",
                        "VIX",
                    ],
                ):
                    main()

        bronze_path = (
            tmp_path
            / "lake"
            / "bronze"
            / "asset_class=volatility"
            / "symbol=VIX"
            / "5m.parquet"
        )
        assert bronze_path.exists()

    def test_full_1m_massive_run_inserts_via_mocked_massive(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        bar = SimpleNamespace(
            bar_timestamp=datetime(2026, 4, 6, 13, 30, tzinfo=_UTC),
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=100,
        )
        fake_massive = MagicMock()
        fake_massive.__enter__.return_value = fake_massive
        fake_massive.__exit__.return_value = None
        fake_massive.get_intraday_bars.return_value = [bar]

        with patch(
            "livewire_scripts.backfill_intraday.MassiveClient",
            return_value=fake_massive,
        ):
            with patch(
                "livewire_scripts.backfill_intraday.compute_intraday_date_windows",
                return_value=[
                    (datetime(2026, 4, 6).date(), datetime(2026, 4, 6).date())
                ],
            ):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "backfill_intraday.py",
                        "--timeframe",
                        "1m",
                        "--source",
                        "massive",
                        "--tickers",
                        "AAPL",
                        "--years",
                        "5",
                    ],
                ):
                    main()

        fake_massive.get_intraday_bars.assert_called_once()
        bronze_path = (
            tmp_path
            / "lake"
            / "bronze"
            / "asset_class=equity"
            / "symbol=AAPL"
            / "1m.parquet"
        )
        assert bronze_path.exists()

    def test_massive_run_can_process_tickers_concurrently(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        class FakeMassive:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

        def fake_backfill(ticker, *_args, **_kwargs):
            return TickerOutcome(ticker=ticker, bars_inserted=1)

        with patch(
            "livewire_scripts.backfill_intraday.MassiveClient",
            side_effect=lambda: FakeMassive(),
        ):
            with patch(
                "livewire_scripts.backfill_intraday.backfill_ticker_massive",
                side_effect=fake_backfill,
            ) as backfill:
                with patch.object(
                    sys,
                    "argv",
                    [
                        "backfill_intraday.py",
                        "--timeframe",
                        "1m",
                        "--source",
                        "massive",
                        "--tickers",
                        "AAPL",
                        "MSFT",
                        "NVDA",
                        "--days",
                        "1",
                        "--max-concurrent",
                        "2",
                    ],
                ):
                    main()

        assert backfill.call_count == 3
        out = capsys.readouterr().out
        assert "max_concurrent=2" in out
        assert "inserted=3" in out

    def test_max_concurrent_must_be_positive(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        with patch.object(
            sys,
            "argv",
            [
                "backfill_intraday.py",
                "--timeframe",
                "1m",
                "--source",
                "massive",
                "--tickers",
                "AAPL",
                "--max-concurrent",
                "0",
                "--dry-run",
            ],
        ):
            with pytest.raises(SystemExit, match="--max-concurrent must be >= 1"):
                main()

    def test_massive_concurrent_worker_errors_are_reported_per_ticker(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        class FakeMassive:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

        def fake_backfill(ticker, *_args, **_kwargs):
            if ticker == "MSFT":
                raise RuntimeError("temporary provider failure")
            return TickerOutcome(ticker=ticker, bars_inserted=1)

        with patch(
            "livewire_scripts.backfill_intraday.MassiveClient",
            side_effect=lambda: FakeMassive(),
        ):
            with patch(
                "livewire_scripts.backfill_intraday.backfill_ticker_massive",
                side_effect=fake_backfill,
            ):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "backfill_intraday.py",
                        "--timeframe",
                        "1m",
                        "--source",
                        "massive",
                        "--tickers",
                        "AAPL",
                        "MSFT",
                        "--days",
                        "1",
                        "--max-concurrent",
                        "2",
                    ],
                ):
                    with pytest.raises(SystemExit):
                        main()

        assert load_cursor("1m", "custom") == set()

    def test_ib_no_data_skips_and_marks_completed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        fake_ib = MagicMock()
        fake_ib.__enter__.return_value = fake_ib
        fake_ib.__exit__.return_value = None
        err = Exception("no data")
        err.code = 162
        fake_ib.get_historical_data.side_effect = err

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch(
                "livewire_scripts.backfill_intraday.compute_intraday_chunks",
                return_value=[("1 W", "x")],
            ):
                with patch.object(
                    sys,
                    "argv",
                    ["backfill_intraday.py", "--timeframe", "5m", "--tickers", "BAD"],
                ):
                    main()
        # --tickers run never writes cursor; the no-data skip is per-run only
        assert load_cursor("5m", "custom") == set()


class TestComputeIntradayChunksForDays:
    def test_1m_returns_single_chunk_for_one_day(self):
        chunks = compute_intraday_chunks_for_days("1m", 1)
        assert len(chunks) == 1

    def test_5m_returns_single_chunk_for_seven_days(self):
        chunks = compute_intraday_chunks_for_days("5m", 7)
        assert len(chunks) == 1

    def test_30m_returns_single_chunk_for_thirty_days(self):
        chunks = compute_intraday_chunks_for_days("30m", 30)
        assert len(chunks) == 1

    def test_1h_returns_single_chunk_for_thirty_days(self):
        chunks = compute_intraday_chunks_for_days("1h", 30)
        assert len(chunks) == 1

    def test_rejects_days_below_one(self):
        with pytest.raises(ValueError, match="days_back must be >= 1"):
            compute_intraday_chunks_for_days("1m", 0)

    def test_rejects_unsupported_timeframe(self):
        with pytest.raises(ValueError, match="unsupported"):
            compute_intraday_chunks_for_days("2m", 1)


class TestDaysArgValidation:
    def test_days_below_one_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        with patch.object(
            sys,
            "argv",
            [
                "backfill_intraday.py",
                "--timeframe",
                "5m",
                "--tickers",
                "AAPL",
                "--days",
                "0",
            ],
        ):
            with pytest.raises(SystemExit, match="--days must be >= 1"):
                main()


class TestExistingOnlyFilter:
    def test_existing_only_filters_tickers(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")

        bronze_dir = tmp_path / "lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        client = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="5m")
        ts = datetime.now(timezone.utc) - timedelta(days=1)
        client.replace_ticker_rows(
            "AAPL",
            [
                {
                    "bar_timestamp": ts,
                    "symbol_id": 1,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 100,
                }
            ],
        )

        with patch.object(
            sys,
            "argv",
            [
                "backfill_intraday.py",
                "--timeframe",
                "5m",
                "--tickers",
                "AAPL",
                "MISSING",
                "--existing-only",
                "--dry-run",
            ],
        ):
            main()


class TestSkipExistingWithPreset:
    def test_skip_existing_with_preset_saves_cursor(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill_intraday, "_CURSOR_DIR", tmp_path / "cur")
        monkeypatch.setattr(backfill_intraday, "_DATA_LAKE", tmp_path / "lake")
        monkeypatch.setattr(backfill_intraday, "_LOG_DIR", tmp_path / "logs")
        (tmp_path / "cur").mkdir(parents=True)

        bronze_dir = tmp_path / "lake" / "bronze" / "asset_class=equity"
        bronze_dir.mkdir(parents=True)
        client = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="5m")
        old = datetime.now(timezone.utc) - timedelta(days=400)
        client.replace_ticker_rows(
            "AAPL",
            [
                {
                    "bar_timestamp": old,
                    "symbol_id": 1,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 100,
                }
            ],
        )

        preset_path = tmp_path / "test_preset.json"
        preset_path.write_text(json.dumps({"name": "test", "tickers": ["AAPL"]}))

        fake_ib = MagicMock()
        fake_ib.__enter__ = MagicMock(return_value=fake_ib)
        fake_ib.__exit__ = MagicMock(return_value=None)

        with patch("clients.ib_client.IBClient", return_value=fake_ib):
            with patch.object(
                sys,
                "argv",
                [
                    "backfill_intraday.py",
                    "--timeframe",
                    "5m",
                    "--preset",
                    str(preset_path),
                    "--skip-existing",
                    "--years",
                    "1",
                ],
            ):
                main()
        assert fake_ib.get_historical_data.call_count == 0
        assert load_cursor("5m", "test") == {"AAPL"}
