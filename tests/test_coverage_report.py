"""Tests for scripts/coverage_report.py."""

from __future__ import annotations

import json
import os
import shutil
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from livewire_scripts import coverage_report
from livewire_scripts.coverage_report import (
    NON_EQUITY_ASSET_CLASSES,
    CoverageResult,
    RecoveryOutcome,
    _resolve_target_date,
    _send_alert,
    auto_recover,
    compute_coverage,
    compute_non_equity_coverage,
    format_missing_blocks,
    format_one_liner,
    main,
    write_coverage_log,
)


def _error_summary(cmd) -> str:
    """The summary is one `--error-summary=<text>` token.

    The two-token form could not carry a value beginning with "--", which is
    exactly what the log-derived summary is (see the 2026-08-08 lost page).
    """
    token = next(a for a in cmd if a.startswith("--error-summary="))
    return token.removeprefix("--error-summary=")


_ET = ZoneInfo("America/New_York")

_DAILY_SCHEMA = pa.schema(
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


def _write_daily(bronze_root: Path, symbol: str, dates: list[date]) -> None:
    sym_dir = bronze_root / "asset_class=equity" / f"symbol={symbol}"
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
    pq.write_table(
        pa.Table.from_pylist(rows, schema=_DAILY_SCHEMA),
        sym_dir / "1d.parquet",
        compression="snappy",
    )


def _write_intraday(bronze_root: Path, symbol: str, timeframe: str, days: list[date]) -> None:
    sym_dir = bronze_root / "asset_class=equity" / f"symbol={symbol}"
    sym_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for d in days:
        ts_et = datetime.combine(d, time(15, 0), tzinfo=_ET)
        rows.append(
            {
                "bar_timestamp": ts_et.astimezone(UTC),
                "symbol_id": 1,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 1000,
            }
        )
    pq.write_table(
        pa.Table.from_pylist(rows, schema=_INTRADAY_SCHEMA),
        sym_dir / f"{timeframe}.parquet",
        compression="snappy",
    )


def _write_raw_symbols(bronze_root: Path, target: date, symbols: list[str]) -> None:
    path = bronze_root.parent / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1" / f"date={target.isoformat()}"
    path.mkdir(parents=True)
    pq.write_table(pa.table({"ticker": symbols}), path / "_symbols.parquet")


@pytest.fixture()
def seeded_bronze(tmp_path):
    """Two symbols (AAPL, MSFT), all coverage timeframes current to 2026-04-06."""
    root = tmp_path / "bronze"
    target = date(2026, 4, 6)  # Monday
    for sym in ("AAPL", "MSFT"):
        _write_daily(root, sym, [date(2026, 4, 3), target])
        _write_intraday(root, sym, "1m", [target])
        _write_intraday(root, sym, "1h", [target])
        _write_intraday(root, sym, "5m", [target])
    return root


# ── compute_coverage ─────────────────────────────────────────────────────────


class TestComputeCoverage:
    def test_all_present(self, seeded_bronze):
        results = compute_coverage(date(2026, 4, 6), bronze_root=seeded_bronze)
        for tf in ("1d", "1m", "1h", "5m"):
            assert results[tf].total == 2
            assert results[tf].present == 2
            assert results[tf].missing_symbols == []
            assert results[tf].ratio == 1.0

    def test_denominator_is_bronze_universe_not_raw_set(self, seeded_bronze):
        # The raw traded set lists only AAPL, but bronze carries AAPL + MSFT.
        # The denominator is the bronze universe (2), not the raw set (1).
        target = date(2026, 4, 6)
        _write_raw_symbols(seeded_bronze, target, ["AAPL", "MSFT"])
        results = compute_coverage(target, bronze_root=seeded_bronze)
        assert results["1d"].total == 2
        assert results["1d"].present == 2

    def test_no_trade_symbol_counts_present(self, tmp_path):
        # WLIIU is stale but absent from the day's traded set -> it did not
        # trade, so it is not "missing".
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        _write_daily(root, "WLIIU", [date(2026, 3, 30)])  # stale
        _write_raw_symbols(root, target, ["AAPL"])  # WLIIU did not trade
        results = compute_coverage(target, bronze_root=root)
        assert results["1d"].total == 2
        assert results["1d"].present == 2
        assert results["1d"].missing_symbols == []

    def test_stale_traded_symbol_counts_missing(self, tmp_path):
        # AAPL is stale AND present in the traded set -> genuinely missing.
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [date(2026, 3, 30)])  # stale
        _write_raw_symbols(root, target, ["AAPL"])
        results = compute_coverage(target, bronze_root=root)
        assert results["1d"].present == 0
        assert results["1d"].missing_symbols == ["AAPL"]

    def test_latest_date_uses_footer_stats_without_full_read(self, tmp_path):
        # _latest_date_in_parquet must resolve via footer statistics and never
        # fall through to a full column read when stats are present.
        from livewire_scripts import coverage_report

        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [date(2026, 4, 3), target])
        path = root / "asset_class=equity" / "symbol=AAPL" / "1d.parquet"

        with patch.object(
            coverage_report.pq,
            "read_table",
            side_effect=AssertionError("full column read must not happen"),
        ):
            latest = coverage_report._latest_date_in_parquet(path, "trade_date")
        assert latest == target

    def test_latest_date_from_string_column_stats(self, tmp_path):
        # Some bronze snapshots store trade_date as a string; footer stats then
        # come back as strings and must be parsed via date.fromisoformat.
        from livewire_scripts import coverage_report

        sym_dir = tmp_path / "asset_class=equity" / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        path = sym_dir / "1d.parquet"
        pq.write_table(
            pa.table({"trade_date": pa.array(["2026-04-03", "2026-04-06"], type=pa.string())}),
            path,
        )
        assert coverage_report._latest_date_in_parquet(path, "trade_date") == date(2026, 4, 6)

    def test_latest_date_falls_back_when_stats_absent(self, tmp_path):
        # When the file has no column statistics, fall back to a full read.
        from livewire_scripts import coverage_report

        sym_dir = tmp_path / "asset_class=equity" / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        path = sym_dir / "1d.parquet"
        pq.write_table(
            pa.table({"trade_date": pa.array([date(2026, 4, 3), date(2026, 4, 6)], type=pa.date32())}),
            path,
            write_statistics=False,
        )
        assert coverage_report._latest_date_in_parquet(path, "trade_date") == date(2026, 4, 6)

    def test_latest_date_empty_parquet_returns_none(self, tmp_path):
        from livewire_scripts import coverage_report

        sym_dir = tmp_path / "asset_class=equity" / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        path = sym_dir / "1d.parquet"
        pq.write_table(
            pa.table({"trade_date": pa.array([], type=pa.date32())}),
            path,
            write_statistics=False,
        )
        assert coverage_report._latest_date_in_parquet(path, "trade_date") is None

    def test_tracks_30m_timeframe(self, tmp_path):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_intraday(root, "AAPL", "30m", [target])
        results = compute_coverage(target, bronze_root=root)
        assert "30m" in results
        assert results["30m"].total == 1
        assert results["30m"].present == 1

    def test_one_symbol_stale_at_5m(self, tmp_path):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        _write_intraday(root, "AAPL", "1m", [target])
        _write_intraday(root, "AAPL", "1h", [target])
        _write_intraday(root, "AAPL", "5m", [date(2026, 3, 1)])  # stale
        results = compute_coverage(target, bronze_root=root)
        assert results["5m"].present == 0
        assert results["5m"].missing_symbols == ["AAPL"]
        assert results["1d"].present == 1

    def test_missing_timeframe_file(self, tmp_path):
        # Symbol exists for 1d only. The denominator is per-timeframe, so a
        # symbol with no intraday file is simply absent from that timeframe's
        # universe (total 0), not counted as a missing symbol.
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        results = compute_coverage(target, bronze_root=root)
        assert results["1d"].present == 1
        for tf in ("1m", "1h", "5m", "30m"):
            assert results[tf].total == 0
            assert results[tf].present == 0
            assert results[tf].missing_symbols == []

    def test_empty_bronze(self, tmp_path):
        results = compute_coverage(date(2026, 4, 6), bronze_root=tmp_path / "empty")
        for tf in ("1d", "1m", "1h", "5m"):
            assert results[tf].total == 0
            assert results[tf].present == 0
            assert results[tf].ratio == 1.0  # vacuous truth

    def test_empty_parquet_snapshot_counts_as_missing(self, tmp_path):
        root = tmp_path / "bronze"
        sym_dir = root / "asset_class=equity" / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist([], schema=_DAILY_SCHEMA),
            sym_dir / "1d.parquet",
            compression="snappy",
        )

        results = compute_coverage(date(2026, 4, 6), bronze_root=root)

        assert results["1d"].total == 1
        assert results["1d"].present == 0
        assert results["1d"].missing_symbols == ["AAPL"]


# ── format helpers ───────────────────────────────────────────────────────────


class TestFormatters:
    def test_one_liner_matches_spec_shape(self, seeded_bronze):
        results = compute_coverage(date(2026, 4, 6), bronze_root=seeded_bronze)
        line = format_one_liner(date(2026, 4, 6), results)
        assert line.startswith("2026-04-06 coverage:")
        assert "1d=2/2 (100.00%)" in line
        assert "1m=2/2 (100.00%)" in line
        assert "1h=2/2 (100.00%)" in line
        assert "5m=2/2 (100.00%)" in line

    def test_missing_blocks_truncates_long_lists(self):
        results = {
            "1d": CoverageResult("1d", 20, 5, [f"S{i}" for i in range(15)]),
            "1m": CoverageResult("1m", 20, 20, []),
            "1h": CoverageResult("1h", 20, 20, []),
            "5m": CoverageResult("5m", 20, 19, ["X"]),
            "30m": CoverageResult("30m", 20, 20, []),
        }
        blocks = format_missing_blocks(results, max_listed=3)
        assert any("1d missing: S0, S1, S2, ... (15 total)" in b for b in blocks)
        assert any("5m missing: X" in b for b in blocks)
        assert not any("1h" in b for b in blocks)


# ── write_coverage_log ───────────────────────────────────────────────────────


class TestWriteCoverageLog:
    def test_appends_when_called_twice(self, tmp_path, monkeypatch):
        monkeypatch.setattr("livewire_scripts.coverage_report._LOG_DIR", tmp_path)
        path = write_coverage_log(date(2026, 4, 6), "first line", ["  detail"])
        write_coverage_log(date(2026, 4, 6), "second line", [])
        content = path.read_text()
        assert "first line" in content
        assert "second line" in content
        assert "  detail" in content


# ── auto_recover ─────────────────────────────────────────────────────────────


class TestAutoRecover:
    def test_no_missing_short_circuits(self):
        outcome = auto_recover("5m", [], bronze_root=Path("/nope"))
        assert outcome.recovered == 0
        assert outcome.attempted == []
        assert not outcome.aborted

    def test_intraday_recovery_ignores_the_symbol_cap(self):
        """Intraday recovery is date-shaped: it republishes the whole day's file.

        It passes no symbols at all, so refusing to run because 101 symbols
        were missing was measuring the wrong quantity — and the refusal was
        self-perpetuating, since recovery only ran below both the alert ratio
        and the cap.
        """
        missing = [f"SYM{i}" for i in range(150)]
        with (
            patch("livewire_scripts.coverage_report.subprocess.run") as mock_run,
            patch(
                "livewire_scripts.coverage_report.compute_coverage",
                return_value={"5m": CoverageResult("5m", total=150, present=150, missing_symbols=[])},
            ),
        ):
            outcome = auto_recover("5m", missing, safety_cap=100, target_date=date(2026, 4, 6))

        assert outcome.aborted is False
        assert mock_run.call_count == 1
        cmd = mock_run.call_args_list[0][0][0]
        assert "repair" in cmd
        # Whole-file republish — no per-symbol argument.
        assert "--tickers" not in cmd

    def test_daily_recovery_batches_instead_of_dropping(self):
        """Over-cap used to be dropped entirely and re-emailed every night."""
        missing = [f"SYM{i}" for i in range(250)]
        with (
            patch("livewire_scripts.coverage_report.subprocess.run") as mock_run,
            patch(
                "livewire_scripts.coverage_report.compute_coverage",
                return_value={"1d": CoverageResult("1d", total=250, present=250, missing_symbols=[])},
            ),
        ):
            outcome = auto_recover("1d", missing, safety_cap=100, target_date=date(2026, 4, 6))

        assert outcome.aborted is False
        assert mock_run.call_count == 3  # 100 + 100 + 50
        batched = []
        for call in mock_run.call_args_list:
            cmd = call[0][0]
            batched.extend(cmd[cmd.index("--tickers") + 1 :])
        assert sorted(batched) == sorted(missing)

    def test_full_recovery_path(self, seeded_bronze):
        # Pretend AAPL is missing at 5m. The mocked subprocess "fixes" it by
        # writing the parquet that compute_coverage sees on the recheck.
        target = date(2026, 4, 6)
        # Remove AAPL 5m so the initial state actually has it missing
        (seeded_bronze / "asset_class=equity" / "symbol=AAPL" / "5m.parquet").unlink()

        def fake_run(cmd, **kwargs):
            _write_intraday(seeded_bronze, "AAPL", "5m", [target])
            return SimpleNamespace(returncode=0)

        with patch("livewire_scripts.coverage_report.subprocess.run", side_effect=fake_run):
            outcome = auto_recover("5m", ["AAPL"], bronze_root=seeded_bronze, target_date=target)
        assert outcome.recovered == 1
        assert outcome.still_missing == []

    def test_daily_recovery_uses_massive_daily_update(self, seeded_bronze):
        target = date(2026, 4, 6)
        (seeded_bronze / "asset_class=equity" / "symbol=AAPL" / "1d.parquet").unlink()

        def fake_run(cmd, **kwargs):
            _write_daily(seeded_bronze, "AAPL", [target])
            return SimpleNamespace(returncode=0)

        with patch("livewire_scripts.coverage_report.subprocess.run", side_effect=fake_run) as mock_run:
            outcome = auto_recover("1d", ["AAPL"], bronze_root=seeded_bronze, target_date=target)
        assert outcome.recovered == 1
        cmd = mock_run.call_args[0][0]
        assert "livewire_ingest.py" in cmd[1]
        assert cmd[2] == "daily"
        assert cmd[cmd.index("--source") + 1] == "massive"
        assert cmd[cmd.index("--target-date") + 1] == "2026-04-06"
        assert "--tickers" in cmd
        assert "--timeframe" not in cmd

    def test_default_recovery_date_is_utc(self, tmp_path):
        from livewire_scripts import coverage_report

        class FrozenDateTime:
            @classmethod
            def now(cls, tz=None):
                if tz is UTC:
                    return datetime(2026, 4, 6, 1, 0, tzinfo=UTC)
                return datetime(2026, 4, 5, 18, 0)

        class FrozenLocalDate(date):
            @classmethod
            def today(cls):
                return cls(2026, 4, 5)

        rechecked = {
            "5m": CoverageResult("5m", total=1, present=0, missing_symbols=["AAPL"]),
        }
        with patch.object(coverage_report, "datetime", FrozenDateTime):
            with patch.object(coverage_report, "date", FrozenLocalDate):
                with patch.object(coverage_report, "compute_coverage", return_value=rechecked) as compute_mock:
                    with patch.object(coverage_report.subprocess, "run") as run_mock:
                        outcome = auto_recover("5m", ["AAPL"], bronze_root=tmp_path)

        command = run_mock.call_args.args[0]
        assert command[command.index("--dates") + 1] == "2026-04-06"
        compute_mock.assert_called_once_with(date(2026, 4, 6), bronze_root=tmp_path)
        assert outcome.still_missing == ["AAPL"]

    def test_partial_recovery_path(self, seeded_bronze):
        target = date(2026, 4, 6)
        # Both stale (file present, old date) so both are in the 5m universe.
        _write_intraday(seeded_bronze, "AAPL", "5m", [date(2026, 3, 1)])
        _write_intraday(seeded_bronze, "MSFT", "5m", [date(2026, 3, 1)])

        def fake_run(cmd, **kwargs):
            # Only AAPL gets repaired
            _write_intraday(seeded_bronze, "AAPL", "5m", [target])
            return SimpleNamespace(returncode=0)

        with patch("livewire_scripts.coverage_report.subprocess.run", side_effect=fake_run):
            outcome = auto_recover("5m", ["AAPL", "MSFT"], bronze_root=seeded_bronze, target_date=target)
        assert outcome.recovered == 1
        assert outcome.still_missing == ["MSFT"]


# ── _send_alert ──────────────────────────────────────────────────────────────


class TestSendAlert:
    def test_invokes_node_script_with_summary(self, tmp_path):
        log_path = tmp_path / "coverage.log"
        log_path.write_text("x")
        outcomes = [
            RecoveryOutcome("5m", ["AAPL"], 0, ["AAPL"]),
            RecoveryOutcome("1h", ["MSFT"], 1, []),
        ]
        with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
            _send_alert(date(2026, 4, 6), outcomes, log_path)
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == sys.executable
        assert "livewire_ops.py" in cmd[1]
        assert cmd[2] == "send-alert"
        assert "--job-name" in cmd
        summary = _error_summary(cmd)
        assert "5m" in summary and "1h" in summary

    def test_aborted_outcome_in_summary(self, tmp_path):
        log_path = tmp_path / "x.log"
        log_path.write_text("")
        outcomes = [RecoveryOutcome("5m", ["A"], 0, ["A"], aborted=True, reason="safety_cap")]
        with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
            _send_alert(date(2026, 4, 6), outcomes, log_path)
        cmd = mock_run.call_args[0][0]
        assert "ABORTED" in _error_summary(cmd)


# ── _resolve_target_date ─────────────────────────────────────────────────────


class TestResolveTargetDate:
    def test_explicit_override_wins(self):
        assert _resolve_target_date(force=False, override=date(2026, 4, 6)) == date(2026, 4, 6)

    def test_measures_the_completed_session_not_the_one_still_to_open(self):
        """The 06:00 UTC job runs at 02:00 ET, hours before that day's open.

        Resolving to `datetime.now(UTC).date()` targeted a session that had not
        traded, so every symbol read as missing: `coverage_2026-06-17.log` holds
        `1d=0/20396 (0.00%)` across all four timeframes. That 0% then fired
        universe-wide auto-recovery, which is what exhausted the 600s budget
        every night and left no coverage log written after 2026-06-17.
        """
        # 02:00 ET on Tue 2026-04-07 → _et_today walks back to Mon 2026-04-06.
        with patch("livewire_scripts.coverage_report._et_today", return_value=date(2026, 4, 6)):
            assert _resolve_target_date(False, None) == date(2026, 4, 6)

    def test_saturday_scheduled_run_still_measures_friday(self):
        """Coverage used to skip the entire weekend.

        At 06:00 UTC Saturday the old UTC-today was Saturday — not a trading day,
        so it returned None without --force. That is why the 2026-08-01 run
        produced no coverage log and no warning: it silently did nothing.
        """
        with patch("livewire_scripts.coverage_report._et_today", return_value=date(2026, 7, 31)):
            assert _resolve_target_date(False, None) == date(2026, 7, 31)

    def test_non_trading_day_without_force(self):
        with patch("livewire_scripts.coverage_report._et_today", return_value=date(2026, 4, 5)):  # Sunday
            assert _resolve_target_date(False, None) is None

    def test_non_trading_day_with_force_falls_back(self):
        # Real calendar: 2026-04-03 is Good Friday, so Thursday is the fallback.
        with patch("livewire_scripts.coverage_report._et_today", return_value=date(2026, 4, 5)):
            assert _resolve_target_date(True, None) == date(2026, 4, 2)


# ── main() ───────────────────────────────────────────────────────────────────


class TestMain:
    def test_no_target_aborts_quietly(self):
        with patch("livewire_scripts.coverage_report._resolve_target_date", return_value=None):
            with patch.object(sys, "argv", ["coverage_report.py"]):
                main()  # No exception

    def test_no_recover_skips_subprocess(self, seeded_bronze, monkeypatch, tmp_path):
        monkeypatch.setattr("livewire_scripts.coverage_report._DATA_LAKE", seeded_bronze.parent)
        monkeypatch.setattr("livewire_scripts.coverage_report._LOG_DIR", tmp_path / "logs")
        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            wraps=lambda d, bronze_root=None: compute_coverage(d, bronze_root=seeded_bronze),
        ):
            with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
                with patch.object(
                    sys,
                    "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06", "--no-recover"],
                ):
                    main()
        assert mock_run.call_count == 0

    def test_above_threshold_no_recovery(self, seeded_bronze, monkeypatch, tmp_path):
        monkeypatch.setattr("livewire_scripts.coverage_report._LOG_DIR", tmp_path / "logs")
        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            wraps=lambda d, bronze_root=None: compute_coverage(d, bronze_root=seeded_bronze),
        ):
            with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
                with patch.object(
                    sys,
                    "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06"],
                ):
                    main()
        assert mock_run.call_count == 0

    def test_below_threshold_full_recovery_no_email(self, tmp_path, monkeypatch):
        # AAPL missing 5m → triggers recovery → mock writes file → INFO log
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        _write_intraday(root, "AAPL", "1m", [target])
        _write_intraday(root, "AAPL", "1h", [target])
        _write_intraday(root, "AAPL", "30m", [target])
        _write_intraday(root, "AAPL", "5m", [date(2026, 3, 1)])  # stale -> triggers recovery
        monkeypatch.setattr("livewire_scripts.coverage_report._LOG_DIR", tmp_path / "logs")

        def fake_run(cmd, **kwargs):
            if "livewire_ingest.py" in str(cmd):
                _write_intraday(root, "AAPL", "5m", [target])
            return SimpleNamespace(returncode=0)

        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            side_effect=lambda d, bronze_root=None: compute_coverage(d, bronze_root=root),
        ):
            with patch("livewire_scripts.coverage_report.subprocess.run", side_effect=fake_run) as mock_run:
                with patch.object(
                    sys,
                    "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06", "--threshold", "0.99"],
                ):
                    main()
        # One subprocess: the fetch (no email since recovered)
        node_calls = [c for c in mock_run.call_args_list if c[0][0][0] == "node"]
        assert node_calls == []

    def test_below_threshold_partial_recovery_sends_email(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        _write_daily(root, "MSFT", [target])
        _write_intraday(root, "AAPL", "1h", [target])
        _write_intraday(root, "MSFT", "1h", [target])
        # Both stale at 5m (file present, old date)
        _write_intraday(root, "AAPL", "5m", [date(2026, 3, 1)])
        _write_intraday(root, "MSFT", "5m", [date(2026, 3, 1)])
        monkeypatch.setattr("livewire_scripts.coverage_report._LOG_DIR", tmp_path / "logs")

        def fake_run(cmd, **kwargs):
            if "livewire_ingest.py" in str(cmd):
                _write_intraday(root, "AAPL", "5m", [target])  # only AAPL recovered
            return SimpleNamespace(returncode=0)

        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            side_effect=lambda d, bronze_root=None: compute_coverage(d, bronze_root=root),
        ):
            with patch("livewire_scripts.coverage_report.subprocess.run", side_effect=fake_run) as mock_run:
                with patch.object(
                    sys,
                    "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06", "--threshold", "0.99"],
                ):
                    main()
        alert_calls = [c for c in mock_run.call_args_list if "livewire_ops.py" in str(c[0][0])]
        assert len(alert_calls) == 1  # email sent for partial recovery

    def test_large_intraday_outage_still_recovers_in_main(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        # 200 symbols all stale at 5m (present in the 5m universe, over the cap)
        for i in range(200):
            sym = f"S{i:03d}"
            _write_daily(root, sym, [target])
            _write_intraday(root, sym, "1h", [target])
            _write_intraday(root, sym, "5m", [date(2026, 3, 1)])
        monkeypatch.setattr("livewire_scripts.coverage_report._LOG_DIR", tmp_path / "logs")

        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            side_effect=lambda d, bronze_root=None: compute_coverage(d, bronze_root=root),
        ):
            with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
                with patch.object(
                    sys,
                    "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06"],
                ):
                    main()
        # A 200-symbol intraday outage used to land in the dead zone: bad
        # enough to alarm, too bad to act. Recovery must now actually fire.
        fetch_calls = [c for c in mock_run.call_args_list if "livewire_ingest.py" in str(c[0][0])]
        alert_calls = [c for c in mock_run.call_args_list if "livewire_ops.py" in str(c[0][0])]
        assert fetch_calls != []
        assert len(alert_calls) == 1


class TestIntradayDenominator:
    """The denominator used to be self-defining, hiding whole classes of gap."""

    def test_symbol_that_traded_but_has_no_intraday_file_counts_missing(self, tmp_path):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        # GOTBARS has 5m; NOFILE traded that day but never got a 5m parquet.
        _write_daily(root, "GOTBARS", [target])
        _write_intraday(root, "GOTBARS", "5m", [target])
        _write_daily(root, "NOFILE", [target])
        _write_raw_symbols(root, target, ["GOTBARS", "NOFILE"])

        results = compute_coverage(target, bronze_root=root)

        # Globbing files on disk put NOFILE outside the 5m universe entirely,
        # so it could never be counted missing and coverage read 100%.
        assert "NOFILE" in results["5m"].missing_symbols
        assert results["5m"].total == 2

    def test_daily_keeps_the_no_trade_exemption(self, tmp_path):
        """A symbol that simply did not trade is not a 1d gap."""
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "TRADED", [target])
        _write_daily(root, "QUIET", [date(2026, 3, 1)])
        _write_raw_symbols(root, target, ["TRADED"])

        results = compute_coverage(target, bronze_root=root)

        assert results["1d"].missing_symbols == []


class TestNonEquityCoverage:
    """These asset classes were in no denominator at any timeframe."""

    def _write_non_equity(self, root, asset_class, symbol, dates):
        sym_dir = root / f"asset_class={asset_class}" / f"symbol={symbol}"
        sym_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"trade_date": pa.array(dates, type=pa.date32())}),
            sym_dir / "1d.parquet",
        )

    def test_stale_volatility_index_is_detected(self, tmp_path):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        self._write_non_equity(root, "volatility", "VIX", [target])
        self._write_non_equity(root, "volatility", "VVIX", [date(2026, 3, 1)])
        self._write_non_equity(root, "rates", "DGS10", [target])

        results = compute_non_equity_coverage(target, bronze_root=root)

        assert results["volatility"].missing_symbols == ["VVIX"]
        assert results["rates"].missing_symbols == []

    def test_absent_asset_class_is_empty_not_an_error(self, tmp_path):
        results = compute_non_equity_coverage(date(2026, 4, 6), bronze_root=tmp_path / "bronze")
        for asset_class in NON_EQUITY_ASSET_CLASSES:
            assert results[asset_class].total == 0


def _count_opens(monkeypatch) -> list[Path]:
    """Record every footer read, delegating to the real one."""
    opens: list[Path] = []
    real = coverage_report._latest_date_in_parquet
    monkeypatch.setattr(
        coverage_report,
        "_latest_date_in_parquet",
        lambda path, column_name: (opens.append(path), real(path, column_name))[1],
    )
    return opens


class TestFooterReadsAreIncremental:
    """A parquet whose mtime has not moved cannot have a new max date.

    Re-reading its footer is pure cost, and on the external exFAT volume that
    cost IS the runtime: a full cold pass measured 2858s on 2026-08-09 against
    an 1800s budget, while the same 1d pass warm takes 29.2s. Threads do not
    fix a cold metadata walk; not doing the walk does.
    """

    def test_an_unchanged_file_is_not_reopened(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        opens = _count_opens(monkeypatch)

        first = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )
        assert len(opens) >= 1
        opens.clear()

        second = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )
        assert opens == [], "an unchanged parquet must not be reopened"
        assert second["1d"].present == first["1d"].present
        assert second["1d"].missing_symbols == first["1d"].missing_symbols

    def test_a_touched_file_is_reread(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )

        parquet = root / "asset_class=equity" / "symbol=NVDA" / "1d.parquet"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)])
        # Bump mtime explicitly. Two writes inside one test can land on the same
        # stat timestamp, and a test that depends on filesystem clock resolution
        # is a flake waiting for a slower machine.
        stamp = parquet.stat().st_mtime + 10
        os.utime(parquet, (stamp, stamp))

        opens = _count_opens(monkeypatch)
        results = coverage_report.compute_coverage(
            date(2026, 8, 7), bronze_root=root, cache_path=cache_path
        )
        assert opens == [parquet], "a rewritten parquet must be reread"
        assert results["1d"].missing_symbols == []

    def test_a_same_mtime_rewrite_is_caught_by_size(self, tmp_path, monkeypatch):
        """exFAT stores mtime at 2-second granularity.

        A republish landing inside that bucket leaves the timestamp unchanged,
        so mtime alone would keep serving the pre-publish max date forever. Any
        real republish that adds a row also changes the size.
        """
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )

        parquet = root / "asset_class=equity" / "symbol=NVDA" / "1d.parquet"
        frozen = parquet.stat().st_mtime
        _write_daily(root, "NVDA", [date(d.year, d.month, d.day) for d in
                                    [date(2026, 7, d) for d in range(1, 25)]]
                     + [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)])
        os.utime(parquet, (frozen, frozen))
        assert parquet.stat().st_mtime == frozen

        opens = _count_opens(monkeypatch)
        results = coverage_report.compute_coverage(
            date(2026, 8, 7), bronze_root=root, cache_path=cache_path
        )
        assert opens == [parquet], "a size change must invalidate the entry"
        assert results["1d"].missing_symbols == []

    def test_no_cache_path_means_no_caching(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        opens = _count_opens(monkeypatch)
        for _ in range(2):
            coverage_report.compute_coverage(date(2026, 8, 6), bronze_root=root)
        assert len(opens) == 2, "without a cache path every run reads every footer"

    def test_a_corrupt_cache_is_not_fatal(self, tmp_path, monkeypatch):
        """Failing the freshness detector because its optimisation file is
        malformed would trade a real signal for a cosmetic one."""
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        cache_path.write_text("{not json", encoding="utf-8")

        results = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )
        assert results["1d"].total == 1

    def test_a_removed_symbol_drops_out_of_the_cache(self, tmp_path):
        """Rebuilt, not mutated — otherwise an archived symbol accumulates forever."""
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 6)])
        _write_daily(root, "HON", [date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )
        assert len(json.loads(cache_path.read_text())) == 2

        shutil.rmtree(root / "asset_class=equity" / "symbol=HON")
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )
        entries = json.loads(cache_path.read_text())
        assert len(entries) == 1
        assert all("NVDA" in key for key in entries)
