"""Tests for scripts/coverage_report.py."""

from __future__ import annotations

import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from livewire_scripts.coverage_report import (
    CoverageResult,
    DEFAULT_THRESHOLD,
    RecoveryOutcome,
    _resolve_target_date,
    _send_alert,
    auto_recover,
    compute_coverage,
    format_missing_blocks,
    format_one_liner,
    main,
    write_coverage_log,
)

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
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "adj_close": 1.5, "volume": 1000,
        }
        for d in dates
    ]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=_DAILY_SCHEMA),
        sym_dir / "1d.parquet",
        compression="snappy",
    )


def _write_intraday(
    bronze_root: Path, symbol: str, timeframe: str, days: list[date]
) -> None:
    sym_dir = bronze_root / "asset_class=equity" / f"symbol={symbol}"
    sym_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for d in days:
        ts_et = datetime.combine(d, time(15, 0), tzinfo=_ET)
        rows.append({
            "bar_timestamp": ts_et.astimezone(timezone.utc),
            "symbol_id": 1,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "volume": 1000,
        })
    fname = "1h.parquet" if timeframe == "1h" else "5m.parquet"
    pq.write_table(
        pa.Table.from_pylist(rows, schema=_INTRADAY_SCHEMA),
        sym_dir / fname,
        compression="snappy",
    )


@pytest.fixture()
def seeded_bronze(tmp_path):
    """Two symbols (AAPL, MSFT), all 3 timeframes current to 2026-04-06."""
    root = tmp_path / "bronze"
    target = date(2026, 4, 6)  # Monday
    for sym in ("AAPL", "MSFT"):
        _write_daily(root, sym, [date(2026, 4, 3), target])
        _write_intraday(root, sym, "1h", [target])
        _write_intraday(root, sym, "5m", [target])
    return root


# ── compute_coverage ─────────────────────────────────────────────────────────


class TestComputeCoverage:
    def test_all_present(self, seeded_bronze):
        results = compute_coverage(date(2026, 4, 6), bronze_root=seeded_bronze)
        for tf in ("1d", "1h", "5m"):
            assert results[tf].total == 2
            assert results[tf].present == 2
            assert results[tf].missing_symbols == []
            assert results[tf].ratio == 1.0

    def test_one_symbol_stale_at_5m(self, tmp_path):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        _write_intraday(root, "AAPL", "1h", [target])
        _write_intraday(root, "AAPL", "5m", [date(2026, 3, 1)])  # stale
        results = compute_coverage(target, bronze_root=root)
        assert results["5m"].present == 0
        assert results["5m"].missing_symbols == ["AAPL"]
        assert results["1d"].present == 1

    def test_missing_timeframe_file(self, tmp_path):
        # Symbol exists for 1d only — 1h/5m parquet absent
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        results = compute_coverage(target, bronze_root=root)
        assert results["1d"].present == 1
        assert results["1h"].present == 0
        assert results["1h"].missing_symbols == ["AAPL"]
        assert results["5m"].missing_symbols == ["AAPL"]

    def test_empty_bronze(self, tmp_path):
        results = compute_coverage(date(2026, 4, 6), bronze_root=tmp_path / "empty")
        for tf in ("1d", "1h", "5m"):
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
        assert "1h=2/2 (100.00%)" in line
        assert "5m=2/2 (100.00%)" in line

    def test_missing_blocks_truncates_long_lists(self):
        results = {
            "1d": CoverageResult("1d", 20, 5, [f"S{i}" for i in range(15)]),
            "1h": CoverageResult("1h", 20, 20, []),
            "5m": CoverageResult("5m", 20, 19, ["X"]),
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

    def test_safety_cap_aborts_without_subprocess(self):
        missing = [f"SYM{i}" for i in range(150)]
        with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
            outcome = auto_recover("5m", missing, safety_cap=100)
        assert outcome.aborted is True
        assert "safety_cap" in outcome.reason
        assert mock_run.call_count == 0

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
            outcome = auto_recover(
                "5m", ["AAPL"], bronze_root=seeded_bronze, target_date=target
            )
        assert outcome.recovered == 1
        assert outcome.still_missing == []

    def test_daily_recovery_uses_fetch_ib_historical(self, seeded_bronze):
        # 1d branch shells out to livewire_ingest.py, not livewire_ingest.py
        target = date(2026, 4, 6)
        (seeded_bronze / "asset_class=equity" / "symbol=AAPL" / "1d.parquet").unlink()

        def fake_run(cmd, **kwargs):
            _write_daily(seeded_bronze, "AAPL", [target])
            return SimpleNamespace(returncode=0)

        with patch("livewire_scripts.coverage_report.subprocess.run", side_effect=fake_run) as mock_run:
            outcome = auto_recover(
                "1d", ["AAPL"], bronze_root=seeded_bronze, target_date=target
            )
        assert outcome.recovered == 1
        cmd = mock_run.call_args[0][0]
        assert "livewire_ingest.py" in cmd[1]
        assert "--timeframe" not in cmd  # daily fetch doesn't pass --timeframe

    def test_partial_recovery_path(self, seeded_bronze):
        target = date(2026, 4, 6)
        (seeded_bronze / "asset_class=equity" / "symbol=AAPL" / "5m.parquet").unlink()
        (seeded_bronze / "asset_class=equity" / "symbol=MSFT" / "5m.parquet").unlink()

        def fake_run(cmd, **kwargs):
            # Only AAPL gets repaired
            _write_intraday(seeded_bronze, "AAPL", "5m", [target])
            return SimpleNamespace(returncode=0)

        with patch("livewire_scripts.coverage_report.subprocess.run", side_effect=fake_run):
            outcome = auto_recover(
                "5m", ["AAPL", "MSFT"], bronze_root=seeded_bronze, target_date=target
            )
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
        idx = cmd.index("--error-summary")
        assert "5m" in cmd[idx + 1] and "1h" in cmd[idx + 1]

    def test_aborted_outcome_in_summary(self, tmp_path):
        log_path = tmp_path / "x.log"
        log_path.write_text("")
        outcomes = [RecoveryOutcome("5m", ["A"], 0, ["A"], aborted=True, reason="safety_cap")]
        with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
            _send_alert(date(2026, 4, 6), outcomes, log_path)
        cmd = mock_run.call_args[0][0]
        idx = cmd.index("--error-summary")
        assert "ABORTED" in cmd[idx + 1]


# ── _resolve_target_date ─────────────────────────────────────────────────────


class TestResolveTargetDate:
    def test_explicit_override_wins(self):
        assert _resolve_target_date(force=False, override=date(2026, 4, 6)) == date(2026, 4, 6)

    def test_trading_day_today(self):
        with patch("livewire_scripts.coverage_report.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 6)  # Monday
            mock_date.fromisoformat = date.fromisoformat
            with patch("livewire_scripts.coverage_report.is_trading_day", return_value=True):
                assert _resolve_target_date(False, None) == date(2026, 4, 6)

    def test_non_trading_day_without_force(self):
        with patch("livewire_scripts.coverage_report.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 5)  # Sunday
            with patch("livewire_scripts.coverage_report.is_trading_day", return_value=False):
                assert _resolve_target_date(False, None) is None

    def test_non_trading_day_with_force_falls_back(self):
        with patch("livewire_scripts.coverage_report.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 5)
            with patch("livewire_scripts.coverage_report.is_trading_day", return_value=False):
                with patch(
                    "livewire_scripts.coverage_report.previous_trading_day",
                    return_value=date(2026, 4, 3),
                ):
                    assert _resolve_target_date(True, None) == date(2026, 4, 3)


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
                    sys, "argv",
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
                    sys, "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06"],
                ):
                    main()
        assert mock_run.call_count == 0

    def test_below_threshold_full_recovery_no_email(self, tmp_path, monkeypatch):
        # AAPL missing 5m → triggers recovery → mock writes file → INFO log
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        _write_intraday(root, "AAPL", "1h", [target])
        # 5m intentionally absent
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
                    sys, "argv",
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
        # Both missing 5m
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
                    sys, "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06", "--threshold", "0.99"],
                ):
                    main()
        alert_calls = [c for c in mock_run.call_args_list if "livewire_ops.py" in str(c[0][0])]
        assert len(alert_calls) == 1  # email sent for partial recovery

    def test_safety_cap_path_in_main(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        # 200 symbols all missing 5m
        for i in range(200):
            sym = f"S{i:03d}"
            _write_daily(root, sym, [target])
            _write_intraday(root, sym, "1h", [target])
        monkeypatch.setattr("livewire_scripts.coverage_report._LOG_DIR", tmp_path / "logs")

        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            side_effect=lambda d, bronze_root=None: compute_coverage(d, bronze_root=root),
        ):
            with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
                with patch.object(
                    sys, "argv",
                    ["coverage_report.py", "--target-date", "2026-04-06"],
                ):
                    main()
        # No fetch subprocess (safety cap), but email IS sent
        fetch_calls = [c for c in mock_run.call_args_list if "livewire_ingest.py" in str(c[0][0])]
        alert_calls = [c for c in mock_run.call_args_list if "livewire_ops.py" in str(c[0][0])]
        assert fetch_calls == []
        assert len(alert_calls) == 1
