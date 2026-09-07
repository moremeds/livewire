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
    CoverageResult,
    RecoveryOutcome,
    _resolve_target_date,
    _send_alert,
    auto_recover,
    compute_coverage,
    compute_non_equity_coverage,
    format_missing_blocks,
    format_non_equity_line,
    format_one_liner,
    format_terminus_block,
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


def test_coverage_emits_its_percentage_and_elapsed_seconds(tmp_path, monkeypatch):
    from clients import ledger

    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("LW_RUN_ID", "coverage-20260902T110000Z-1")
    coverage_report.emit_coverage_measurements(
        {"1d": CoverageResult("1d", total=100, present=100, missing_symbols=[])},
        elapsed_s=1432.0,
    )
    assert ledger.query("select name, scope, value, source from measurements order by name") == [
        {"name": "coverage_elapsed_s", "scope": "all", "value": 1432.0, "source": "measured"},
        {"name": "coverage_pct", "scope": "1d", "value": 1.0, "source": "measured"},
        {"name": "coverage_total", "scope": "1d", "value": 100.0, "source": "measured"},
    ]


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


def _disk_only(tmp_path: Path) -> dict:
    """compute_coverage kwargs whose registry contributes no symbols.

    The 1d denominator is `on_disk | registry`. A test about the footer cache,
    the no-trade exemption or the log shape is not about that union, and without
    this it silently measures the SHIPPED registry's 515 sp500+ndx100 members
    against a two-file tmp tree.
    """
    presets = tmp_path / "presets"
    presets.mkdir(exist_ok=True)
    (presets / "none.json").write_text(json.dumps({"name": "none", "tickers": []}))
    registry = tmp_path / "gaps-empty.json"
    registry.write_text(
        json.dumps(
            [
                {
                    "id": "g1-g2-g3-equity-daily",
                    "gap": ["G1", "G3", "G14"],
                    "asset_class": "equity",
                    "timeframe": "1d",
                    "universe": ["none"],
                    "check": "denominator_diff",
                    "params": {},
                    "tier": "A",
                    "since": "2026-08-31",
                    "test": "tests/test_gap_engine.py::test_a_missing_file_with_no_terminus_is_still_g3_tier_a",
                }
            ]
        )
    )
    return {"registry_path": registry, "presets_dir": presets}


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
    def test_all_present(self, seeded_bronze, tmp_path):
        results = compute_coverage(date(2026, 4, 6), bronze_root=seeded_bronze, **_disk_only(tmp_path))
        for tf in ("1d", "1m", "1h", "5m"):
            assert results[tf].total == 2
            assert results[tf].present == 2
            assert results[tf].missing_symbols == []
            assert results[tf].ratio == 1.0

    def test_denominator_is_bronze_universe_not_raw_set(self, seeded_bronze, tmp_path):
        # The raw traded set lists only AAPL, but bronze carries AAPL + MSFT.
        # The denominator is the bronze universe (2), not the raw set (1).
        target = date(2026, 4, 6)
        _write_raw_symbols(seeded_bronze, target, ["AAPL", "MSFT"])
        results = compute_coverage(target, bronze_root=seeded_bronze, **_disk_only(tmp_path))
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
        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))
        assert results["1d"].total == 2
        assert results["1d"].present == 2
        assert results["1d"].missing_symbols == []

    def test_stale_traded_symbol_counts_missing(self, tmp_path):
        # AAPL is stale AND present in the traded set -> genuinely missing.
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [date(2026, 3, 30)])  # stale
        _write_raw_symbols(root, target, ["AAPL"])
        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))
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

    def test_one_corrupt_parquet_does_not_kill_the_whole_scan(self, tmp_path):
        # Nine production aborts came from exactly this: a truncated footer
        # raising `Parquet magic bytes not found in footer` out of `pool.map`,
        # so one file out of ~70K took the 11:00 UTC job down and coverage
        # measured nothing at all that night. The corrupt symbol must count as
        # MISSING -- the conservative direction, which keeps it in the ratio and
        # the alert -- while every other symbol is still measured.
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_daily(root, "AAPL", [target])
        _write_daily(root, "MSFT", [target])
        _write_raw_symbols(root, target, ["AAPL", "MSFT"])

        corrupt = root / "asset_class=equity" / "symbol=MSFT" / "1d.parquet"
        # Truncating a real parquet reproduces the production error exactly;
        # random bytes would exercise a different pyarrow path.
        corrupt.write_bytes(corrupt.read_bytes()[:-8])

        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))

        assert results["1d"].total == 2
        assert results["1d"].present == 1
        assert results["1d"].missing_symbols == ["MSFT"]

    def test_tracks_30m_timeframe(self, tmp_path):
        root = tmp_path / "bronze"
        target = date(2026, 4, 6)
        _write_intraday(root, "AAPL", "30m", [target])
        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))
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
        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))
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
        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))
        assert results["1d"].present == 1
        for tf in ("1m", "1h", "5m", "30m"):
            assert results[tf].total == 0
            assert results[tf].present == 0
            assert results[tf].missing_symbols == []

    def test_empty_bronze(self, tmp_path):
        results = compute_coverage(date(2026, 4, 6), bronze_root=tmp_path / "empty", **_disk_only(tmp_path))
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

        results = compute_coverage(date(2026, 4, 6), bronze_root=root, **_disk_only(tmp_path))

        assert results["1d"].total == 1
        assert results["1d"].present == 0
        assert results["1d"].missing_symbols == ["AAPL"]


# ── format helpers ───────────────────────────────────────────────────────────


class TestFormatters:
    def test_one_liner_matches_spec_shape(self, seeded_bronze, tmp_path):
        results = compute_coverage(date(2026, 4, 6), bronze_root=seeded_bronze, **_disk_only(tmp_path))
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
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path))
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

    def test_a_withheld_terminus_is_reported_but_never_fetched(self):
        """The peers' finding: queueing a fetch for an instrument we could not
        prove still prints is work that cannot succeed. It stays missing (the
        ratio must not lie) and it is still named in still_missing."""
        with (
            patch("livewire_scripts.coverage_report.subprocess.run") as mock_run,
            patch(
                "livewire_scripts.coverage_report.compute_coverage",
                return_value={"1d": CoverageResult("1d", total=2, present=1, missing_symbols=[])},
            ),
        ):
            outcome = auto_recover("1d", ["AAPL", "BK"], target_date=date(2026, 4, 6), withheld=("BK",))

        cmd = mock_run.call_args_list[0][0][0]
        assert cmd[cmd.index("--tickers") + 1 :] == ["AAPL"]
        assert outcome.attempted == ["AAPL"]
        assert outcome.still_missing == ["BK"]

    def test_an_all_withheld_gap_launches_no_subprocess_at_all(self):
        with patch("livewire_scripts.coverage_report.subprocess.run") as mock_run:
            outcome = auto_recover("1d", ["BK"], target_date=date(2026, 4, 6), withheld=("BK",))

        assert mock_run.call_count == 0
        assert outcome.attempted == []
        assert outcome.still_missing == ["BK"]

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
        # Every argument, not just bronze_root. A re-check that drops
        # registry_path/presets_dir runs the DISK-GLOB denominator, so a
        # registry-only symbol with no file to glob reads as recovered by a
        # fetch that could not have touched it.
        compute_mock.assert_called_once_with(
            date(2026, 4, 6),
            bronze_root=tmp_path,
            registry_path=None,
            presets_dir=None,
            as_of=None,
        )
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
        monkeypatch.setenv("MDW_DATA_LAKE", str(seeded_bronze.parent))
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            wraps=lambda d, bronze_root=None, cache_path=None, as_of=None, registry_path=None, presets_dir=None: (
                compute_coverage(d, bronze_root=seeded_bronze, as_of=as_of, **_disk_only(tmp_path))
            ),
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
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
        # main() writes the Tier A manifest and the decision queue under
        # <data-lake>/repairs/. Without this the test writes them into the
        # REAL warehouse.
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))
        with (
            patch(
                "livewire_scripts.coverage_report.compute_coverage",
                wraps=lambda d, bronze_root=None, cache_path=None, as_of=None, registry_path=None, presets_dir=None: (
                    compute_coverage(d, bronze_root=seeded_bronze, as_of=as_of, **_disk_only(tmp_path))
                ),
            ),
            patch(
                # This test is about the EQUITY threshold. Since the non-equity
                # denominator became registry-backed, an empty non-equity tree
                # legitimately reports every preset member missing and pages -- a
                # different code path, covered by its own tests above.
                "livewire_scripts.coverage_report.compute_non_equity_coverage",
                return_value={},
            ),
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
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
        # main() writes the Tier A manifest and the decision queue under
        # <data-lake>/repairs/. Without this the test writes them into the
        # REAL warehouse.
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))

        def fake_run(cmd, **kwargs):
            if "livewire_ingest.py" in str(cmd):
                _write_intraday(root, "AAPL", "5m", [target])
            return SimpleNamespace(returncode=0)

        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            side_effect=lambda d, bronze_root=None, cache_path=None, as_of=None, registry_path=None, presets_dir=None: (
                compute_coverage(d, bronze_root=root, as_of=as_of, **_disk_only(tmp_path))
            ),
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
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
        # main() writes the Tier A manifest and the decision queue under
        # <data-lake>/repairs/. Without this the test writes them into the
        # REAL warehouse.
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))

        def fake_run(cmd, **kwargs):
            if "livewire_ingest.py" in str(cmd):
                _write_intraday(root, "AAPL", "5m", [target])  # only AAPL recovered
            return SimpleNamespace(returncode=0)

        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            side_effect=lambda d, bronze_root=None, cache_path=None, as_of=None, registry_path=None, presets_dir=None: (
                compute_coverage(d, bronze_root=root, as_of=as_of, **_disk_only(tmp_path))
            ),
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
        monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
        # main() writes the Tier A manifest and the decision queue under
        # <data-lake>/repairs/. Without this the test writes them into the
        # REAL warehouse.
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))

        with patch(
            "livewire_scripts.coverage_report.compute_coverage",
            side_effect=lambda d, bronze_root=None, cache_path=None, as_of=None, registry_path=None, presets_dir=None: (
                compute_coverage(d, bronze_root=root, as_of=as_of, **_disk_only(tmp_path))
            ),
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

        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))

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

        results = compute_coverage(target, bronze_root=root, **_disk_only(tmp_path))

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

        # The denominator is now the registry universe, so every preset member
        # with no file is also missing. The assertion that matters is unchanged:
        # a symbol whose newest bar predates the target is reported, and a
        # current one is not.
        assert "VVIX" in results["volatility"].missing_symbols
        assert "VIX" not in results["volatility"].missing_symbols
        assert "DGS10" not in results["rates"].missing_symbols

    def test_absent_asset_class_is_empty_not_an_error(self, tmp_path):
        # An empty bronze tree is not an error, and with a registry denominator
        # it is also not an empty result: every preset member is countable and
        # missing, which is the whole point -- a symbol that never landed used to
        # be invisible.
        results = compute_non_equity_coverage(date(2026, 4, 6), bronze_root=tmp_path / "bronze")
        assert set(results) == {"volatility", "futures", "rates", "fx", "cmdty"}
        for result in results.values():
            assert result.present == 0
            assert len(result.missing_symbols) == result.total


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
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )
        assert len(opens) >= 1
        opens.clear()

        second = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )
        assert opens == [], "an unchanged parquet must not be reopened"
        assert second["1d"].present == first["1d"].present
        assert second["1d"].missing_symbols == first["1d"].missing_symbols

    def test_a_touched_file_is_reread(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
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
            date(2026, 8, 7), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
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
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )

        parquet = root / "asset_class=equity" / "symbol=NVDA" / "1d.parquet"
        frozen = parquet.stat().st_mtime
        _write_daily(
            root,
            "NVDA",
            [date(d.year, d.month, d.day) for d in [date(2026, 7, d) for d in range(1, 25)]]
            + [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)],
        )
        os.utime(parquet, (frozen, frozen))
        assert parquet.stat().st_mtime == frozen

        opens = _count_opens(monkeypatch)
        results = coverage_report.compute_coverage(
            date(2026, 8, 7), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )
        assert opens == [parquet], "a size change must invalidate the entry"
        assert results["1d"].missing_symbols == []

    def test_no_cache_path_means_no_caching(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        opens = _count_opens(monkeypatch)
        for _ in range(2):
            coverage_report.compute_coverage(date(2026, 8, 6), bronze_root=root, **_disk_only(tmp_path))
        assert len(opens) == 2, "without a cache path every run reads every footer"

    def test_a_corrupt_cache_is_not_fatal(self, tmp_path, monkeypatch):
        """Failing the freshness detector because its optimisation file is
        malformed would trade a real signal for a cosmetic one."""
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        cache_path.write_text("{not json", encoding="utf-8")

        results = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )
        assert results["1d"].total == 1

    def test_a_removed_symbol_drops_out_of_the_cache(self, tmp_path):
        """Rebuilt, not mutated — otherwise an archived symbol accumulates forever."""
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 6)])
        _write_daily(root, "HON", [date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )
        assert len(json.loads(cache_path.read_text())) == 2

        shutil.rmtree(root / "asset_class=equity" / "symbol=HON")
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )
        entries = json.loads(cache_path.read_text())
        assert len(entries) == 1
        assert all("NVDA" in key for key in entries)


class TestTheCacheCannotKillTheRun:
    """_load_footer_cache promises a bad cache costs one slow run, not a failure.

    It only guards against malformed JSON. A malformed *entry* used to raise
    ValueError inside pool.map and take the whole coverage pass with it — the
    freshness detector dying of its own optimisation file.
    """

    def test_a_malformed_latest_is_a_miss_not_an_exception(self, tmp_path):
        parquet = tmp_path / "1d.parquet"
        parquet.write_text("x", encoding="utf-8")
        stat = parquet.stat()
        cache = {str(parquet): {"mtime": stat.st_mtime, "size": stat.st_size, "latest": "not-a-date"}}

        with patch.object(coverage_report, "_latest_date_in_parquet", return_value=date(2026, 8, 6)):
            latest, cached, stamp = coverage_report._latest_date_with_cache(parquet, "trade_date", cache)

        assert latest == date(2026, 8, 6), "falls back to a real footer read"
        assert cached is False
        assert stamp == (stat.st_mtime, stat.st_size)

    def test_a_non_dict_entry_is_a_miss(self, tmp_path):
        parquet = tmp_path / "1d.parquet"
        parquet.write_text("x", encoding="utf-8")

        with patch.object(coverage_report, "_latest_date_in_parquet", return_value=None):
            latest, cached, _ = coverage_report._latest_date_with_cache(
                parquet, "trade_date", {str(parquet): "garbage"}
            )

        assert latest is None
        assert cached is False

    def test_a_whole_run_survives_a_corrupt_entry(self, tmp_path):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        parquet = root / "asset_class=equity" / "symbol=NVDA" / "1d.parquet"
        stat = parquet.stat()
        cache_path = tmp_path / "cache.json"
        cache_path.write_text(
            json.dumps({str(parquet): {"mtime": stat.st_mtime, "size": stat.st_size, "latest": "garbage"}}),
            encoding="utf-8",
        )

        results = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path, **_disk_only(tmp_path)
        )

        assert results["1d"].present == 1, "the run completes and measures correctly"


def test_non_equity_denominator_includes_fx_and_cmdty(tmp_path):
    # Both were absent from the hardcoded tuple, so a stale DXY or a stale gold
    # contract was invisible to coverage at every timeframe.
    bronze = tmp_path / "bronze"
    (bronze / "asset_class=rates" / "symbol=DGS10").mkdir(parents=True)
    results = compute_non_equity_coverage(date(2026, 8, 28), bronze_root=bronze)
    assert "fx" in results
    assert "cmdty" in results


def test_a_non_equity_symbol_that_never_landed_is_counted_missing(tmp_path):
    # The whole point of the registry denominator: DGS30 is in the rates preset
    # and has no directory at all, so a disk glob cannot see it.
    bronze = tmp_path / "bronze"
    (bronze / "asset_class=rates" / "symbol=DGS10").mkdir(parents=True)
    results = compute_non_equity_coverage(date(2026, 8, 28), bronze_root=bronze)
    assert "DGS30" in results["rates"].missing_symbols


def test_rates_is_graded_against_the_newest_session_its_lane_actually_owed(tmp_path):
    """FRED publishes a session behind (spec 8.1), so rates must be graded at T+2.

    Asking only about the run's target made rates invisible on EVERY night, not
    just some: at 15:30 UTC on 08-29 the 08-28 session is not yet due for rates,
    and by the next run the target had advanced to 08-28, so 08-27 was never
    revisited by anybody. total=0 maps to ratio 1.0, so it read green forever --
    a detector reporting perfect health because it enumerated nothing.

    The grade is therefore against 08-27, the newest session rates owed, and an
    empty tree is 4 real gaps rather than a silent zero.
    """
    bronze = tmp_path / "bronze"
    (bronze / "asset_class=rates").mkdir(parents=True)
    results = compute_non_equity_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    assert results["rates"].measured_session == date(2026, 8, 27)
    assert results["rates"].total == 4
    assert results["rates"].missing_symbols == ["DGS10", "DGS3", "DGS30", "DGS5"]
    # And the line says which session it graded, so "rates=0/4" cannot be read as
    # a statement about 08-28.
    assert "rates=0/4@2026-08-27" in format_non_equity_line(date(2026, 8, 28), results)


def test_every_non_equity_row_is_declared_xnys_or_rejected():
    # gap_registry.XNYS_CALENDAR_ASSET_CLASSES is the recorded blind spot. This
    # test does not fix the calendar; it makes adding a sixth asset class an
    # explicit act rather than a silent inheritance.
    from clients.gap_registry import XNYS_CALENDAR_ASSET_CLASSES, load_registry

    for row in load_registry(Path("registry/gaps.json")):
        assert row.asset_class in XNYS_CALENDAR_ASSET_CLASSES, row.id


def _registry_for(tmp_path: Path, tickers: list[str]) -> Path:
    """A one-row registry plus the preset it names, both under tmp_path.

    Pair it with presets_dir=tmp_path / "presets" at the call site; production
    resolves preset names against a directory it is GIVEN, never against the CWD.
    """
    presets = tmp_path / "presets"
    presets.mkdir(exist_ok=True)
    (presets / "t.json").write_text(json.dumps({"name": "t", "tickers": tickers}))
    registry = tmp_path / "gaps.json"
    registry.write_text(
        json.dumps(
            [
                {
                    "id": "g1-g2-g3-equity-daily",
                    "gap": ["G1", "G3", "G14"],
                    "asset_class": "equity",
                    "timeframe": "1d",
                    "universe": ["t"],
                    "check": "denominator_diff",
                    "params": {},
                    "tier": "A",
                    "since": "2026-08-31",
                    "test": "tests/test_gap_engine.py::test_a_missing_file_with_no_terminus_is_still_g3_tier_a",
                }
            ]
        )
    )
    return registry


from tests.test_coverage_orchestration import _seed_action_store  # noqa: E402


def _sessions(start: date, end: date) -> list[date]:
    from clients.trading_calendar import trading_dates_in_range

    return trading_dates_in_range(start, end)


def test_a_preset_member_with_no_parquet_is_counted_missing(tmp_path):
    # BK, measured 2026-09-01: an sp500 member with no 1d.parquet at all. The
    # disk-glob denominator cannot express this symbol, which is why it read as
    # 100% healthy for weeks.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_raw_symbols(bronze, date(2026, 8, 28), ["AAPL", "BK"])
    results = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "BK"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    assert "BK" in results["1d"].missing_symbols


def test_a_terminus_symbol_is_in_neither_present_nor_missing(tmp_path):
    # EQR left the tape 2026-08-18. Counting it missing queues an impossible
    # repair; counting it present is what hid it. It is in NEITHER, and the
    # ratio must not be able to read green because of it.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "EQR", [date(2026, 8, 17)])
    # EQR prints on the 17th and never again. A symbol that never appears on the
    # tape at all is not a terminus -- that shape is BK, renamed rather than
    # delisted -- so the fixture has to show it leaving, not merely being absent.
    _write_raw_symbols(bronze, date(2026, 8, 17), ["AAPL", "EQR"])
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    # The coverage denominator applies the same three gates the classifier does,
    # so the store has to have been asked -- see the companion test below.
    _seed_action_store(tmp_path, "EQR", datetime(2026, 8, 28, 6, 0, tzinfo=UTC))
    results = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    result = results["1d"]
    assert "EQR" in dict(result.terminus_symbols)
    assert "EQR" not in result.missing_symbols
    # The regression this test exists for: the no-trade exemption also counts an
    # absent symbol PRESENT, so subtracting terminus from `missing` alone is a
    # no-op and the ratio still reads 100%.
    assert result.total == result.present + len(result.missing_symbols)
    assert result.total == 1


def test_a_one_day_absence_is_still_exempted_as_no_trade(tmp_path):
    # The exemption stays load-bearing. Without it the interior scan flags 96.6%
    # of the universe, and this test is the guard on that.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "SLND", [date(2026, 8, 27)])
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 27)):
        _write_raw_symbols(bronze, session, ["AAPL", "SLND"])
    _write_raw_symbols(bronze, date(2026, 8, 28), ["AAPL"])
    results = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "SLND"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    assert "SLND" not in results["1d"].missing_symbols
    assert results["1d"].terminus_symbols == ()


def test_before_the_deadline_the_session_is_not_expected_at_all(tmp_path):
    # Spec section 11 criterion 11, exercised through compute_coverage rather
    # than through build_denominator. Passing as_of=session_due_at(target_date)
    # would make the due filter tautologically true, so the ONLY test that can
    # catch a regression here is one that goes through the real caller with a
    # real clock. 04:21 UTC on 2026-08-29 is before the 15:00 UTC deadline for
    # session 2026-08-28, so nothing is due and nothing is missing.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 27)])
    _write_raw_symbols(bronze, date(2026, 8, 28), ["AAPL"])
    early = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 4, 21, tzinfo=UTC),
    )
    assert early["1d"].total == 0
    late = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    assert late["1d"].missing_symbols == ["AAPL"]


def test_terminus_is_not_computed_for_symbols_outside_the_registry(tmp_path):
    # SLND is on disk but not in any preset. The terminus threshold is calibrated
    # on 515 liquid names; the illiquid on-disk tail genuinely does not print for
    # days, so applying it there manufactures the 96.6% disease.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "SLND", [date(2026, 8, 5)])
    for session in _sessions(date(2026, 8, 6), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    results = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    assert results["1d"].terminus_symbols == ()


def test_a_registry_only_symbol_survives_a_recovery_that_could_not_fetch_it(tmp_path):
    # BK has no 1d.parquet, so the subprocess writes nothing. Before the recheck
    # was given registry_path/presets_dir it re-ran the DISK-GLOB denominator,
    # where BK is not in the universe at all -- so it fell out of
    # missing_symbols and was reported "recovered" by a fetch that did nothing.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_raw_symbols(bronze, date(2026, 8, 28), ["AAPL", "BK"])
    kwargs = {
        "registry_path": _registry_for(tmp_path, ["AAPL", "BK"]),
        "presets_dir": tmp_path / "presets",
        "as_of": datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    }
    with patch.object(coverage_report.subprocess, "run") as run_mock:
        outcome = auto_recover("1d", ["BK"], bronze_root=bronze, target_date=date(2026, 8, 28), **kwargs)

    assert run_mock.call_count == 1
    assert outcome.still_missing == ["BK"]
    assert outcome.recovered == 0


def test_main_writes_both_repair_artifacts(seeded_bronze, monkeypatch, tmp_path):
    # The Task 7 wiring: without this the classifier exists and nothing scheduled
    # ever calls it, which is the state gap_scan's deletion would have left.
    lake = tmp_path / "lake"
    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))
    monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))
    with patch(
        "livewire_scripts.coverage_report.compute_coverage",
        wraps=lambda d, bronze_root=None, cache_path=None, as_of=None, registry_path=None, presets_dir=None: (
            compute_coverage(d, bronze_root=seeded_bronze, as_of=as_of, **_disk_only(tmp_path))
        ),
    ):
        with patch("livewire_scripts.coverage_report.subprocess.run"):
            with patch.object(sys, "argv", ["coverage_report.py", "--target-date", "2026-04-06", "--no-recover"]):
                main()

    manifest = lake / "repairs" / "tier_a_2026-04-06.json"
    queue = lake / "repairs" / "decisions_2026-04-06.json"
    assert manifest.is_file() and queue.is_file()
    assert "repairs" in json.loads(manifest.read_text())
    assert isinstance(json.loads(queue.read_text()), list)
    # And the log carries the count, so a scan that silently stopped producing
    # findings is visible rather than absent.
    log_text = (tmp_path / "logs" / "coverage_2026-04-06.log").read_text()
    assert "scan:" in log_text


def test_an_unasked_action_store_leaves_a_terminus_in_the_denominator(tmp_path):
    """The regression: coverage applied ONE of the three gates, not three.

    A symbol whose trailing absence the corporate-action store cannot speak to --
    because it was never asked -- was removed from the denominator anyway, so
    "we could not check" raised the coverage ratio instead of lowering it. The
    classifier calls the same symbol a repairable G1 in the same run. Both
    surfaces must now agree: it stays countable, and it is missing.
    """
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "EQR", [date(2026, 8, 17)])
    # EQR prints on the 17th and never again. A symbol that never appears on the
    # tape at all is not a terminus -- that shape is BK, renamed rather than
    # delisted -- so the fixture has to show it leaving, not merely being absent.
    _write_raw_symbols(bronze, date(2026, 8, 17), ["AAPL", "EQR"])
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    # No _seed_action_store call: the store has no fetch receipt for EQR.
    results = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    result = results["1d"]
    # Withheld: the store cannot speak to this absence, so no terminus is claimed
    # and the symbol stays COUNTABLE. Before the gates were unified, coverage
    # dropped it from `total` on the tape evidence alone.
    assert result.terminus_symbols == ()
    assert result.total == 2
    # And it is MISSING, not exempted. The no-trade exemption is for a symbol
    # that did not print today; this one has a qualifying absence run nobody
    # could explain, and letting the exemption absorb it is the mechanism that
    # hid EA/AVB/EQR. The classifier reaches the same verdict (G1 Tier B).
    assert result.missing_symbols == ["EQR"]
    assert result.unconfirmed_terminus_symbols == ("EQR",)


def test_a_stale_raw_tape_keeps_every_symbol_in_the_coverage_denominator(tmp_path):
    """raw_tape_covers, on the coverage surface. A partial flat-file outage must
    not be able to empty the denominator -- the failure mode that gate exists for
    is exactly "every symbol looks terminated at once"."""
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "EQR", [date(2026, 8, 17)])
    # The tape stops four sessions short of the target day.
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 24)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    _seed_action_store(tmp_path, "EQR", datetime(2026, 8, 28, 6, 0, tzinfo=UTC))
    results = compute_coverage(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC),
    )
    assert results["1d"].terminus_symbols == ()
    assert results["1d"].total == 2
    # tape_ok is false, so the suffix test never runs and there is no candidate
    # to withhold -- the symbol is an ordinary no-trade, not an unconfirmed
    # terminus. A stalled lane must not manufacture 13,000 of those either.
    assert results["1d"].unconfirmed_terminus_symbols == ()


def test_the_equity_deadline_gate_is_the_early_return_not_build_denominator(tmp_path):
    """A landmine test: `build_denominator` does NOT gate equity coverage.

    compute_coverage passes `as_of` into build_denominator, which makes it look
    as though the ingestion deadline is applied there. It is not -- that call
    filters SESSIONS, and the 1d branch keeps every returned symbol regardless of
    whether its session list is empty. The early `session_due_at(...) > as_of`
    return is the only thing standing between a pre-deadline run and one phantom
    tail gap per symbol (497 of the first run's 501 findings). Delete the early
    return believing the denominator handles it, and this test fails.
    """
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 27)])
    kwargs = dict(
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "BK"]),
        presets_dir=tmp_path / "presets",
    )
    # 04:00 UTC on 08-28: the job that fills 08-28 has not even started.
    early = compute_coverage(date(2026, 8, 28), as_of=datetime(2026, 8, 28, 4, 0, tzinfo=UTC), **kwargs)
    assert (early["1d"].total, early["1d"].present) == (0, 0)
    # 16:00 UTC the next day: due, and the two registry symbols are countable.
    due = compute_coverage(date(2026, 8, 28), as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC), **kwargs)
    assert due["1d"].total == 2

    # And the sessions build_denominator returns are empty in the early case --
    # proving the filter ran and that keeping only `expected` would still have
    # yielded both symbols, i.e. the early return is load-bearing.
    from clients.coverage_denominator import build_denominator

    expected = build_denominator(
        [tmp_path / "presets" / "t.json"],
        "equity",
        "1d",
        date(2026, 8, 28),
        date(2026, 8, 28),
        as_of=datetime(2026, 8, 28, 4, 0, tzinfo=UTC),
    )
    assert len(expected) == 2
    assert all(series.sessions == () for series in expected)


def test_a_pre_deadline_run_does_not_erase_the_1d_footer_cache(tmp_path):
    """The not-due branch must not make the next real run pay a cold footer walk.

    _save_footer_cache REPLACES the cache file, so returning early without
    carrying 1d entries forward deleted ~13,270 of them -- and the run that
    triggers this branch is precisely the pre-deadline one that then penalises
    the 15:30 job it precedes.
    """
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    cache_path = tmp_path / "cache.json"
    kwargs = dict(
        bronze_root=bronze,
        cache_path=cache_path,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
    )
    compute_coverage(date(2026, 8, 28), as_of=datetime(2026, 8, 29, 16, 0, tzinfo=UTC), **kwargs)
    seeded = json.loads(cache_path.read_text())
    assert any(key.endswith("1d.parquet") for key in seeded)

    compute_coverage(date(2026, 8, 28), as_of=datetime(2026, 8, 28, 4, 0, tzinfo=UTC), **kwargs)
    after = json.loads(cache_path.read_text())
    assert {k: v for k, v in after.items() if k.endswith("1d.parquet")} == {
        k: v for k, v in seeded.items() if k.endswith("1d.parquet")
    }


def test_the_log_names_unconfirmed_termini_separately_from_confirmed_ones(tmp_path):
    """They are counted missing, so the operator triaging that list needs the flag.

    A symbol here stopped printing for a full trading week and the
    corporate-action store could not say why -- an ordinary `missing:` entry
    implies a fetch will fix it, and for this one it may not.
    """
    result = CoverageResult(
        timeframe="1d",
        total=3,
        present=1,
        missing_symbols=["EQR"],
        terminus_symbols=(("AVB", date(2026, 8, 14)),),
        unconfirmed_terminus_symbols=("EQR",),
    )
    blocks = format_terminus_block({"1d": result})
    assert blocks == [
        "  1d terminus: AVB@2026-08-14",
        "  1d unconfirmed terminus (counted missing): EQR",
    ]
