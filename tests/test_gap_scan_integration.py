"""End-to-end cover for the two pieces the unit tests reach around:
``actual_sessions`` (the parquet reader) and ``scan``/``main`` (orchestration).

The bars below are REAL AAPL daily bars, read once from
``~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=AAPL/1d.parquet``
on 2026-09-01 and frozen here. 2026-05-13/14/15 are real XNYS sessions. Nothing
here hits the network, and no value is invented.
"""

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.coverage_denominator import ExpectedSeries
from clients.gap_engine import actual_sessions, load_unresolved
from clients.gap_registry import RegistryError
from livewire_scripts.gap_scan import main, scan

# (trade_date, open, high, low, close, adj_close, volume) — frozen real AAPL
AAPL_BARS = [
    (date(2026, 5, 13), 293.41, 300.92, 293.41, 298.87, 298.87, 28879979),
    (date(2026, 5, 14), 299.82, 300.45, 295.38, 298.21, 298.21, 20829569),
    (date(2026, 5, 15), 297.78, 303.20, 296.52, 300.23, 300.23, 30112201),
]
AAPL_SYMBOL_ID = 3095618998234100
SESSIONS = tuple(bar[0] for bar in AAPL_BARS)


def _write_bronze(root: Path, symbol: str, bars: list[tuple]) -> None:
    """Write a bronze parquet in the real equity schema and layout."""
    path = root / "asset_class=equity" / f"symbol={symbol}" / "1d.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "trade_date": pa.array([b[0] for b in bars], pa.date32()),
                "symbol_id": pa.array([AAPL_SYMBOL_ID] * len(bars), pa.int64()),
                "open": pa.array([b[1] for b in bars], pa.float64()),
                "high": pa.array([b[2] for b in bars], pa.float64()),
                "low": pa.array([b[3] for b in bars], pa.float64()),
                "close": pa.array([b[4] for b in bars], pa.float64()),
                "adj_close": pa.array([b[5] for b in bars], pa.float64()),
                "volume": pa.array([b[6] for b in bars], pa.int64()),
            }
        ),
        path,
    )


def _series(symbol: str = "AAPL") -> ExpectedSeries:
    return ExpectedSeries(symbol, "equity", "1d", SESSIONS)


def test_actual_sessions_reads_the_real_bronze_layout(tmp_path):
    """Guards the path shape and the `trade_date` column name together.

    Either being wrong silently returns an empty set, which reads as G3 for
    every symbol in the lake — a total false alarm that looks like a finding.
    """
    _write_bronze(tmp_path, "AAPL", AAPL_BARS)
    assert actual_sessions(tmp_path, _series()) == set(SESSIONS)


def test_actual_sessions_treats_a_missing_file_as_empty_not_an_error(tmp_path):
    assert actual_sessions(tmp_path, _series("BF.B")) == set()


def test_actual_sessions_sees_a_partial_file(tmp_path):
    _write_bronze(tmp_path, "AAPL", AAPL_BARS[:2])
    assert actual_sessions(tmp_path, _series()) == {SESSIONS[0], SESSIONS[1]}


def _registry_and_presets(tmp_path, tickers: list[str]) -> tuple[Path, Path]:
    presets = tmp_path / "presets"
    presets.mkdir(exist_ok=True)
    (presets / "probe.json").write_text(json.dumps({"name": "probe", "asset_class": "equity", "tickers": tickers}))
    registry = tmp_path / "gaps.json"
    registry.write_text(
        json.dumps(
            [
                {
                    "id": "probe-equity-daily",
                    "gap": ["G1", "G2", "G3"],
                    "asset_class": "equity",
                    "timeframe": "1d",
                    "universe": ["probe"],
                    "check": "denominator_diff",
                    "params": {},
                    "tier": "A",
                    "since": "2026-09-01",
                    "test": "tests/test_gap_engine.py::test_missing_file_is_g3",
                }
            ]
        )
    )
    return registry, presets


def test_scan_reports_a_never_ingested_symbol_as_g3(tmp_path):
    """The denominator must not be derived from disk contents.

    BF.B is a real S&P 500 member. With no parquet written for it, a
    disk-globbing detector cannot enumerate it at all; the registry
    denominator must still expect it.
    """
    bronze = tmp_path / "bronze"
    _write_bronze(bronze, "AAPL", AAPL_BARS)
    registry, presets = _registry_and_presets(tmp_path, ["AAPL", "BF.B"])

    findings = scan(
        bronze,
        registry,
        presets,
        start=SESSIONS[0],
        end=SESSIONS[-1],
        as_of=date(2026, 9, 1),
    )
    assert [(f.symbol, f.gap) for f in findings] == [("BF.B", "G3")]
    assert findings[0].sessions == SESSIONS


def test_scan_honours_the_unresolved_ledger(tmp_path):
    bronze = tmp_path / "bronze"
    registry, presets = _registry_and_presets(tmp_path, ["BF.B"])
    ledger = tmp_path / "unresolved.json"
    ledger.write_text(
        json.dumps(
            [
                {
                    "symbol": "BF.B",
                    "asset_class": "equity",
                    "timeframe": "1d",
                    "session": s.isoformat(),
                    "reason": "no source",
                    "as_of": "2026-09-01",
                }
                for s in SESSIONS
            ]
        )
    )
    assert (
        scan(
            bronze,
            registry,
            presets,
            start=SESSIONS[0],
            end=SESSIONS[-1],
            as_of=date(2026, 9, 1),
            unresolved_path=ledger,
        )
        == []
    )


def test_main_writes_both_artifacts_and_mutates_no_bronze(tmp_path, capsys):
    bronze = tmp_path / "bronze"
    _write_bronze(bronze, "AAPL", AAPL_BARS)
    registry, presets = _registry_and_presets(tmp_path, ["AAPL", "BF.B"])
    manifest = tmp_path / "manifest.json"
    decisions = tmp_path / "decisions.json"
    before = {p: p.read_bytes() for p in bronze.rglob("*.parquet")}

    rc = main(
        [
            "--bronze-root",
            str(bronze),
            "--registry",
            str(registry),
            "--presets-dir",
            str(presets),
            "--start",
            SESSIONS[0].isoformat(),
            "--end",
            SESSIONS[-1].isoformat(),
            "--as-of",
            "2026-09-01",
            "--manifest-out",
            str(manifest),
            "--decisions-out",
            str(decisions),
        ]
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out) == {
        "findings": 1,
        "tier_a": 1,
        "tier_b": 0,
    }
    repairs = json.loads(manifest.read_text())["repairs"]
    assert [entry["symbol"] for entry in repairs] == ["BF.B"]
    assert json.loads(decisions.read_text()) == []
    # Phase 1 is read-only: every bronze byte is unchanged.
    assert {p: p.read_bytes() for p in bronze.rglob("*.parquet")} == before


def test_main_routes_a_pre_floor_gap_to_the_decision_queue(tmp_path, capsys):
    """Below the Massive floor only IB can source the bar, and IB is 2FA-gated,
    so the finding must land in the decision queue and never in the manifest."""
    bronze = tmp_path / "bronze"
    registry, presets = _registry_and_presets(tmp_path, ["BF.B"])
    manifest = tmp_path / "manifest.json"
    decisions = tmp_path / "decisions.json"

    rc = main(
        [
            "--bronze-root",
            str(bronze),
            "--registry",
            str(registry),
            "--presets-dir",
            str(presets),
            # 2019-03-01 is a real XNYS session, well below the 2021-07-12 floor.
            "--start",
            "2019-03-01",
            "--end",
            "2019-03-01",
            "--as-of",
            "2026-09-01",
            "--manifest-out",
            str(manifest),
            "--decisions-out",
            str(decisions),
        ]
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["tier_b"] == 1
    assert json.loads(manifest.read_text())["repairs"] == []
    queued = json.loads(decisions.read_text())
    assert queued[0]["symbol"] == "BF.B"
    assert queued[0]["verdict"] == "inconclusive"
    assert queued[0]["heal_by_days"] < 0


def test_an_underspecified_ledger_entry_is_rejected_not_defaulted(tmp_path):
    """Defaulting a missing timeframe would silence every timeframe at once."""
    ledger = tmp_path / "unresolved.json"
    ledger.write_text(json.dumps([{"symbol": "BF.B", "session": "2026-08-27"}]))
    with pytest.raises(ValueError, match="asset_class"):
        load_unresolved(ledger)


def test_a_row_resolving_to_no_symbols_fails_the_run(tmp_path):
    """A zero denominator reports all-green for a reason unrelated to the data —
    the same failure mode as the disk-glob detector, from the registry side."""
    registry, presets = _registry_and_presets(tmp_path, [])
    with pytest.raises(RegistryError, match="resolves to no symbols"):
        scan(
            tmp_path / "bronze",
            registry,
            presets,
            start=SESSIONS[0],
            end=SESSIONS[-1],
            as_of=date(2026, 9, 1),
        )
