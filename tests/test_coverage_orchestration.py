"""coverage_report is the one detector: denominator -> classify -> artifacts.

This file replaces tests/test_gap_scan.py and tests/test_gap_scan_integration.py.
The writer tests below are those files' tests with only the import line changed;
the end-to-end tests are new, and they are what proves the classifier has a
production caller rather than merely still existing.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.corporate_action_store import CorporateActionStore
from clients.gap_engine import Finding
from clients.gap_registry import RegistryError
from clients.massive_client import MassiveDividend, MassivePageEvidence
from clients.trading_calendar import trading_dates_in_range
from livewire_scripts.coverage_report import (
    scan_findings,
    write_decision_requests,
    write_tier_a_manifest,
)

FLOOR = date(2021, 7, 12)

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


def _finding(symbol: str, session: date, tier: str = "A") -> Finding:
    return Finding(
        symbol=symbol,
        asset_class="equity",
        timeframe="1d",
        gap="G1",
        sessions=(session,),
        heal_by_days=(session - FLOOR).days,
        tier=tier,
        source="massive",
    )


def test_tier_a_manifest_is_ordered_by_heal_by(tmp_path):
    """Sessions nearest the rolling Massive floor lose the cheap repair path first."""
    urgent = _finding("MUNJ", date(2021, 8, 2))
    relaxed = _finding("AAPL", date(2026, 8, 27))
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([relaxed, urgent], path)
    manifest = json.loads(path.read_text())
    assert [entry["symbol"] for entry in manifest["repairs"]] == ["MUNJ", "AAPL"]


def test_tier_b_uses_the_triage_breaks_verdict_vocabulary(tmp_path):
    """Spec 15: adopt the existing vocabulary rather than inventing a schema."""
    path = tmp_path / "decisions.json"
    write_decision_requests([_finding("MUNJ", date(2026, 8, 27), tier="B")], path)
    requests = json.loads(path.read_text())
    assert requests[0]["verdict"] == "inconclusive"
    assert requests[0]["symbol"] == "MUNJ"


def test_a_terminus_gets_its_own_verdict_not_inconclusive(tmp_path):
    """Spec 10.8 added `terminus` as a fifth verdict because "we could not tell"
    and "the instrument stopped printing" are different answers, and an operator
    triaging the queue acts differently on each."""
    left_the_tape = Finding(
        symbol="EQR",
        asset_class="equity",
        timeframe="1d",
        gap="G14",
        sessions=(date(2026, 8, 18),),
        heal_by_days=None,
        tier="B",
        source="massive",
    )
    path = tmp_path / "decisions.json"
    write_decision_requests([left_the_tape], path)
    assert json.loads(path.read_text())[0]["verdict"] == "terminus"


def test_tier_b_findings_never_enter_the_repair_manifest(tmp_path):
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([_finding("MUNJ", date(2026, 8, 27), tier="B")], path)
    assert json.loads(path.read_text())["repairs"] == []


def test_a_finding_with_no_expiry_sorts_last_not_first(tmp_path):
    """heal_by_days is None when the repair source has no rolling window.
    Sorting None first would put the never-expiring repairs at the top of the
    urgency queue, which is exactly backwards."""
    expiring = _finding("MUNJ", date(2021, 8, 2))
    never = Finding(
        symbol="DGS10",
        asset_class="rates",
        timeframe="1d",
        gap="G1",
        sessions=(date(2026, 8, 27),),
        heal_by_days=None,
        tier="A",
        source="fred",
    )
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([never, expiring], path)
    assert [e["symbol"] for e in json.loads(path.read_text())["repairs"]] == [
        "MUNJ",
        "DGS10",
    ]


def _sessions(start: date, end: date) -> list[date]:
    return trading_dates_in_range(start, end)


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
    pq.write_table(pa.Table.from_pylist(rows, schema=_DAILY_SCHEMA), sym_dir / "1d.parquet")


def _write_raw_symbols(bronze_root: Path, target: date, symbols: list[str]) -> None:
    path = bronze_root.parent / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1" / f"date={target.isoformat()}"
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"ticker": symbols}), path / "_symbols.parquet")


def _registry_for(tmp_path: Path, tickers: list[str]) -> Path:
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


def _seed_action_store(tmp_path: Path, symbol: str, fetched_at: datetime) -> None:
    """Record an ordinary dividend and a fetch receipt for *symbol*.

    scan_findings withholds G14 unless the corporate-action store was asked about
    the symbol on or after the terminus (spec section 11 criterion 8). Without
    this the symbol correctly falls back to a repairable G1 -- see
    test_an_unasked_corporate_action_store_downgrades_a_terminus_to_a_repairable_gap.
    """
    store = CorporateActionStore(tmp_path)
    store.reconcile(
        symbol,
        [
            MassiveDividend(
                provider_event_id=f"div-{symbol}",
                ticker=symbol,
                ex_dividend_date=date(2026, 6, 29),
                cash_amount=Decimal("0.675"),
                currency="USD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="d" * 64,
            )
        ],
        fetched_at=fetched_at,
    )
    store.record_fetch(
        symbol,
        [MassivePageEvidence("splits", "artifact://x", "a" * 64, fetched_at, "sha256:" + "1" * 64)],
        fetched_at,
        full_reconcile=True,
    )


def _tape_with_eqr_leaving(tmp_path: Path) -> Path:
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", _sessions(date(2026, 8, 3), date(2026, 8, 28)))
    _write_daily(bronze, "EQR", _sessions(date(2026, 8, 3), date(2026, 8, 17)))
    for session in _sessions(date(2026, 8, 3), date(2026, 8, 17)):
        _write_raw_symbols(bronze, session, ["AAPL", "EQR"])
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    return bronze


def test_a_terminus_reaches_the_decision_queue_and_not_the_tier_a_manifest(tmp_path):
    # The whole chain, end to end: registry -> denominator -> on-disk diff ->
    # classify -> artifacts. EQR left the tape, so it must land in the Tier B
    # queue with verdict "terminus" and must NOT land in the Tier A manifest,
    # where the repair executor would fetch nothing forever.
    bronze = _tape_with_eqr_leaving(tmp_path)
    _seed_action_store(tmp_path, "EQR", datetime(2026, 8, 28, 6, 0, tzinfo=UTC))

    findings = scan_findings(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert [(f.symbol, f.gap, f.tier) for f in findings] == [("EQR", "G14", "B")]

    manifest, queue = tmp_path / "a.json", tmp_path / "b.json"
    write_tier_a_manifest(findings, manifest)
    write_decision_requests(findings, queue)
    assert json.loads(manifest.read_text())["repairs"] == []
    assert json.loads(queue.read_text())[0]["verdict"] == "terminus"


def test_a_registry_member_with_no_file_is_a_g3_in_the_tier_a_manifest(tmp_path):
    # BK: an sp500 member with no 1d.parquet and a live tape presence. Nothing
    # about it is a terminus, so it is a plain repairable G3 -- and Tier A,
    # because equity inside Massive's rolling window is repairable unattended.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", _sessions(date(2026, 8, 3), date(2026, 8, 28)))
    for session in _sessions(date(2026, 8, 3), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL", "BK"])

    findings = scan_findings(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "BK"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert [(f.symbol, f.gap, f.tier) for f in findings] == [("BK", "G3", "A")]


def test_a_session_before_its_deadline_produces_no_findings(tmp_path):
    # The 497 phantoms, at the artifact layer. A run at 04:21 UTC must write an
    # empty manifest, not one tail gap per symbol in the universe.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", _sessions(date(2026, 8, 3), date(2026, 8, 27)))
    for session in _sessions(date(2026, 8, 3), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    findings = scan_findings(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 4, 21, tzinfo=UTC),
    )
    assert findings == []


def test_a_registry_row_resolving_to_no_symbols_fails_rather_than_scoring_green(tmp_path):
    # Carried over from gap_scan.scan. A zero denominator reports all-green for a
    # reason that has nothing to do with the data -- the disk-glob failure this
    # engine replaces, reintroduced from the registry side.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    with pytest.raises(RegistryError, match="resolves to no symbols"):
        scan_findings(
            date(2026, 8, 28),
            bronze_root=bronze,
            registry_path=_registry_for(tmp_path, []),
            presets_dir=tmp_path / "presets",
            as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
        )


def test_an_unasked_corporate_action_store_downgrades_a_terminus_to_a_repairable_gap(tmp_path):
    """Criterion 8, fail-closed, at the orchestration layer.

    EQR's tape evidence is identical to the test above; the only difference is
    that nothing ever asked the corporate-action store about it. "We could not
    check" must render as a repairable gap, never as a delisting -- so the symbol
    lands in the Tier A manifest as an ordinary G1 rather than in the decision
    queue as a G14.
    """
    bronze = _tape_with_eqr_leaving(tmp_path)

    findings = scan_findings(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert [(f.symbol, f.gap, f.tier) for f in findings] == [("EQR", "G1", "A")]


def test_a_stale_raw_tape_blocks_every_g14_at_once(tmp_path):
    """The loud failure: one stalled flat-file lane must not report the whole
    universe delisted on the same morning. With no partition for the target
    session the tape gate withholds G14 for every symbol."""
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", _sessions(date(2026, 8, 3), date(2026, 8, 28)))
    _write_daily(bronze, "EQR", _sessions(date(2026, 8, 3), date(2026, 8, 17)))
    for session in _sessions(date(2026, 8, 3), date(2026, 8, 17)):
        _write_raw_symbols(bronze, session, ["AAPL", "EQR"])
    # The lane stopped on 08-17: partitions for 08-18..08-28 never landed.
    _seed_action_store(tmp_path, "EQR", datetime(2026, 8, 28, 6, 0, tzinfo=UTC))

    findings = scan_findings(
        date(2026, 8, 28),
        bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert "G14" not in {f.gap for f in findings}
