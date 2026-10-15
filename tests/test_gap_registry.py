import json
from pathlib import Path

import pytest

from clients.gap_registry import RegistryError, load_registry

REPO = Path(__file__).resolve().parents[1]

VALID_ROW = {
    "id": "g3-equity-sp500-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "equity",
    "timeframe": "1d",
    "universe": ["sp500"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3",
}


def test_shipped_registry_loads():
    rows = load_registry(REPO / "registry" / "gaps.json")
    assert rows, "shipped registry must not be empty"
    assert all(row.test for row in rows)


def test_row_without_test_is_rejected(tmp_path):
    """Spec 4.5: a row without a test is not coverage, it is a claim."""
    row = dict(VALID_ROW)
    del row["test"]
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="test"):
        load_registry(path)


def test_unknown_gap_id_is_rejected(tmp_path):
    row = dict(VALID_ROW, gap=["G99"])
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="G99"):
        load_registry(path)


def test_unknown_tier_is_rejected(tmp_path):
    row = dict(VALID_ROW, tier="Z")
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="tier"):
        load_registry(path)


def test_undispatched_check_is_rejected(tmp_path):
    """scan() runs denominator_diff for every row regardless of this field, so a
    row naming another check would silently emit G1/G2/G3 under a false name."""
    row = dict(VALID_ROW, check="adjusted_deviation_bps")
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="unknown check"):
        load_registry(path)


def test_asset_class_without_a_calendar_is_rejected(tmp_path):
    """The denominator is XNYS-only. A new asset class must not inherit that
    blind spot silently."""
    row = dict(VALID_ROW, asset_class="crypto")
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="no calendar mapping"):
        load_registry(path)


def test_g13_is_a_valid_gap_id(tmp_path):
    row = dict(VALID_ROW, gap=["G13"])
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    assert load_registry(path)[0].gap == ("G13",)


def test_djia_row_resolves_the_thirty_constituents():
    """The djia registry row must name a preset that exists and resolves."""
    from clients.ingestion_common import load_preset

    rows = load_registry(REPO / "registry" / "gaps.json")
    row = next((r for r in rows if r.id == "g1-g2-g3-djia-daily"), None)
    assert row is not None, "registry must carry a djia equity-daily row"
    assert list(row.universe) == ["djia"]
    assert row.asset_class == "equity" and row.timeframe == "1d"
    _, tickers, _ = load_preset(REPO / "presets" / "djia.json")
    assert len(tickers) == 30 and len(set(tickers)) == 30


def test_fx_ib_row_resolves_the_six_dxy_legs():
    """argon's gold factor reads the six DXY legs among these; a missed night must surface as G1."""
    from clients.gap_engine import repair_source
    from clients.ingestion_common import load_preset, make_contract

    rows = load_registry(REPO / "registry" / "gaps.json")
    row = next(r for r in rows if r.id == "g1-g2-g3-fx-ib-daily")
    assert (row.asset_class, row.timeframe, list(row.universe)) == ("fx_ib", "1d", ["fx-ib"])
    assert repair_source("fx_ib") == "ib"
    _, tickers, _ = load_preset(REPO / "presets" / "fx-ib.json")
    assert {"EURUSD", "GBPUSD", "USDCAD", "USDCHF", "USDJPY", "USDSEK"} <= set(tickers)
    # Direct IDEALPRO pairs: stored as quoted, never inverted.
    contracts = [make_contract(t, "fx_ib") for t in tickers]
    assert all(c.secType == "CASH" and c.exchange == "IDEALPRO" for c in contracts)
    assert [c.symbol + c.currency for c in contracts] == tickers


def test_volatility_intraday_rows_read_bar_timestamps_and_route_to_ib(tmp_path):
    """5m/30m volatility is IB-fed: a tail is Tier B, and sessions come from bar_timestamp.

    No row covered it, so 2026-08-21..26 sat missing in 13 series for seven weeks.
    """
    from datetime import UTC, date, datetime

    import pyarrow as pa
    import pyarrow.parquet as pq

    from clients.coverage_denominator import ExpectedSeries
    from clients.gap_engine import actual_sessions, classify, repair_source
    from clients.ingestion_common import load_preset

    rows = {r.id: r for r in load_registry(REPO / "registry" / "gaps.json")}
    for tf in ("5m", "30m"):
        row = rows[f"g1-g3-volatility-{tf}"]
        assert (row.asset_class, row.timeframe, list(row.universe)) == ("volatility", tf, ["volatility-intraday"])
    _, tickers, _ = load_preset(REPO / "presets" / "volatility-intraday.json")
    assert {"VIX", "VVIX", "VXGDX"} <= set(tickers)
    assert repair_source("volatility") == "cboe"
    assert repair_source("volatility", "5m") == "ib"

    path = tmp_path / "asset_class=volatility" / "symbol=VVIX" / "5m.parquet"
    path.parent.mkdir(parents=True)
    stamps = [datetime(2026, 8, 19, 13, 30, tzinfo=UTC), datetime(2026, 8, 20, 19, 55, tzinfo=UTC)]
    pq.write_table(pa.table({"bar_timestamp": pa.array(stamps, pa.timestamp("us", tz="UTC"))}), path)
    sessions = (date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21))
    series = ExpectedSeries("VVIX", "volatility", "5m", sessions)

    present = actual_sessions(tmp_path, series)
    assert present == {date(2026, 8, 19), date(2026, 8, 20)}
    [finding] = classify(series, present, massive_floor=date(2021, 1, 1))
    assert (finding.gap, finding.sessions, finding.tier, finding.source) == ("G1", (date(2026, 8, 21),), "B", "ib")
