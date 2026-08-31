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
