"""Tests for livewire_scripts/universe_sync.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from clients.tag_registry import TagRegistry
from clients.universe_client import UniverseFetchError
from livewire_scripts.universe_sync import (
    Movement,
    _archive_delisted,
    apply_sync,
    compute_movements,
    main,
    update_preset_tickers,
)


class TestComputeMovements:
    def test_new_ticker(self):
        live = {"sp500": {"AAPL", "NEW"}}
        existing = {"sp500": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert Movement("add", "NEW", from_tags=[], to_tags=["sp500"]) in moves

    def test_removed_ticker(self):
        live = {"sp500": {"AAPL"}}
        existing = {"sp500": {"AAPL", "OLD"}}
        moves = compute_movements(live, existing)
        assert Movement("remove", "OLD", from_tags=["sp500"], to_tags=[]) in moves

    def test_promotion_r2k_to_sp500(self):
        live = {"sp500": {"AAPL", "APP"}, "r2k": {"BETA"}}
        existing = {"sp500": {"AAPL"}, "r2k": {"APP", "BETA"}}
        moves = compute_movements(live, existing)
        assert (
            Movement("promotion", "APP", from_tags=["r2k"], to_tags=["sp500"]) in moves
        )

    def test_demotion_sp500_to_r2k(self):
        live = {"sp500": {"AAPL"}, "r2k": {"BETA", "OLD"}}
        existing = {"sp500": {"AAPL", "OLD"}, "r2k": {"BETA"}}
        moves = compute_movements(live, existing)
        assert (
            Movement("demotion", "OLD", from_tags=["sp500"], to_tags=["r2k"]) in moves
        )

    def test_no_changes(self):
        live = {"sp500": {"AAPL"}}
        existing = {"sp500": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert moves == []

    def test_multi_index_ticker_stays(self):
        live = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        existing = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert moves == []

    def test_drops_from_one_index_keeps_other(self):
        live = {"sp500": {"AAPL"}, "ndx100": set()}
        existing = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert len(moves) == 1
        assert moves[0].type == "demotion"
        assert moves[0].from_tags == ["sp500", "ndx100"]
        assert moves[0].to_tags == ["sp500"]

    def test_added_to_second_index(self):
        live = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        existing = {"sp500": {"AAPL"}, "ndx100": set()}
        moves = compute_movements(live, existing)
        assert len(moves) == 1
        assert moves[0].type == "promotion"

    def test_empty_live_and_existing(self):
        moves = compute_movements({}, {})
        assert moves == []


class TestUpdatePresetTickers:
    def test_updates_tickers_array(self, tmp_path):
        preset_path = tmp_path / "sp500.json"
        preset_path.write_text(
            json.dumps(
                {
                    "name": "sp500",
                    "description": "S&P 500",
                    "tickers": ["AAPL", "OLD"],
                    "pairs": [["AAPL", "OLD"]],
                    "groups": {},
                    "source": "wikipedia",
                }
            )
        )
        update_preset_tickers(preset_path, {"AAPL", "NEW"})
        data = json.loads(preset_path.read_text())
        assert data["tickers"] == ["AAPL", "NEW"]
        assert data["pairs"] == [["AAPL", "OLD"]]
        assert data["name"] == "sp500"

    def test_creates_new_preset(self, tmp_path):
        preset_path = tmp_path / "interests.json"
        update_preset_tickers(
            preset_path,
            {"AAPL", "TSLA"},
            name="interests",
            description="Personal watchlist",
        )
        data = json.loads(preset_path.read_text())
        assert data["tickers"] == ["AAPL", "TSLA"]
        assert data["name"] == "interests"


class TestApplySync:
    def test_adds_new_tickers_to_registry(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        movements = [Movement("add", "NEW", from_tags=[], to_tags=["sp500"])]
        apply_sync(reg, movements)
        assert "sp500" in reg.get("NEW").tags

    def test_promotion_updates_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("APP", {"r2k"}, status="active")
        movements = [Movement("promotion", "APP", from_tags=["r2k"], to_tags=["sp500"])]
        apply_sync(reg, movements)
        entry = reg.get("APP")
        assert "sp500" in entry.tags
        assert "r2k" not in entry.tags

    def test_demotion_updates_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("OLD", {"sp500"}, status="active")
        movements = [Movement("demotion", "OLD", from_tags=["sp500"], to_tags=["r2k"])]
        apply_sync(reg, movements)
        entry = reg.get("OLD")
        assert "r2k" in entry.tags
        assert "sp500" not in entry.tags

    def test_remove_clears_index_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("GONE", {"sp500"}, status="active")
        movements = [Movement("remove", "GONE", from_tags=["sp500"], to_tags=[])]
        apply_sync(reg, movements)
        entry = reg.get("GONE")
        assert "sp500" not in entry.tags

    def test_interest_tag_preserved_on_remove(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("GONE", {"sp500", "interest"}, status="active")
        movements = [Movement("remove", "GONE", from_tags=["sp500"], to_tags=[])]
        apply_sync(reg, movements)
        entry = reg.get("GONE")
        assert "interest" in entry.tags
        assert "sp500" not in entry.tags

    def test_logs_changes(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        movements = [Movement("add", "NEW", from_tags=[], to_tags=["sp500"])]
        apply_sync(reg, movements)
        assert len(reg.changelog) == 1
        assert reg.changelog[0].type == "add"


class TestArchiveDelisted:
    def test_moves_bronze_to_delisted(self, tmp_path):
        data_lake = tmp_path / "data-lake"
        src = data_lake / "bronze" / "asset_class=equity" / "symbol=TWTR"
        src.mkdir(parents=True)
        (src / "1d.parquet").write_text("fake")

        assert _archive_delisted("TWTR", data_lake) is True
        assert not src.exists()
        assert (
            data_lake
            / "bronze-delisted"
            / "asset_class=equity"
            / "symbol=TWTR"
            / "1d.parquet"
        ).exists()

    def test_returns_false_if_no_bronze(self, tmp_path):
        assert _archive_delisted("FAKE", tmp_path) is False


class TestMain:
    def _setup_workspace(self, tmp_path, monkeypatch):
        warehouse = tmp_path / "warehouse"
        warehouse.mkdir()
        presets = tmp_path / "presets"
        presets.mkdir()
        for name, tickers in [
            ("sp500", ["AAPL"]),
            ("ndx100", ["AAPL"]),
            ("r2k", ["ACME"]),
        ]:
            (presets / f"{name}.json").write_text(
                json.dumps(
                    {
                        "name": name,
                        "tickers": tickers,
                        "pairs": [],
                        "groups": {},
                        "source": "test",
                    }
                )
            )
        monkeypatch.setattr("livewire_scripts.universe_sync._WAREHOUSE_DIR", warehouse)
        monkeypatch.setattr("livewire_scripts.universe_sync._PRESET_DIR", presets)
        monkeypatch.setattr(
            "livewire_scripts.universe_sync._DATA_LAKE", warehouse / "data-lake"
        )
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        return warehouse, presets

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        return_value=set(f"T{i}" for i in range(500)),
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_ndx100",
        return_value=set(f"N{i}" for i in range(100)),
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_r2k",
        return_value=set(f"R{i}" for i in range(1900)),
    )
    def test_full_sync_dry_run(
        self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch
    ):
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        main(["--dry-run"])
        assert not (warehouse / "registry.json").exists()

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        return_value=set(f"T{i}" for i in range(500)) | {"MSFT"},
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_ndx100",
        return_value=set(f"N{i}" for i in range(100)),
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_r2k",
        return_value=set(f"R{i}" for i in range(1900)),
    )
    def test_full_sync_writes_registry(
        self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch
    ):
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        main([])
        assert (warehouse / "registry.json").exists()
        reg = TagRegistry(warehouse / "registry.json")
        assert "sp500" in reg.get("MSFT").tags

    @patch("livewire_scripts.universe_sync.fetch_sp500", return_value={"AAPL"})
    @patch("livewire_scripts.universe_sync.fetch_ndx100", return_value={"AAPL"})
    @patch("livewire_scripts.universe_sync.fetch_r2k", return_value={"ACME"})
    def test_aborts_on_suspiciously_few_tickers(
        self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch
    ):
        self._setup_workspace(tmp_path, monkeypatch)
        with pytest.raises(SystemExit):
            main([])

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        return_value=set(f"T{i}" for i in range(500)),
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_ndx100",
        return_value=set(f"N{i}" for i in range(100)),
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_r2k",
        return_value=set(f"R{i}" for i in range(1900)),
    )
    def test_interests_flag(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, presets = self._setup_workspace(tmp_path, monkeypatch)
        main(["--interests", "TSLA", "GME"])
        reg = TagRegistry(warehouse / "registry.json")
        assert "interest" in reg.get("TSLA").tags
        assert "interest" in reg.get("GME").tags
        interests_preset = json.loads((presets / "interests.json").read_text())
        assert "TSLA" in interests_preset["tickers"]

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        return_value=set(f"T{i}" for i in range(500)),
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_ndx100",
        return_value=set(f"N{i}" for i in range(100)),
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_r2k",
        return_value=set(f"R{i}" for i in range(1900)),
    )
    def test_no_changes_shows_current(
        self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch
    ):
        warehouse, presets = self._setup_workspace(tmp_path, monkeypatch)
        # First run: seed the registry
        main([])
        # Second run: no changes
        main([])
        reg = TagRegistry(warehouse / "registry.json")
        assert len(reg.all_tickers()) > 0

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        side_effect=UniverseFetchError("network down"),
    )
    def test_fetch_failure_exits(self, mock_sp, tmp_path, monkeypatch):
        self._setup_workspace(tmp_path, monkeypatch)
        with pytest.raises(SystemExit):
            main([])
