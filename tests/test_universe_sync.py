"""Tests for livewire_scripts/universe_sync.py."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from clients.tag_registry import TagRegistry
from clients.universe_client import TickerStatus, UniverseFetchError
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
        assert Movement("promotion", "APP", from_tags=["r2k"], to_tags=["sp500"]) in moves

    def test_demotion_sp500_to_r2k(self):
        live = {"sp500": {"AAPL"}, "r2k": {"BETA", "OLD"}}
        existing = {"sp500": {"AAPL", "OLD"}, "r2k": {"BETA"}}
        moves = compute_movements(live, existing)
        assert Movement("demotion", "OLD", from_tags=["sp500"], to_tags=["r2k"]) in moves

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
        assert (data_lake / "bronze-delisted" / "asset_class=equity" / "symbol=TWTR" / "1d.parquet").exists()

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
        monkeypatch.setattr("livewire_scripts.universe_sync._DATA_LAKE", warehouse / "data-lake")
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
    def test_full_sync_dry_run(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        main(["--dry-run"])
        assert not (warehouse / "registry.json").exists()

    @patch("livewire_scripts.universe_sync.fetch_sp500", return_value={f"T{i}" for i in range(500)})
    @patch("livewire_scripts.universe_sync.fetch_ndx100", return_value={f"N{i}" for i in range(100)})
    @patch("livewire_scripts.universe_sync.fetch_r2k", side_effect=AssertionError("must not be fetched"))
    def test_an_unselected_index_is_never_fetched(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        """slickcharts dropped every Russell page (404, measured 2026-09-02) and the
        gap registry's equity row only names sp500 and ndx100, so the scheduled job
        asks for those two. One dead source must not block the two the denominator
        actually uses."""
        self._setup_workspace(tmp_path, monkeypatch)
        main(["--dry-run", "--indexes", "sp500", "ndx100"])
        mock_r2k.assert_not_called()

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        return_value={"AAPL"} | {f"T{i}" for i in range(500)},
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_ndx100",
        return_value={"AAPL"} | {f"N{i}" for i in range(100)},
    )
    @patch("livewire_scripts.universe_sync.fetch_r2k", side_effect=AssertionError("must not be fetched"))
    def test_an_unselected_index_is_not_read_as_a_mass_removal(
        self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch, capsys
    ):
        """The trap this flag could have walked into. `existing` used to be built
        over every INDEX_TAG, so an index absent from `live` had all of its members
        diffed against nothing and emitted as removals -- r2k alone is 1886 tickers
        in the real preset. MIN_EXPECTED_CONSTITUENTS cannot catch it: that guard
        only inspects indexes that were actually fetched."""
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        main(["--dry-run", "--indexes", "sp500", "ndx100"])
        out = capsys.readouterr().out
        # ACME is the r2k-only member of the fixture preset.
        assert "ACME" not in out
        assert "REMOVE" not in out
        assert not (warehouse / "registry.json").exists()

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        return_value={"AAPL"} | {f"T{i}" for i in range(500)},
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_ndx100",
        return_value={"AAPL"} | {f"N{i}" for i in range(100)},
    )
    @patch("livewire_scripts.universe_sync.fetch_r2k", side_effect=AssertionError("must not be fetched"))
    def test_an_unselected_index_keeps_its_preset_file(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        """A SECOND write path, and the one that actually fired. The preset writer
        reads the REGISTRY rather than `live`, and the registry is only tagged for
        the indexes that were fetched -- so an unselected index resolved to the
        empty set and its file was truncated. Measured on the first real apply,
        2026-09-02: `Updated r2k.json: 0 tickers`, 1886 gone, while the movements
        table above was entirely correct. Guarding one write path is not enough."""
        _, presets = self._setup_workspace(tmp_path, monkeypatch)
        before = (presets / "r2k.json").read_text()
        main(["--indexes", "sp500", "ndx100"])
        assert (presets / "r2k.json").read_text() == before
        assert json.loads((presets / "sp500.json").read_text())["tickers"]

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
    def test_full_sync_writes_registry(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        main([])
        assert (warehouse / "registry.json").exists()
        reg = TagRegistry(warehouse / "registry.json")
        assert "sp500" in reg.get("MSFT").tags

    @patch("livewire_scripts.universe_sync.fetch_sp500", return_value={"AAPL"})
    @patch("livewire_scripts.universe_sync.fetch_ndx100", return_value={"AAPL"})
    @patch("livewire_scripts.universe_sync.fetch_r2k", return_value={"ACME"})
    def test_aborts_on_suspiciously_few_tickers(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
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
    def test_no_changes_shows_current(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, presets = self._setup_workspace(tmp_path, monkeypatch)
        # First run: seed the registry
        main([])
        # Second run: no changes
        main([])
        reg = TagRegistry(warehouse / "registry.json")
        assert len(reg.all_tickers()) > 0

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
    @patch("livewire_scripts.universe_sync.check_tickers_bulk")
    def test_dead_ticker_check_with_polygon(self, mock_bulk, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, presets = self._setup_workspace(tmp_path, monkeypatch)
        monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
        # Seed registry with AAPL in sp500 first
        reg = TagRegistry(warehouse / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_tags("DEAD", {"sp500"}, status="active")
        reg.save()
        # Live data doesn't have DEAD, so it becomes a "remove" movement
        mock_sp.return_value = set(f"T{i}" for i in range(500)) | {"AAPL"}
        mock_bulk.return_value = {
            "DEAD": TickerStatus(
                ticker="DEAD",
                active=False,
                delisted_utc="2024-01-01",
                list_date="2010-03-15",
            )
        }
        main([])
        reg2 = TagRegistry(warehouse / "registry.json")
        entry = reg2.get("DEAD")
        assert entry.status == "delisted"
        assert entry.earliest_available == "2010-03-15"

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
    def test_skip_dead_flag(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
        main(["--skip-dead"])
        # Should complete without calling Polygon

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        return_value=set(f"T{i}" for i in range(500)) | {"AAPL"},
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_ndx100",
        return_value=set(f"N{i}" for i in range(100)) | {"AAPL"},
    )
    @patch(
        "livewire_scripts.universe_sync.fetch_r2k",
        return_value=set(f"R{i}" for i in range(1900)),
    )
    def test_seeds_registry_and_adds_missing_tags(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        """Test seeding: new tickers get set_tags, existing tickers get add_tag."""
        warehouse, presets = self._setup_workspace(tmp_path, monkeypatch)
        # Pre-seed registry with sp500 tickers so they have no movement,
        # but are missing from registry → exercises the set_tags seeding path
        # Also pre-seed AAPL with only sp500 tag so ndx100 add_tag fires
        reg = TagRegistry(warehouse / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        # Seed presets with full live data so there are no movements for sp500
        sp500_tickers = sorted(set(f"T{i}" for i in range(500)) | {"AAPL"})
        (presets / "sp500.json").write_text(
            json.dumps(
                {
                    "name": "sp500",
                    "tickers": sp500_tickers,
                    "pairs": [],
                    "groups": {},
                    "source": "test",
                }
            )
        )
        ndx100_tickers = sorted(set(f"N{i}" for i in range(100)) | {"AAPL"})
        (presets / "ndx100.json").write_text(
            json.dumps(
                {
                    "name": "ndx100",
                    "tickers": ndx100_tickers,
                    "pairs": [],
                    "groups": {},
                    "source": "test",
                }
            )
        )
        r2k_tickers = sorted(set(f"R{i}" for i in range(1900)))
        (presets / "r2k.json").write_text(
            json.dumps(
                {
                    "name": "r2k",
                    "tickers": r2k_tickers,
                    "pairs": [],
                    "groups": {},
                    "source": "test",
                }
            )
        )
        reg.save()
        main([])
        reg2 = TagRegistry(warehouse / "registry.json")
        # AAPL should have both tags (ndx100 added via seeding add_tag)
        entry = reg2.get("AAPL")
        assert "sp500" in entry.tags
        assert "ndx100" in entry.tags
        # T0 should be in registry via set_tags seeding (not a movement)
        assert reg2.get("T0") is not None

    @patch(
        "livewire_scripts.universe_sync.fetch_sp500",
        side_effect=UniverseFetchError("network down"),
    )
    def test_fetch_failure_exits(self, mock_sp, tmp_path, monkeypatch):
        self._setup_workspace(tmp_path, monkeypatch)
        with pytest.raises(SystemExit):
            main([])
