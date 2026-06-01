"""Tests for clients/tag_registry.py."""

from __future__ import annotations

from clients.tag_registry import (
    TagRegistry,
)


class TestRegistryLoadSave:
    def test_empty_registry(self, tmp_path):
        path = tmp_path / "registry.json"
        reg = TagRegistry(path)
        assert reg.all_tickers() == set()
        assert reg.by_tag("sp500") == set()

    def test_save_and_reload(self, tmp_path):
        path = tmp_path / "registry.json"
        reg = TagRegistry(path)
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.save()

        reg2 = TagRegistry(path)
        assert reg2.by_tag("sp500") == {"AAPL"}
        assert reg2.by_tag("ndx100") == {"AAPL"}
        assert reg2.get("AAPL").status == "active"

    def test_load_corrupted_file(self, tmp_path):
        path = tmp_path / "registry.json"
        path.write_text("not json")
        reg = TagRegistry(path)
        assert reg.all_tickers() == set()


class TestRegistryOperations:
    def test_set_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        assert reg.get("AAPL").tags == {"sp500", "ndx100"}

    def test_add_tag(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.add_tag("AAPL", "ndx100")
        assert reg.get("AAPL").tags == {"sp500", "ndx100"}

    def test_add_tag_new_ticker(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.add_tag("AAPL", "sp500")
        assert reg.get("AAPL").tags == {"sp500"}

    def test_remove_tag(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.remove_tag("AAPL", "sp500")
        assert reg.get("AAPL").tags == {"ndx100"}

    def test_remove_tag_nonexistent_ticker(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.remove_tag("FAKE", "sp500")  # should not raise

    def test_by_tag(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.set_tags("MSFT", {"sp500"}, status="active")
        reg.set_tags("ACME", {"r2k"}, status="active")
        assert reg.by_tag("sp500") == {"AAPL", "MSFT"}
        assert reg.by_tag("ndx100") == {"AAPL"}
        assert reg.by_tag("r2k") == {"ACME"}

    def test_by_tags_intersection(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.set_tags("MSFT", {"sp500"}, status="active")
        assert reg.by_tags({"sp500", "ndx100"}) == {"AAPL"}

    def test_active_only(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_tags("TWTR", {"sp500"}, status="delisted")
        assert reg.by_tag("sp500", active_only=True) == {"AAPL"}
        assert reg.by_tag("sp500", active_only=False) == {"AAPL", "TWTR"}

    def test_get_unknown_ticker(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        assert reg.get("FAKE") is None

    def test_mark_delisted(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("TWTR", {"sp500"}, status="active")
        reg.mark_delisted("TWTR", delisted_at="2022-10-28")
        entry = reg.get("TWTR")
        assert entry.status == "delisted"
        assert entry.delisted_at == "2022-10-28"
        assert entry.tags == {"sp500"}

    def test_mark_delisted_default_date(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("X", {"sp500"}, status="active")
        reg.mark_delisted("X")
        assert reg.get("X").delisted_at is not None

    def test_mark_delisted_nonexistent_ticker(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.mark_delisted("FAKE")  # should not raise

    def test_set_tags_preserves_added_at(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        original_added = reg.get("AAPL").added_at
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        assert reg.get("AAPL").added_at == original_added

    def test_set_earliest(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_earliest("AAPL", "1993-01-29", source="ib")
        assert reg.get("AAPL").earliest_available == "1993-01-29"
        assert reg.get("AAPL").earliest_source == "ib"

    def test_set_earliest_nonexistent_ticker(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_earliest("FAKE", "2020-01-01")  # should not raise

    def test_earliest_available_persists(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_earliest("AAPL", "1993-01-29", source="ib")
        reg.save()

        reg2 = TagRegistry(tmp_path / "r.json")
        assert reg2.get("AAPL").earliest_available == "1993-01-29"
        assert reg2.get("AAPL").earliest_source == "ib"

    def test_set_tags_preserves_earliest(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_earliest("AAPL", "1993-01-29", source="ib")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        assert reg.get("AAPL").earliest_available == "1993-01-29"

    def test_by_tags_active_only_filters_delisted(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.set_tags("TWTR", {"sp500", "ndx100"}, status="delisted")
        assert reg.by_tags({"sp500", "ndx100"}, active_only=True) == {"AAPL"}
        assert reg.by_tags({"sp500", "ndx100"}, active_only=False) == {"AAPL", "TWTR"}

    def test_save_with_delisted_entry(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("TWTR", {"sp500"}, status="active")
        reg.mark_delisted("TWTR", delisted_at="2022-10-28")
        reg.save()
        reg2 = TagRegistry(tmp_path / "r.json")
        assert reg2.get("TWTR").delisted_at == "2022-10-28"


class TestChangelog:
    def test_log_promotion(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("APP", {"r2k"}, status="active")
        reg.log_change("promotion", "APP", from_tags=["r2k"], to_tags=["sp500"])
        reg.save()

        reg2 = TagRegistry(tmp_path / "r.json")
        assert len(reg2.changelog) == 1
        assert reg2.changelog[0].type == "promotion"
        assert reg2.changelog[0].ticker == "APP"

    def test_changelog_cap(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        for i in range(600):
            reg.log_change("add", f"T{i}", from_tags=[], to_tags=["sp500"])
        reg.save()
        reg2 = TagRegistry(tmp_path / "r.json")
        assert len(reg2.changelog) == 500
