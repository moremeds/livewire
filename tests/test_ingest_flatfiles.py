from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from livewire_scripts import ingest_flatfiles
from livewire_scripts.ingest_flatfiles import _parse_dates, _require_credentials, main


def test_credentials_are_required(monkeypatch):
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_KEY", raising=False)
    with pytest.raises(SystemExit, match="Missing required"):
        _require_credentials()


def test_repair_dates_are_explicit():
    args = MagicMock(mode="repair", dates=["2026-06-05"], start=None, end=None)
    assert _parse_dates(args, ()) == [date(2026, 6, 5)]


def test_parse_dates_supports_backfill_catchup_range_and_rejects_incomplete_repair():
    days = (date(2026, 6, 1), date(2026, 6, 4), date(2026, 6, 5))
    assert _parse_dates(MagicMock(mode="backfill"), days) == list(days)
    assert _parse_dates(MagicMock(mode="catch-up", days=1), days) == [date(2026, 6, 4), date(2026, 6, 5)]
    args = MagicMock(mode="repair", dates=None, start="2026-06-05", end="2026-06-05")
    assert _parse_dates(args, days) == [date(2026, 6, 5)]
    with pytest.raises(SystemExit, match="repair requires"):
        _parse_dates(MagicMock(mode="repair", dates=None, start=None, end=None), days)


def test_discover_is_read_only(monkeypatch, tmp_path):
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "x")
    monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "y")
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    plan = MagicMock(
        earliest=date(2026, 6, 1), latest=date(2026, 6, 5), dates=(date(2026, 6, 5),), compressed_bytes=1, free_bytes=2
    )
    with (
        patch("livewire_scripts.ingest_flatfiles.MassiveFlatfileClient"),
        patch("livewire_scripts.ingest_flatfiles.discover_plan", return_value=plan),
        patch("livewire_scripts.ingest_flatfiles.download_dates") as download,
    ):
        assert main(["discover"]) == 0
    download.assert_not_called()


@pytest.mark.parametrize("mode", ["backfill", "catch-up", "repair"])
def test_main_executes_full_pipeline_modes(monkeypatch, tmp_path, mode):
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "x")
    monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "y")
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    day = date(2026, 6, 5)
    plan = MagicMock(
        earliest=day,
        latest=day,
        dates=(day,),
        compressed_bytes=1,
        projected_bytes=2,
        free_bytes=3,
    )
    state = MagicMock()
    args = [mode]
    if mode == "repair":
        args.extend(["--dates", day.isoformat()])
    with (
        patch("livewire_scripts.ingest_flatfiles.MassiveFlatfileClient"),
        patch("livewire_scripts.ingest_flatfiles.MassiveFlatfileState", return_value=state),
        patch("livewire_scripts.ingest_flatfiles.discover_plan", return_value=plan),
        patch("livewire_scripts.ingest_flatfiles.require_capacity") as capacity,
        patch(
            "livewire_scripts.ingest_flatfiles.download_dates", return_value=MagicMock(downloaded=1, skipped=0)
        ) as download,
        patch("livewire_scripts.ingest_flatfiles.publish_dates", return_value={"tickers": 1}) as publish,
    ):
        assert main(args) == 0
    assert capacity.call_count == (1 if mode == "backfill" else 0)
    assert download.call_count == 1
    assert publish.call_count == 1
    assert state.reset_publish_scope.call_count == (1 if mode == "repair" else 0)


def test_main_reports_capacity_failure_without_downloading(monkeypatch, tmp_path):
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "x")
    monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "y")
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    day = date(2026, 6, 5)
    plan = MagicMock(
        earliest=day,
        latest=day,
        dates=(day,),
        compressed_bytes=1,
        projected_bytes=2,
        free_bytes=3,
    )
    with (
        patch("livewire_scripts.ingest_flatfiles.MassiveFlatfileClient"),
        patch("livewire_scripts.ingest_flatfiles.discover_plan", return_value=plan),
        patch("livewire_scripts.ingest_flatfiles.require_capacity", side_effect=RuntimeError("insufficient")),
        patch("livewire_scripts.ingest_flatfiles.download_dates") as download,
        pytest.raises(SystemExit, match="insufficient"),
    ):
        main(["backfill"])
    download.assert_not_called()


class TestVerifyPublishCoverage:
    """Publish used to exit 0 no matter how little it wrote."""

    class _Store:
        def __init__(self, symbols):
            self._symbols = symbols

        def symbols_for_date(self, day):
            return set(self._symbols)

    def test_under_publish_fails_the_run(self):
        store = self._Store({f"T{i}" for i in range(100)})
        rc = ingest_flatfiles.verify_publish_coverage(store, [date(2026, 6, 5)], {"tickers": 40, "resumed": 0})
        assert rc == 1

    def test_full_publish_passes(self):
        store = self._Store({f"T{i}" for i in range(100)})
        rc = ingest_flatfiles.verify_publish_coverage(store, [date(2026, 6, 5)], {"tickers": 100, "resumed": 0})
        assert rc == 0

    def test_resumed_run_is_not_judged(self):
        """A resumed run legitimately publishes fewer than the window holds."""
        store = self._Store({f"T{i}" for i in range(100)})
        rc = ingest_flatfiles.verify_publish_coverage(store, [date(2026, 6, 5)], {"tickers": 0, "resumed": 31})
        assert rc == 0

    def test_empty_raw_is_not_a_failure(self):
        rc = ingest_flatfiles.verify_publish_coverage(
            self._Store(set()), [date(2026, 6, 5)], {"tickers": 0, "resumed": 0}
        )
        assert rc == 0
