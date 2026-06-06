from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from livewire_scripts.ingest_flatfiles import _parse_dates, _require_credentials, main


def test_credentials_are_required(monkeypatch):
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_KEY", raising=False)
    with pytest.raises(SystemExit, match="Missing required"):
        _require_credentials()


def test_repair_dates_are_explicit():
    args = MagicMock(mode="repair", dates=["2026-06-05"], start=None, end=None)
    assert _parse_dates(args, ()) == [date(2026, 6, 5)]


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
