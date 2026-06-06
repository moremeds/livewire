from datetime import date
from unittest.mock import MagicMock

import pytest

from clients.massive_flatfile_client import FlatfileObjectInfo, FlatfileObjectStatus
from clients.massive_flatfile_state import MassiveFlatfileState
from livewire_scripts.flatfile_downloader import download_dates


def _info(day, status=FlatfileObjectStatus.AVAILABLE):
    return FlatfileObjectInfo(day, f"key/{day}.csv.gz", status, size_bytes=123, error=status.value)


def test_download_dates_skips_durable_completed_raw_date(tmp_path):
    day = date(2026, 6, 5)
    state = MassiveFlatfileState(tmp_path / "cursors")
    state.mark_raw_completed(day)
    store = MagicMock()
    store.has_raw_date.return_value = True
    client = MagicMock()
    stats = download_dates(client, store, state, [day])
    assert stats.skipped == 1
    client.inspect_date.assert_not_called()


def test_download_dates_retries_transient_inspection_then_stages(tmp_path):
    day = date(2026, 6, 5)
    client = MagicMock()
    client.inspect_date.side_effect = [_info(day, FlatfileObjectStatus.TRANSIENT_ERROR), _info(day)]
    store = MagicMock()
    store.has_raw_date.return_value = False
    store.stage_gzip.return_value = {"rows": 10, "symbols": 2}
    sleeps = []

    stats = download_dates(client, store, MassiveFlatfileState(tmp_path / "cursors"), [day], sleep_fn=sleeps.append)

    assert stats.inspected == 2
    assert stats.downloaded == 1
    assert stats.bytes == 123
    assert sleeps == [1]


def test_download_dates_rejects_missing_expected_day(tmp_path):
    day = date(2026, 6, 5)
    client = MagicMock()
    client.inspect_date.return_value = _info(day, FlatfileObjectStatus.NOT_FOUND)
    store = MagicMock()
    store.has_raw_date.return_value = False
    state = MassiveFlatfileState(tmp_path / "cursors")

    with pytest.raises(RuntimeError, match="unavailable"):
        download_dates(client, store, state, [day])
    assert '"event": "raw_unavailable"' in state.manifest_path.read_text()
