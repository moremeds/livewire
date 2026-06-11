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


def test_download_dates_records_missing_day_and_continues(tmp_path):
    # NOT_FOUND is non-fatal: Massive only stages SIP trading days, so an unscheduled
    # NYSE closure (e.g. presidential funeral) legitimately has no flat file. The batch
    # must keep going, the date must be persisted to raw_unavailable, and a subsequent
    # run must skip it without re-inspecting.
    missing = date(2026, 6, 5)
    later = date(2026, 6, 6)
    client = MagicMock()
    client.inspect_date.side_effect = [_info(missing, FlatfileObjectStatus.NOT_FOUND), _info(later)]
    store = MagicMock()
    store.has_raw_date.return_value = False
    store.stage_gzip.return_value = {"rows": 1, "symbols": 1}
    state = MassiveFlatfileState(tmp_path / "cursors")

    stats = download_dates(client, store, state, [missing, later])

    assert stats.unavailable == 1
    assert stats.downloaded == 1
    assert state.raw_unavailable(missing)
    assert '"event": "raw_unavailable"' in state.manifest_path.read_text()

    # Resumed run: the cached unavailable marker must keep inspect_date from firing again.
    client.reset_mock()
    stats = download_dates(client, store, state, [missing])
    assert stats.skipped == 1
    client.inspect_date.assert_not_called()


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (FlatfileObjectStatus.FORBIDDEN, "forbidden"),
        (FlatfileObjectStatus.TRANSIENT_ERROR, "retries exhausted"),
    ],
)
def test_download_dates_rejects_non_downloadable_objects(tmp_path, status, message):
    day = date(2026, 6, 5)
    client = MagicMock()
    client.inspect_date.return_value = _info(day, status)
    store = MagicMock()
    store.has_raw_date.return_value = False
    with pytest.raises(RuntimeError, match=message):
        download_dates(client, store, MassiveFlatfileState(tmp_path / "cursors"), [day], max_retries=0)


def test_download_dates_records_staging_failure(tmp_path):
    day = date(2026, 6, 5)
    client = MagicMock()
    client.inspect_date.return_value = _info(day)
    store = MagicMock()
    store.has_raw_date.return_value = False
    store.stage_gzip.side_effect = ValueError("bad gzip")
    state = MassiveFlatfileState(tmp_path / "cursors")
    with pytest.raises(ValueError, match="bad gzip"):
        download_dates(client, store, state, [day])
    assert '"event": "raw_failed"' in state.manifest_path.read_text()
