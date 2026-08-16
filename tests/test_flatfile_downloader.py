from datetime import date
from pathlib import Path
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


def test_the_gz_downloads_beside_its_staged_output_not_into_tmpdir(tmp_path):
    # $TMPDIR is the internal volume; raw_root is the external lake. A whole-market
    # gz per worker on the wrong filesystem is what the capacity planner cannot see.
    day = date(2026, 6, 5)
    raw_root = tmp_path / "lake" / "raw" / "massive"
    store = MagicMock()
    store.raw_root = raw_root
    store.has_raw_date.return_value = False
    store.stage_gzip.return_value = {"rows": 1, "symbols": 1}
    client = MagicMock()
    client.inspect_date.return_value = _info(day)
    seen: list[Path] = []
    client.download_date_to_path.side_effect = lambda _d, dest: seen.append(dest)

    download_dates(client, store, MassiveFlatfileState(tmp_path / "cursors"), [day])

    assert seen, "download was never attempted"
    assert raw_root in seen[0].parents


def test_download_dates_retries_transient_inspection_then_stages(tmp_path):
    day = date(2026, 6, 5)
    client = MagicMock()
    client.inspect_date.side_effect = [_info(day, FlatfileObjectStatus.TRANSIENT_ERROR), _info(day)]
    store = MagicMock()
    store.raw_root = tmp_path / "raw"
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
    store.raw_root = tmp_path / "raw"
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


def test_repair_mode_reinspects_a_date_marked_unavailable(tmp_path):
    """`repair` is the operator explicitly retrying a date; honour that.

    The durable NOT_FOUND is right for routine catch-up but made the blacklist
    permanent: one transient 404 (a mid-publish race, an object briefly
    replaced) blacklisted the trade date forever, and
    `flatfile-ingest repair --dates <date>` skipped it too — leaving a hand
    edit of the state file as the only recovery.
    """
    day = date(2026, 6, 5)
    client = MagicMock()
    client.inspect_date.return_value = _info(day, FlatfileObjectStatus.NOT_FOUND)
    store = MagicMock()
    store.raw_root = tmp_path / "raw"
    store.has_raw_date.return_value = False
    store.stage_gzip.return_value = {"rows": 1, "symbols": 1}
    state = MassiveFlatfileState(tmp_path / "cursors")

    download_dates(client, store, state, [day])
    assert state.raw_unavailable(day)

    # The object is now present; repair mode must ask again rather than skip.
    client.reset_mock()
    client.inspect_date.return_value = _info(day)
    stats = download_dates(client, store, state, [day], replace=True)

    client.inspect_date.assert_called_once()
    assert stats.downloaded == 1


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
    store.raw_root = tmp_path / "raw"
    store.has_raw_date.return_value = False
    with pytest.raises(RuntimeError, match=message):
        download_dates(client, store, MassiveFlatfileState(tmp_path / "cursors"), [day], max_retries=0)


def test_download_dates_records_staging_failure(tmp_path):
    day = date(2026, 6, 5)
    client = MagicMock()
    client.inspect_date.return_value = _info(day)
    store = MagicMock()
    store.raw_root = tmp_path / "raw"
    store.has_raw_date.return_value = False
    store.stage_gzip.side_effect = ValueError("bad gzip")
    state = MassiveFlatfileState(tmp_path / "cursors")
    with pytest.raises(ValueError, match="bad gzip"):
        download_dates(client, store, state, [day])
    assert '"event": "raw_failed"' in state.manifest_path.read_text()
