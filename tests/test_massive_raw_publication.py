"""Raw-date publication locks and staging durability."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from clients.massive_raw_publication import raw_date_lock, validate_and_fsync_raw_stage


def test_two_shared_readers_enter_before_an_exclusive_writer(tmp_path):
    raw_root = tmp_path / "raw"
    warehouse_alias = tmp_path / "warehouse-alias"
    raw_root.mkdir()
    warehouse_alias.symlink_to(raw_root, target_is_directory=True)
    day = date(2024, 6, 3)
    readers_ready = threading.Event()
    release_readers = threading.Event()
    writer_entered = threading.Event()
    count = 0
    count_lock = threading.Lock()

    def reader():
        nonlocal count
        with raw_date_lock(warehouse_alias, day, shared=True):
            with count_lock:
                count += 1
                if count == 2:
                    readers_ready.set()
            assert release_readers.wait(timeout=2)

    def writer():
        assert readers_ready.wait(timeout=2)
        with raw_date_lock(raw_root, day):
            writer_entered.set()

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(reader), pool.submit(reader), pool.submit(writer)]
        assert readers_ready.wait(timeout=2)
        assert not writer_entered.wait(timeout=0.1)
        release_readers.set()
        for future in futures:
            future.result(timeout=2)

    assert writer_entered.is_set()


def test_stage_validation_decodes_all_files_and_syncs_marker_and_directory(tmp_path):
    stage = tmp_path / "stage"
    stage.mkdir()
    pq.write_table(pa.table({"ticker": ["AAPL"]}), stage / "bucket=001.parquet")
    pq.write_table(pa.table({"ticker": ["AAPL"]}), stage / "_symbols.parquet")
    (stage / "_SUCCESS").write_text("rows=1\n", encoding="utf-8")

    with (
        patch("clients.massive_raw_publication.os.fsync") as fsync,
        patch("clients.massive_raw_publication.fsync_directory") as fsync_directory,
    ):
        validate_and_fsync_raw_stage(stage)

    assert fsync.call_count == 3
    fsync_directory.assert_called_once_with(stage)
