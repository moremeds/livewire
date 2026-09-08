import csv
import gzip
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.massive_flatfile_store import RAW_SCHEMA, MassiveFlatfileStore


def _write_day(path, rows):
    with gzip.open(path, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_stage_gzip_preserves_all_symbols(tmp_path):
    source = tmp_path / "day.csv.gz"
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        for ticker in ["AAPL", "MSFT"]:
            writer.writerow(
                {
                    "ticker": ticker,
                    "volume": 10,
                    "open": 1,
                    "close": 2,
                    "high": 3,
                    "low": 0.5,
                    "window_start": 1717421400000000000,
                    "transactions": 1,
                }
            )
    store = MassiveFlatfileStore(tmp_path, bucket_count=4)
    stats = store.stage_gzip(date(2024, 6, 3), source)
    assert stats == {"rows": 2, "symbols": 2}
    assert store.has_raw_date(date(2024, 6, 3))
    assert store.symbols_for_date(date(2024, 6, 3)) == {"AAPL", "MSFT"}
    assert store.raw_stats(date(2024, 6, 3))["rows"] == 2
    assert store.stage_gzip(date(2024, 6, 3), source)["rows"] == 2
    # Raw staging buckets use the shared zstd codec, not snappy.
    bucket_files = list(store.raw_path(date(2024, 6, 3)).glob("bucket=*.parquet"))
    assert bucket_files
    for bucket_file in bucket_files:
        metadata = pq.read_metadata(bucket_file)
        assert metadata.row_group(0).column(0).compression == "ZSTD"


def test_scan_bucket_by_ticker_merges_dates_without_full_bucket_concat(tmp_path):
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    first = tmp_path / "first.csv.gz"
    second = tmp_path / "second.csv.gz"
    _write_day(
        first,
        [
            {
                "ticker": "AAPL",
                "volume": 10,
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 0.5,
                "window_start": 1717421400000000000,
                "transactions": 1,
            },
            {
                "ticker": "MSFT",
                "volume": 20,
                "open": 2,
                "close": 3,
                "high": 4,
                "low": 1.5,
                "window_start": 1717421400000000000,
                "transactions": 1,
            },
        ],
    )
    _write_day(
        second,
        [
            {
                "ticker": "AAPL",
                "volume": 11,
                "open": 2,
                "close": 3,
                "high": 4,
                "low": 1.5,
                "window_start": 1717507800000000000,
                "transactions": 1,
            },
            {
                "ticker": "MSFT",
                "volume": 21,
                "open": 3,
                "close": 4,
                "high": 5,
                "low": 2.5,
                "window_start": 1717507800000000000,
                "transactions": 1,
            },
        ],
    )
    days = [date(2024, 6, 3), date(2024, 6, 4)]
    store.stage_gzip(days[0], first)
    store.stage_gzip(days[1], second)

    grouped = list(store.scan_bucket_by_ticker(0, days, batch_size=1))
    assert [ticker for ticker, _ in grouped] == ["AAPL", "MSFT"]
    assert [len(rows) for _, rows in grouped] == [2, 2]
    assert grouped[0][1][0]["bar_timestamp"] < grouped[0][1][1]["bar_timestamp"]


def test_stage_rejects_duplicate_composite_keys(tmp_path):
    source = tmp_path / "duplicate.csv.gz"
    row = {
        "ticker": "AAPL",
        "volume": 10,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": 1717421400000000000,
        "transactions": 1,
    }
    _write_day(source, [row, row])
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)

    with pytest.raises(ValueError, match="duplicate"):
        store.stage_gzip(date(2024, 6, 3), source)
    assert not store.has_raw_date(date(2024, 6, 3))


def test_stage_rejects_empty_and_invalid_files_and_can_replace(tmp_path):
    day = date(2024, 6, 3)
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    empty = tmp_path / "empty.csv.gz"
    _write_day(empty, [])
    with pytest.raises(ValueError, match="no rows"):
        store.stage_gzip(day, empty)

    invalid = tmp_path / "invalid.csv.gz"
    _write_day(
        invalid,
        [
            {
                "ticker": "AAPL",
                "volume": 10,
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 0.5,
                "window_start": "bad",
                "transactions": 1,
            }
        ],
    )
    with pytest.raises(ValueError):
        store.stage_gzip(day, invalid)

    good = tmp_path / "good.csv.gz"
    row = {
        "ticker": "AAPL",
        "volume": 10,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": 1717421400000000000,
        "transactions": 1,
    }
    _write_day(good, [row])
    store.stage_gzip(day, good)
    stale_old = store.raw_path(day).with_name(f".old-{store.raw_path(day).name}")
    stale_old.mkdir()
    _write_day(good, [row, {**row, "ticker": "MSFT"}])
    assert store.stage_gzip(day, good, replace=True)["symbols"] == 2
    assert not store.raw_path(day).with_name(f".old-{store.raw_path(day).name}").exists()


def test_read_recovers_interrupted_directory_swap(tmp_path):
    day = date(2024, 6, 3)
    source = tmp_path / "day.csv.gz"
    row = {
        "ticker": "AAPL",
        "volume": 10,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": 1717421400000000000,
        "transactions": 1,
    }
    _write_day(source, [row])
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    store.stage_gzip(day, source)
    final = store.raw_path(day)
    previous = final.with_name(f".old-{final.name}")
    final.rename(previous)  # Simulate a SIGKILL between the two directory renames.

    assert store.has_raw_date(day)
    assert store.raw_stats(day)["symbols"] == 1
    assert not previous.exists()


def test_same_date_nonreplace_writers_publish_one_complete_directory(tmp_path):
    day = date(2024, 6, 3)
    first = tmp_path / "first.csv.gz"
    second = tmp_path / "second.csv.gz"
    base = {
        "volume": 10,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": 1717421400000000000,
        "transactions": 1,
    }
    _write_day(first, [{**base, "ticker": "AAPL"}])
    _write_day(second, [{**base, "ticker": "MSFT"}])

    def stage(path):
        return MassiveFlatfileStore(tmp_path, bucket_count=1).stage_gzip(day, path)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(stage, [first, second]))

    assert [{key: result[key] for key in ("rows", "symbols")} for result in results] == [
        {"rows": 1, "symbols": 1},
        {"rows": 1, "symbols": 1},
    ]
    store_symbols = MassiveFlatfileStore(tmp_path, bucket_count=1).symbols_for_date(day)
    assert store_symbols in ({"AAPL"}, {"MSFT"})


def test_staging_decode_failure_preserves_the_prior_raw_date(tmp_path):
    day = date(2024, 6, 3)
    first = tmp_path / "first.csv.gz"
    replacement = tmp_path / "replacement.csv.gz"
    base = {
        "volume": 10,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": 1717421400000000000,
        "transactions": 1,
    }
    _write_day(first, [{**base, "ticker": "AAPL"}])
    _write_day(replacement, [{**base, "ticker": "MSFT"}])
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    store.stage_gzip(day, first)
    before = {path.name: path.read_bytes() for path in store.raw_path(day).iterdir()}

    with (
        patch("clients.massive_flatfile_store.validate_and_fsync_raw_stage", side_effect=OSError("bad parquet page")),
        pytest.raises(OSError, match="bad parquet page"),
    ):
        store.stage_gzip(day, replacement, replace=True)

    assert {path.name: path.read_bytes() for path in store.raw_path(day).iterdir()} == before


def test_bucket_scan_releases_the_shared_date_lock_before_yielding_tickers(tmp_path):
    day = date(2024, 6, 3)
    source = tmp_path / "day.csv.gz"
    base = {
        "volume": 10,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": 1717421400000000000,
        "transactions": 1,
    }
    _write_day(source, [{**base, "ticker": "AAPL"}, {**base, "ticker": "MSFT"}])
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    store.stage_gzip(day, source)
    scan = store.scan_bucket_by_ticker(0, [day])
    assert next(scan)[0] == "AAPL"
    writer_entered = threading.Event()

    def writer():
        with store._date_lock(day):
            writer_entered.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(writer)
        assert writer_entered.wait(timeout=1), "scan must not hold its shared lock while yielding"
    scan.close()


def test_scan_detects_cross_date_duplicate_and_ignores_empty_bucket(tmp_path):
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    days = [date(2024, 6, 3), date(2024, 6, 4)]
    row = {
        "ticker": "AAPL",
        "volume": 10,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": 1717421400000000000,
        "transactions": 1,
    }
    for index, day in enumerate(days):
        source = tmp_path / f"{index}.csv.gz"
        _write_day(source, [row])
        store.stage_gzip(day, source)
    with pytest.raises(ValueError, match="duplicate raw"):
        list(store.scan_bucket_by_ticker(0, days))

    empty_day = date(2024, 6, 5)
    store.raw_path(empty_day).mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([], schema=RAW_SCHEMA), store.bucket_path(empty_day, 0))
    assert list(store.scan_bucket_by_ticker(0, [empty_day])) == []


def test_validate_table_rejects_empty_and_unsorted():
    with pytest.raises(ValueError, match="empty"):
        MassiveFlatfileStore._validate_table(pa.Table.from_pylist([], schema=RAW_SCHEMA))
    rows = [
        {
            "ticker": "MSFT",
            "bar_timestamp": datetime(2024, 6, 3, tzinfo=UTC),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1,
        },
        {
            "ticker": "AAPL",
            "bar_timestamp": datetime(2024, 6, 3, tzinfo=UTC),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1,
        },
    ]
    with pytest.raises(ValueError, match="not sorted"):
        MassiveFlatfileStore._validate_table(pa.Table.from_pylist(rows, schema=RAW_SCHEMA))


def test_empty_raw_stats_and_missing_symbols(tmp_path):
    store = MassiveFlatfileStore(tmp_path)
    assert store.symbols_for_date(date(2024, 6, 3)) == set()
    assert store.raw_stats(date(2024, 6, 3)) == {
        "rows": 0,
        "symbols": 0,
        "size_bytes": 0,
        "earliest": None,
        "latest": None,
    }
