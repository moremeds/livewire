import csv
import gzip
from datetime import UTC, date, datetime

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
