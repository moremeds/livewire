import csv
import gzip
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.massive_daily_flatfile_store import RAW_DAILY_SCHEMA, MassiveDailyFlatfileStore


def _write_day(path, rows):
    with gzip.open(path, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        writer.writerows(rows)


def _row(ticker, ts_ns, vol=10):
    return {
        "ticker": ticker,
        "volume": vol,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": ts_ns,
        "transactions": 1,
    }


# Midnight UTC of 2024-06-03 and 2024-06-04 in epoch ns.
_TS_20240603 = 1717372800_000_000_000
_TS_20240604 = 1717459200_000_000_000


def test_stage_gzip_writes_buckets_and_symbols(tmp_path):
    source = tmp_path / "day.csv.gz"
    _write_day(source, [_row("AAPL", _TS_20240603), _row("MSFT", _TS_20240603)])
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=4)
    stats = store.stage_gzip(date(2024, 6, 3), source)
    assert stats == {"rows": 2, "symbols": 2}
    assert store.has_raw_date(date(2024, 6, 3))
    assert store.symbols_for_date(date(2024, 6, 3)) == {"AAPL", "MSFT"}
    raw = store.raw_stats(date(2024, 6, 3))
    assert raw["rows"] == 2
    assert raw["symbols"] == 2
    assert raw["size_bytes"] > 0
    # Raw daily staging buckets use the shared zstd codec, not snappy.
    bucket_files = list(store.raw_path(date(2024, 6, 3)).glob("bucket=*.parquet"))
    assert bucket_files
    for bucket_file in bucket_files:
        metadata = pq.read_metadata(bucket_file)
        assert metadata.row_group(0).column(0).compression == "ZSTD"


def test_stage_gzip_is_idempotent(tmp_path):
    source = tmp_path / "day.csv.gz"
    _write_day(source, [_row("AAPL", _TS_20240603)])
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=2)
    first = store.stage_gzip(date(2024, 6, 3), source)
    second = store.stage_gzip(date(2024, 6, 3), source)
    assert first["rows"] == second["rows"] == 1


def test_scan_bucket_by_ticker_merges_dates(tmp_path):
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    first = tmp_path / "first.csv.gz"
    second = tmp_path / "second.csv.gz"
    _write_day(first, [_row("AAPL", _TS_20240603), _row("MSFT", _TS_20240603)])
    _write_day(second, [_row("AAPL", _TS_20240604), _row("MSFT", _TS_20240604)])
    days = [date(2024, 6, 3), date(2024, 6, 4)]
    store.stage_gzip(days[0], first)
    store.stage_gzip(days[1], second)

    grouped = list(store.scan_bucket_by_ticker(0, days, batch_size=1))
    assert [ticker for ticker, _ in grouped] == ["AAPL", "MSFT"]
    assert [len(rows) for _, rows in grouped] == [2, 2]
    aapl_dates = [r["trade_date"] for r in grouped[0][1]]
    assert aapl_dates == ["2024-06-03", "2024-06-04"]


def test_stage_rejects_duplicate_keys(tmp_path):
    source = tmp_path / "dup.csv.gz"
    row = _row("AAPL", _TS_20240603)
    _write_day(source, [row, row])
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    with pytest.raises(ValueError, match="duplicate"):
        store.stage_gzip(date(2024, 6, 3), source)
    assert not store.has_raw_date(date(2024, 6, 3))


def test_stage_rejects_empty_and_invalid(tmp_path):
    day = date(2024, 6, 3)
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    empty = tmp_path / "empty.csv.gz"
    _write_day(empty, [])
    with pytest.raises(ValueError, match="no rows"):
        store.stage_gzip(day, empty)

    invalid = tmp_path / "invalid.csv.gz"
    bad = _row("AAPL", _TS_20240603)
    bad["window_start"] = "not-a-number"
    _write_day(invalid, [bad])
    with pytest.raises(ValueError):
        store.stage_gzip(day, invalid)


def test_stage_replace_swaps_existing_dir(tmp_path):
    day = date(2024, 6, 3)
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    src = tmp_path / "v1.csv.gz"
    _write_day(src, [_row("AAPL", _TS_20240603)])
    store.stage_gzip(day, src)
    stale = store.raw_path(day).with_name(f".old-{store.raw_path(day).name}")
    stale.mkdir()
    src2 = tmp_path / "v2.csv.gz"
    _write_day(src2, [_row("AAPL", _TS_20240603), _row("MSFT", _TS_20240603)])
    stats = store.stage_gzip(day, src2, replace=True)
    assert stats["symbols"] == 2
    assert not stale.exists()


def test_scan_detects_cross_date_duplicate(tmp_path):
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    days = [date(2024, 6, 3), date(2024, 6, 4)]
    # Both days use the same window_start (same trade_date) — composite key collides on scan.
    for index, day in enumerate(days):
        source = tmp_path / f"{index}.csv.gz"
        _write_day(source, [_row("AAPL", _TS_20240603)])
        store.stage_gzip(day, source)
    with pytest.raises(ValueError, match="duplicate raw daily"):
        list(store.scan_bucket_by_ticker(0, days))


def test_scan_ignores_empty_bucket(tmp_path):
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    empty_day = date(2024, 6, 5)
    store.raw_path(empty_day).mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([], schema=RAW_DAILY_SCHEMA), store.bucket_path(empty_day, 0))
    assert list(store.scan_bucket_by_ticker(0, [empty_day])) == []


def test_validate_table_rejects_empty_and_unsorted():
    with pytest.raises(ValueError, match="empty"):
        MassiveDailyFlatfileStore._validate_table(pa.Table.from_pylist([], schema=RAW_DAILY_SCHEMA))
    unsorted_rows = [
        {"ticker": "MSFT", "trade_date": "2024-06-03", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1},
        {"ticker": "AAPL", "trade_date": "2024-06-03", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1},
    ]
    with pytest.raises(ValueError, match="not sorted"):
        MassiveDailyFlatfileStore._validate_table(pa.Table.from_pylist(unsorted_rows, schema=RAW_DAILY_SCHEMA))


def test_empty_raw_stats_and_missing_symbols(tmp_path):
    store = MassiveDailyFlatfileStore(tmp_path)
    assert store.symbols_for_date(date(2024, 6, 3)) == set()
    assert store.raw_stats(date(2024, 6, 3)) == {"rows": 0, "symbols": 0, "size_bytes": 0}
