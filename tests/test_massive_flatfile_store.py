import csv
import gzip
from datetime import date

from clients.massive_flatfile_store import MassiveFlatfileStore


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

    import pytest

    with pytest.raises(ValueError, match="duplicate"):
        store.stage_gzip(date(2024, 6, 3), source)
    assert not store.has_raw_date(date(2024, 6, 3))
