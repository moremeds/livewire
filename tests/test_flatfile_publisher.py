import csv
import gzip
from datetime import date

from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from livewire_scripts.flatfile_publisher import publish_dates


def test_publish_dates_writes_every_timeframe(tmp_path):
    source = tmp_path / "day.csv.gz"
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"]
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "AAPL",
                "volume": 10,
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 0.5,
                "window_start": 1717421400000000000,
                "transactions": 1,
            }
        )
    day = date(2024, 6, 3)
    store = MassiveFlatfileStore(tmp_path, bucket_count=4)
    store.stage_gzip(day, source)
    stats = publish_dates(store, MassiveFlatfileState(tmp_path / "cursors"), [day], tmp_path / "bronze")
    assert stats["tickers"] == 1
    for tf in ("1m", "5m", "30m", "1h"):
        assert IntradayBronzeClient(tmp_path / "bronze", tf).get_existing_symbols() == {"AAPL"}


def test_publish_dates_preserves_case_distinct_provider_symbols(tmp_path):
    source = tmp_path / "day.csv.gz"
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        for ticker in ("BCPC", "BCpC"):
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
    day = date(2024, 6, 3)
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    store.stage_gzip(day, source)
    bronze = tmp_path / "bronze"
    publish_dates(store, MassiveFlatfileState(tmp_path / "cursors"), [day], bronze)

    assert IntradayBronzeClient(bronze, "1m").get_existing_symbols() == {"BCPC", "BCpC"}


def test_publish_dates_handles_empty_replace_and_resume(tmp_path):
    state = MassiveFlatfileState(tmp_path / "cursors")
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    assert publish_dates(store, state, [], tmp_path / "bronze") == {"tickers": 0, "rows_1m": 0}

    source = tmp_path / "day.csv.gz"
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "AAPL",
                "volume": 10,
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 0.5,
                "window_start": 1717421400000000000,
                "transactions": 1,
            }
        )
    day = date(2024, 6, 3)
    store.stage_gzip(day, source)
    stats = publish_dates(store, state, [day], tmp_path / "bronze", replace_complete=True, scope="history")
    assert stats == {"tickers": 1, "rows_1m": 1}
    assert publish_dates(store, state, [day], tmp_path / "bronze", scope="history") == {"tickers": 0, "rows_1m": 0}

    partial = MassiveFlatfileState(tmp_path / "partial")
    partial.mark_ticker_completed("partial", 0, "AAPL")
    assert publish_dates(store, partial, [day], tmp_path / "other", scope="partial") == {"tickers": 0, "rows_1m": 0}
