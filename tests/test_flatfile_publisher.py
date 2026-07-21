import csv
import gzip
from datetime import date

import pyarrow.parquet as pq

from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from livewire_scripts.flatfile_publisher import publish_dates


def _write_flatfile(source, ticker="AAPL", window_start=1717421400000000000):
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": ticker,
                "volume": 10,
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 0.5,
                "window_start": window_start,
                "transactions": 1,
            }
        )


def _write_many(source, tickers, window_start=1717421400000000000):
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        for ticker in tickers:
            writer.writerow(
                {
                    "ticker": ticker,
                    "volume": 10,
                    "open": 1,
                    "close": 2,
                    "high": 3,
                    "low": 0.5,
                    "window_start": window_start,
                    "transactions": 1,
                }
            )


def test_publish_dates_writes_every_timeframe(tmp_path):
    source = tmp_path / "day.csv.gz"
    _write_flatfile(source)
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
    assert publish_dates(store, state, [], tmp_path / "bronze") == {
        "tickers": 0,
        "rows_1m": 0,
        "resumed": 0,
        "quarantined": [],
    }

    source = tmp_path / "day.csv.gz"
    _write_flatfile(source)
    day = date(2024, 6, 3)
    store.stage_gzip(day, source)
    stats = publish_dates(store, state, [day], tmp_path / "bronze", replace_complete=True, scope="history")
    assert stats == {"tickers": 1, "rows_1m": 1, "resumed": 0, "quarantined": []}

    # Re-run of a completed scope: the bucket is already done, so `resumed`
    # is what tells the caller not to read 0 published as under-publishing.
    again = publish_dates(store, state, [day], tmp_path / "bronze", scope="history")
    assert again["tickers"] == 0
    assert again["resumed"] > 0

    partial = MassiveFlatfileState(tmp_path / "partial")
    partial.mark_ticker_completed("partial", 0, "AAPL")
    resumed_ticker = publish_dates(store, partial, [day], tmp_path / "other", scope="partial")
    assert resumed_ticker["tickers"] == 0
    assert resumed_ticker["resumed"] == 1


def test_publish_dates_recovers_corrupt_derived_timeframe_from_1m(tmp_path):
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    bronze = tmp_path / "bronze"
    day1 = date(2024, 6, 3)
    day2 = date(2024, 6, 4)

    source1 = tmp_path / "day1.csv.gz"
    source2 = tmp_path / "day2.csv.gz"
    _write_flatfile(source1, window_start=1717421400000000000)
    _write_flatfile(source2, window_start=1717507800000000000)
    store.stage_gzip(day1, source1)
    publish_dates(store, MassiveFlatfileState(tmp_path / "cursors1"), [day1], bronze, scope="seed")

    corrupt = bronze / "symbol=AAPL" / "1h.parquet"
    corrupt.write_bytes(b"PAR1" + b"\x00" * 128)

    store.stage_gzip(day2, source2)
    stats = publish_dates(store, MassiveFlatfileState(tmp_path / "cursors2"), [day2], bronze, scope="catchup")

    assert stats["tickers"] == 1
    one_hour = pq.ParquetFile(corrupt)
    assert one_hour.metadata.num_rows == 2


def test_corrupt_1m_parquet_quarantines_the_symbol_and_run_continues(tmp_path):
    """One truncated file used to abort the entire whole-market publish.

    A corrupt NULG/1m.parquet (written 2026-07-11) failed every nightly equity
    intraday run from 2026-07-14 onward — all ~12K symbols lost, silently.
    """
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    bronze = tmp_path / "bronze"
    day = date(2024, 6, 3)
    source = tmp_path / "day.csv.gz"
    _write_many(source, ["AAPL", "NULG"])
    store.stage_gzip(day, source)
    state = MassiveFlatfileState(tmp_path / "cursors")

    # Truncated parquet: valid magic at the head, no footer.
    bad_dir = bronze / "symbol=NULG"
    bad_dir.mkdir(parents=True)
    (bad_dir / "1m.parquet").write_bytes(b"PAR1" + b"\x00" * 512)

    stats = publish_dates(store, state, [day], bronze, scope="s1")

    assert stats["quarantined"] == ["NULG"]
    # The healthy symbol still published — the run was not aborted.
    assert stats["tickers"] == 1
    assert (bronze / "symbol=AAPL" / "1m.parquet").exists()
    # The bad file was moved aside, not left to fail again tomorrow.
    assert not (bad_dir / "1m.parquet").exists()
    quarantined = list((tmp_path / "quarantine").glob("*/symbol=NULG/1m.parquet"))
    assert len(quarantined) == 1
