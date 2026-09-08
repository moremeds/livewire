import csv
import gzip
import json
from datetime import date

import pyarrow.parquet as pq
import pytest

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


def test_invalid_input_symbol_does_not_skip_later_symbols(tmp_path, monkeypatch):
    from livewire_scripts import flatfile_publisher

    source = tmp_path / "day.csv.gz"
    _write_many(source, ["AAPL", "MSFT"])
    day = date(2024, 6, 3)
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    store.stage_gzip(day, source)
    original = flatfile_publisher._bronze_rows

    def bad_input(symbol, rows):
        if symbol == "AAPL":
            raise TypeError("invalid input volume")
        return original(symbol, rows)

    monkeypatch.setattr(flatfile_publisher, "_bronze_rows", bad_input)
    state = MassiveFlatfileState(tmp_path / "cursors")
    stats = publish_dates(store, state, [day], tmp_path / "bronze", scope="bad-input")
    assert stats["failed"] == ["AAPL"] and stats["quarantined"] == []
    assert stats["tickers"] == 1
    assert not state.bucket_completed("bad-input", 0)
    assert IntradayBronzeClient(tmp_path / "bronze", "1m").get_existing_symbols() == {"MSFT"}


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
        "resumed_buckets": 0,
        "buckets": 0,
        "quarantined": [],
        "failed": [],
    }

    source = tmp_path / "day.csv.gz"
    _write_flatfile(source)
    day = date(2024, 6, 3)
    store.stage_gzip(day, source)
    stats = publish_dates(store, state, [day], tmp_path / "bronze", replace_complete=True, scope="history")
    assert stats == {
        "tickers": 1,
        "rows_1m": 1,
        "resumed": 0,
        "resumed_buckets": 0,
        "buckets": 1,
        "quarantined": [],
        "failed": [],
    }

    # Re-run of a completed scope: the whole bucket is skipped. That is counted
    # separately from resumed *tickers* — a skipped bucket's tickers were never
    # enumerated, so it is the only part of the window a coverage check cannot
    # measure, and it must not be conflated with tickers already published.
    again = publish_dates(store, state, [day], tmp_path / "bronze", scope="history")
    assert again["tickers"] == 0
    assert again["resumed_buckets"] == again["buckets"] > 0
    assert again["resumed"] == 0

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
    assert not state.bucket_completed("s1", 0)


@pytest.mark.parametrize("replace_complete", [False, True])
def test_failed_derived_publish_keeps_bucket_retryable_and_later_symbol_runs(tmp_path, monkeypatch, replace_complete):
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    bronze = tmp_path / "bronze"
    day = date(2024, 6, 3)
    source = tmp_path / "day.csv.gz"
    _write_many(source, ["AAPL", "MSFT"])
    store.stage_gzip(day, source)
    state = MassiveFlatfileState(tmp_path / "cursors")
    original = IntradayBronzeClient.replace_ticker_rows
    original_merge = IntradayBronzeClient.merge_ticker_rows

    def replace(self, ticker, rows, **kwargs):
        if ticker == "AAPL" and self.timeframe == "5m":
            raise OSError("injected write failure")
        return original(self, ticker, rows, **kwargs)

    def merge(self, ticker, rows, **kwargs):
        if ticker == "AAPL" and self.timeframe == "5m":
            raise OSError("injected write failure")
        return original_merge(self, ticker, rows, **kwargs)

    monkeypatch.setattr(IntradayBronzeClient, "replace_ticker_rows", replace)
    monkeypatch.setattr(IntradayBronzeClient, "merge_ticker_rows", merge)
    stats = publish_dates(store, state, [day], bronze, scope="retry", replace_complete=replace_complete)
    assert stats["failed"] == ["AAPL"]
    assert stats["quarantined"] == []
    assert stats["tickers"] == 1
    assert state.ticker_completed("retry", 0, "MSFT")
    assert not state.bucket_completed("retry", 0)
    assert (bronze / "symbol=AAPL/1m.parquet").exists()

    monkeypatch.setattr(IntradayBronzeClient, "replace_ticker_rows", original)
    monkeypatch.setattr(IntradayBronzeClient, "merge_ticker_rows", original_merge)
    retry = publish_dates(store, state, [day], bronze, scope="retry", replace_complete=replace_complete)
    assert retry["tickers"] == 1
    assert retry["resumed"] == 1
    assert state.bucket_completed("retry", 0)


@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("fail_after_ticker", [False, True])
def test_bad_bucket_keeps_later_bucket_and_partial_progress_retryable(
    tmp_path, monkeypatch, workers, fail_after_ticker
):
    store = MassiveFlatfileStore(tmp_path, bucket_count=1)
    source = tmp_path / "day.csv.gz"
    _write_many(source, ["AAPL", "MSFT"])
    day = date(2024, 6, 3)
    store.stage_gzip(day, source)
    rows = dict(store.scan_bucket_by_ticker(0, [day]))
    monkeypatch.setattr(store, "available_buckets", lambda _days: {0, 1})

    def broken_scan(bucket, _days):
        if bucket == 0:
            if fail_after_ticker:
                yield "AAPL", rows["AAPL"]
            raise OSError("raw bucket tail unreadable")
        yield "MSFT", rows["MSFT"]

    monkeypatch.setattr(store, "scan_bucket_by_ticker", broken_scan)
    state = MassiveFlatfileState(tmp_path / "cursors")
    bronze = tmp_path / "bronze"
    stats = publish_dates(store, state, [day], bronze, scope="bucket-retry", workers=workers)

    assert stats["failed"] == ["bucket:0"]
    assert stats["tickers"] == stats["rows_1m"] == 1 + int(fail_after_ticker)
    assert stats["buckets"] == 2
    assert not state.bucket_completed("bucket-retry", 0)
    assert state.bucket_completed("bucket-retry", 1)
    assert state.ticker_completed("bucket-retry", 0, "AAPL") is fail_after_ticker
    for tf in ("1m", "5m", "30m", "1h"):
        assert "MSFT" in IntradayBronzeClient(bronze, tf).get_existing_symbols()
    failures = [json.loads(line) for line in state.manifest_path.read_text().splitlines()]
    failure = next(row for row in failures if row["event"] == "bucket_failed")
    assert failure["bucket"] == 0 and failure["scope"] == "bucket-retry"
    assert failure["error"] == "OSError: raw bucket tail unreadable"

    def repaired_scan(bucket, _days):
        ticker = "AAPL" if bucket == 0 else "MSFT"
        yield ticker, rows[ticker]

    monkeypatch.setattr(store, "scan_bucket_by_ticker", repaired_scan)
    # A fresh process resumes the healthy bucket; an incomplete ticker mark
    # may be replayed idempotently if it had not reached a durable state save.
    resumed_state = MassiveFlatfileState(tmp_path / "cursors")
    retry = publish_dates(store, resumed_state, [day], bronze, scope="bucket-retry", workers=workers)
    assert retry["failed"] == []
    assert retry["resumed_buckets"] == 1
    assert retry["tickers"] + retry["resumed"] == 1
    assert resumed_state.bucket_completed("bucket-retry", 0)
    assert resumed_state.bucket_completed("bucket-retry", 1)
    assert IntradayBronzeClient(bronze, "1m").get_existing_symbols() == {"AAPL", "MSFT"}
