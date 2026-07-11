from __future__ import annotations

import csv
import gzip
from datetime import date

from clients.bronze_client import BronzeClient
from clients.massive_daily_flatfile_store import MassiveDailyFlatfileStore


def _write_day(path, rows: list[dict]) -> None:
    with gzip.open(path, "wt", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        writer.writerows(rows)


def _raw_row(ticker: str, timestamp_ns: int, close: float, volume: int) -> dict:
    return {
        "ticker": ticker,
        "volume": volume,
        "open": close - 1.0,
        "close": close,
        "high": close + 1.0,
        "low": close - 2.0,
        "window_start": timestamp_ns,
        "transactions": 1,
    }


def _bronze_row(day: str, close: float, volume: int) -> dict:
    return {
        "trade_date": day,
        "symbol_id": 1,
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "adj_close": close,
        "volume": volume,
    }


def test_audit_manifest_and_targeted_repair_round_trip(tmp_path):
    from livewire_scripts.daily_bronze_repair import (
        apply_manifest_directory,
        audit_bucket,
        audit_to_directory,
        read_manifest,
        write_manifest,
    )

    day1 = date(2024, 6, 3)
    day2 = date(2024, 6, 4)
    source1 = tmp_path / "day1.csv.gz"
    source2 = tmp_path / "day2.csv.gz"
    _write_day(
        source1,
        [
            _raw_row("AAPL", 1717387200_000_000_000, 100.0, 1000),
            _raw_row("RAWONLY", 1717387200_000_000_000, 10.0, 50),
        ],
    )
    _write_day(source2, [_raw_row("AAPL", 1717473600_000_000_000, 101.0, 1100)])

    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    store.stage_gzip(day1, source1)
    store.stage_gzip(day2, source2)
    bronze_dir = tmp_path / "bronze"
    bronze = BronzeClient(bronze_dir)
    bronze.replace_ticker_rows(
        "AAPL",
        [
            _bronze_row("2020-01-02", 50.0, 500),
            _bronze_row("2024-06-03", 90.0, 100),
        ],
    )

    mismatches, stats = audit_bucket(store, bronze_dir, {"AAPL"}, 0, [day1, day2])

    assert stats.bronze_tickers == 1
    assert stats.rows_compared == 2
    assert stats.mismatches == 2
    assert stats.raw_only_tickers == 1
    assert [(item.trade_date, item.kind) for item in mismatches] == [
        ("2024-06-03", "values"),
        ("2024-06-04", "missing"),
    ]
    assert mismatches[0].bronze_close == 90.0
    assert mismatches[0].raw_close == 100.0
    assert bronze.read_symbol_rows("AAPL")[-1]["close"] == 90.0

    manifest = tmp_path / "bucket=000.parquet"
    write_manifest(manifest, mismatches)
    loaded = read_manifest(manifest)
    assert loaded == mismatches

    repair = apply_manifest_directory(bronze_dir, tmp_path)
    assert repair == {"tickers": 1, "rows": 2}
    repaired_rows = bronze.read_symbol_rows("AAPL")
    assert [row["trade_date"] for row in repaired_rows] == ["2020-01-02", "2024-06-03", "2024-06-04"]
    assert repaired_rows[-2]["close"] == 100.0
    assert repaired_rows[-1]["volume"] == 1100

    remaining, post_stats = audit_bucket(store, bronze_dir, {"AAPL"}, 0, [day1, day2])
    assert remaining == []
    assert post_stats.mismatches == 0

    audit_dir = tmp_path / "audit"
    summary = audit_to_directory(store, bronze_dir, audit_dir)
    assert summary == {
        "bronze_tickers": 1,
        "existing_symbols": 1,
        "first_staged_date": "2024-06-03",
        "last_staged_date": "2024-06-04",
        "mismatches": 0,
        "raw_only_tickers": 1,
        "rows_compared": 2,
        "staged_days": 2,
    }
    assert read_manifest(audit_dir / "bucket=000.parquet") == []
