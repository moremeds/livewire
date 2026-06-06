"""Publish bucketed Massive raw data into canonical per-symbol bronze."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from clients.symbol_ids import stable_symbol_id
from clients.timeframe_aggregator import aggregate_bars

DERIVED_TIMEFRAMES = ("5m", "30m", "1h")


def _bronze_rows(ticker: str, rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    for source in rows:
        row = dict(source)
        row.pop("ticker")
        row["symbol_id"] = stable_symbol_id(ticker)
        result.append(row)
    return result


def publish_dates(
    store: MassiveFlatfileStore,
    state: MassiveFlatfileState,
    days: list[date],
    bronze_dir: Path,
    *,
    replace_complete: bool = False,
    scope: str | None = None,
) -> dict[str, int]:
    if not days:
        return {"tickers": 0, "rows_1m": 0}
    scope = scope or f"{days[0].isoformat()}_{days[-1].isoformat()}_{len(days)}"
    published = 0
    rows_written = 0
    for bucket in sorted(store.available_buckets(days)):
        if state.bucket_completed(scope, bucket):
            continue
        state.record("bucket_started", scope=scope, bucket=bucket)
        for ticker, raw_rows in store.scan_bucket_by_ticker(bucket, days):
            if state.ticker_completed(scope, bucket, ticker):
                continue
            state.record("ticker_started", scope=scope, bucket=bucket, ticker=ticker)
            rows = _bronze_rows(ticker, raw_rows)
            one_minute = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="1m")
            if replace_complete:
                rows_written += one_minute.replace_ticker_rows(ticker, rows)
            else:
                rows_written += one_minute.merge_ticker_rows(ticker, rows, overwrite_existing=True)
            complete_one_minute = rows if replace_complete else one_minute.read_symbol_rows(ticker)
            for timeframe in DERIVED_TIMEFRAMES:
                derived = aggregate_bars(complete_one_minute, source_tf="1m", target_tf=timeframe)
                IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=timeframe).replace_ticker_rows(ticker, derived)
            published += 1
            state.mark_ticker_completed(scope, bucket, ticker)
        state.mark_bucket_completed(scope, bucket)
    return {"tickers": published, "rows_1m": rows_written}
