"""Publish bucketed Massive raw data into canonical per-symbol bronze."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    workers: int = 1,
) -> dict[str, int]:
    if not days:
        return {"tickers": 0, "rows_1m": 0}
    scope = scope or f"{days[0].isoformat()}_{days[-1].isoformat()}_{len(days)}"
    totals = {"tickers": 0, "rows_1m": 0}
    totals_lock = threading.Lock()

    def _process_bucket(bucket: int) -> None:
        if state.bucket_completed(scope, bucket):
            return
        state.record("bucket_started", scope=scope, bucket=bucket)
        local_published = 0
        local_rows = 0
        # Hoist client creation per-bucket; each worker thread gets its own instances.
        one_minute = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="1m")
        derived_clients = {tf: IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=tf) for tf in DERIVED_TIMEFRAMES}
        for ticker, raw_rows in store.scan_bucket_by_ticker(bucket, days):
            if state.ticker_completed(scope, bucket, ticker):
                continue
            state.record("ticker_started", scope=scope, bucket=bucket, ticker=ticker)
            rows = _bronze_rows(ticker, raw_rows)
            if replace_complete:
                local_rows += one_minute.replace_ticker_rows(ticker, rows)
            else:
                local_rows += one_minute.merge_ticker_rows(ticker, rows, overwrite_existing=True)
            complete_one_minute = rows if replace_complete else one_minute.read_symbol_rows(ticker)
            for timeframe in DERIVED_TIMEFRAMES:
                derived = aggregate_bars(complete_one_minute, source_tf="1m", target_tf=timeframe)
                derived_clients[timeframe].replace_ticker_rows(ticker, derived)
            local_published += 1
            state.mark_ticker_completed(scope, bucket, ticker)
        state.mark_bucket_completed(scope, bucket)
        with totals_lock:
            totals["tickers"] += local_published
            totals["rows_1m"] += local_rows

    buckets = sorted(store.available_buckets(days))

    if workers <= 1:
        for bucket in buckets:
            _process_bucket(bucket)
        return totals

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="flatfile-pub") as pool:
        futures = {pool.submit(_process_bucket, b): b for b in buckets}
        first_exc: Exception | None = None
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                if first_exc is None:
                    first_exc = exc
                    for pending in futures:
                        pending.cancel()
        if first_exc is not None:
            raise first_exc
    return totals
