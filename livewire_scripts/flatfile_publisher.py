"""Publish bucketed Massive raw data into canonical per-symbol bronze."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pyarrow as pa

from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from clients.symbol_ids import stable_symbol_id
from clients.timeframe_aggregator import aggregate_bars

DERIVED_TIMEFRAMES = ("5m", "30m", "1h")
log = logging.getLogger(__name__)


def _bronze_rows(ticker: str, rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    for source in rows:
        row = dict(source)
        row.pop("ticker")
        row["symbol_id"] = stable_symbol_id(ticker)
        result.append(row)
    return result


def _merge_or_rebuild_derived(
    symbol: str,
    timeframe: str,
    derived_client: IntradayBronzeClient,
    one_minute_client: IntradayBronzeClient,
    rows: list[dict],
) -> None:
    try:
        derived_client.merge_ticker_rows(symbol, rows, overwrite_existing=True)
    except (OSError, pa.ArrowInvalid) as exc:
        log_path = derived_client.bronze_dir / f"symbol={symbol}" / f"{timeframe}.parquet"

        log.warning(
            "%s: rebuilding corrupt %s from 1m snapshot after read failure: %s",
            symbol,
            log_path,
            exc,
        )
        one_minute_rows = one_minute_client.read_symbol_rows(symbol)
        rebuilt = aggregate_bars(one_minute_rows, source_tf="1m", target_tf=timeframe)
        derived_client.replace_ticker_rows(symbol, rebuilt)


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
            # Aggregation windows (5m/30m/1h) are anchored on calendar-day boundaries
            # (see timeframe_aggregator._window_start), so a single trading day's 1m bars
            # only produce that day's derived windows — never crossing into a neighbor day.
            # That means we can aggregate from JUST the new rows and merge the result into
            # the derived parquet, instead of re-reading the full 5-year 1m history and
            # re-aggregating it from scratch on every incremental publish.
            if replace_complete:
                # Backfill: `rows` IS the complete history for this scope. Replace 1m, then
                # aggregate-and-replace each derived timeframe from the same complete set.
                local_rows += one_minute.replace_ticker_rows(ticker, rows)
                for timeframe in DERIVED_TIMEFRAMES:
                    derived = aggregate_bars(rows, source_tf="1m", target_tf=timeframe)
                    derived_clients[timeframe].replace_ticker_rows(ticker, derived)
            else:
                # Catch-up / repair: `rows` is only the new days. Merge into 1m, then
                # aggregate just the new bars and merge into each derived parquet —
                # overwrite_existing=True replaces any overlapping window timestamps.
                local_rows += one_minute.merge_ticker_rows(ticker, rows, overwrite_existing=True)
                for timeframe in DERIVED_TIMEFRAMES:
                    derived = aggregate_bars(rows, source_tf="1m", target_tf=timeframe)
                    _merge_or_rebuild_derived(
                        ticker,
                        timeframe,
                        derived_clients[timeframe],
                        one_minute,
                        derived,
                    )
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
