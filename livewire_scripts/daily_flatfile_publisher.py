"""Publish bucketed Massive daily raw data into canonical per-symbol bronze 1d parquet.

Policy: only writes per-ticker 1d.parquet for symbols that do NOT already have a
bronze daily snapshot. The existing IB-backed `daily` command owns the preset
universe (sp500 ∪ ndx100 ∪ r2k), some of which carry pre-2003 history that
day_aggs cannot supply. This pipeline strictly widens — it fills the new-symbol
gap (~17.5K tickers per the audit) and leaves established per-ticker files
untouched.
"""

from __future__ import annotations

import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

from clients.bronze_client import BronzeClient
from clients.massive_daily_flatfile_store import MassiveDailyFlatfileStore
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.symbol_ids import stable_symbol_id


def _bronze_rows(ticker: str, rows: list[dict]) -> list[dict]:
    """Translate raw store rows (ticker + OHLCV + trade_date) to bronze 1d row dicts."""
    symbol_id = stable_symbol_id(ticker)
    result: list[dict] = []
    for source in rows:
        result.append(
            {
                "trade_date": source["trade_date"],
                "symbol_id": symbol_id,
                "open": source["open"],
                "high": source["high"],
                "low": source["low"],
                "close": source["close"],
                "adj_close": source["close"],
                "volume": int(source["volume"]),
            }
        )
    return result


def _process_bucket_worker(
    warehouse_dir: str,
    bucket_count: int,
    bronze_dir: str,
    days_iso: list[str],
    bucket: int,
    existing_symbols: frozenset[str],
) -> tuple[int, int, int]:
    """ProcessPool entrypoint — each worker re-instantiates its own clients.

    Returns (tickers_written, rows_written, tickers_skipped).
    """
    days = [date.fromisoformat(d) for d in days_iso]
    store = MassiveDailyFlatfileStore(Path(warehouse_dir), bucket_count=bucket_count)
    bronze = BronzeClient(bronze_dir=bronze_dir, asset_class="equity")
    written = 0
    skipped = 0
    rows_written = 0
    for ticker, raw_rows in store.scan_bucket_by_ticker(bucket, days):
        if ticker in existing_symbols:
            skipped += 1
            continue
        rows = _bronze_rows(ticker, raw_rows)
        if not rows:
            continue
        rows_written += bronze.replace_ticker_rows(ticker, rows)
        written += 1
    return written, rows_written, skipped


def publish_daily_dates(
    store: MassiveDailyFlatfileStore,
    state: MassiveFlatfileState,
    days: list[date],
    bronze_dir: Path,
    *,
    scope: str | None = None,
    workers: int = 1,
    use_processes: bool = True,
    existing_symbols: frozenset[str] | None = None,
) -> dict[str, int]:
    """Publish per-bucket; safe to resume via per-(scope, bucket) state cursor.

    `existing_symbols` is the set of tickers whose per-ticker 1d.parquet already
    exists in `bronze_dir`; those are skipped (the IB-backed `daily` command owns
    them). If None, the set is read once from disk before fan-out so workers see
    a frozen snapshot — no cross-worker race when two buckets touch the same new
    ticker is possible because `scan_bucket_by_ticker` is bucket-scoped (each
    ticker hashes to exactly one bucket).

    Parallelism: process-pool by default since per-bucket work is CPU-bound
    (pyarrow parquet decode + per-ticker writes). Set use_processes=False to
    fall back to threads (e.g. for in-test stubbing).
    """
    if not days:
        return {"tickers": 0, "rows_1d": 0, "skipped_existing": 0}
    scope = scope or f"daily_{days[0].isoformat()}_{days[-1].isoformat()}_{len(days)}"
    if existing_symbols is None:
        existing_symbols = frozenset(BronzeClient(bronze_dir=bronze_dir, asset_class="equity").get_existing_symbols())
    totals = {"tickers": 0, "rows_1d": 0, "skipped_existing": 0}
    totals_lock = threading.Lock()

    buckets = sorted(store.available_buckets(days))
    pending = [b for b in buckets if not state.bucket_completed(scope, b)]

    def _record_start(bucket: int) -> None:
        state.record("bucket_started", scope=scope, bucket=bucket)

    def _record_done(bucket: int, written: int, rows: int, skipped: int) -> None:
        with totals_lock:
            totals["tickers"] += written
            totals["rows_1d"] += rows
            totals["skipped_existing"] += skipped
        state.mark_bucket_completed(scope, bucket)

    if workers <= 1:
        for bucket in pending:
            _record_start(bucket)
            written, rows, skipped = _process_bucket_worker(
                str(store.warehouse_dir),
                store.bucket_count,
                str(bronze_dir),
                [d.isoformat() for d in days],
                bucket,
                existing_symbols,
            )
            _record_done(bucket, written, rows, skipped)
        return totals

    executor_cls = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    days_iso = [d.isoformat() for d in days]
    with executor_cls(max_workers=workers) as pool:
        futures = {}
        for bucket in pending:
            _record_start(bucket)
            fut = pool.submit(
                _process_bucket_worker,
                str(store.warehouse_dir),
                store.bucket_count,
                str(bronze_dir),
                days_iso,
                bucket,
                existing_symbols,
            )
            futures[fut] = bucket
        first_exc: Exception | None = None
        for fut in as_completed(futures):
            bucket = futures[fut]
            try:
                written, rows, skipped = fut.result()
            except Exception as exc:
                if first_exc is None:
                    first_exc = exc
                    for pending_fut in futures:
                        pending_fut.cancel()
                continue
            _record_done(bucket, written, rows, skipped)
        if first_exc is not None:
            raise first_exc
    return totals
