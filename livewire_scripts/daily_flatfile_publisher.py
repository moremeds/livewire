"""Publish bucketed Massive daily raw data into canonical per-symbol bronze 1d parquet.

Policy: this lane owns the ~17.5K SIP symbols outside the preset universe and
keeps them current. The preset universe (sp500 ∪ ndx100 ∪ r2k) is owned by the
`daily` command — some of those carry pre-2003 history day_aggs cannot supply —
so those symbols are passed in as `protected_symbols` and skipped here.

Writes MERGE rather than replace. The previous policy skipped every symbol that
already had a 1d.parquet, which meant a symbol this lane created was never
written again: ~17.5K tickers were frozen at whatever 7-day window happened to
be in force the night they first appeared, and the run still exited 0. Replace
was also latently wrong for the same reason — a re-run would truncate a
symbol's history to the current window.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

from clients.bronze_client import BronzeClient
from clients.massive_daily_flatfile_store import MassiveDailyFlatfileStore
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.symbol_ids import stable_symbol_id

log = logging.getLogger(__name__)


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
                "source": "massive",
                "price_basis": "raw",
            }
        )
    return result


def _process_bucket_worker(
    warehouse_dir: str,
    bucket_count: int,
    bronze_dir: str,
    days_iso: list[str],
    bucket: int,
    protected_symbols: frozenset[str],
) -> tuple[int, int, int, list[dict[str, str]]]:
    """ProcessPool entrypoint — each worker re-instantiates its own clients.

    Returns (tickers_written, rows_written, tickers_skipped, failures).
    """
    days = [date.fromisoformat(d) for d in days_iso]
    store = MassiveDailyFlatfileStore(Path(warehouse_dir), bucket_count=bucket_count)
    bronze = BronzeClient(bronze_dir=bronze_dir, asset_class="equity")
    written = 0
    skipped = 0
    rows_written = 0
    failures = []
    try:
        for ticker, raw_rows in store.scan_bucket_by_ticker(bucket, days):
            if ticker in protected_symbols:
                skipped += 1
                continue
            try:
                rows = _bronze_rows(ticker, raw_rows)
                if not rows:
                    continue
                # Merge keeps history outside this catch-up window intact.
                rows_written += bronze.merge_ticker_rows(ticker, rows)
                written += 1
            except (ValueError, OSError, TypeError, KeyError) as exc:
                failures.append({"symbol": ticker, "error": f"{type(exc).__name__}: {exc}"})
    except Exception as exc:
        # A raw-bucket decode/scan failure cannot identify every remaining ticker.
        # Preserve completed work and leave this bucket retryable.
        failures.append({"symbol": f"bucket:{bucket}", "error": f"{type(exc).__name__}: {exc}"})
    return written, rows_written, skipped, failures


def publish_daily_dates(
    store: MassiveDailyFlatfileStore,
    state: MassiveFlatfileState,
    days: list[date],
    bronze_dir: Path,
    *,
    scope: str | None = None,
    workers: int = 1,
    use_processes: bool = True,
    protected_symbols: frozenset[str] | None = None,
) -> dict[str, int]:
    """Publish per-bucket; safe to resume via per-(scope, bucket) state cursor.

    `protected_symbols` is the preset universe owned by the `daily` command;
    those are skipped. Everything else is merged and kept current. Defaults to
    the empty set — callers pass the preset union explicitly. It used to default
    to *every* symbol already on disk, which froze each symbol permanently the
    moment this lane created it.

    Parallelism: process-pool by default since per-bucket work is CPU-bound
    (pyarrow parquet decode + per-ticker writes). Set use_processes=False to
    fall back to threads (e.g. for in-test stubbing).
    """
    if not days:
        return {"tickers": 0, "rows_1d": 0, "skipped_existing": 0, "failed": 0}
    scope = scope or f"daily_{days[0].isoformat()}_{days[-1].isoformat()}_{len(days)}"
    if protected_symbols is None:
        protected_symbols = frozenset()
    totals = {"tickers": 0, "rows_1d": 0, "skipped_existing": 0, "failed": 0}
    totals_lock = threading.Lock()

    buckets = sorted(store.available_buckets(days))
    pending = [b for b in buckets if not state.bucket_completed(scope, b)]

    def _record_start(bucket: int) -> None:
        state.record("bucket_started", scope=scope, bucket=bucket)

    def _record_done(bucket: int, written: int, rows: int, skipped: int, failures: list[dict[str, str]]) -> None:
        with totals_lock:
            totals["tickers"] += written
            totals["rows_1d"] += rows
            totals["skipped_existing"] += skipped
            totals["failed"] += len(failures)
        if failures:
            for failure in failures:
                log.error(
                    "Daily publish failed: bucket=%d symbol=%s error=%s; healthy work continues, bucket retry required",
                    bucket,
                    failure["symbol"],
                    failure["error"],
                )
                state.record("bucket_failed", scope=scope, bucket=bucket, **failure)
        else:
            state.mark_bucket_completed(scope, bucket)

    if workers <= 1:
        for bucket in pending:
            _record_start(bucket)
            try:
                written, rows, skipped, failures = _process_bucket_worker(
                    str(store.warehouse_dir),
                    store.bucket_count,
                    str(bronze_dir),
                    [d.isoformat() for d in days],
                    bucket,
                    protected_symbols,
                )
            except BrokenExecutor:
                raise
            except Exception as exc:
                written, rows, skipped = 0, 0, 0
                failures = [{"symbol": f"bucket:{bucket}", "error": f"{type(exc).__name__}: {exc}"}]
            _record_done(bucket, written, rows, skipped, failures)
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
                protected_symbols,
            )
            futures[fut] = bucket
        for fut in as_completed(futures):
            bucket = futures[fut]
            try:
                written, rows, skipped, failures = fut.result()
            except BrokenExecutor:
                raise  # Pool loss is a lane failure, never a successful partial run.
            except Exception as exc:
                written, rows, skipped = 0, 0, 0
                failures = [{"symbol": f"bucket:{bucket}", "error": f"{type(exc).__name__}: {exc}"}]
            _record_done(bucket, written, rows, skipped, failures)
    return totals
