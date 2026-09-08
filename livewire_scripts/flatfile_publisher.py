"""Publish bucketed Massive raw data into canonical per-symbol bronze."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TypedDict

import pyarrow as pa
import pyarrow.parquet as pq

from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from clients.parquet_io import fsync_directory, symbol_lock
from clients.symbol_ids import stable_symbol_id
from clients.symbol_paths import canonical_symbol, encode_symbol
from clients.timeframe_aggregator import aggregate_bars

DERIVED_TIMEFRAMES = ("5m", "30m", "1h")
log = logging.getLogger(__name__)


class PublishStats(TypedDict):
    """What one publish run covered.

    `resumed` counts *tickers* skipped as already complete; those were published
    by an earlier run of the same scope, so a coverage check may count them as
    covered. `resumed_buckets` counts whole buckets skipped — their tickers were
    never enumerated this run, so they are the only genuinely unmeasurable part
    of the window, and `buckets` gives the denominator to size that against.

    These were one field. Conflating them meant a single already-complete bucket
    out of 256 read as "this is a resumed run" and disabled the coverage check
    entirely — which is nearly every nightly catch-up, since catch-up reuses the
    scope string.

    `quarantined` holds symbols whose parquet was unreadable and was moved
    aside; each needs a targeted backfill.
    `failed` names incomplete symbols or `bucket:<n>` when raw iteration or a
    bucket operation failed before every symbol could be enumerated.
    """

    tickers: int
    rows_1m: int
    resumed: int
    resumed_buckets: int
    buckets: int
    quarantined: list[str]
    failed: list[str]


def _bronze_rows(ticker: str, rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    for source in rows:
        row = dict(source)
        row.pop("ticker")
        row["symbol_id"] = stable_symbol_id(ticker)
        result.append(row)
    return result


def quarantine_corrupt_parquet(path: Path) -> Path | None:
    """Move an unreadable parquet aside so the run can continue past it.

    A corrupt 1m file cannot be rebuilt from a sibling — it IS the source, and
    the publisher only holds the current window, so rewriting it here would
    silently truncate that symbol's history. Moving it aside makes the loss
    explicit and recoverable by a targeted backfill.
    """
    with symbol_lock(path):
        if not path.exists():
            return None
        try:
            pq.ParquetFile(path).read()
        except (OSError, pa.ArrowInvalid):
            pass
        else:
            # The failed operation may have been a write failure, or another
            # writer may already have repaired it. Never quarantine valid data.
            return None
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        target_dir = path.parent.parent.parent / "quarantine" / stamp / path.parent.name
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / path.name
            path.replace(target)
            fsync_directory(path.parent)
            fsync_directory(target_dir)
        except OSError as exc:  # pragma: no cover - last-resort path
            log.error("could not quarantine %s: %s", path, exc)
            return None
        return target


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
        source_path = one_minute_client.bronze_dir / f"symbol={encode_symbol(canonical_symbol(symbol))}" / "1m.parquet"
        with symbol_lock(source_path):
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
) -> PublishStats:
    if not days:
        return {
            "tickers": 0,
            "rows_1m": 0,
            "resumed": 0,
            "resumed_buckets": 0,
            "buckets": 0,
            "quarantined": [],
            "failed": [],
        }
    scope = scope or f"{days[0].isoformat()}_{days[-1].isoformat()}_{len(days)}"
    totals: PublishStats = {
        "tickers": 0,
        "rows_1m": 0,
        "resumed": 0,
        "resumed_buckets": 0,
        "buckets": 0,
        "quarantined": [],
        "failed": [],
    }
    totals_lock = threading.Lock()

    def _process_bucket(bucket: int) -> None:
        local_published = 0
        local_rows = 0
        local_resumed = 0
        try:
            if state.bucket_completed(scope, bucket):
                with totals_lock:
                    totals["resumed_buckets"] += 1
                return
            state.record("bucket_started", scope=scope, bucket=bucket)
            local_failed = False
            # Hoist client creation per-bucket; each worker thread gets its own instances.
            one_minute = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="1m")
            derived_clients = {
                tf: IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=tf) for tf in DERIVED_TIMEFRAMES
            }
            for ticker, raw_rows in store.scan_bucket_by_ticker(bucket, days):
                if state.ticker_completed(scope, bucket, ticker):
                    local_resumed += 1
                    continue
                state.record("ticker_started", scope=scope, bucket=bucket, ticker=ticker)
                # Aggregation windows (5m/30m/1h) are anchored on calendar-day boundaries
                # (see timeframe_aggregator._window_start), so a single trading day's 1m bars
                # only produce that day's derived windows — never crossing into a neighbor day.
                # That means we can aggregate from JUST the new rows and merge the result into
                # the derived parquet, instead of re-reading the full 5-year 1m history and
                # re-aggregating it from scratch on every incremental publish.
                stage = "input"
                try:
                    rows = _bronze_rows(ticker, raw_rows)
                    stage = "1m"
                    if replace_complete:
                        local_rows += one_minute.replace_ticker_rows(ticker, rows)
                    else:
                        local_rows += one_minute.merge_ticker_rows(ticker, rows, overwrite_existing=True)
                    for timeframe in DERIVED_TIMEFRAMES:
                        stage = timeframe
                        derived = aggregate_bars(rows, source_tf="1m", target_tf=timeframe)
                        if replace_complete:
                            derived_clients[timeframe].replace_ticker_rows(ticker, derived)
                        else:
                            _merge_or_rebuild_derived(
                                ticker, timeframe, derived_clients[timeframe], one_minute, derived
                            )
                except (OSError, pa.ArrowInvalid, ValueError, TypeError, KeyError) as exc:
                    moved = None
                    if stage == "1m":
                        moved = quarantine_corrupt_parquet(
                            one_minute.bronze_dir / f"symbol={encode_symbol(canonical_symbol(ticker))}" / "1m.parquet"
                        )
                    log.error(
                        "%s/%s: publish incomplete; quarantine=%s; readable files retained; "
                        "later symbols continue. Inspect error and retry same scope; "
                        "quarantined 1m requires history backfill. Clears after all timeframes publish: %s",
                        ticker,
                        stage,
                        moved,
                        exc,
                    )
                    with totals_lock:
                        totals["quarantined" if moved is not None else "failed"].append(ticker)
                    local_failed = True
                    continue
                local_published += 1
                state.mark_ticker_completed(scope, bucket, ticker)
            if not local_failed:
                state.mark_bucket_completed(scope, bucket)
        except Exception as exc:  # noqa: BLE001 - one unreadable raw bucket must not stop independent buckets
            with totals_lock:
                totals["failed"].append(f"bucket:{bucket}")
            log.error(
                "bucket:%s: publish incomplete for scope=%s; completed ticker writes retained; "
                "later buckets continue. Inspect raw bucket and retry same scope; "
                "clears when this bucket completes: %s",
                bucket,
                scope,
                exc,
            )
            try:
                state.record("bucket_failed", scope=scope, bucket=bucket, error=f"{type(exc).__name__}: {exc}")
            except Exception as record_error:  # noqa: BLE001 - failure evidence cannot abort healthy buckets
                log.error("bucket:%s: could not record failure evidence: %s", bucket, record_error)
        finally:
            with totals_lock:
                totals["tickers"] += local_published
                totals["rows_1m"] += local_rows
                totals["resumed"] += local_resumed

    buckets = sorted(store.available_buckets(days))
    totals["buckets"] = len(buckets)

    if workers <= 1:
        for bucket in buckets:
            _process_bucket(bucket)
        return totals

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="flatfile-pub") as pool:
        futures = [pool.submit(_process_bucket, bucket) for bucket in buckets]
        for fut in as_completed(futures):
            fut.result()
    return totals
