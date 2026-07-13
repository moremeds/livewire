"""Audit and repair bronze daily rows from staged Massive day aggregates."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from clients.bronze_client import BronzeClient
from clients.massive_daily_flatfile_store import MassiveDailyFlatfileStore
from clients.symbol_ids import stable_symbol_id
from clients.symbol_paths import encode_symbol

_VALUE_FIELDS = ("open", "high", "low", "close", "adj_close", "volume")


@dataclass(frozen=True)
class DailyMismatch:
    ticker: str
    trade_date: str
    kind: str
    changed_fields: str
    bronze_open: float | None
    bronze_high: float | None
    bronze_low: float | None
    bronze_close: float | None
    bronze_adj_close: float | None
    bronze_volume: int | None
    raw_open: float
    raw_high: float
    raw_low: float
    raw_close: float
    raw_volume: int


@dataclass(frozen=True)
class AuditStats:
    bronze_tickers: int = 0
    raw_only_tickers: int = 0
    rows_compared: int = 0
    mismatches: int = 0


_MANIFEST_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("trade_date", pa.string()),
        ("kind", pa.string()),
        ("changed_fields", pa.string()),
        ("bronze_open", pa.float64()),
        ("bronze_high", pa.float64()),
        ("bronze_low", pa.float64()),
        ("bronze_close", pa.float64()),
        ("bronze_adj_close", pa.float64()),
        ("bronze_volume", pa.int64()),
        ("raw_open", pa.float64()),
        ("raw_high", pa.float64()),
        ("raw_low", pa.float64()),
        ("raw_close", pa.float64()),
        ("raw_volume", pa.int64()),
    ]
)


def audit_bucket(
    store: MassiveDailyFlatfileStore,
    bronze_dir: Path,
    existing_symbols: set[str],
    bucket: int,
    days: list[date],
) -> tuple[list[DailyMismatch], AuditStats]:
    """Compare one raw hash bucket with existing canonical bronze snapshots."""
    mismatches: list[DailyMismatch] = []
    bronze_tickers = 0
    raw_only_tickers = 0
    rows_compared = 0
    bronze_client = BronzeClient(bronze_dir=bronze_dir, asset_class="equity")
    for ticker, raw_rows in store.scan_bucket_by_ticker(bucket, days):
        if ticker not in existing_symbols:
            raw_only_tickers += 1
            continue
        bronze_tickers += 1
        bronze_path = bronze_dir / f"symbol={encode_symbol(ticker)}" / "1d.parquet"
        if not bronze_path.exists():
            raise FileNotFoundError(f"bronze snapshot disappeared during audit: {bronze_path}")
        bronze_rows = {row["trade_date"]: row for row in bronze_client.read_symbol_rows(ticker)}

        for raw in raw_rows:
            rows_compared += 1
            trade_date = str(raw["trade_date"])
            bronze = bronze_rows.get(trade_date)
            expected = {
                "open": float(raw["open"]),
                "high": float(raw["high"]),
                "low": float(raw["low"]),
                "close": float(raw["close"]),
                "adj_close": float(raw["close"]),
                "volume": int(raw["volume"]),
            }
            changed = _changed_fields(bronze, expected)
            if not changed:
                continue
            mismatches.append(
                DailyMismatch(
                    ticker=ticker,
                    trade_date=trade_date,
                    kind="missing" if bronze is None else "values",
                    changed_fields=",".join(changed),
                    bronze_open=_optional_float(bronze, "open"),
                    bronze_high=_optional_float(bronze, "high"),
                    bronze_low=_optional_float(bronze, "low"),
                    bronze_close=_optional_float(bronze, "close"),
                    bronze_adj_close=_optional_float(bronze, "adj_close"),
                    bronze_volume=_optional_int(bronze, "volume"),
                    raw_open=expected["open"],
                    raw_high=expected["high"],
                    raw_low=expected["low"],
                    raw_close=expected["close"],
                    raw_volume=expected["volume"],
                )
            )

    return mismatches, AuditStats(
        bronze_tickers=bronze_tickers,
        raw_only_tickers=raw_only_tickers,
        rows_compared=rows_compared,
        mismatches=len(mismatches),
    )


def staged_days(store: MassiveDailyFlatfileStore) -> list[date]:
    """Return complete staged dates without recursively scanning the raw lake."""
    return sorted(
        date.fromisoformat(path.parent.name.removeprefix("date=")) for path in store.raw_root.glob("date=*/_SUCCESS")
    )


def audit_to_directory(
    store: MassiveDailyFlatfileStore,
    bronze_dir: Path,
    output_dir: Path,
    *,
    days: list[date] | None = None,
    on_bucket: Callable[[int, AuditStats], None] | None = None,
) -> dict[str, int | str]:
    """Audit every staged bucket and checkpoint rollback-capable manifests."""
    selected_days = days if days is not None else staged_days(store)
    if not selected_days:
        raise ValueError("no complete staged daily aggregate dates found")
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_symbols = BronzeClient(bronze_dir=bronze_dir, asset_class="equity").get_existing_symbols()
    totals = AuditStats()
    for bucket in range(store.bucket_count):
        mismatches, stats = audit_bucket(store, bronze_dir, existing_symbols, bucket, selected_days)
        write_manifest(output_dir / f"bucket={bucket:03d}.parquet", mismatches)
        if on_bucket is not None:
            on_bucket(bucket, stats)
        totals = AuditStats(
            bronze_tickers=totals.bronze_tickers + stats.bronze_tickers,
            raw_only_tickers=totals.raw_only_tickers + stats.raw_only_tickers,
            rows_compared=totals.rows_compared + stats.rows_compared,
            mismatches=totals.mismatches + stats.mismatches,
        )
    summary: dict[str, int | str] = {
        **asdict(totals),
        "existing_symbols": len(existing_symbols),
        "staged_days": len(selected_days),
        "first_staged_date": selected_days[0].isoformat(),
        "last_staged_date": selected_days[-1].isoformat(),
    }
    _write_json(output_dir / "summary.json", summary)
    return summary


def apply_manifest_directory(bronze_dir: Path, manifest_dir: Path) -> dict[str, int]:
    """Apply checkpointed manifests one bucket at a time."""
    tickers = 0
    rows = 0
    for path in sorted(manifest_dir.glob("bucket=*.parquet")):
        result = apply_mismatches(bronze_dir, read_manifest(path))
        tickers += result["tickers"]
        rows += result["rows"]
    return {"tickers": tickers, "rows": rows}


def write_manifest(path: Path, mismatches: list[DailyMismatch]) -> None:
    """Atomically write the rollback-capable mismatch manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [asdict(item) for item in sorted(mismatches, key=lambda item: (item.ticker, item.trade_date))]
    table = pa.Table.from_pylist(rows, schema=_MANIFEST_SCHEMA)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, temp, compression="zstd", compression_level=3)
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()


def read_manifest(path: Path) -> list[DailyMismatch]:
    return [DailyMismatch(**row) for row in pq.read_table(path, schema=_MANIFEST_SCHEMA).to_pylist()]


def apply_mismatches(bronze_dir: Path, mismatches: list[DailyMismatch]) -> dict[str, int]:
    """Overwrite only manifest keys with their authoritative raw values."""
    by_ticker: dict[str, list[dict]] = {}
    for item in mismatches:
        by_ticker.setdefault(item.ticker, []).append(
            {
                "trade_date": item.trade_date,
                "symbol_id": stable_symbol_id(item.ticker),
                "open": item.raw_open,
                "high": item.raw_high,
                "low": item.raw_low,
                "close": item.raw_close,
                "adj_close": item.raw_close,
                "volume": item.raw_volume,
                "source": "massive",
                "price_basis": "raw",
            }
        )
    bronze = BronzeClient(bronze_dir=bronze_dir, asset_class="equity")
    rows = 0
    for ticker in sorted(by_ticker):
        bronze.merge_ticker_rows(ticker, by_ticker[ticker])
        rows += len(by_ticker[ticker])
    return {"tickers": len(by_ticker), "rows": rows}


def _changed_fields(bronze: dict | None, expected: dict) -> list[str]:
    if bronze is None:
        return list(_VALUE_FIELDS)
    return [field for field in _VALUE_FIELDS if bronze[field] != expected[field]]


def _optional_float(row: dict | None, field: str) -> float | None:
    return None if row is None else float(row[field])


def _optional_int(row: dict | None, field: str) -> int | None:
    return None if row is None else int(row[field])


def _write_json(path: Path, payload: dict[str, int | str]) -> None:
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()
