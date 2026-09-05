"""Shared parquet publish and validation helpers.

Used by both BronzeClient (daily) and IntradayBronzeClient. The publish
function writes to a temp file, validates it, then atomically renames into
place. Validation checks row count, sort order, and duplicates on the
specified sort column.
"""

from __future__ import annotations

import fcntl
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Compression codec for all bronze parquet writes. zstd level 3 is ~28% smaller
# than snappy on OHLCV bars (measured on real equity 1m/5m/30m/1h files) at
# effectively the same write CPU. It is lossless and transparent to every reader
# (pyarrow / pandas / DuckDB decompress automatically), so the on-disk
# filename and format are unchanged. On the HDD-backed lake, fewer bytes means a
# lighter cold read pass on every subsequent merge-and-rewrite.
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3


@contextmanager
def symbol_lock(parquet_path: Path) -> Iterator[Path]:
    """Serialize writers for one parquet path using a local POSIX lock.

    The persistent sidecar is intentionally kept beside the parquet so separate
    Livewire processes resolve the same lock. This requires a local filesystem
    with working ``flock`` semantics; verified on the production exFAT data lake.
    """
    lock_path = parquet_path.with_suffix(parquet_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield lock_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def publish_parquet(
    out_path: Path,
    table: pa.Table,
    sort_column: str,
) -> Path:
    """Atomically publish a parquet file: write temp -> validate -> rename.

    Raises ValueError on validation failure (row count, sort order, dupes).
    Raises KeyError if sort_column doesn't exist in the table.
    The temp file is always cleaned up.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f".{out_path.name}.{os.getpid()}.{time.time_ns()}.tmp")

    try:
        pq.write_table(
            table,
            tmp_path,
            compression=PARQUET_COMPRESSION,
            compression_level=PARQUET_COMPRESSION_LEVEL,
        )
        validate_parquet_file(tmp_path, expected_rows=table.num_rows, sort_column=sort_column)
        os.replace(tmp_path, out_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return out_path


def validate_parquet_file(
    path: Path,
    expected_rows: int,
    sort_column: str,
) -> None:
    """Validate a parquet file: row count, ascending sort, no duplicates.

    Raises ValueError on row count, sort order, or duplicate failures.
    Raises KeyError if sort_column doesn't exist in the file.
    """
    # First read schema to check column existence
    schema = pq.read_schema(path)
    if sort_column not in schema.names:
        raise KeyError(f"sort column {sort_column!r} not in parquet")

    table = pq.ParquetFile(path).read(columns=[sort_column])
    if table.num_rows != expected_rows:
        raise ValueError(f"{path}: expected {expected_rows} rows, found {table.num_rows}")

    raw_values = table.column(sort_column).to_pylist()
    # Dates become ISO text (string-sortable); everything else keeps its own
    # type. str() on an int sorted "10" before "2", so an 11-row ledger emit
    # was unpublishable and a genuinely unsorted [1, 10, 2] validated clean.
    values = [v.isoformat() if isinstance(v, (date, datetime)) else v for v in raw_values]
    if values != sorted(values):
        raise ValueError(f"{path}: {sort_column} values are not sorted ascending")
    if len(values) != len(set(values)):
        raise ValueError(f"{path}: duplicate {sort_column} values detected")
