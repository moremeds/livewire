"""Shared parquet publish and validation helpers.

Used by both BronzeClient (daily) and IntradayBronzeClient. The publish
function writes to a temp file, validates it, then atomically renames into
place. Validation checks row count, sort order, and duplicates on the
specified sort column.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


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
        pq.write_table(table, tmp_path, compression="snappy")
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

    table = pq.read_table(path, columns=[sort_column])
    if table.num_rows != expected_rows:
        raise ValueError(f"{path}: expected {expected_rows} rows, found {table.num_rows}")

    raw_values = table.column(sort_column).to_pylist()
    values = [v.isoformat() if isinstance(v, (date, datetime)) else str(v) for v in raw_values]
    if values != sorted(values):
        raise ValueError(f"{path}: {sort_column} values are not sorted ascending")
    if len(values) != len(set(values)):
        raise ValueError(f"{path}: duplicate {sort_column} values detected")
