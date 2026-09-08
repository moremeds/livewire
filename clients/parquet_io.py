"""Shared parquet publish and validation helpers.

Used by both BronzeClient (daily) and IntradayBronzeClient. The publish
function writes to a temp file, validates it, then atomically renames into
place. Validation checks row count, sort order, and duplicates on the
specified sort column.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
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


def fsync_directory(path: Path) -> None:
    """fsync a directory so a rename inside it survives a crash.

    One directory fsync per commit is the rule the sharded CAS was rebuilt
    around (pm:2026-09-05-source-evidence-flat-exfat-directory); four copies of
    this function is four chances to forget it.
    """
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_atomic(path: Path, payload: object) -> None:
    """Publish JSON the way the lake publishes parquet: temp -> fsync -> replace.

    The temp file is a sibling of the destination so `os.replace` stays within
    one filesystem; `.resolve()` is deliberately not called, because the lake
    root is a directory of symlinks into the exFAT volume.
    """
    destination = Path(path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        fsync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def path_lock(lock_path: Path, *, blocking: bool = True, shared: bool = False) -> Iterator[bool]:
    """Hold a POSIX flock on `lock_path`, creating it and its parent on demand.

    Yields True while the lock is held; yields False, having taken nothing, when
    `blocking=False` and another open file description holds it. flock is
    released with the fd -- SIGKILL included -- so there is no cleanup path to
    write and none to get wrong.

    Shared lock semantics let snapshot readers briefly exclude only the source
    writers they need. Keep all local coordination on this primitive so writers
    agree on the same lock domain.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    flags = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
    if not blocking:
        flags |= fcntl.LOCK_NB
    with lock_path.open("a", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), flags)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextmanager
def silver_input_lock(bronze_root: Path) -> Iterator[None]:
    """Freeze equity daily and action inputs only while copying a snapshot."""
    with ExitStack() as locks:
        for asset in ("corporate_action", "equity"):
            locks.enter_context(path_lock(bronze_root / f"asset_class={asset}" / ".inputs.lock"))
        yield


@contextmanager
def symbol_directory_lock(directory: Path, *, shared: bool = False) -> Iterator[bool]:
    """Keep the lock outside a partition that an archive operation may move."""
    with path_lock(directory.parent / ".symbol-locks" / f"{directory.name}.lock", shared=shared) as held:
        yield held


@contextmanager
def symbol_lock(parquet_path: Path, *, directory_exclusive: bool = False) -> Iterator[Path]:
    """Serialize writers for one parquet path using a local POSIX lock.

    The persistent sidecar is intentionally kept beside the parquet so separate
    Livewire processes resolve the same lock. This requires a local filesystem
    with working ``flock`` semantics; verified on the production exFAT data lake.
    """
    lock_path = parquet_path.with_suffix(parquet_path.suffix + ".lock")
    with ExitStack() as locks:
        asset_root = parquet_path.parent.parent
        if asset_root.parent.name == "bronze" and (
            (asset_root.name == "asset_class=equity" and parquet_path.name == "1d.parquet")
            or (asset_root.name == "asset_class=corporate_action" and parquet_path.name == "events.parquet")
        ):
            locks.enter_context(path_lock(asset_root / ".inputs.lock", shared=True))
        locks.enter_context(symbol_directory_lock(parquet_path.parent, shared=not directory_exclusive))
        locks.enter_context(path_lock(lock_path))
        yield lock_path


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
        with tmp_path.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp_path, out_path)
        fsync_directory(out_path.parent)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return out_path


def restore_parquet_exact(backup: Path, target: Path, expected_sha256: str) -> None:
    """Restore validated daily Parquet bytes; caller holds the target symbol lock."""
    payload = backup.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("backup checksum mismatch: refusing to restore")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.{time.time_ns()}.restore.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        validate_parquet_file(temporary, pq.read_metadata(temporary).num_rows, "trade_date")
        os.replace(temporary, target)
        fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


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

    # Decode every column before replacing the last valid file. A valid footer
    # and date column do not establish that the price/volume pages are readable.
    table = pq.ParquetFile(path).read()
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
