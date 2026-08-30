"""Small crash-safe append-only Parquet log primitive."""

from __future__ import annotations

import fcntl
import os
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


@contextmanager
def _lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    with path.open("a", encoding="utf-8") as handle:
        os.chmod(path, 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class AtomicParquetLog:
    """Serialize compare-and-append with a persistent exact-path lock."""

    def __init__(self, path: Path, schema: pa.Schema) -> None:
        self.path = path
        self.schema = schema

    def read(self) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        table = pq.ParquetFile(self.path).read()
        if table.schema != self.schema:
            raise ValueError(f"{self.path}: schema mismatch")
        return table.to_pylist()

    def append(
        self,
        row: dict[str, object],
        *,
        key: str,
        validate: Callable[[list[dict[str, object]], dict[str, object]], None],
    ) -> bool:
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        with _lock(lock_path):
            rows = self.read()
            existing = [item for item in rows if item[key] == row[key]]
            if existing:
                if existing != [row]:
                    raise ValueError(f"{key} already exists with different content")
                return False
            validate(rows, row)
            self._publish([*rows, row])
            return True

    def _publish(self, rows: list[dict[str, object]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.path.parent, 0o700)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            table = pa.Table.from_pylist(rows, schema=self.schema)
            pq.write_table(table, temporary, compression="zstd", compression_level=3)
            os.chmod(temporary, 0o600)
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
            written = pq.ParquetFile(temporary).read()
            if written.schema != self.schema or written.num_rows != len(rows):
                raise ValueError("append-only Parquet validation failed")
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
            _fsync_directory(self.path.parent)
        finally:
            temporary.unlink(missing_ok=True)
