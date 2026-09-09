"""Serialized, crash-recoverable publication for one Massive raw date directory."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from clients.parquet_io import fsync_directory, path_lock


def _previous_path(final: Path) -> Path:
    return final.with_name(f".old-{final.name}")


@contextmanager
def raw_date_lock(raw_root: Path, day: date, *, shared: bool = False) -> Iterator[None]:
    """Lock one actual raw-date root, independent of its warehouse alias.

    Readers take ``LOCK_SH`` only while opening their Parquet file descriptors;
    publishers and swap recovery take ``LOCK_EX``. Resolving the raw root means
    two configured warehouse aliases pointing at the same physical raw tree
    cannot accidentally publish under different lock paths.
    """
    root = Path(raw_root).expanduser().resolve()
    path = root / ".locks" / f"date={day.isoformat()}.lock"
    with path_lock(path, shared=shared):
        yield


def validate_and_fsync_raw_stage(stage: Path) -> None:
    """Fully decode and durably flush a staged raw date before its swap.

    A Parquet footer is insufficient: a bad page can remain unread until a
    future bucket scan. Every staged column is decoded before the success marker
    is made durable, so a failed candidate never replaces the last publication.

    The staging directory lives on the exFAT lake volume, where macOS writes an
    AppleDouble sidecar ``._x.parquet`` beside every ``x.parquet``. A sidecar is
    not Parquet, so decoding one aborts the whole ingest. Readers glob
    ``bucket=*.parquet``, which can never match a ``._`` name; this validator is
    the one place that has to filter them out itself.
    """
    stage = Path(stage)
    parquet_paths = sorted(path for path in stage.glob("*.parquet") if not path.name.startswith("._"))
    if not parquet_paths:
        raise ValueError(f"{stage}: staged raw date has no parquet files")
    marker = stage / "_SUCCESS"
    if not marker.is_file():
        raise ValueError(f"{stage}: staged raw date has no _SUCCESS marker")
    for path in parquet_paths:
        with path.open("rb") as handle:
            pq.ParquetFile(handle).read()
            os.fsync(handle.fileno())
    with marker.open("rb") as handle:
        os.fsync(handle.fileno())
    fsync_directory(stage)


def recover_raw_date(final: Path) -> None:
    """Restore the last complete directory after an interrupted replace swap."""
    previous = _previous_path(final)
    if final.exists():
        if previous.exists():
            shutil.rmtree(previous)
            fsync_directory(final.parent)
        return
    if previous.exists():
        previous.rename(final)
        fsync_directory(final.parent)


def publish_raw_date(temp: Path, final: Path) -> None:
    """Publish a fully staged date directory while preserving a rollback copy."""
    recover_raw_date(final)
    previous = _previous_path(final)
    if not final.exists():
        temp.rename(final)
        fsync_directory(final.parent)
        return

    final.rename(previous)
    fsync_directory(final.parent)
    try:
        temp.rename(final)
        fsync_directory(final.parent)
    except BaseException:
        # A normal failure restores the last usable directory immediately. A
        # SIGKILL leaves ``previous`` behind, which recover_raw_date restores.
        if not final.exists() and previous.exists():
            previous.rename(final)
            fsync_directory(final.parent)
        raise
    shutil.rmtree(previous)
    fsync_directory(final.parent)
