"""Resumable Massive flat-file downloader."""

from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from clients.massive_flatfile_client import FlatfileObjectStatus, MassiveFlatfileClient
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore


@dataclass
class DownloadStats:
    inspected: int = 0
    downloaded: int = 0
    skipped: int = 0
    unavailable: int = 0
    failed: int = 0
    bytes: int = 0
    rows: int = 0
    symbols: int = 0


def download_dates(
    client: MassiveFlatfileClient,
    store: MassiveFlatfileStore,
    state: MassiveFlatfileState,
    dates: list[date],
    *,
    replace: bool = False,
    max_retries: int = 3,
    sleep_fn=time.sleep,
) -> DownloadStats:
    stats = DownloadStats()
    for day in dates:
        if store.has_raw_date(day) and state.raw_completed(day) and not replace:
            stats.skipped += 1
            continue
        info = None
        for attempt in range(max_retries + 1):
            info = client.inspect_date(day)
            stats.inspected += 1
            if info.status != FlatfileObjectStatus.TRANSIENT_ERROR:
                break
            if attempt < max_retries:
                sleep_fn(2**attempt)
        assert info is not None
        if info.status == FlatfileObjectStatus.NOT_FOUND:
            stats.unavailable += 1
            state.mark_raw_unavailable(day, info.error or "not found")
            raise RuntimeError(f"Expected Massive flat file is unavailable for {day}")
        if info.status == FlatfileObjectStatus.FORBIDDEN:
            stats.failed += 1
            state.mark_raw_failed(day, info.error or "forbidden")
            raise RuntimeError(f"Massive flat-file access forbidden for {day}")
        if info.status == FlatfileObjectStatus.TRANSIENT_ERROR:
            stats.failed += 1
            state.mark_raw_failed(day, info.error or "transient error")
            raise RuntimeError(f"Massive flat-file inspection retries exhausted for {day}")
        state.record("raw_started", date=day.isoformat())
        try:
            with tempfile.TemporaryDirectory() as td:
                gzip_path = Path(td) / f"{day}.csv.gz"
                client.download_date_to_path(day, gzip_path)
                result = store.stage_gzip(day, gzip_path, replace=replace)
        except Exception as exc:
            stats.failed += 1
            state.mark_raw_failed(day, str(exc))
            raise
        state.mark_raw_completed(day)
        stats.downloaded += 1
        stats.bytes += info.size_bytes or 0
        stats.rows += result["rows"]
        stats.symbols += result["symbols"]
    return stats
