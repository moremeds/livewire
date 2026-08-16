"""Resumable Massive flat-file downloader."""

from __future__ import annotations

import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    workers: int = 1,
    sleep_fn=time.sleep,
) -> DownloadStats:
    stats = DownloadStats()
    stats_lock = threading.Lock()

    def _bump(field: str, value: int = 1) -> None:
        with stats_lock:
            setattr(stats, field, getattr(stats, field) + value)

    def _process(day: date) -> None:
        # Skip already-completed days regardless of `replace` — the cursor exists for a reason.
        # `replace=True` (repair mode) still applies to the publish phase where it overwrites
        # bronze rows; the raw staging itself is idempotent and need not be redone.
        if store.has_raw_date(day) and state.raw_completed(day):
            _bump("skipped")
            return
        # Skip days the provider has already told us are missing (e.g. NYSE special closures
        # the local calendar doesn't know about). Persisted in raw_unavailable so a single
        # NOT_FOUND doesn't get re-inspected on every restart.
        #
        # `replace=True` is repair mode — the operator explicitly asking to try this date
        # again. Honouring the durable NOT_FOUND there made the blacklist permanent: a
        # transient 404 (a mid-publish race, an object briefly replaced) blacklisted the
        # trade date forever and `flatfile-ingest repair --dates <date>` skipped it, leaving
        # hand-editing the state file as the only recovery.
        if state.raw_unavailable(day) and not replace:
            _bump("skipped")
            return
        info = None
        for attempt in range(max_retries + 1):
            info = client.inspect_date(day)
            _bump("inspected")
            if info.status != FlatfileObjectStatus.TRANSIENT_ERROR:
                break
            if attempt < max_retries:
                sleep_fn(2**attempt)
        assert info is not None
        if info.status == FlatfileObjectStatus.NOT_FOUND:
            # Not a fatal error: Massive only stages SIP trading days, so a date the local
            # calendar treats as open can legitimately be absent (e.g. presidential funerals).
            # Record it durably and move on; the rest of the batch should still complete.
            _bump("unavailable")
            state.mark_raw_unavailable(day, info.error or "not found")
            return
        if info.status == FlatfileObjectStatus.FORBIDDEN:
            _bump("failed")
            state.mark_raw_failed(day, info.error or "forbidden")
            raise RuntimeError(f"Massive flat-file access forbidden for {day}")
        if info.status == FlatfileObjectStatus.TRANSIENT_ERROR:
            _bump("failed")
            state.mark_raw_failed(day, info.error or "transient error")
            raise RuntimeError(f"Massive flat-file inspection retries exhausted for {day}")
        state.record("raw_started", date=day.isoformat())
        try:
            # dir= pins the download beside the staged output instead of $TMPDIR.
            # $TMPDIR is /var/folders on the internal volume, while raw_root lives on
            # the external lake — so a whole-market gz per worker was landing on the
            # smaller filesystem the capacity planner does not measure.
            store.raw_root.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=store.raw_root) as td:
                gzip_path = Path(td) / f"{day}.csv.gz"
                client.download_date_to_path(day, gzip_path)
                result = store.stage_gzip(day, gzip_path, replace=replace)
        except Exception as exc:
            _bump("failed")
            state.mark_raw_failed(day, str(exc))
            raise
        state.mark_raw_completed(day)
        _bump("downloaded")
        _bump("bytes", info.size_bytes or 0)
        _bump("rows", result["rows"])
        _bump("symbols", result["symbols"])

    if workers <= 1:
        for day in dates:
            _process(day)
        return stats

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="flatfile-dl") as pool:
        futures = {pool.submit(_process, day): day for day in dates}
        first_exc: Exception | None = None
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                if first_exc is None:
                    first_exc = exc
                    for pending in futures:
                        pending.cancel()
        if first_exc is not None:
            raise first_exc
    return stats
