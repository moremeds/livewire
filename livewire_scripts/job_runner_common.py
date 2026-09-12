"""What the two scheduled-job runners share.

`run_daily_update_job` and `run_intraday_catchup_job` share the process-group
guard, the log-file seams, and the log tail that feeds a page body. Paging
itself is notify.page_for_lane + notify.send — a subprocess argv here is the
shape pm:2026-07-28-lane-alert-paths-missing describes.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from clients import ledger


@contextmanager
def process_group_guard(proc) -> Iterator[None]:
    """Cancel every descendant on timeout, interruption, or supervisor SIGTERM."""

    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    main_thread = threading.current_thread() is threading.main_thread()
    previous = signal.signal(signal.SIGTERM, terminate) if main_thread else None
    try:
        yield
        if proc.returncode:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    except BaseException:
        try:
            # start_new_session makes the child's PID its group ID. Looking up
            # the PID fails if the leader exited while descendants still live.
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
        raise
    finally:
        if main_thread:
            signal.signal(signal.SIGTERM, previous)


def utc_now() -> datetime:
    return datetime.now(UTC)


def append_log(log_file: Path, message: str) -> None:
    """Append a line to log_file, creating parent dirs as needed."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")


def build_log_file(log_dir: Path, prefix: str, now: datetime | None = None) -> Path:
    current = now or utc_now()
    return log_dir / f"{prefix}_{current:%Y-%m-%d}.log"


def tail_of(log_file: Path, lines: int = 60) -> str:
    """The last `lines` of a lane log — the context block in a page body."""
    try:
        with log_file.open(encoding="utf-8") as handle:
            return "".join(deque(handle, maxlen=lines))
    except FileNotFoundError:
        return ""


def emit_progress(*, scope: str, completed: int, total: int, run_id: str) -> None:
    """Heartbeat the universe position so a lane SIGKILLed at its budget still says how far it got.

    One writer for every lane: corporate-actions and silver read the same
    ``(progress, progress_total)`` pair off the ledger, so `status` grades them
    with one shaped query and a second copy cannot drift.
    """
    now = utc_now()
    rows = [
        {
            "name": name,
            "scope": scope,
            "measured_at": now,
            "value": float(value),
            "unit": "symbols",
            "source": "measured",
            "run_id": run_id,
        }
        for name, value in (("progress", completed), ("progress_total", total))
    ]
    try:
        ledger.emit("measurements", rows, run_id=run_id)
    except Exception as exc:  # pragma: no cover - telemetry must not fail a good run
        print(f"WARNING: could not write measurements: {exc}", file=sys.stderr)
