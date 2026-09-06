"""What the two scheduled-job runners share.

`run_daily_update_job` and `run_intraday_catchup_job` page through the same
alert contract. Two encodings of that contract is the shape
pm:2026-07-28-lane-alert-paths-missing describes.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from clients import ledger
from clients.parquet_io import path_lock
from livewire_scripts.paths import lake_lock_path


@dataclass(frozen=True)
class AlertRequest:
    run_date: str
    log_file: Path
    error_summary: str
    repo_root: Path
    attempts: int | None = None
    exit_code: int | None = None


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


def build_alert_command(
    python_bin: str,
    alert_script: Path,
    request: AlertRequest,
    *,
    job_name: str,
) -> list[str]:
    command = [
        python_bin,
        str(alert_script),
        "send-alert",
        "--run-date",
        request.run_date,
        "--log-file",
        str(request.log_file),
        # One token. The two-token form breaks whenever the summary begins with
        # "--", which is how the 2026-08-08 page was lost.
        f"--error-summary={request.error_summary}",
        "--repo-root",
        str(request.repo_root),
        "--job-name",
        job_name,
    ]
    if request.attempts is not None:
        command.extend(["--attempts", str(request.attempts)])
    if request.exit_code is not None:
        command.extend(["--exit-code", str(request.exit_code)])
    return command


#: `lane_results.blocker` for a lane that never got the lake-io lock. Not a new
#: outcome value: `blocked` already means "this lane did not run because
#: something else stopped it", and `_emit_lane` already declines to measure a
#: blocked lane, so a zero-length lane cannot drag the budget p95 toward 0.
LAKE_LOCK_BLOCKER = "lake_lock"


def _emit_lake_lock_wait(lane: str, waited_s: float) -> None:
    """Record the wait. A ledger failure must never kill a lane."""
    run = os.environ.get("LW_RUN_ID")
    if not run:
        return
    try:
        ledger.emit(
            "measurements",
            [
                {
                    "name": "lake_lock_wait_s",
                    "scope": lane,
                    "measured_at": utc_now(),
                    "value": float(waited_s),
                    "unit": "s",
                    "source": "measured",
                    "run_id": run,
                }
            ],
            run_id=run,
        )
    except Exception as exc:  # noqa: BLE001 - logged but tolerated, same rule as _emit_lane
        print(f"WARNING: could not record lake_lock_wait_s for {lane}: {exc}", file=sys.stderr)


@contextmanager
def lake_lock(
    lane: str,
    *,
    poll_s: float,
    budget_s: float,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> Iterator[float | None]:
    """Serialize one lane against every other process that writes the lake.

    Yields the seconds spent waiting, with the lock held, or yields None having
    taken nothing when `budget_s` elapsed without ever acquiring -- the caller
    then records the lane `outcome='blocked', blocker=LAKE_LOCK_BLOCKER` and
    moves on. The wait is bounded on BOTH sides on purpose: a blocking flock
    cannot be bounded without threads or signals, and a daily job blocked
    forever behind a wedged 6h intraday phase loses a whole night in silence.

    Priority is the poll interval and the arrival order, not a scheduler: the
    daily job polls at 1s and takes a freed lock at once; the intraday job polls
    at 60s and is the one that waits (spec
    2026-09-06-tiered-nightly-pipeline-design.md section 3).

    The wait is NOT part of the lane's `elapsed_s` -- the lane clock starts
    after this yields. Counting idle time as lane time would teach
    `status`'s budget-drift check that a lane needs a bigger budget because it
    spent two hours waiting.
    """
    lock_path = lake_lock_path()
    started = monotonic()
    while True:
        with path_lock(lock_path, blocking=False) as held:
            if held:
                waited = monotonic() - started
                _emit_lake_lock_wait(lane, waited)
                yield waited
                return
        waited = monotonic() - started
        if waited >= budget_s:
            _emit_lake_lock_wait(lane, waited)
            yield None
            return
        sleep_fn(poll_s)
