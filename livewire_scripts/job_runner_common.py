"""What the two scheduled-job runners share.

`run_daily_update_job`'s lanes and `sync_runner`'s phases share the
process-group guard, the single Popen body that uses it, the log-file seams,
the log tail that feeds a page body, and the executions receipt every
subprocess attempt writes. The intraday-catchup wrapper stays on plain
subprocess.run: its daily-backfill child must remain in the wrapper's process
group so a group SIGTERM reaches it as a catchable signal and the child's own
per-phase guard can reap the leaves. Paging itself is notify.page_for_lane +
notify.send — a subprocess argv here is the shape
pm:2026-07-28-lane-alert-paths-missing describes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
from collections import deque
from collections.abc import Iterator, Sequence
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


def run_in_own_process_group(
    command: Sequence[str],
    *,
    stdout=None,
    stderr=subprocess.STDOUT,
    text: bool = True,
    env=None,
    check: bool = False,
    timeout=None,
) -> subprocess.CompletedProcess:
    """One Popen + process_group_guard body for every lane/phase subprocess.

    start_new_session makes the child a session leader so the guard can reach
    the whole tree. ``subprocess.run(timeout=...)`` would kill only the direct
    child; a lane that fans out would leave workers orphaned and still holding
    per-parquet flocks. Group kill is safe: bronze publication is temp ->
    validate -> os.replace(), so a killed writer leaves a temp file and the
    kernel releases every flock with the fds.
    """
    with subprocess.Popen(
        list(command),
        stdout=stdout,
        stderr=stderr,
        text=text,
        env=env,
        start_new_session=True,
    ) as proc:
        with process_group_guard(proc):
            out, _err = proc.communicate(timeout=timeout)
        result = subprocess.CompletedProcess(list(command), proc.returncode, out)
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode, result.args)
    return result


_SHA_RE = re.compile(r"[0-9a-f]{40}", re.IGNORECASE)
_identity_cache: dict[str, str | None] = {}


def executing_code_sha(repo_root: Path) -> str | None:
    """The immutable identity of the code actually executing, or None.

    Resolved once per root from the *physical* path (callers pass a
    ``Path(__file__).resolve()``-derived root): under
    ``<warehouse>/releases/<sha>`` the directory name is the sha, so a
    ``current`` repoint mid-run cannot relabel an open run. A live checkout
    reports ``git rev-parse HEAD``. A supplied ``LW_RELEASE_SHA`` is only ever
    cross-checked against that physical identity (a contradiction warns and
    loses) — when the root can verify nothing, the answer is UNKNOWN (None),
    never an unverifiable env claim.
    """
    root = Path(repo_root).resolve()
    key = str(root)
    if key not in _identity_cache:
        _identity_cache[key] = _resolve_identity(root)
    return _identity_cache[key]


def _resolve_identity(root: Path) -> str | None:
    resolved = root.name if root.parent.name == "releases" and _SHA_RE.fullmatch(root.name) else _git_head(root)
    supplied = os.environ.get("LW_RELEASE_SHA")
    if resolved is not None and supplied and supplied != resolved:
        print(
            f"WARNING: LW_RELEASE_SHA={supplied} disagrees with executing root {resolved}; using the root",
            file=sys.stderr,
        )
    return resolved


def _git_head(root: Path) -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = out.stdout.strip() if out.returncode == 0 else ""
    return sha if _SHA_RE.fullmatch(sha) else None


def pin_executing_sha(repo_root: Path) -> str | None:
    """Resolve the executing identity once and pin it for this process's children.

    Entrypoints call this before spawning lanes so every ``LW_RELEASE_SHA``
    reader downstream sees the same validated value. An unresolvable identity
    clears the variable — an inherited unverified claim is worse than UNKNOWN.
    A ``current`` selection that differs from the executing code is logged,
    never relabeled.
    """
    identity = executing_code_sha(repo_root)
    if identity is None:
        os.environ.pop("LW_RELEASE_SHA", None)
    else:
        os.environ["LW_RELEASE_SHA"] = identity
    deployed = deployment_sha()
    if deployed and deployed != identity:
        print(
            f"WARNING: executing identity {identity or 'UNKNOWN'} differs from current selection {deployed}",
            file=sys.stderr,
        )
    return identity


def deployment_sha() -> str | None:
    """What the mutable ``current`` link selects right now — the deployment choice.

    Mutable by design, so it is never the executing code's identity; a run that
    started under an earlier ``current`` keeps its own sha. Recorded separately
    in each process_attempt receipt.
    """
    try:
        from livewire_scripts.release import current_sha

        return current_sha()
    except Exception:
        return None


def hash_files(paths) -> str | None:
    """sha256 over the sorted file contents; None when any input is unreadable."""
    digest = hashlib.sha256()
    for path in sorted(Path(p) for p in paths):
        try:
            digest.update(path.read_bytes())
        except OSError:
            return None
    return digest.hexdigest()


def emit_process_attempt(
    *,
    script: str,
    attempt: int,
    command: Sequence,
    started: datetime,
    ended: datetime,
    raw_exit_code: int | None,
    effective_exit_code: int,
    completion_reason: str,
    receipt_extra: dict | None = None,
) -> None:
    """Append one ``executions`` receipt for a single subprocess attempt.

    ``receipt_json`` keeps the distinction ``lane_results`` cannot: the
    process's raw exit, the effective result the caller acted on, and why they
    differ (``exit`` / ``timeout`` / ``summary_override``). ``exit_code`` stays
    effective so existing reads keep their meaning. ``script`` is the existing
    job/phase/lane name — the same token ``lane_results.lane`` records — so an
    attempt joins to its lane row on ``(run_id, script == lane)``. Telemetry
    never fails a lane, and with no ``LW_RUN_ID`` there is no run to attach to.
    """
    run = os.environ.get("LW_RUN_ID")
    if not run:
        return
    argv = [str(part) for part in command]
    receipt = {
        "schema_version": 1,
        "kind": "process_attempt",
        "raw_exit_code": raw_exit_code,
        "effective_exit_code": effective_exit_code,
        "completion_reason": completion_reason,
        "deployment_sha": deployment_sha(),
    }
    if receipt_extra:
        receipt.update(receipt_extra)
    try:
        ledger.emit(
            "executions",
            [
                {
                    "evidence_hash": None,
                    "script": script,
                    "attempt": int(attempt),
                    "args_json": json.dumps({"argv": argv[:24], "truncated_args": max(0, len(argv) - 24)}),
                    "release_sha": executing_code_sha(Path(__file__).resolve().parents[1]),
                    "started": started,
                    "ended": ended,
                    "exit_code": int(effective_exit_code),
                    "receipt_json": json.dumps(receipt),
                    "run_id": run,
                }
            ],
            run_id=run,
        )
    except Exception as exc:  # pragma: no cover - telemetry must not fail a lane
        print(f"WARNING: could not write executions row for {script}: {exc}", file=sys.stderr)
