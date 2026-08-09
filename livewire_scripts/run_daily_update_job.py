#!/usr/bin/env python3
"""Retrying runner for the scheduled daily parquet-first sync."""

from __future__ import annotations

import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE
from livewire_scripts.paths import warehouse_dir as resolve_warehouse_dir
from livewire_scripts.sync_runner import TIMEOUT_EXIT_CODE

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
INGEST_SCRIPT = REPO_ROOT / "scripts" / "livewire_ingest.py"
OPS_SCRIPT = REPO_ROOT / "scripts" / "livewire_ops.py"
QUALITY_SCRIPT = REPO_ROOT / "scripts" / "livewire_quality.py"
STORE_SCRIPT = REPO_ROOT / "scripts" / "livewire_store.py"

# Volatility is synced via CBOE directly (run_cboe_volatility_sync) and fx via
# Yahoo/Massive (run_fx_sync); these are the IB-backed asset classes the daily job
# iterates. cmdty uses IB MIDPOINT contracts and had no owning lane before, so it
# went stale.
#
# fx left this list when Yahoo took over the asset class: `resolve_fx_pair()` accepts
# only the 36 hardcoded `SUPPORTED_IB_FX_PAIRS`, which contains no NDF currency and
# cannot express a non-six-letter symbol like DXY, so an IB-driven fx lane could never
# cover the universe the preset now declares.
ASSET_CLASSES = ["equity", "futures", "cmdty"]

#: Nightly fx catch-up window. Wide enough to absorb a missed run without re-seeding
#: the full rolling history every night.
FX_CATCHUP_DAYS = 7


@dataclass(frozen=True)
class RunnerConfig:
    warehouse_dir: Path
    log_dir: Path
    daily_update_script: Path
    alert_script: Path
    python_bin: str
    node_bin: str
    max_attempts: int
    retry_delay_seconds: int


@dataclass(frozen=True)
class AlertRequest:
    run_date: str
    log_file: Path
    attempts: int | None
    exit_code: int | None
    error_summary: str
    repo_root: Path


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default

    value = int(raw)
    if value < 1:
        raise ValueError(f"{name} must be >= 1, got {value}")
    return value


def build_config() -> RunnerConfig:
    warehouse_dir = resolve_warehouse_dir()
    log_dir = Path(os.getenv("MDW_DAILY_UPDATE_LOG_DIR", str(warehouse_dir / "logs"))).expanduser()
    node_bin = os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"

    return RunnerConfig(
        warehouse_dir=warehouse_dir,
        log_dir=log_dir,
        daily_update_script=Path(os.getenv("MDW_DAILY_UPDATE_SCRIPT", str(INGEST_SCRIPT))).expanduser(),
        alert_script=Path(
            os.getenv(
                "MDW_DAILY_UPDATE_ALERT_SCRIPT",
                str(OPS_SCRIPT),
            )
        ).expanduser(),
        python_bin=os.getenv("MDW_DAILY_UPDATE_PYTHON_BIN", sys.executable),
        node_bin=node_bin,
        max_attempts=_read_positive_int_env("MDW_DAILY_UPDATE_MAX_ATTEMPTS", 3),
        retry_delay_seconds=_read_positive_int_env("MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", 300),
    )


def build_log_file(log_dir: Path, now: datetime | None = None) -> Path:
    current = now or datetime.now(UTC)
    return log_dir / f"daily_update_{current:%Y-%m-%d}.log"


def append_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")


def build_daily_update_command(config: RunnerConfig, daily_update_args: Sequence[str]) -> list[str]:
    return [config.python_bin, str(config.daily_update_script), "daily", *daily_update_args]


def build_cboe_volatility_command(config: RunnerConfig) -> list[str]:
    """Build command for CBOE volatility sync (uses preset by default)."""
    return [config.python_bin, str(INGEST_SCRIPT), "cboe-vol"]


def build_corporate_action_command(
    config: RunnerConfig,
    *,
    full_reconcile: bool,
    dry_run: bool,
) -> list[str]:
    command = [config.python_bin, str(INGEST_SCRIPT), "corporate-actions"]
    if full_reconcile:
        command.append("--full-reconcile")
    if dry_run:
        command.append("--dry-run")
    return command


def build_silver_rebuild_command(config: RunnerConfig, *, dry_run: bool) -> list[str]:
    command = [config.python_bin, str(STORE_SCRIPT), "rebuild-silver", "--full"]
    if dry_run:
        command.append("--dry-run")
    return command


def build_alert_command(config: RunnerConfig, request: AlertRequest) -> list[str]:
    command = [
        config.python_bin,
        str(config.alert_script),
        "send-alert",
        "--run-date",
        request.run_date,
        "--log-file",
        str(request.log_file),
        "--error-summary",
        request.error_summary,
        "--repo-root",
        str(request.repo_root),
        "--job-name",
        "daily_update",
    ]
    if request.attempts is not None:
        command.extend(["--attempts", str(request.attempts)])
    if request.exit_code is not None:
        command.extend(["--exit-code", str(request.exit_code)])
    return command


def node_binary_exists(node_bin: str) -> bool:
    if Path(node_bin).is_absolute():
        return Path(node_bin).exists()
    return shutil.which(node_bin) is not None


@dataclass(frozen=True)
class JobDeadline:
    """A monotonic wall-clock budget for the WHOLE scheduled job.

    `main()` runs seven lanes sequentially: corporate-actions, equity, futures,
    cmdty, CBOE, FX, Silver. A per-lane budget of N hours therefore permits a
    7N-hour job, which is how the 2026-07-28 run reached 19.44h while every
    individual lane still looked bounded.

    Measured whole-job wall clock across 2026-07-01..28: healthy runs peak at
    3.27h (07-25; 07-26 3.22h, 07-27 2.94h); anomalies ran 4.96h, 8.10h,
    10.32h, 19.44h. The job starts 06:00 UTC and the watchdog checks at 10:30
    UTC, so the budget must sit in the narrow band (3.27h, 4.5h). 4h gives
    ~22% headroom over the worst healthy run — tight, and the honest number.

    A 4h budget would have killed the 07-22 run at 4.96h. Whether that run was
    healthy-but-slow or an early instance of the same wedge is unknown. If
    ~5h runs turn out to be normal, raise MDW_DAILY_JOB_DEADLINE_SECONDS and
    move the watchdog with it rather than silently killing real work.
    """

    total_seconds: float
    started_at: float
    clock: callable = time.monotonic

    @classmethod
    def start(cls, total_seconds: float | None = None, clock: callable = time.monotonic) -> JobDeadline:
        budget = (
            float(os.getenv("MDW_DAILY_JOB_DEADLINE_SECONDS", str(4 * 60 * 60)))
            if total_seconds is None
            else float(total_seconds)
        )
        return cls(total_seconds=budget, started_at=clock(), clock=clock)

    def remaining(self) -> float:
        return self.total_seconds - (self.clock() - self.started_at)


def _run_in_own_process_group(command, *, stdout, env, timeout):
    """Run `command` in its own session and kill the whole group on timeout.

    `subprocess.run(timeout=...)` calls `process.kill()`, which signals only
    the direct child. A lane that fans out (`corporate-actions --workers 4`)
    would leave every worker orphaned — still running, still holding the
    per-parquet `fcntl.flock`, still wedged — and launchd would start the next
    instance into lock contention with processes it believes it killed.

    Killing the group is safe: bronze publication is temp -> validate ->
    os.replace(), so a killed writer leaves a temp file rather than a torn
    parquet, and the kernel releases every flock when the fds close.
    """
    with subprocess.Popen(
        list(command),
        stdout=stdout,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        start_new_session=True,
    ) as proc:
        try:
            proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.communicate()
            raise
        return subprocess.CompletedProcess(list(command), proc.returncode)


def run_daily_update_attempt(
    command: Sequence[str],
    log_file: Path,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    timeout: float | None = None,
    deadline: JobDeadline | None = None,
) -> subprocess.CompletedProcess:
    budget = timeout if timeout is not None else (deadline.remaining() if deadline is not None else None)
    if budget is not None and budget <= 0:
        # A non-positive timeout is a crash, not a skip. The job is already over
        # its total budget; do not start another lane.
        append_log(log_file, "=== Deadline exhausted before this lane started ===")
        return subprocess.CompletedProcess(list(command), TIMEOUT_EXIT_CODE)
    with log_file.open("a", encoding="utf-8") as handle:
        try:
            return runner(list(command), stdout=handle, env=env, timeout=budget)
        except subprocess.TimeoutExpired:
            # `budget` may be None when no deadline was threaded through; do not
            # format it unconditionally or the handler itself raises TypeError
            # and the timeout escapes as an unhandled exception.
            spent = "no budget" if budget is None else f"{budget:.0f}s"
            append_log(log_file, f"=== Timed out after {spent} (process group killed) ===")
            return subprocess.CompletedProcess(list(command), TIMEOUT_EXIT_CODE)


def send_failure_alert(
    config: RunnerConfig,
    request: AlertRequest,
    log_file: Path,
    env: dict[str, str] | None = None,
    runner: callable | None = None,
) -> subprocess.CompletedProcess | None:
    """Send the failure email.

    `runner` defaults to `subprocess.run` **late**, not as a default argument —
    a default captured at import time cannot be patched, and the alert is the
    one path production has no other way to observe.
    """
    if not node_binary_exists(config.node_bin):
        append_log(
            log_file,
            f"WARNING: node binary not found at {config.node_bin}; skipping failure email",
        )
        return None

    if not config.alert_script.exists():
        append_log(
            log_file,
            f"WARNING: alert script not found at {config.alert_script}; skipping failure email",
        )
        return None

    alert_command = build_alert_command(config, request)
    append_log(log_file, f"Triggering failure alert via: {' '.join(alert_command)}")
    return (runner or subprocess.run)(
        alert_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        check=False,
    )


def extract_error_summary(log_file: Path) -> str:
    try:
        text = log_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "Daily update failed, and the log file was not found."

    from livewire_scripts.daily_outcomes import parse_last_summary_json

    summary = parse_last_summary_json(text)
    if summary is not None:
        parts = [
            f"updated={summary.get('updated', 0)}",
            f"no_trade={summary.get('no_trade', 0)}",
            f"partial={summary.get('partial', 0)}",
            f"errors={summary.get('errors', 0)}",
            f"target_date={summary.get('target_date', '?')}",
            f"source={summary.get('source', '?')}",
            f"asset_class={summary.get('asset_class', '?')}",
        ]
        top = summary.get("top_errors") or []
        if top:
            msg, count = top[0]
            parts.append(f'dominant error ({count}x): "{msg}"')
        return "Daily update failed — " + ", ".join(parts)

    # Legacy fallback: last meaningful line (no per-ticker line counting — that
    # regex is what once reported success lines as the dominant "error").
    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if stripped and not stripped.startswith("==="):
            return stripped
    return "Daily update failed with no error summary captured in the log."


def _spawn_post_success_quality(runner, log_file, args, label, timeout=120):
    """Run a post-success quality subcommand; a failure logs a warning only.

    These jobs must never flip a successful daily run to failure.
    """
    try:
        result = runner(
            [sys.executable, str(QUALITY_SCRIPT), *args],
            timeout=timeout,
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            append_log(log_file, f"WARNING: {label} failed: exit_code={result.returncode}")
    except Exception as exc:  # pragma: no cover - logged but tolerated
        append_log(log_file, f"WARNING: {label} failed: {exc}")


_LEGACY_DONE_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


def completed_scopes(log_file: Path) -> set[str]:
    scopes: set[str] = set()
    try:
        for line in log_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("=== Done "):
                remainder = line.removeprefix("=== Done ").strip()
                if not remainder:
                    continue
                token = remainder.split(maxsplit=1)[0]
                scopes.add("*" if _LEGACY_DONE_TIMESTAMP_RE.fullmatch(token) else token)
    except FileNotFoundError:
        return set()
    return scopes


def skipped_scopes(log_file: Path) -> set[str]:
    """Scopes the run deliberately skipped (Gateway down, blocked prerequisite).

    A skipped scope is neither done nor missing. Without this the watchdog
    reads a 2FA-gated Gateway as "the sync never ran".
    """
    scopes: set[str] = set()
    try:
        for line in log_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("=== Skipped "):
                remainder = line.removeprefix("=== Skipped ").strip()
                if remainder:
                    scopes.add(remainder.split(maxsplit=1)[0])
    except FileNotFoundError:
        return set()
    return scopes


def log_has_completion_marker(log_file: Path) -> bool:
    return bool(completed_scopes(log_file))


def _completion_scope_from_args(args: Sequence[str]) -> str:
    if "--asset-class" not in args:
        return "daily"
    index = args.index("--asset-class")
    try:
        return args[index + 1]
    except IndexError:
        return "daily"


def _page_failure(
    config: RunnerConfig,
    log_file: Path,
    exit_code: int,
    *,
    attempts: int | None,
    env: dict[str, str] | None = None,
) -> None:
    """Send the failure alert and persist it if the send itself fails.

    Factored out of `run_with_retries` so `_run_scheduled_lane` can page too —
    it had no alert path at all, which is why the 2026-07-28 corporate-action
    wedge produced no alert from this job.

    It takes **no runner parameter on purpose.** Both callers hold the lane
    runner `_run_in_own_process_group`, and threading it in here is what made
    every page raise `TypeError` and kill the whole job instead of alerting
    (2026-08-02: corporate-actions failed 1 symbol of 14,577 and the six
    remaining lanes, Silver included, never ran). The two runners are not
    interchangeable: the lane runner is keyword-only on `stdout/env/timeout`
    and returns a `CompletedProcess` with **no stdout**, because a lane streams
    into the log file. The alert needs its output captured, both to log the
    send and to hand `record_undelivered_alert` something to persist.
    """
    alert_request = AlertRequest(
        run_date=log_file.stem.removeprefix("daily_update_"),
        log_file=log_file,
        attempts=attempts,
        exit_code=exit_code,
        error_summary=extract_error_summary(log_file),
        repo_root=REPO_ROOT,
    )
    alert_result = send_failure_alert(config, alert_request, log_file, env=env)
    if alert_result is None:
        return

    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode == 0:
        append_log(log_file, f"Failure alert sent successfully. {alert_output}".strip())
    else:
        append_log(
            log_file,
            (f"WARNING: failure alert returned non-zero exit code {alert_result.returncode}. {alert_output}").strip(),
        )
        record_undelivered_alert(config, alert_request, alert_output, log_file)


def run_with_retries(
    config: RunnerConfig,
    daily_update_args: Sequence[str],
    env: dict[str, str] | None = None,
    sleep_fn: callable = time.sleep,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
    completion_scope: str | None = None,
    deadline: JobDeadline | None = None,
) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    command = tuple(build_daily_update_command(config, daily_update_args))
    done_scope = completion_scope or _completion_scope_from_args(daily_update_args)

    append_log(log_file, f"=== Daily Update {started_at:%Y-%m-%dT%H:%M:%SZ} ===\n")
    append_log(log_file, f"Runner command: {' '.join(command)}")
    append_log(
        log_file,
        (
            "Runner config: "
            f"attempts={config.max_attempts} "
            f"retry_delay_seconds={config.retry_delay_seconds} "
            f"hostname={socket.gethostname()}"
        ),
    )

    final_exit_code = 1
    for attempt in range(1, config.max_attempts + 1):
        append_log(
            log_file,
            f"=== Attempt {attempt}/{config.max_attempts} {now_fn():%Y-%m-%dT%H:%M:%SZ} ===",
        )
        result = run_daily_update_attempt(command, log_file, env=env, runner=runner, deadline=deadline)
        final_exit_code = result.returncode

        if result.returncode == GATEWAY_DOWN_EXIT_CODE:
            # A down Gateway means 2FA, IBKR maintenance, or a session
            # conflict. Retrying burns 3x the retry delay and never helps, and
            # this is not a data failure — the caller keeps it out of the gate
            # for lanes that do not read IB.
            append_log(
                log_file,
                f"=== Skipped {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} (IB Gateway unreachable) ===",
            )
            return GATEWAY_DOWN_EXIT_CODE

        if result.returncode == TIMEOUT_EXIT_CODE:
            # `break`, NOT `return`: the only send_failure_alert call sits after
            # this loop, so an early return would make the timeout the one
            # failure mode that never pages — in the mechanism whose whole
            # purpose is to page. A wedge is also not transient; retrying just
            # spends the rest of the total deadline for nothing.
            append_log(
                log_file,
                f"=== Timed out {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} (no retry) ===",
            )
            final_exit_code = TIMEOUT_EXIT_CODE
            break

        if result.returncode == 0:
            append_log(
                log_file,
                (f"=== Done {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} (attempt {attempt}/{config.max_attempts}) ==="),
            )
            return 0

        append_log(
            log_file,
            (
                "=== Attempt failed "
                f"{now_fn():%Y-%m-%dT%H:%M:%SZ} "
                f"(attempt {attempt}/{config.max_attempts}, exit_code={result.returncode}) ==="
            ),
        )

        if attempt < config.max_attempts:
            append_log(
                log_file,
                f"Retrying in {config.retry_delay_seconds} seconds...",
            )
            sleep_fn(config.retry_delay_seconds)

    append_log(
        log_file,
        (f"=== Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} after {config.max_attempts} attempt(s) ==="),
    )

    _page_failure(config, log_file, final_exit_code, attempts=config.max_attempts, env=env)
    return final_exit_code


def run_post_success_quality(
    config: RunnerConfig,
    log_file: Path,
    runner: callable = subprocess.run,
) -> None:
    """Run coverage, weekly, and the nightly digest exactly once, last.

    These used to fire inside each asset class's success branch — up to four
    coverage runs and four digest emails a night, all of them before the
    Silver rebuild. `_silver_section` parses the same log for Silver's
    SUMMARY_JSON, which had not been written yet, so the window_regressions
    warning the digest is supposed to carry could never appear.
    """
    # Coverage may launch a recovery subprocess, so give it a longer budget.
    # 600s was not a budget, it was a guillotine: the footer pass is ~150-300s
    # per timeframe across five timeframes, so coverage timed out every night
    # from 2026-07-07 and the weekly report has been an empty stub since.
    # `FOOTER_READ_WORKERS` takes 5.3x off that; this absorbs what threads
    # cannot — a cold glob measured at 281s for a single timeframe.
    _spawn_post_success_quality(runner, log_file, ["coverage"], "coverage report", timeout=1800)
    # weekly self-skips on non-Sunday.
    _spawn_post_success_quality(runner, log_file, ["weekly"], "weekly quality report")
    run_date = log_file.stem.removeprefix("daily_update_")

    # Coverage only ever compares the target day against each file's max date,
    # so an interior hole three months back is arithmetically invisible to it.
    # `health` is the only scheduled detector of interior gaps — it was
    # reachable by hand only, so nothing in production ever scanned for one.
    # Weekly because a full scan reads whole columns across the universe.
    if _is_sunday(run_date):
        _spawn_post_success_quality(
            runner,
            log_file,
            ["health", "--intraday", "--timeframe", "5m"],
            "interior gap scan",
            timeout=3600,
        )

    _spawn_post_success_quality(
        runner,
        log_file,
        ["digest", "--run-date", run_date, "--email"],
        "nightly digest",
    )


def _is_sunday(run_date: str) -> bool:
    try:
        return datetime.strptime(run_date, "%Y-%m-%d").weekday() == 6
    except ValueError:
        return False


def undelivered_dir(config: RunnerConfig) -> Path:
    """Queue for scheduled-job alerts that could not be sent.

    Deliberately not MDW_UNDELIVERED_DIR — that knob belongs to per-flag
    quality alerts, and folding two different producers into one directory
    would make the watchdog's count mean two different things.
    """
    return config.log_dir / "alerts_undelivered"


def record_undelivered_alert(
    config: RunnerConfig,
    request: AlertRequest,
    alert_output: str,
    log_file: Path,
) -> Path | None:
    """Persist an alert that could not be sent.

    A failed send previously left only a WARNING inside the same log nobody
    reads when the job is broken — the alert channel and the thing it reports
    on died from one cause (a missing `.env`) and the outage ran six days
    unnoticed. This leaves a durable artifact the watchdog and digest count.
    """
    target_dir = undelivered_dir(config)
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{request.run_date}_{log_file.stem}.txt"
        path.write_text(
            "\n".join(
                [
                    f"run_date: {request.run_date}",
                    f"log_file: {request.log_file}",
                    f"attempts: {request.attempts}",
                    f"exit_code: {request.exit_code}",
                    f"send_output: {alert_output}",
                    "",
                    request.error_summary,
                    "",
                ]
            ),
            encoding="utf-8",
        )
    except OSError as exc:  # pragma: no cover - last-resort path
        append_log(log_file, f"WARNING: could not persist undelivered alert: {exc}")
        return None
    append_log(log_file, f"Undelivered alert persisted to {path}")
    return path


def run_cboe_volatility_sync(
    config: RunnerConfig,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
    deadline: JobDeadline | None = None,
) -> int:
    """Sync all CBOE volatility indices directly from CBOE API.

    This used to carry its own copy of the lane body, which emitted byte-for-byte
    the same log lines as `_run_scheduled_lane` but silently missed every
    improvement made there — including the failure alert and the job deadline.
    """
    return _run_scheduled_lane(
        config,
        build_cboe_volatility_command(config),
        "CBOE Volatility Sync",
        "cboe",
        env=env,
        runner=runner,
        now_fn=now_fn,
        deadline=deadline,
    )


def _run_scheduled_lane(
    config: RunnerConfig,
    command: list[str],
    label: str,
    done_scope: str,
    *,
    env: dict[str, str] | None,
    runner: callable,
    now_fn: callable,
    deadline: JobDeadline | None = None,
) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    append_log(log_file, f"=== {label} {started_at:%Y-%m-%dT%H:%M:%SZ} ===")
    append_log(log_file, f"Command: {' '.join(command)}")
    result = run_daily_update_attempt(command, log_file, env=env, runner=runner, deadline=deadline)
    if result.returncode == 0:
        append_log(log_file, f"=== Done {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} ===")
        return result.returncode

    append_log(
        log_file,
        f"=== {label} Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===",
    )
    # This function had no alert path at all, so a corporate-actions, CBOE, FX
    # or Silver failure was visible only in a log nobody reads — which is
    # exactly what happened to the 2026-07-28 corporate-action wedge. A down
    # Gateway stays silent: degraded is not failed.
    if result.returncode != GATEWAY_DOWN_EXIT_CODE:
        _page_failure(config, log_file, result.returncode, attempts=None, env=env)
    return result.returncode


def build_fx_command(config: RunnerConfig) -> list[str]:
    """Build the nightly fx catch-up command.

    ``--days`` keeps the nightly run to a short window; the deep seed is a separate
    manual run with the flag omitted. Both merge, so the accumulated intraday history
    is preserved either way.
    """
    return [config.python_bin, str(INGEST_SCRIPT), "fx", "--days", str(FX_CATCHUP_DAYS)]


def run_fx_sync(
    config: RunnerConfig,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
    deadline: JobDeadline | None = None,
) -> int:
    """Sync DXY and FX pairs via Yahoo (daily) and Massive (pair intraday)."""
    return _run_scheduled_lane(
        config,
        build_fx_command(config),
        "FX Sync",
        "fx",
        env=env,
        runner=runner,
        now_fn=now_fn,
    )


def run_corporate_action_sync(
    config: RunnerConfig,
    *,
    dry_run: bool,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
    deadline: JobDeadline | None = None,
) -> int:
    now = now_fn()
    command = build_corporate_action_command(
        config,
        full_reconcile=now.weekday() == 6,
        dry_run=dry_run,
    )
    return _run_scheduled_lane(
        config,
        command,
        "Corporate Action Sync",
        "corporate-actions",
        env=env,
        runner=runner,
        now_fn=lambda: now,
    )


def run_silver_rebuild(
    config: RunnerConfig,
    *,
    dry_run: bool,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
    deadline: JobDeadline | None = None,
) -> int:
    return _run_scheduled_lane(
        config,
        build_silver_rebuild_command(config, dry_run=dry_run),
        "Silver Rebuild",
        "silver",
        env=env,
        runner=runner,
        now_fn=now_fn,
    )


def main(argv: Sequence[str] | None = None) -> int:
    config = build_config()
    args = list(argv or sys.argv[1:])
    env = os.environ.copy()
    # ONE budget for the whole job. Seven lanes run sequentially below, so a
    # per-lane budget of N hours would permit a 7N-hour job.
    deadline = JobDeadline.start()

    # If --asset-class is explicitly specified, run just that one.
    if "--asset-class" in args:
        return run_with_retries(
            config, args, env=env, completion_scope=_completion_scope_from_args(args), deadline=deadline
        )

    dry_run = "--dry-run" in args
    action_code = run_corporate_action_sync(config, dry_run=dry_run, env=env, deadline=deadline)

    # Otherwise, run all asset classes sequentially.
    lane_codes: dict[str, int] = {}
    for asset_class in ASSET_CLASSES:
        lane_codes[asset_class] = run_with_retries(
            config,
            args + ["--asset-class", asset_class],
            env=env,
            completion_scope=asset_class,
            deadline=deadline,
        )

    # Massive owns equity daily whenever IB cannot answer. Silver reads equity
    # bronze and the corporate-action store and nothing else, both Massive-backed
    # — so without this a Gateway outage silently gated the adjusted rebuild for
    # the whole ~13K universe, the same cascade the lane split was meant to end.
    #
    # `_requires_ib_preflight` exempts `daily --source massive`, so the retry
    # cannot hit the preflight again. Futures and cmdty deliberately get no
    # fallback: Massive does not carry those asset classes, and a fallback there
    # would manufacture a success out of missing data.
    if lane_codes.get("equity") == GATEWAY_DOWN_EXIT_CODE:
        lane_codes["equity"] = run_with_retries(
            config,
            args + ["--asset-class", "equity", "--source", "massive"],
            env=env,
            completion_scope="equity",
            deadline=deadline,
        )

    # Sync all volatility indices via CBOE API (authoritative source)
    cboe_code = run_cboe_volatility_sync(config, env=env, deadline=deadline)

    # DXY + FX pairs via Yahoo/Massive. Neither reads IB, so this lane is unaffected
    # by a 2FA-gated or down Gateway.
    fx_code = run_fx_sync(config, env=env, deadline=deadline)

    # A Gateway outage is a degraded run, not a failed one. It must not mark
    # the job failed and must not gate anything that does not read IB.
    degraded = sorted(name for name, code in lane_codes.items() if code == GATEWAY_DOWN_EXIT_CODE)
    failed = {name: code for name, code in lane_codes.items() if code not in (0, GATEWAY_DOWN_EXIT_CODE)}

    final_code = action_code or cboe_code or fx_code or next(iter(failed.values()), 0)

    # Silver reads equity bronze and the corporate-action store — nothing
    # else. Gating it on every lane meant one stale FX contract or a 2FA-gated
    # Gateway blocked the adjusted rebuild for the whole equity universe.
    silver_inputs_ok = action_code == 0 and lane_codes.get("equity", 0) == 0
    if silver_inputs_ok:
        silver_code = run_silver_rebuild(config, dry_run=dry_run, env=env, deadline=deadline)
        if silver_code != 0:
            final_code = final_code or silver_code
    else:
        blocker = "corporate-actions" if action_code != 0 else "equity"
        append_log(
            build_log_file(config.log_dir, _utc_now()),
            f"=== Skipped silver {_utc_now():%Y-%m-%dT%H:%M:%SZ} (blocked by {blocker}) ===",
        )

    if degraded:
        append_log(
            build_log_file(config.log_dir, _utc_now()),
            f"DEGRADED: IB Gateway unreachable; lanes skipped: {', '.join(degraded)}",
        )

    # Last, so the digest sees fresh coverage AND Silver's SUMMARY_JSON.
    run_post_success_quality(config, build_log_file(config.log_dir, _utc_now()))

    return final_code


if __name__ == "__main__":
    raise SystemExit(main())
