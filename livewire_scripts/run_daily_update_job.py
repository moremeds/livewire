#!/usr/bin/env python3
"""Retrying runner for the scheduled daily parquet-first sync."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from clients import ledger
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

LANE_ORDER = ("futures", "cmdty", "cboe", "fx", "corporate-actions", "equity", "silver")
LANE_BUDGET_S: dict[str, float] = {
    "futures": 30 * 60,
    "cmdty": 30 * 60,
    "cboe": 30 * 60,
    "fx": 30 * 60,
    "corporate-actions": 3 * 60 * 60,
    "equity": 2 * 60 * 60,
    "silver": 2 * 60 * 60,
}
DEFAULT_LANE_BUDGET_S = 30 * 60
_OUTCOME_BY_EXIT = {0: "done", TIMEOUT_EXIT_CODE: "timeout", GATEWAY_DOWN_EXIT_CODE: "blocked"}
_EPOCH = date(1970, 1, 1)


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


def run_id() -> str:
    """Return the run id minted by the process entrypoint."""
    value = os.environ.get("LW_RUN_ID")
    if not value:
        raise RuntimeError("LW_RUN_ID is not set; main() mints it")
    return value


def _release_sha() -> str | None:
    try:
        return Path(os.readlink(resolve_warehouse_dir() / "current")).name
    except OSError:
        return None


def _file_sha(paths) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _emit_lane(
    scope,
    *,
    started,
    ended,
    exit_code,
    elapsed_s,
    outcome,
    blocker=None,
    log_file: Path | None = None,
) -> None:
    """Record a lane fact without letting ledger failure kill the lane."""
    try:
        ledger.emit(
            "lane_results",
            [
                {
                    "run_id": run_id(),
                    "lane": scope,
                    "started": started,
                    "ended": ended,
                    "exit_code": exit_code,
                    "budget_s": LANE_BUDGET_S.get(scope, DEFAULT_LANE_BUDGET_S),
                    "elapsed_s": elapsed_s,
                    "outcome": outcome,
                    "blocker": blocker,
                }
            ],
            run_id=run_id(),
        )
    except Exception as exc:  # pragma: no cover - logged but tolerated
        message = f"WARNING: could not write lane_results for {scope}: {exc}"
        if log_file is not None:
            append_log(log_file, message)
        else:
            print(message, file=sys.stderr)


def _emit_last_session(scope: str, session: date | None) -> None:
    if session is None:
        return
    try:
        ledger.emit(
            "measurements",
            [
                {
                    "name": "last_session",
                    "scope": scope,
                    "measured_at": _utc_now(),
                    "value": float((session - _EPOCH).days),
                    "unit": "epoch_days",
                    "source": "measured",
                    "run_id": run_id(),
                }
            ],
            run_id=run_id(),
        )
    except Exception as exc:  # pragma: no cover - reporting must not kill a lane
        print(f"WARNING: could not write last_session for {scope}: {exc}", file=sys.stderr)


def _last_session_from_log(log_file: Path, offset: int = 0) -> date | None:
    from livewire_scripts.daily_outcomes import parse_all_summary_json

    try:
        with log_file.open(encoding="utf-8") as handle:
            handle.seek(offset)
            summaries = parse_all_summary_json(handle.read())
    except FileNotFoundError:
        return None
    for summary in reversed(summaries):
        if target := summary.get("target_date"):
            return date.fromisoformat(target)
    return None


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
        # One token. The two-token form breaks whenever the summary begins with
        # "--", which is how the 2026-08-08 page was lost.
        f"--error-summary={request.error_summary}",
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
) -> subprocess.CompletedProcess:
    with log_file.open("a", encoding="utf-8") as handle:
        try:
            return runner(list(command), stdout=handle, env=env, timeout=timeout)
        except subprocess.TimeoutExpired:
            spent = "no budget" if timeout is None else f"{timeout:.0f}s"
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


def _spawn_post_success_quality(runner, log_file, args, label, timeout=120, script=None):
    """Run a post-success subcommand; a failure logs a warning only.

    These jobs must never flip a successful daily run to failure.

    `script` exists so the housekeeping sweep can reuse this rather than get a
    second copy of the try/except + WARNING shape that would drift from this one.
    """
    try:
        result = runner(
            [sys.executable, str(script or QUALITY_SCRIPT), *args],
            timeout=timeout,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            append_log(log_file, f"WARNING: {label} failed: exit_code={result.returncode}")
        return result
    except Exception as exc:  # pragma: no cover - logged but tolerated
        append_log(log_file, f"WARNING: {label} failed: {exc}")
        return subprocess.CompletedProcess(args, 1, stdout=str(exc))


def _completion_scope_from_args(args: Sequence[str]) -> str:
    if "--asset-class" not in args:
        return "daily"
    index = args.index("--asset-class")
    try:
        return args[index + 1]
    except IndexError:
        return "daily"


def silver_is_blocked() -> str | None:
    """Name the prerequisite lane blocking Silver in this run."""
    rows = ledger.query(
        "select lane, exit_code from lane_results "
        f"where run_id = '{run_id()}' and outcome is not null "
        "and lane in ('corporate-actions', 'equity') order by ended"
    )
    by_lane = {row["lane"]: row["exit_code"] for row in rows}
    for lane in ("corporate-actions", "equity"):
        # The ledger is the gate's source of truth. A missing terminal fact is
        # not success: if an emit failed, running Silver would silently turn a
        # failed prerequisite into a green adjusted rebuild.
        if lane not in by_lane or by_lane[lane] != 0:
            return lane
    return None


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
        record_failed_send(alert_request.run_date, None)
        return

    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode == 0:
        append_log(log_file, f"Failure alert sent successfully. {alert_output}".strip())
    else:
        append_log(
            log_file,
            (f"WARNING: failure alert returned non-zero exit code {alert_result.returncode}. {alert_output}").strip(),
        )
        record_failed_send(alert_request.run_date, alert_result)


def record_failed_send(run_date: str, result: subprocess.CompletedProcess | None) -> None:
    """Record a failed alert send as an execution fact."""
    try:
        output = "" if result is None else (result.stdout or "")
        if isinstance(output, bytes):
            output = output.decode("utf-8", "replace")
        ledger.emit(
            "executions",
            [
                {
                    "evidence_hash": None,
                    "script": "send_alert",
                    "attempt": 1,
                    "args_json": json.dumps({"run_date": run_date}),
                    "release_sha": _release_sha(),
                    "started": _utc_now(),
                    "ended": _utc_now(),
                    "exit_code": 1 if result is None else result.returncode,
                    "receipt_json": json.dumps({"output": output}),
                    "run_id": run_id(),
                }
            ],
            run_id=run_id(),
        )
    except Exception as exc:  # pragma: no cover - a failed reporter cannot fail the job
        print(f"WARNING: could not record failed alert send: {exc}", file=sys.stderr)


def run_with_retries(
    config: RunnerConfig,
    daily_update_args: Sequence[str],
    env: dict[str, str] | None = None,
    sleep_fn: callable = time.sleep,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
    completion_scope: str | None = None,
) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    command = tuple(build_daily_update_command(config, daily_update_args))
    done_scope = completion_scope or _completion_scope_from_args(daily_update_args)
    budget = LANE_BUDGET_S.get(done_scope, DEFAULT_LANE_BUDGET_S)

    append_log(log_file, f"=== Daily Update {started_at:%Y-%m-%dT%H:%M:%SZ} ===\n")
    append_log(log_file, f"Runner command: {' '.join(command)}")
    append_log(
        log_file,
        (
            "Runner config: "
            f"attempts={config.max_attempts} "
            f"retry_delay_seconds={config.retry_delay_seconds} "
            f"budget_s={budget} "
            f"hostname={socket.gethostname()}"
        ),
    )
    _emit_lane(
        done_scope,
        started=started_at,
        ended=None,
        exit_code=None,
        elapsed_s=None,
        outcome=None,
        log_file=log_file,
    )
    clock = time.monotonic()
    lane_log_offset = log_file.stat().st_size

    final_exit_code = 1
    for attempt in range(1, config.max_attempts + 1):
        append_log(
            log_file,
            f"=== Attempt {attempt}/{config.max_attempts} {now_fn():%Y-%m-%dT%H:%M:%SZ} ===",
        )
        remaining = budget if attempt == 1 else max(0.0, budget - (time.monotonic() - clock))
        if remaining == 0:
            result = subprocess.CompletedProcess(list(command), TIMEOUT_EXIT_CODE)
        else:
            result = run_daily_update_attempt(command, log_file, env=env, runner=runner, timeout=remaining)
        final_exit_code = result.returncode

        if result.returncode == GATEWAY_DOWN_EXIT_CODE:
            # A down Gateway means 2FA, IBKR maintenance, or a session
            # conflict. Retrying burns 3x the retry delay and never helps, and
            # this is not a data failure — the caller keeps it out of the gate
            # for lanes that do not read IB.
            ended_at = now_fn()
            append_log(
                log_file,
                f"=== Skipped {done_scope} {ended_at:%Y-%m-%dT%H:%M:%SZ} (IB Gateway unreachable) ===",
            )
            return _finish_lane(
                done_scope, log_file, started_at, clock, GATEWAY_DOWN_EXIT_CODE, ended_at, lane_log_offset
            )

        if result.returncode == TIMEOUT_EXIT_CODE:
            # `break`, NOT `return`: the only send_failure_alert call sits after
            # this loop, so an early return would make the timeout the one
            # failure mode that never pages — in the mechanism whose whole
            # purpose is to page. A wedge is also not transient; retrying just
            # wastes the rest of the run budget for nothing.
            append_log(
                log_file,
                f"=== Timed out {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} (no retry) ===",
            )
            final_exit_code = TIMEOUT_EXIT_CODE
            break

        if result.returncode == 0:
            ended_at = now_fn()
            append_log(
                log_file,
                (f"=== Done {done_scope} {ended_at:%Y-%m-%dT%H:%M:%SZ} (attempt {attempt}/{config.max_attempts}) ==="),
            )
            return _finish_lane(done_scope, log_file, started_at, clock, 0, ended_at, lane_log_offset)

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

    ended_at = now_fn()
    append_log(
        log_file,
        (f"=== Failed {ended_at:%Y-%m-%dT%H:%M:%SZ} after {config.max_attempts} attempt(s) ==="),
    )

    _page_failure(config, log_file, final_exit_code, attempts=config.max_attempts, env=env)
    return _finish_lane(done_scope, log_file, started_at, clock, final_exit_code, ended_at, lane_log_offset)


def _finish_lane(done_scope, log_file, started_at, clock, exit_code, ended_at, log_offset=0) -> int:
    _emit_lane(
        done_scope,
        started=started_at,
        ended=ended_at,
        exit_code=exit_code,
        elapsed_s=time.monotonic() - clock,
        outcome=_OUTCOME_BY_EXIT.get(exit_code, "failed"),
        blocker="ib_unreachable" if exit_code == GATEWAY_DOWN_EXIT_CODE else None,
        log_file=log_file,
    )
    _emit_last_session(done_scope, _last_session_from_log(log_file, log_offset))
    return exit_code


def run_post_success_quality(
    config: RunnerConfig,
    log_file: Path,
    runner: callable = subprocess.run,
) -> None:
    """Run weekly and the nightly digest exactly once, last.

    These used to fire inside each asset class's success branch — up to four
    digest emails a night, all of them before the Silver rebuild. Running the
    tail last ensures Silver's measurements exist before the digest reads them.
    """
    tail_started = _utc_now()
    tail_clock = time.monotonic()
    # Coverage runs as com.livewire.coverage, not here. It was given 600s, then
    # 1800s; a cold full pass measured 2858s on 2026-08-09. The bug was putting
    # a guessed budget around work whose runtime is dominated by cold I/O on an
    # external volume — so it now has its own job and no budget at all.
    # weekly self-skips on non-Sunday.
    results = [_spawn_post_success_quality(runner, log_file, ["weekly"], "weekly quality report")]
    run_date = log_file.stem.removeprefix("daily_update_")

    # The interior gap scan runs as com.livewire.interior-gap-scan, not here.
    # It was given 3600s; measured 2026-08-16 against the real lake it needs
    # ~3115s BEFORE the cold penalty — 302s for the cold glob alone, then
    # 191.5 ms per symbol across 14,687 symbols (133.1 ms reading whole
    # bar_timestamp columns, 58.4 ms detecting gaps). It has run exactly once,
    # on 2026-08-16, and that once it was killed at the budget. Same disease as
    # coverage: a guessed timeout around work whose runtime is dominated by
    # cold I/O on an external volume. Its own job, and no budget at all.

    digest_result = _spawn_post_success_quality(
        runner,
        log_file,
        ["digest", "--run-date", run_date, "--email"],
        "nightly digest",
    )
    results.append(digest_result)
    if digest_result.returncode != 0:
        record_failed_send(run_date, digest_result)

    # Retention sweep, last — the digest must already have been sent. It can
    # only warn: a sweep that deleted nothing is never worth failing a
    # successful ingest run for; the terminal tail row carries the failure.
    results.append(
        _spawn_post_success_quality(
            runner,
            log_file,
            ["housekeeping", "--apply"],
            "housekeeping",
            timeout=600,
            script=OPS_SCRIPT,
        )
    )

    tail_code = next((result.returncode for result in results if result.returncode != 0), 0)

    _emit_lane(
        "digest",
        started=tail_started,
        ended=_utc_now(),
        exit_code=tail_code,
        elapsed_s=time.monotonic() - tail_clock,
        outcome="done" if tail_code == 0 else "failed",
        log_file=log_file,
    )


def run_cboe_volatility_sync(
    config: RunnerConfig,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
) -> int:
    """Sync all CBOE volatility indices directly from CBOE API.

    This used to carry its own copy of the lane body, which emitted byte-for-byte
    the same log lines as `_run_scheduled_lane` but silently missed every
    improvement made there — including the failure alert and lane budget.
    """
    return _run_scheduled_lane(
        config,
        build_cboe_volatility_command(config),
        "CBOE Volatility Sync",
        "cboe",
        env=env,
        runner=runner,
        now_fn=now_fn,
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
) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    append_log(log_file, f"=== {label} {started_at:%Y-%m-%dT%H:%M:%SZ} ===")
    append_log(log_file, f"Command: {' '.join(command)}")
    _emit_lane(
        done_scope,
        started=started_at,
        ended=None,
        exit_code=None,
        elapsed_s=None,
        outcome=None,
        log_file=log_file,
    )
    budget = LANE_BUDGET_S.get(done_scope, DEFAULT_LANE_BUDGET_S)
    clock = time.monotonic()
    lane_log_offset = log_file.stat().st_size
    result = run_daily_update_attempt(command, log_file, env=env, runner=runner, timeout=budget)
    ended_at = now_fn()
    _emit_lane(
        done_scope,
        started=started_at,
        ended=ended_at,
        exit_code=result.returncode,
        elapsed_s=time.monotonic() - clock,
        outcome=_OUTCOME_BY_EXIT.get(result.returncode, "failed"),
        blocker="ib_unreachable" if result.returncode == GATEWAY_DOWN_EXIT_CODE else None,
        log_file=log_file,
    )
    _emit_last_session(done_scope, _last_session_from_log(log_file, lane_log_offset))
    if result.returncode == 0:
        append_log(log_file, f"=== Done {done_scope} {ended_at:%Y-%m-%dT%H:%M:%SZ} ===")
        return result.returncode

    append_log(
        log_file,
        f"=== {label} Failed {ended_at:%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===",
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
        now_fn=now_fn,
    )


def run_silver_rebuild(
    config: RunnerConfig,
    *,
    dry_run: bool,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    now_fn: callable = _utc_now,
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


def _without_flag(args: Sequence[str], flag: str) -> list[str]:
    """Drop every `flag value` / `flag=value` occurrence.

    Appending `--source massive` to args that already carry `--source ib` is
    not enough: argparse honours the LAST occurrence, but
    `_requires_ib_preflight` reads the FIRST (`_arg_value`,
    `scripts/livewire_ingest.py:75`). The preflight would still gate on `ib`,
    exit 86 again, and the fallback would silently defeat itself — the retry
    would look like it ran while nothing changed.
    """
    kept: list[str] = []
    skip = False
    for arg in args:
        if skip:
            skip = False
            continue
        if arg == flag:
            skip = True
            continue
        if arg.startswith(f"{flag}="):
            continue
        kept.append(arg)
    return kept


def _run_main(argv: Sequence[str] | None = None) -> int:
    os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("daily-update"))
    config = build_config()
    args = list(argv or sys.argv[1:])
    env = os.environ.copy()
    started_at = _utc_now()
    job_name = "daily-update" if "--asset-class" not in args else f"daily-update-{_completion_scope_from_args(args)}"
    run_row = {
        "run_id": run_id(),
        "job": job_name,
        "host": socket.gethostname(),
        "release_sha": _release_sha(),
        "presets_sha": _file_sha((REPO_ROOT / "presets").glob("*.json")),
        "registry_sha": _file_sha([REPO_ROOT / "registry" / "gaps.json"]),
        "started": started_at,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.emit("runs", [run_row], run_id=run_id())

    def close_run(code: int, *, degraded: bool = False) -> int:
        verdict = "DEGRADED" if degraded else ("FAILED" if code else "OK")
        ledger.emit(
            "runs",
            [run_row | {"ended": _utc_now(), "exit_code": code, "verdict": verdict}],
            run_id=run_id(),
        )
        return code

    # If --asset-class is explicitly specified, run just that one.
    if "--asset-class" in args:
        code = run_with_retries(config, args, env=env, completion_scope=_completion_scope_from_args(args))
        source_index = args.index("--source") + 1 if "--source" in args else len(args)
        source = args[source_index] if source_index < len(args) else "ib"
        return close_run(code, degraded=code == GATEWAY_DOWN_EXIT_CODE and source == "ib")

    dry_run = "--dry-run" in args
    lane_codes: dict[str, int] = {
        asset_class: run_with_retries(
            config,
            args + ["--asset-class", asset_class],
            env=env,
            completion_scope=asset_class,
        )
        for asset_class in ("futures", "cmdty")
    }

    cboe_code = run_cboe_volatility_sync(config, env=env)
    fx_code = run_fx_sync(config, env=env)
    action_code = run_corporate_action_sync(config, dry_run=dry_run, env=env)
    lane_codes["equity"] = run_with_retries(
        config,
        args + ["--asset-class", "equity"],
        env=env,
        completion_scope="equity",
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
    ib_lanes = set(ASSET_CLASSES)
    if lane_codes.get("equity") == GATEWAY_DOWN_EXIT_CODE:
        lane_codes["equity"] = run_with_retries(
            config,
            _without_flag(args, "--source") + ["--asset-class", "equity", "--source", "massive"],
            env=env,
            completion_scope="equity",
        )
        # Equity stops being an IB lane the moment Massive answers for it, so
        # an 86 from the FALLBACK is not a Gateway outage and must not degrade.
        # Otherwise both providers failing would leave `failed` empty, exit 0,
        # and skip Silver while reporting success. Same rule as sync_runner:
        # degrade eligibility is membership of the IB set, not the exit code.
        ib_lanes.discard("equity")

    # A Gateway outage is a degraded run, not a failed one. It must not mark
    # the job failed and must not gate anything that does not read IB.
    def _is_degraded(name: str, code: int) -> bool:
        return code == GATEWAY_DOWN_EXIT_CODE and name in ib_lanes

    degraded = sorted(name for name, code in lane_codes.items() if _is_degraded(name, code))
    failed = {name: code for name, code in lane_codes.items() if code != 0 and not _is_degraded(name, code)}

    final_code = action_code or cboe_code or fx_code or next(iter(failed.values()), 0)

    # Silver reads equity bronze and the corporate-action store — nothing
    # else. Gating it on every lane meant one stale FX contract or a 2FA-gated
    # Gateway blocked the adjusted rebuild for the whole equity universe.
    blocker = silver_is_blocked()
    if blocker is None:
        log_file = build_log_file(config.log_dir, _utc_now())
        silver_log_offset = log_file.stat().st_size if log_file.exists() else 0
        silver_code = run_silver_rebuild(config, dry_run=dry_run, env=env)
        if silver_code != 0:
            final_code = final_code or silver_code
        from livewire_scripts.daily_outcomes import parse_all_summary_json

        try:
            with log_file.open(encoding="utf-8") as handle:
                handle.seek(silver_log_offset)
                summaries = parse_all_summary_json(handle.read())
        except FileNotFoundError:
            summaries = []
        silver_summaries = [summary for summary in summaries if "window_regressions" in summary]
        if silver_summaries:
            summary = silver_summaries[-1]
            ledger.emit(
                "measurements",
                [
                    {
                        "name": "silver_failed",
                        "scope": "silver",
                        "measured_at": _utc_now(),
                        "value": float(summary.get("failed", 0)),
                        "unit": "symbols",
                        "source": "measured",
                        "run_id": run_id(),
                    },
                    {
                        "name": "silver_window_regressions",
                        "scope": "silver",
                        "measured_at": _utc_now(),
                        "value": float(summary.get("window_regressions", 0)),
                        "unit": "symbols",
                        "source": "measured",
                        "run_id": run_id(),
                    },
                ],
                run_id=run_id(),
            )
    else:
        log_file = build_log_file(config.log_dir, _utc_now())
        now = _utc_now()
        append_log(log_file, f"=== Skipped silver {now:%Y-%m-%dT%H:%M:%SZ} (blocked by {blocker}) ===")
        _emit_lane(
            "silver",
            started=now,
            ended=now,
            exit_code=None,
            elapsed_s=0.0,
            outcome="blocked",
            blocker=blocker,
            log_file=log_file,
        )

    if degraded:
        append_log(
            build_log_file(config.log_dir, _utc_now()),
            f"DEGRADED: IB Gateway unreachable; lanes skipped: {', '.join(degraded)}",
        )

    # Last, so the digest sees fresh coverage AND Silver's SUMMARY_JSON.
    run_post_success_quality(config, build_log_file(config.log_dir, _utc_now()))

    return close_run(final_code, degraded=bool(degraded))


def main(argv: Sequence[str] | None = None) -> int:
    """Run the job and close an opened ledger run even on an unexpected crash."""
    try:
        return _run_main(argv)
    except Exception:
        active_run = os.environ.get("LW_RUN_ID")
        if active_run:
            try:
                rows = ledger.query(
                    "select run_id, job, host, release_sha, presets_sha, registry_sha, started "
                    f"from runs where run_id = '{active_run}' "
                    "group by all having max(ended) is null order by started desc limit 1"
                )
                if rows:
                    ledger.emit(
                        "runs",
                        [rows[0] | {"ended": _utc_now(), "exit_code": 1, "verdict": "FAILED"}],
                        run_id=active_run,
                    )
            except Exception as close_exc:  # pragma: no cover - preserve the original crash
                print(f"WARNING: could not close failed run {active_run}: {close_exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
