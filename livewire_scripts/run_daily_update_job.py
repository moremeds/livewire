#!/usr/bin/env python3
"""Retrying runner for the scheduled daily parquet-first sync."""

from __future__ import annotations

import os
import re
import shutil
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

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
INGEST_SCRIPT = REPO_ROOT / "scripts" / "livewire_ingest.py"
OPS_SCRIPT = REPO_ROOT / "scripts" / "livewire_ops.py"
QUALITY_SCRIPT = REPO_ROOT / "scripts" / "livewire_quality.py"
STORE_SCRIPT = REPO_ROOT / "scripts" / "livewire_store.py"

# Volatility is synced via CBOE directly (run_cboe_volatility_sync); these are
# the IB-backed asset classes the daily job iterates. cmdty/fx use IB MIDPOINT
# contracts and had no owning lane before, so they went stale.
ASSET_CLASSES = ["equity", "futures", "cmdty", "fx"]


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


def run_daily_update_attempt(
    command: Sequence[str],
    log_file: Path,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
) -> subprocess.CompletedProcess:
    with log_file.open("a", encoding="utf-8") as handle:
        return runner(
            list(command),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            check=False,
        )


def send_failure_alert(
    config: RunnerConfig,
    request: AlertRequest,
    log_file: Path,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
) -> subprocess.CompletedProcess | None:
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
    return runner(
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


def run_with_retries(
    config: RunnerConfig,
    daily_update_args: Sequence[str],
    env: dict[str, str] | None = None,
    sleep_fn: callable = time.sleep,
    runner: callable = subprocess.run,
    now_fn: callable = _utc_now,
    completion_scope: str | None = None,
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
        result = run_daily_update_attempt(command, log_file, env=env, runner=runner)
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

        if result.returncode == 0:
            append_log(
                log_file,
                (f"=== Done {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} (attempt {attempt}/{config.max_attempts}) ==="),
            )
            # Coverage + weekly were tied to a container entrypoint that no
            # longer exists; run them here after a successful daily job so
            # they resume daily. Coverage may launch a recovery subprocess,
            # so give it a longer budget. weekly self-skips on non-Sunday.
            # Run these before the digest so the digest sees fresh coverage.
            _spawn_post_success_quality(runner, log_file, ["coverage"], "coverage report", timeout=600)
            _spawn_post_success_quality(runner, log_file, ["weekly"], "weekly quality report")
            # One trustworthy nightly digest replaces the noisy per-warrant
            # summary email.
            run_date = log_file.stem.removeprefix("daily_update_")
            _spawn_post_success_quality(
                runner,
                log_file,
                ["digest", "--run-date", run_date, "--email"],
                "nightly digest",
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

    alert_request = AlertRequest(
        run_date=log_file.stem.removeprefix("daily_update_"),
        log_file=log_file,
        attempts=config.max_attempts,
        exit_code=final_exit_code,
        error_summary=extract_error_summary(log_file),
        repo_root=REPO_ROOT,
    )
    alert_result = send_failure_alert(
        config,
        alert_request,
        log_file,
        env=env,
        runner=runner,
    )
    if alert_result is None:
        return final_exit_code

    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode == 0:
        append_log(log_file, f"Failure alert sent successfully. {alert_output}".strip())
    else:
        append_log(
            log_file,
            (f"WARNING: failure alert returned non-zero exit code {alert_result.returncode}. {alert_output}").strip(),
        )

    return final_exit_code


def run_cboe_volatility_sync(
    config: RunnerConfig,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
    now_fn: callable = _utc_now,
) -> int:
    """Sync all CBOE volatility indices directly from CBOE API."""
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    command = build_cboe_volatility_command(config)

    append_log(
        log_file,
        f"=== CBOE Volatility Sync {started_at:%Y-%m-%dT%H:%M:%SZ} ===",
    )
    append_log(log_file, f"Command: {' '.join(command)}")

    result = run_daily_update_attempt(command, log_file, env=env, runner=runner)

    if result.returncode == 0:
        append_log(
            log_file,
            f"=== Done cboe {now_fn():%Y-%m-%dT%H:%M:%SZ} ===",
        )
    else:
        append_log(
            log_file,
            f"=== CBOE Volatility Sync Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===",
        )

    return result.returncode


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
    result = run_daily_update_attempt(command, log_file, env=env, runner=runner)
    if result.returncode == 0:
        append_log(log_file, f"=== Done {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} ===")
    else:
        append_log(
            log_file,
            f"=== {label} Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===",
        )
    return result.returncode


def run_corporate_action_sync(
    config: RunnerConfig,
    *,
    dry_run: bool,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
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
        now_fn=lambda: now,
    )


def run_silver_rebuild(
    config: RunnerConfig,
    *,
    dry_run: bool,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
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


def main(argv: Sequence[str] | None = None) -> int:
    config = build_config()
    args = list(argv or sys.argv[1:])
    env = os.environ.copy()

    # If --asset-class is explicitly specified, run just that one.
    if "--asset-class" in args:
        return run_with_retries(config, args, env=env, completion_scope=_completion_scope_from_args(args))

    dry_run = "--dry-run" in args
    action_code = run_corporate_action_sync(config, dry_run=dry_run, env=env)

    # Otherwise, run all asset classes sequentially.
    lane_codes: dict[str, int] = {}
    for asset_class in ASSET_CLASSES:
        lane_codes[asset_class] = run_with_retries(
            config,
            args + ["--asset-class", asset_class],
            env=env,
            completion_scope=asset_class,
        )

    # Sync all volatility indices via CBOE API (authoritative source)
    cboe_code = run_cboe_volatility_sync(config, env=env)

    # A Gateway outage is a degraded run, not a failed one. It must not mark
    # the job failed and must not gate anything that does not read IB.
    degraded = sorted(name for name, code in lane_codes.items() if code == GATEWAY_DOWN_EXIT_CODE)
    failed = {name: code for name, code in lane_codes.items() if code not in (0, GATEWAY_DOWN_EXIT_CODE)}

    final_code = action_code or cboe_code or next(iter(failed.values()), 0)

    # Silver reads equity bronze and the corporate-action store — nothing
    # else. Gating it on every lane meant one stale FX contract or a 2FA-gated
    # Gateway blocked the adjusted rebuild for the whole equity universe.
    silver_inputs_ok = action_code == 0 and lane_codes.get("equity", 0) == 0
    if silver_inputs_ok:
        silver_code = run_silver_rebuild(config, dry_run=dry_run, env=env)
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

    return final_code


if __name__ == "__main__":
    raise SystemExit(main())
