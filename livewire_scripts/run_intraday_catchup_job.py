#!/usr/bin/env python3
"""Single-shot runner for the scheduled intraday catch-up.

This wrapper exists to mirror the env loading and failure-alert pipeline of
`run_daily_update_job` while delegating all work to `daily-backfill`, which
already owns retry-until-done and activity-based stall detection. No external
retry loop here; one attempt, one alert on terminal failure.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from livewire_scripts.job_runner_common import AlertRequest, append_log
from livewire_scripts.job_runner_common import build_alert_command as _build_alert_command
from livewire_scripts.job_runner_common import build_log_file as _build_log_file
from livewire_scripts.job_runner_common import utc_now as _utc_now
from livewire_scripts.paths import warehouse_dir as resolve_warehouse_dir

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
INGEST_SCRIPT = REPO_ROOT / "scripts" / "livewire_ingest.py"
OPS_SCRIPT = REPO_ROOT / "scripts" / "livewire_ops.py"


@dataclass(frozen=True)
class IntradayCatchupConfig:
    warehouse_dir: Path
    log_dir: Path
    ingest_script: Path
    alert_script: Path
    python_bin: str
    node_bin: str


def build_config() -> IntradayCatchupConfig:
    warehouse_dir = resolve_warehouse_dir()
    log_dir = Path(os.getenv("MDW_INTRADAY_CATCHUP_LOG_DIR", str(warehouse_dir / "logs"))).expanduser()
    node_bin = os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"
    return IntradayCatchupConfig(
        warehouse_dir=warehouse_dir,
        log_dir=log_dir,
        ingest_script=Path(os.getenv("MDW_INTRADAY_CATCHUP_SCRIPT", str(INGEST_SCRIPT))).expanduser(),
        alert_script=Path(os.getenv("MDW_INTRADAY_CATCHUP_ALERT_SCRIPT", str(OPS_SCRIPT))).expanduser(),
        python_bin=os.getenv("MDW_INTRADAY_CATCHUP_PYTHON_BIN", sys.executable),
        node_bin=node_bin,
    )


def build_log_file(log_dir: Path, now: datetime | None = None) -> Path:
    return _build_log_file(log_dir, "intraday_catchup", now)


def build_intraday_catchup_command(config: IntradayCatchupConfig) -> list[str]:
    return [config.python_bin, str(config.ingest_script), "daily-backfill"]


def build_alert_command(config: IntradayCatchupConfig, request: AlertRequest) -> list[str]:
    return _build_alert_command(config.python_bin, config.alert_script, request, job_name="intraday_catchup")


def _node_binary_exists(node_bin: str) -> bool:
    if Path(node_bin).is_absolute():
        return Path(node_bin).exists()
    return shutil.which(node_bin) is not None


def _extract_error_summary(log_file: Path) -> str:
    """Summarize an intraday-catchup failure.

    Prefers the structured SUMMARY_JSON line emitted by daily-backfill (naming
    the phases that failed); falls back to the last meaningful log line.
    """
    try:
        text = log_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "Intraday catchup failed, and the log file was not found."

    from livewire_scripts.daily_outcomes import parse_last_summary_json

    summary = parse_last_summary_json(text)
    if summary is not None and summary.get("failed"):
        return "Intraday catchup failed — phases failed: " + ", ".join(summary["failed"])

    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if stripped and not stripped.startswith("==="):
            return stripped
    return "Intraday catchup failed with no error summary captured in the log."


def _send_failure_alert(
    config: IntradayCatchupConfig,
    log_file: Path,
    exit_code: int,
    env: dict[str, str] | None,
    runner: Callable[..., subprocess.CompletedProcess],
    now_fn: Callable[[], datetime],
) -> int:
    """Log failure header, dispatch send-alert subprocess (or skip if prereqs missing), return exit_code."""
    append_log(
        log_file,
        f"=== Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} (exit_code={exit_code}) ===",
    )

    if not _node_binary_exists(config.node_bin):
        append_log(log_file, f"WARNING: node binary not found at {config.node_bin}; skipping failure email")
        return exit_code

    if not config.alert_script.exists():
        append_log(log_file, f"WARNING: alert script not found at {config.alert_script}; skipping failure email")
        return exit_code

    alert_request = AlertRequest(
        run_date=log_file.stem.removeprefix("intraday_catchup_"),
        log_file=log_file,
        attempts=1,
        exit_code=exit_code,
        error_summary=_extract_error_summary(log_file),
        repo_root=REPO_ROOT,
    )
    alert_command = build_alert_command(config, alert_request)
    append_log(log_file, f"Triggering failure alert via: {' '.join(alert_command)}")
    alert_result = runner(
        alert_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        check=False,
    )
    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode == 0:
        append_log(log_file, f"Failure alert sent successfully. {alert_output}".strip())
    else:
        append_log(
            log_file,
            f"WARNING: failure alert returned non-zero exit code {alert_result.returncode}. {alert_output}".strip(),
        )
    return exit_code


def run_intraday_catchup(
    config: IntradayCatchupConfig,
    env: dict[str, str] | None = None,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    now_fn: Callable[[], datetime] = _utc_now,
) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    command = build_intraday_catchup_command(config)

    append_log(log_file, f"=== Intraday Catchup {started_at:%Y-%m-%dT%H:%M:%SZ} ===")
    append_log(log_file, f"Runner command: {' '.join(command)}")
    append_log(log_file, f"hostname={socket.gethostname()}")

    with log_file.open("a", encoding="utf-8") as handle:
        result = runner(
            list(command),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            check=False,
        )

    if result.returncode == 0:
        append_log(log_file, f"=== Done {now_fn():%Y-%m-%dT%H:%M:%SZ} ===")
        return 0

    return _send_failure_alert(config, log_file, result.returncode, env, runner, now_fn)


def main(argv: Sequence[str] | None = None) -> int:
    # No positional args expected; all behavior is env-driven and the daily-backfill
    # subcommand has no required CLI flags.
    _ = list(argv or sys.argv[1:])  # accepted but ignored, for symmetry with run-daily-job
    config = build_config()
    env = os.environ.copy()
    return run_intraday_catchup(config, env=env, runner=subprocess.run)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
