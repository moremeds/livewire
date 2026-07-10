#!/usr/bin/env python3
"""Watchdog for the scheduled daily parquet-first sync."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:  # pragma: no cover - direct script bootstrap only
    sys.path.insert(0, str(REPO_ROOT))

from livewire_scripts.run_daily_update_job import (
    ASSET_CLASSES,
    AlertRequest,
    RunnerConfig,
    append_log,
    build_config,
    completed_scopes,
    log_has_completion_marker,
    send_failure_alert,
)

WATCHDOG_ALERT_SENT_EXIT_CODE = 1
WATCHDOG_ALERT_FAILED_EXIT_CODE = 2


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alert if today's scheduled daily update did not complete.")
    parser.add_argument(
        "--run-date",
        help="Run date to inspect in YYYY-MM-DD format. Defaults to today in UTC.",
    )
    return parser.parse_args(list(argv))


def build_daily_log_file(log_dir: Path, run_date: str) -> Path:
    return log_dir / f"daily_update_{run_date}.log"


def build_watchdog_log_file(log_dir: Path, run_date: str) -> Path:
    return log_dir / f"daily_update_watchdog_{run_date}.log"


def build_watchdog_marker_file(warehouse_dir: Path, run_date: str) -> Path:
    return warehouse_dir / "state" / "daily-update-watchdog" / f"{run_date}.alerted"


def determine_watchdog_error(log_file: Path, run_date: str) -> str:
    if not log_file.exists():
        return (
            "Watchdog detected that the scheduled daily update did not start on "
            f"{run_date}; expected log file {log_file} was not created."
        )

    return (
        "Watchdog detected that the scheduled daily update did not complete "
        f"successfully on {run_date}; no completion marker was found in {log_file}."
    )


def build_intraday_log_file(log_dir: Path, run_date: str) -> Path:
    return log_dir / f"intraday_catchup_{run_date}.log"


def determine_intraday_watchdog_error(log_file: Path, run_date: str) -> str:
    if not log_file.exists():
        return (
            "Watchdog detected that the intraday catch-up did not start on "
            f"{run_date}; expected log file {log_file} was not created."
        )

    return (
        "Watchdog detected that the intraday catch-up did not complete "
        f"successfully on {run_date}; no completion marker was found in {log_file}."
    )


def record_alert_marker(marker_file: Path, message: str) -> None:
    marker_file.parent.mkdir(parents=True, exist_ok=True)
    marker_file.write_text(f"{message}\n", encoding="utf-8")


def run_watchdog(
    config: RunnerConfig,
    *,
    run_date: str,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
) -> int:
    daily_log_file = build_daily_log_file(config.log_dir, run_date)
    intraday_log_file = build_intraday_log_file(config.log_dir, run_date)
    watchdog_log_file = build_watchdog_log_file(config.log_dir, run_date)
    marker_file = build_watchdog_marker_file(config.warehouse_dir, run_date)
    quality_marker = config.log_dir / f"quality_summary_{run_date}.marker"

    daily_scopes = completed_scopes(daily_log_file)
    required_daily_scopes = set(ASSET_CLASSES) | {"cboe"}
    missing_daily_scopes = sorted(required_daily_scopes - daily_scopes)
    daily_complete = ("*" in daily_scopes) or not missing_daily_scopes
    quality_complete = quality_marker.exists()
    intraday_complete = log_has_completion_marker(intraday_log_file)

    if daily_complete and quality_complete and intraday_complete:
        return 0

    reasons: list[str] = []
    if not daily_complete:
        reason = determine_watchdog_error(daily_log_file, run_date)
        if "*" not in daily_scopes:
            reason += f" missing completion scopes: {', '.join(missing_daily_scopes)}."
        reasons.append(reason)
    elif not quality_complete:
        reasons.append(
            f"Daily sync completed on {run_date} but the end-of-day quality "
            f"summary marker is missing at {quality_marker}."
        )
    if not intraday_complete:
        reasons.append(determine_intraday_watchdog_error(intraday_log_file, run_date))
    reason = " ".join(reasons)
    append_log(watchdog_log_file, f"=== Daily Update Watchdog {run_date} ===")
    append_log(watchdog_log_file, reason)

    if marker_file.exists():
        append_log(
            watchdog_log_file,
            f"Alert already sent for {run_date}; skipping duplicate failure email.",
        )
        return WATCHDOG_ALERT_SENT_EXIT_CODE

    request = AlertRequest(
        run_date=run_date,
        log_file=daily_log_file,
        attempts=None,
        exit_code=None,
        error_summary=reason,
        repo_root=REPO_ROOT,
    )
    alert_result = send_failure_alert(
        config,
        request,
        watchdog_log_file,
        env=env,
        runner=runner,
    )
    if alert_result is None:
        append_log(watchdog_log_file, "Watchdog could not send a failure alert.")
        return WATCHDOG_ALERT_FAILED_EXIT_CODE

    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode != 0:
        append_log(
            watchdog_log_file,
            (
                f"WARNING: watchdog failure alert returned non-zero exit code {alert_result.returncode}. {alert_output}"
            ).strip(),
        )
        return WATCHDOG_ALERT_FAILED_EXIT_CODE

    append_log(
        watchdog_log_file,
        f"Watchdog failure alert sent successfully. {alert_output}".strip(),
    )
    record_alert_marker(marker_file, reason)
    return WATCHDOG_ALERT_SENT_EXIT_CODE


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    run_date = args.run_date or datetime.now(UTC).strftime("%Y-%m-%d")
    config = build_config()
    return run_watchdog(config, run_date=run_date, env=os.environ.copy())


if __name__ == "__main__":
    raise SystemExit(main())
