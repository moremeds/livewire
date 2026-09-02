#!/usr/bin/env python3
"""Page when the graded status surface says BAD. It parses no log prose."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:  # pragma: no cover - direct script bootstrap only
    sys.path.insert(0, str(REPO_ROOT))

from clients import ledger
from livewire_scripts.paths import data_lake_dir
from livewire_scripts.run_daily_update_job import (
    AlertRequest,
    RunnerConfig,
    build_config,
    record_failed_send,
    send_failure_alert,
)
from livewire_scripts.status import Verdict, collect

ALERT_FAILED_EXIT_CODE = 3


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alert when today's graded status contains BAD.")
    parser.add_argument("--run-date", help="Date to inspect in YYYY-MM-DD; defaults to today in UTC.")
    return parser.parse_args(list(argv))


def build_daily_log_file(log_dir: Path, run_date: str) -> Path:
    return log_dir / f"daily_update_{run_date}.log"


def build_watchdog_marker_file(warehouse_dir: Path, run_date: str) -> Path:
    return warehouse_dir / "state" / "daily-update-watchdog" / f"{run_date}.alerted"


def record_alert_marker(marker_file: Path, message: str) -> None:
    marker_file.parent.mkdir(parents=True, exist_ok=True)
    marker_file.write_text(f"{message}\n", encoding="utf-8")


def run_watchdog(config: RunnerConfig, run_date: str, runner=None) -> int:
    marker_file = build_watchdog_marker_file(config.warehouse_dir, run_date)
    sections = collect(date.fromisoformat(run_date), config.log_dir, data_lake_dir())
    bad = [section for section in sections if section.verdict is Verdict.BAD]
    by_name = {section.name: section for section in sections}
    missing_jobs = []
    for job, ran_name, finished_name in (
        ("Daily update", "Daily update ran", "Daily update finished"),
        ("Intraday catch-up", "Intraday catch-up ran", "Intraday catch-up finished"),
    ):
        ran = by_name.get(ran_name)
        finished = by_name.get(finished_name)
        if ran is not None and ran.verdict is Verdict.UNKNOWN and finished is not None and finished.verdict is Verdict.OK:
            missing_jobs.append(job)
    if (not bad and not missing_jobs) or marker_file.exists():
        return 0

    reasons = [section.lines[0] if section.lines else section.name for section in bad]
    reasons.extend(f"{job} did not start on {run_date}" for job in missing_jobs)
    reason = "; ".join(reasons)
    log_file = build_daily_log_file(config.log_dir, run_date)
    result = send_failure_alert(
        config,
        AlertRequest(
            run_date=run_date,
            log_file=log_file,
            attempts=None,
            exit_code=1,
            error_summary=reason,
            repo_root=REPO_ROOT,
        ),
        log_file,
        runner=runner,
    )
    if result is None or result.returncode != 0:
        record_failed_send(run_date, result)
        return ALERT_FAILED_EXIT_CODE
    record_alert_marker(marker_file, reason)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("watchdog"))
    args = parse_args(list(argv or sys.argv[1:]))
    run_date = args.run_date or datetime.now(UTC).date().isoformat()
    return run_watchdog(build_config(), run_date)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
