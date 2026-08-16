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

from livewire_scripts.daily_outcomes import parse_all_summary_json
from livewire_scripts.run_daily_update_job import (
    ASSET_CLASSES,
    AlertRequest,
    RunnerConfig,
    append_log,
    build_config,
    JOB_COMPLETE_MARKER,
    completed_scopes,
    job_tail_complete,
    log_has_completion_marker,
    send_failure_alert,
    skipped_scopes,
    undelivered_dir,
)

WATCHDOG_ALERT_SENT_EXIT_CODE = 1
WATCHDOG_ALERT_FAILED_EXIT_CODE = 2

# Silver is the served artifact. It was absent from the required set, so a
# rebuild that never ran — or ran and failed — passed the watchdog silently
# while Apex kept serving the previous revision.
# fx is named explicitly rather than inherited from ASSET_CLASSES: it left the IB loop
# when Yahoo/Massive took over the asset class, but it still logs `=== Done fx ===` and
# a silent failure there must still raise the watchdog.
REQUIRED_DAILY_SCOPES = set(ASSET_CLASSES) | {"cboe", "fx", "silver"}


def stale_equity_summary(log_file: Path) -> str | None:
    """Return a reason string if the equity lane ingested nothing.

    Scope markers only prove the process reached the end. A run where every
    ticker errored or returned no bars still writes `=== Done equity ===`, so
    the watchdog needs the outcome counts to tell "ran" from "worked".
    """
    try:
        text = log_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    equity = [s for s in parse_all_summary_json(text) if s.get("asset_class") == "equity"]
    if not equity:
        return None
    latest = equity[-1]
    updated = latest.get("updated", 0)
    errors = latest.get("errors", 0)
    # `no_trade` used to satisfy this too, which made the check fire on every
    # run whose target trading day was already fully ingested. The job runs at
    # 06:00 UTC daily, so the UTC-Sunday and UTC-Monday runs both target the
    # same Friday that the UTC-Saturday run already published: updated=0 with
    # a full no_trade sweep is the CORRECT outcome there. It paged anyway on
    # 2026-07-26, 2026-07-27 and 2026-08-03 — three false alarms, and it would
    # have paged on every quiet weekend from now on.
    #
    # `no_trade` is documented as "the instrument didn't trade, not a failure"
    # and never fails a run in `resolve_exit_code`; honouring that here costs
    # nothing, because the case this check was written for — a day genuinely
    # missing from the lake — is measured by the coverage job against the
    # actual parquet, not guessed from a counter.
    if updated == 0 and errors:
        return (
            f"Equity lane completed but published nothing: updated=0, "
            f"errors={errors}, no_trade={latest.get('no_trade', 0)}."
        )
    return None


def undelivered_alert_count(config: RunnerConfig) -> int:
    try:
        return len(list(undelivered_dir(config).glob("*.txt")))
    except OSError:  # pragma: no cover - unreadable dir
        return 0


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
    degraded_scopes = sorted(skipped_scopes(daily_log_file))
    missing_daily_scopes = sorted(REQUIRED_DAILY_SCOPES - daily_scopes - set(degraded_scopes))
    daily_complete = ("*" in daily_scopes) or not missing_daily_scopes
    # A missing quality marker only means "failed" once the run is past its
    # post-success tail. That tail runs after the job's 4h deadline with
    # budgets of its own (the Sunday interior gap scan alone is 3600s), and
    # the digest — which writes this marker — is second-to-last in it.
    # Measured 2026-08-16: watchdog checked at 10:30:00Z, marker written at
    # 10:36:49Z. Same shape on 2026-08-04 (digest 10:49Z) and 2026-08-06
    # (digest 10:39Z). Four pages whose entire content was "not yet".
    tail_pending = not quality_marker.exists() and not job_tail_complete(daily_log_file)
    quality_complete = quality_marker.exists() or tail_pending
    intraday_complete = log_has_completion_marker(intraday_log_file)
    stale_reason = stale_equity_summary(daily_log_file)
    undelivered = undelivered_alert_count(config)

    if (
        daily_complete
        and quality_complete
        and intraday_complete
        and not degraded_scopes
        and stale_reason is None
        and undelivered == 0
    ):
        if tail_pending:
            # Not a page, but not nothing either: a tail that never finishes
            # would otherwise be invisible here forever. `status` grades the
            # digest and coverage freshness independently, which is what
            # catches the permanent case.
            append_log(
                watchdog_log_file,
                f"=== Daily Update Watchdog {run_date} ===\n"
                f"Quality marker absent but the run has not logged "
                f"'{JOB_COMPLETE_MARKER}' — post-success tail still in flight, not alerting.",
            )
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
    if stale_reason is not None:
        reasons.append(stale_reason)
    if degraded_scopes:
        reasons.append(f"DEGRADED: lanes skipped on {run_date}: {', '.join(degraded_scopes)}.")
    if undelivered:
        reasons.append(
            f"{undelivered} alert(s) could not be delivered and are queued in "
            f"{undelivered_dir(config)}; the alert channel itself may be broken."
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
