"""What the two scheduled-job runners share.

`run_daily_update_job` and `run_intraday_catchup_job` page through the same
alert contract. Two encodings of that contract is the shape
pm:2026-07-28-lane-alert-paths-missing describes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


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
