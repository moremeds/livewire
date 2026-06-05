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
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

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


@dataclass(frozen=True)
class AlertRequest:
    run_date: str
    log_file: Path
    exit_code: int
    error_summary: str
    repo_root: Path


def _utc_now() -> datetime:
    return datetime.now(UTC)


def build_config() -> IntradayCatchupConfig:
    warehouse_dir = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))).expanduser()
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
    current = now or _utc_now()
    return log_dir / f"intraday_catchup_{current:%Y-%m-%d}.log"


def build_intraday_catchup_command(config: IntradayCatchupConfig) -> list[str]:
    return [config.python_bin, str(config.ingest_script), "daily-backfill"]


def build_alert_command(config: IntradayCatchupConfig, request: AlertRequest) -> list[str]:
    return [
        config.python_bin,
        str(config.alert_script),
        "send-alert",
        "--run-date", request.run_date,
        "--log-file", str(request.log_file),
        "--error-summary", request.error_summary,
        "--repo-root", str(request.repo_root),
        "--job-name", "intraday_catchup",
        "--attempts", "1",
        "--exit-code", str(request.exit_code),
    ]


def run_intraday_catchup(  # pragma: no cover  (covered in Task 5)
    config: IntradayCatchupConfig,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
    now_fn: callable = _utc_now,
) -> int:
    raise NotImplementedError("Filled in Task 5")


def main(argv: Sequence[str] | None = None) -> int:  # pragma: no cover  (covered in Task 5)
    raise NotImplementedError("Filled in Task 5")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
