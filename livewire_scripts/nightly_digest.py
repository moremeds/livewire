#!/usr/bin/env python3
"""Build the single nightly digest for Livewire.

Assembles one plain-text report from the machine-readable SUMMARY_JSON lines the
daily jobs now emit, plus the day's coverage line and disk headroom. This
replaces the noisy per-warrant daily-summary email: one digest on success, and
a rare truthful failure mail only on systemic failure.

Every section renders "(not found)" for missing inputs — build_digest never
raises, so a missing log can never suppress the whole digest.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.paths import data_lake_dir, log_dir
from livewire_scripts.status import (
    _coverage_section,
    _disk_section,
    _outcomes_section,
    _phases_section,
    _quality_jobs_section,
    _silver_section,
)

_LOG_DIR: Path | None = None
_DATA_LAKE: Path | None = None
_FAILURE_EMAIL_SCRIPT = _PROJECT_ROOT / "livewire_node" / "send_daily_update_failure_email.mjs"


def build_digest(run_date: date, log_dir: Path, data_lake: Path) -> str:
    """Assemble the nightly digest text. Never raises on missing inputs."""
    run = run_date.isoformat()
    sections: list[list[str]] = [
        [f"Livewire nightly digest — {run}"],
        _outcomes_section(run, log_dir),
        _phases_section(run, log_dir),
        _silver_section(run, log_dir),
        _quality_jobs_section(run, log_dir),
        _coverage_section(run, log_dir),
        _disk_section(data_lake, log_dir.parent),
    ]
    return "\n\n".join("\n".join(section) for section in sections) + "\n"


def _send_email(body: str, run_date: date, node_bin: str, log_dir: Path, runner) -> int:
    body_file = log_dir / f"nightly_digest_{run_date.isoformat()}.txt"
    body_file.parent.mkdir(parents=True, exist_ok=True)
    body_file.write_text(body, encoding="utf-8")
    result = runner(
        [
            node_bin,
            str(_FAILURE_EMAIL_SCRIPT),
            "--mode",
            "digest",
            "--run-date",
            run_date.isoformat(),
            "--body-file",
            str(body_file),
        ],
        check=False,
    )
    return int(result.returncode or 0)


def main(argv=None, runner=subprocess.run) -> int:
    parser = argparse.ArgumentParser(description="Build the Livewire nightly digest")
    parser.add_argument("--run-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--email", action="store_true", help="Send the digest via Nodemailer")
    parser.add_argument("--log-dir", type=Path, default=_LOG_DIR or log_dir())
    parser.add_argument("--data-lake", type=Path, default=_DATA_LAKE or data_lake_dir())
    args = parser.parse_args(argv)

    body = build_digest(args.run_date, args.log_dir, args.data_lake)
    print(body)
    if args.email:
        node_bin = os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"
        rc = _send_email(body, args.run_date, node_bin, args.log_dir, runner)
        if rc == 0:
            # The digest is the post-daily quality artifact; write the marker
            # only after the email path succeeds so the watchdog cannot mask a failed send.
            marker = args.log_dir / f"quality_summary_{args.run_date.isoformat()}.marker"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(f"nightly digest {args.run_date.isoformat()}\n", encoding="utf-8")
        return rc
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
