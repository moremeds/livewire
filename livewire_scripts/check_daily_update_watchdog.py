#!/usr/bin/env python3
"""Page when the graded status surface says BAD. It parses no log prose.

Dedup is the ledger: `notify.page_from_sections` fingerprints the BAD sections,
and `notify.send` records the row whether it sent, failed, or skipped.
"""

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
from livewire_scripts import notify
from livewire_scripts.paths import data_lake_dir, log_dir
from livewire_scripts.status import collect

ALERT_FAILED_EXIT_CODE = 3


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alert when today's graded status contains BAD.")
    parser.add_argument("--run-date", help="Date to inspect in YYYY-MM-DD; defaults to today in UTC.")
    return parser.parse_args(list(argv))


def run_watchdog(run_date: date, *, now: datetime | None = None, runner=None) -> int:
    sections = collect(run_date, log_dir(), data_lake_dir(), now=now)
    notice = notify.page_from_sections(sections, run_date)
    if notice is None:
        return 0
    return 0 if notify.send(notice, runner=runner) == 0 else ALERT_FAILED_EXIT_CODE


def main(argv: Sequence[str] | None = None) -> int:
    os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("watchdog"))
    args = parse_args(list(argv or sys.argv[1:]))
    run_date = date.fromisoformat(args.run_date) if args.run_date else datetime.now(UTC).date()
    return run_watchdog(run_date)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
