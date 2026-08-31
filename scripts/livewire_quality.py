#!/usr/bin/env python3
"""Livewire quality and monitoring command surface."""

from __future__ import annotations

import argparse
import importlib
import inspect
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from livewire_scripts.scheduled_env import load_scheduled_env

COMMANDS = {
    "audit-legacy-basis": "livewire_scripts.audit_legacy_basis",
    "audit-split-basis": "livewire_scripts.audit_split_basis",
    "calibrate-daily-basis": "livewire_scripts.calibrate_daily_basis",
    "health": "livewire_scripts.health_check",
    "coverage": "livewire_scripts.coverage_report",
    "gap-scan": "livewire_scripts.gap_scan",
    "report": "livewire_scripts.data_quality_report",
    "resolve-split-basis": "livewire_scripts.resolve_split_basis",
    "triage-breaks": "livewire_scripts.triage_breaks",
    "validate-adjusted-history": "livewire_scripts.validate_adjusted_history",
    "weekly": "livewire_scripts.weekly_quality_summary",
    "watchdog": "livewire_scripts.check_daily_update_watchdog",
    "warehouse": "livewire_scripts.warehouse_health_report",
    "digest": "livewire_scripts.nightly_digest",
}


def _dispatch_module(module_name: str, argv: Sequence[str], display_name: str) -> int:
    module = importlib.import_module(module_name)
    original_argv = sys.argv
    sys.argv = [display_name, *argv]
    try:
        signature = inspect.signature(module.main)
        result = module.main(list(argv)) if signature.parameters else module.main()
    finally:
        sys.argv = original_argv
    return int(result or 0)


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Livewire quality commands")
    parser.add_argument("command", choices=COMMANDS.keys(), help="Quality command to run")
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0
    args = parser.parse_args(argv[:1])
    rest = argv[1:]
    if args.command in {"watchdog", "coverage", "health"}:
        # All three are launched cold by launchd; the remaining quality
        # commands inherit env from a scheduled parent job. Without this,
        # coverage resolves MASSIVE_API_KEY and the SMTP credentials to nothing
        # — it would measure the gap and then be unable to recover it or say so.
        # `health` joined the list when the interior gap scan left the daily
        # job's tail for com.livewire.interior-gap-scan: it used to inherit the
        # scheduled parent's env and now has no parent, so MDW_DATA_LAKE_DIR /
        # MDW_LOG_DIR would resolve to defaults that may not be this warehouse.
        load_scheduled_env(REPO_ROOT)
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_quality.py {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
