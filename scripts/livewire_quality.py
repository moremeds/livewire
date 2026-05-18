#!/usr/bin/env python3
"""Livewire quality and monitoring command surface."""

from __future__ import annotations

import argparse
import importlib
import inspect
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

COMMANDS = {
    "health": "livewire_scripts.health_check",
    "coverage": "livewire_scripts.coverage_report",
    "report": "livewire_scripts.data_quality_report",
    "weekly": "livewire_scripts.weekly_quality_summary",
    "watchdog": "livewire_scripts.check_daily_update_watchdog",
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
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_quality.py {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
