#!/usr/bin/env python3
"""Livewire ingestion command surface."""

from __future__ import annotations

import argparse
import importlib
import inspect
import subprocess
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

COMMANDS = {
    "daily": "livewire_scripts.daily_update",
    "historical": "livewire_scripts.fetch_ib_historical",
    "robust": "livewire_scripts.run_ib_fetch_robust",
    "cboe-vol": "livewire_scripts.fetch_cboe_volatility",
    "intraday-backfill": "livewire_scripts.backfill_intraday",
    "intraday-status": "livewire_scripts.intraday_update",
    "probe-intraday": "livewire_scripts.probe_ib_intraday",
    "universe": "livewire_scripts.universe_screener",
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


def _dispatch_backfill_all(argv: Sequence[str]) -> int:
    if argv:
        raise SystemExit("backfill-all does not accept arguments")
    return subprocess.call(["bash", str(REPO_ROOT / "tools" / "run_backfill_all.sh")])


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Livewire ingestion commands")
    parser.add_argument(
        "command",
        choices=[*COMMANDS.keys(), "backfill-all"],
        help="Ingestion command to run",
    )
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0
    args = parser.parse_args(argv[:1])
    rest = argv[1:]

    if args.command == "backfill-all":
        return _dispatch_backfill_all(rest)
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_ingest.py {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
