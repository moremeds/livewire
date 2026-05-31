#!/usr/bin/env python3
"""Livewire ingestion command surface."""

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
    "daily": "livewire_scripts.daily_update",
    "historical": "livewire_scripts.fetch_ib_historical",
    "robust": "livewire_scripts.run_ib_fetch_robust",
    "cboe-vol": "livewire_scripts.fetch_cboe_volatility",
    "fred-rates": "livewire_scripts.fetch_fred_rates",
    "intraday-backfill": "livewire_scripts.backfill_intraday",
    "flatfile-ingest": "livewire_scripts.ingest_flatfiles",
    "universe": "livewire_scripts.universe_screener",
}

# Commands that talk to IB Gateway. cboe-vol uses CBOE's public API.
IB_COMMANDS = {
    "daily",
    "historical",
    "robust",
    "intraday-backfill",
    "universe",
    "backfill-all",
    "daily-backfill",
}


def _dispatch_module(module_name: str, argv: Sequence[str], display_name: str) -> int:
    module = importlib.import_module(module_name)
    original_argv = sys.argv
    sys.argv = [display_name, *argv]
    try:
        signature = inspect.signature(module.main)
        try:
            result = module.main(list(argv)) if signature.parameters else module.main()
        except SystemExit as exc:
            if exc.code in (0, None):
                return 0
            raise
    finally:
        sys.argv = original_argv
    return int(result or 0)


def _dispatch_backfill_all(argv: Sequence[str]) -> int:
    return _dispatch_module(
        "livewire_scripts.backfill_runner",
        list(argv),
        "livewire_ingest.py backfill-all",
    )


def _dispatch_daily_backfill(argv: Sequence[str]) -> int:
    return _dispatch_module(
        "livewire_scripts.sync_runner", list(argv), "livewire_ingest.py daily-backfill"
    )


def _arg_value(argv: Sequence[str], flag: str, default: str) -> str:
    """Return a simple string option without owning subcommand parsing."""
    for idx, item in enumerate(argv):
        if item == flag and idx + 1 < len(argv):
            return argv[idx + 1]
        if item.startswith(f"{flag}="):
            return item.split("=", 1)[1]
    return default


def _requires_ib_preflight(command: str, rest: Sequence[str]) -> bool:
    if {"-h", "--help"}.intersection(rest):
        return False
    if command == "daily":
        return _arg_value(rest, "--source", "ib") != "massive"
    if command == "historical":
        source = _arg_value(rest, "--source", "auto")
        asset_class = _arg_value(rest, "--asset-class", "equity")
        is_backfill = "--backfill" in rest
        if source == "massive" and asset_class == "equity":
            return False
        return True
    if command == "intraday-backfill":
        source = _arg_value(rest, "--source", "ib")
        asset_class = _arg_value(rest, "--asset-class", "equity")
        return not (source == "massive" and asset_class == "equity")
    return command in IB_COMMANDS


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Livewire ingestion commands")
    parser.add_argument(
        "command",
        choices=[*COMMANDS.keys(), "backfill-all", "daily-backfill"],
        help="Ingestion command to run",
    )
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0
    args = parser.parse_args(argv[:1])
    rest = argv[1:]

    if _requires_ib_preflight(args.command, rest):
        from clients.ib_gateway_preflight import assert_gateway_up

        assert_gateway_up()

    if args.command == "backfill-all":
        return _dispatch_backfill_all(rest)
    if args.command == "daily-backfill":
        return _dispatch_daily_backfill(rest)
    return _dispatch_module(
        COMMANDS[args.command], rest, f"livewire_ingest.py {args.command}"
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
