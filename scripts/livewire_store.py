#!/usr/bin/env python3
"""Livewire storage command surface."""

from __future__ import annotations

import argparse
import importlib
import inspect
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

COMMANDS = {
    "rebuild-postgres": "livewire_scripts.rebuild_postgres_from_parquet",
    "smoke-postgres": "livewire_scripts.smoke_postgres_analytical",
    "sync-r2": "livewire_scripts.sync_to_r2",
    "migrate-parquet": "livewire_scripts.migrate_parquet_filename",
    "migrate-price-basis": "livewire_scripts.migrate_equity_price_basis",
    "repair-split-basis": "livewire_scripts.repair_split_basis",
    "repair-legacy-basis": "livewire_scripts.repair_legacy_basis",
    "resolve-yahoo-basis": "livewire_scripts.resolve_yahoo_basis",
    "rollback-legacy-basis": "livewire_scripts.rollback_legacy_basis",
    "archive-otc": "livewire_scripts.archive_otc_symbols",
    "rebuild-silver": "livewire_scripts.rebuild_silver",
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
    parser = argparse.ArgumentParser(description="Livewire storage commands")
    parser.add_argument("command", choices=COMMANDS.keys(), help="Storage command to run")
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0
    args = parser.parse_args(argv[:1])
    rest = argv[1:]
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_store.py {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
