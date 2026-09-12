#!/usr/bin/env python3
"""Livewire operational command surface."""

from __future__ import annotations

import argparse
import importlib
import inspect
import os  # noqa: F401  (re-export for backwards-compatible tests)
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from livewire_scripts.scheduled_env import (  # noqa: F401  (re-export for backwards-compatible tests)
    _load_env_file,
    load_scheduled_env,
)

COMMANDS = {
    "run-daily-job": "livewire_scripts.run_daily_update_job",
    "run-intraday-catchup-job": "livewire_scripts.run_intraday_catchup_job",
    "release": "livewire_scripts.release",
    "housekeeping": "livewire_scripts.housekeeping",
    "ledger": "livewire_scripts.ledger_cli",
    "status": "livewire_scripts.status",
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


def _dispatch_notify(argv: Sequence[str]) -> int:
    """Send one notice through notify.send; the executions row is the receipt."""
    from livewire_scripts import notify

    parser = argparse.ArgumentParser(prog="livewire_ops.py notify")
    parser.add_argument("--kind", choices=notify.KINDS, required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--body-file", type=Path, required=True)
    parser.add_argument("--force", action="store_true", help="Bypass the 24h fingerprint dedup")
    args = parser.parse_args(list(argv))
    notice = notify.Notice(
        args.kind,
        args.subject,
        args.body_file.read_text(encoding="utf-8"),
        notify.fingerprint(args.kind, [args.subject]),
    )
    return notify.send(notice, force=args.force)


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Livewire operational commands")
    parser.add_argument(
        "command",
        choices=[*COMMANDS.keys(), "notify"],
        help="Operational command to run",
    )
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0
    args = parser.parse_args(argv[:1])
    rest = argv[1:]

    if args.command == "notify":
        return _dispatch_notify(rest)
    if args.command in {"run-daily-job", "run-intraday-catchup-job"}:
        load_scheduled_env(REPO_ROOT)
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_ops.py {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
