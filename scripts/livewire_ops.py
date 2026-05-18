#!/usr/bin/env python3
"""Livewire operational command surface."""

from __future__ import annotations

import argparse
import importlib
import inspect
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

COMMANDS = {
    "run-daily-job": "livewire_scripts.run_daily_update_job",
    "ibc-install": "livewire_scripts.install_ibc_secure_service",
    "ibc-start": "livewire_scripts.start_ibc_gateway_keychain",
}


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        try:
            parsed = shlex.split(value, comments=False, posix=True)
        except ValueError:
            parsed = [value.strip()]
        os.environ[key] = parsed[0] if parsed else ""


def _load_run_daily_env() -> None:
    warehouse = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
    for env_file in (Path.home() / ".secrets", REPO_ROOT / ".env", warehouse / ".env"):
        _load_env_file(env_file.expanduser())


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


def _dispatch_send_alert(argv: Sequence[str]) -> int:
    node_bin = os.getenv("MDW_NODE_BIN", "node")
    script = REPO_ROOT / "livewire_node" / "send_daily_update_failure_email.mjs"
    return subprocess.call([node_bin, str(script), *argv])


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Livewire operational commands")
    parser.add_argument(
        "command",
        choices=[*COMMANDS.keys(), "send-alert"],
        help="Operational command to run",
    )
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0
    args = parser.parse_args(argv[:1])
    rest = argv[1:]

    if args.command == "send-alert":
        return _dispatch_send_alert(rest)
    if args.command == "run-daily-job":
        _load_run_daily_env()
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_ops.py {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
