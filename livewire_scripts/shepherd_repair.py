#!/usr/bin/env python3
"""Run exact, staged, reversible Shepherd repairs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from clients.shepherd_repair import ShepherdRepair
from livewire_scripts.paths import data_lake_dir


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-root", type=Path, default=data_lake_dir())
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("preflight", "stage", "transaction"):
        command = commands.add_parser(name)
        command.add_argument("--manifest", type=Path, required=True)
    for name in ("publish", "verify", "rollback"):
        command = commands.add_parser(name)
        command.add_argument("--manifest", type=Path, required=True)
        command.add_argument(
            "--staged-receipt" if name == "publish" else "--publish-receipt",
            type=Path,
            required=True,
        )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.manifest.is_absolute():
        raise ValueError("repair manifest path must be absolute")
    repair = ShepherdRepair(args.data_lake_root)
    if args.command == "preflight":
        result = repair.preflight(args.manifest)
    elif args.command == "stage":
        result = repair.stage(args.manifest)
    elif args.command == "transaction":
        result = repair.transaction(args.manifest)
    elif args.command == "publish":
        result = repair.publish(args.manifest, args.staged_receipt)
    elif args.command == "verify":
        result = repair.verify(args.manifest, args.publish_receipt)
    else:
        result = repair.rollback(args.manifest, args.publish_receipt)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("state") != "ROLLED_BACK" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
