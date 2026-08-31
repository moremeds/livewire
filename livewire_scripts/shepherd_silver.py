#!/usr/bin/env python3
"""Publish and independently verify PIT lineage over canonical Silver bars."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from clients.pit_silver_revision import PitSilverRevisionPublisher, daily_bar_cutoff
from clients.silver_revision import SilverRevisionPublisher
from livewire_scripts.paths import data_lake_dir
from livewire_scripts.shepherd_actions import export_actions
from livewire_scripts.shepherd_daily import plan_daily


def publish_pit(
    index_id: str,
    membership_revision: int,
    as_of: datetime,
    *,
    data_lake_root: Path,
) -> dict[str, Any]:
    root = Path(data_lake_root).expanduser()
    daily = plan_daily(index_id, membership_revision, daily_bar_cutoff(as_of), data_lake_root=root)
    symbols = sorted({unit["symbol"] for unit in daily["workUnits"]})
    silver = SilverRevisionPublisher(root / "silver").read_current()
    if silver is None:
        raise ValueError("missing current Silver revision")
    actions = export_actions(symbols, silver.corporate_actions_as_of, data_lake_root=root)
    revision = PitSilverRevisionPublisher(root).publish(
        index_id=index_id,
        membership_revision=membership_revision,
        as_of=as_of,
        actions_receipt=actions,
    )
    return {
        "version": 1,
        "operation": "shepherd-silver-publish",
        "revision": revision.revision,
        "status": revision.status,
        "inputHash": revision.input_hash,
        "manifestPath": str(revision.manifest_path),
        "actionSummary": actions["summary"],
        "changedPaths": [str(path) for path in revision.changed_paths],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-root", type=Path, default=data_lake_dir())
    sub = parser.add_subparsers(dest="command", required=True)
    publish = sub.add_parser("publish")
    publish.add_argument("--index", choices=("sp500", "ndx100"), required=True)
    publish.add_argument("--membership-revision", type=int, required=True)
    publish.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--manifest", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = Path(args.data_lake_root).expanduser()
    if args.command == "publish":
        result = publish_pit(
            args.index,
            args.membership_revision,
            args.as_of,
            data_lake_root=root,
        )
    else:
        if args.manifest is not None and not args.manifest.is_absolute():
            raise ValueError("PIT Silver manifest path must be absolute")
        result = PitSilverRevisionPublisher(root).verify(args.manifest)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
