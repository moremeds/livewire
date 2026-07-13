#!/usr/bin/env python3
"""Apply or roll back an approved split-basis audit manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from collections.abc import Sequence
from pathlib import Path

from clients.bronze_client import BronzeClient
from clients.parquet_io import symbol_lock
from livewire_scripts.paths import data_lake_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--approve", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    parser.add_argument("--data-lake-root", type=Path)
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _restore_exact(backup: Path, target: Path) -> None:
    temp = target.with_name(f".{target.name}.{os.getpid()}.rollback.tmp")
    try:
        shutil.copyfile(backup, temp)
        os.replace(temp, target)
    finally:
        temp.unlink(missing_ok=True)


def run(argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else args.data_lake_root or data_lake_dir()
    root = root.resolve()
    bronze_root = (root / "bronze/asset_class=equity").resolve()
    manifest_path = args.manifest.resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError("unsupported manifest schema")
    manifest_root = Path(payload.get("data_lake_root", "")).resolve()
    if manifest_root != root:
        raise ValueError(f"manifest data-lake root {manifest_root} does not match active root {root}")
    client = BronzeClient(bronze_root, "equity")
    for item in payload["symbols"]:
        if not item.get("eligible"):
            raise ValueError(f"{item['symbol']}: manifest item must be eligible and approved")
        if not item.get("approved") and not args.approve:
            raise ValueError(f"{item['symbol']}: manifest item must be eligible and approved")
        target = Path(item["path"]).resolve()
        if not target.is_relative_to(bronze_root):
            raise ValueError("manifest target is outside equity Bronze")
        if args.rollback:
            backup = Path(item.get("backup_path", "")).resolve()
            if not backup.is_file():
                raise ValueError(f"{item['symbol']}: rollback backup is missing")
            if _sha256(target) != item.get("applied_sha256"):
                raise ValueError(f"{item['symbol']}: stale target blocks rollback")
        elif _sha256(target) != item["source_sha256"]:
            raise ValueError(f"{item['symbol']}: stale manifest source hash")
    changed = 0
    for item in payload["symbols"]:
        if not item.get("approved"):
            item["approved"] = True
        target = Path(item["path"]).resolve()
        with symbol_lock(target):
            if args.rollback:
                backup = Path(item.get("backup_path", "")).resolve()
                if not backup.is_file():
                    raise ValueError(f"{item['symbol']}: rollback backup is missing")
                if _sha256(target) != item.get("applied_sha256"):
                    raise ValueError(f"{item['symbol']}: stale target blocks rollback")
                _restore_exact(backup, target)
                if _sha256(target) != item["source_sha256"]:
                    raise ValueError(f"{item['symbol']}: rollback hash mismatch")
            else:
                if _sha256(target) != item["source_sha256"]:
                    raise ValueError(f"{item['symbol']}: stale manifest source hash")
                backup = manifest_path.with_name(f"{item['symbol']}.{item['source_sha256']}.parquet.bak")
                if not backup.exists():
                    shutil.copyfile(target, backup)
                rows = client.read_symbol_rows(item["symbol"])
                by_date = {row["trade_date"]: row for row in rows}
                for replacement in item["replacements"]:
                    by_date[replacement["trade_date"]] = replacement["proposed"]
                normalized = client._normalize_rows(list(by_date.values()), item["symbol"])
                client._publish_symbol_rows(item["symbol"], normalized)
                item["backup_path"] = str(backup)
                item["applied_sha256"] = _sha256(target)
            changed += 1
    _write_atomic(manifest_path, payload)
    print(json.dumps({"changed": changed, "rollback": bool(args.rollback)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
