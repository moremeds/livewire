#!/usr/bin/env python3
"""Apply or roll back an approved split-basis audit manifest."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from clients.bronze_client import BronzeClient
from clients.parquet_io import path_lock, publish_parquet, restore_parquet_exact, symbol_lock, write_json_atomic
from clients.source_evidence import sha256_file
from clients.symbol_paths import encode_symbol
from livewire_scripts.paths import data_lake_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--approve", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    parser.add_argument("--data-lake-root", type=Path)
    return parser.parse_args(list(argv) if argv is not None else None)


def run(argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else args.data_lake_root or data_lake_dir()
    root = root.resolve()
    bronze_root = (root / "bronze/asset_class=equity").resolve()
    manifest_path = args.manifest.resolve()
    with path_lock(manifest_path.with_suffix(manifest_path.suffix + ".lock")):
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
            if target != client.symbol_path(item["symbol"]).resolve():
                raise ValueError("manifest target does not match symbol")
            current_hash = sha256_file(target)
            if current_hash not in {item["source_sha256"], item.get("applied_sha256")}:
                raise ValueError(f"{item['symbol']}: stale target blocks {'rollback' if args.rollback else 'apply'}")
            if args.rollback and current_hash != item["source_sha256"]:
                if not Path(item.get("backup_path", "")).is_file():
                    raise ValueError(f"{item['symbol']}: rollback backup is missing")
        changed = 0
        for item in payload["symbols"]:
            if not item.get("approved"):
                item["approved"] = True
            target = Path(item["path"]).resolve()
            with symbol_lock(target):
                current_hash = sha256_file(target)
                if current_hash not in {item["source_sha256"], item.get("applied_sha256")}:
                    raise ValueError(
                        f"{item['symbol']}: stale target blocks {'rollback' if args.rollback else 'apply'}"
                    )
                if args.rollback:
                    if current_hash != item["source_sha256"]:
                        backup = Path(item.get("backup_path", ""))
                        if not backup.is_file():
                            raise ValueError(f"{item['symbol']}: rollback backup is missing")
                        item["repair_state"] = "rolling_back"
                        write_json_atomic(manifest_path, payload)
                        restore_parquet_exact(backup, target, item["source_sha256"])
                        changed += 1
                    item["repair_state"] = "restored"
                else:
                    backup = Path(
                        item.get("backup_path")
                        or manifest_path.with_name(
                            f"{encode_symbol(item['symbol'])}.{item['source_sha256']}.parquet.bak"
                        )
                    )
                    if backup.exists():
                        if sha256_file(backup) != item["source_sha256"]:
                            raise ValueError(f"{item['symbol']}: original backup checksum mismatch")
                    elif current_hash == item["source_sha256"]:
                        restore_parquet_exact(target, backup, item["source_sha256"])
                    else:
                        raise ValueError(f"{item['symbol']}: applied target has no original backup")
                    item["backup_path"] = str(backup)
                    if current_hash != item.get("applied_sha256"):
                        candidate = manifest_path.with_name(
                            f"{encode_symbol(item['symbol'])}.{item['source_sha256']}.candidate.parquet"
                        )
                        rows = client.read_symbol_rows(item["symbol"])
                        by_date = {row["trade_date"]: row for row in rows}
                        for replacement in item["replacements"]:
                            by_date[replacement["trade_date"]] = replacement["proposed"]
                        normalized = client._normalize_rows(list(by_date.values()), item["symbol"])
                        publish_parquet(candidate, client._table_from_rows(normalized), "trade_date")
                        item["candidate_path"] = str(candidate)
                        item["applied_sha256"] = sha256_file(candidate)
                        item["repair_state"] = "applying"
                        # The exact post-image hash and original backup are durable
                        # before mutation, so either side of a crash is recognizable.
                        write_json_atomic(manifest_path, payload)
                        restore_parquet_exact(candidate, target, item["applied_sha256"])
                        changed += 1
                    item["repair_state"] = "applied"
                # Each completed item survives interruption of a later item.
                write_json_atomic(manifest_path, payload)
        print(json.dumps({"changed": changed, "rollback": bool(args.rollback)}, sort_keys=True))
        return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
