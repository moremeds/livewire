"""Restore bronze parquet saved by ``repair-legacy-basis`` before it mutated them.

Bronze is the system of record and the repair overwrites rows in place, so the
pre-repair bytes exist only in the batch's ``backup/`` directory. Restoring
verifies both the backup checksum and the active data-lake root before writing —
the same contract the repair enforces.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from clients.bronze_client import BronzeClient
from clients.parquet_io import restore_parquet_exact, symbol_lock, write_json_atomic
from clients.source_evidence import sha256_file
from clients.symbol_paths import canonical_symbol
from livewire_scripts.paths import data_lake_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--tickers", nargs="+", help="restore only these symbols (default: every backed-up symbol)")
    return parser.parse_args(list(argv) if argv is not None else None)


def run(argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    cursor_path = args.output_dir / "cursor.json"
    if not cursor_path.is_file():
        raise ValueError(f"no repair cursor in {args.output_dir}")
    identity = json.loads(cursor_path.read_text()).get("identity", {})
    recorded_root = identity.get("data_lake_root")
    # Same contract as the repair: never touch a different lake than the one repaired.
    if recorded_root != str(root.resolve()):
        raise ValueError(f"repair output data_lake_root {recorded_root} does not match active root {root.resolve()}")
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    wanted = {canonical_symbol(t) for t in args.tickers} if args.tickers else None
    restored: list[str] = []
    already_restored: list[str] = []
    missing: list[str] = []
    blocked: list[str] = []
    for sidecar_path in sorted((args.output_dir / "symbols").glob("*.json")):
        sidecar = json.loads(sidecar_path.read_text())
        symbol = sidecar.get("symbol")
        # "in_progress" means publication may or may not have reached Bronze. The
        # recorded source/applied hashes below distinguish restore, no-op, and drift.
        if sidecar.get("status") not in ("done", "in_progress", "rollback_in_progress", "rolled_back"):
            continue
        if wanted is not None and symbol not in wanted:
            continue
        backup_path = Path(sidecar.get("backup_path", ""))
        if not backup_path.is_file():
            missing.append(symbol)
            continue
        backup_sha256 = sidecar.get("backup_sha256")
        if not backup_sha256 or sha256_file(backup_path) != backup_sha256:
            raise ValueError(f"{symbol}: backup checksum mismatch: refusing to restore")
        destination = bronze.symbol_path(symbol)
        with symbol_lock(destination):
            current_sha256 = sha256_file(destination) if destination.is_file() else None
            if current_sha256 == backup_sha256:
                write_json_atomic(sidecar_path, {**sidecar, "status": "rolled_back"})
                already_restored.append(symbol)
                continue
            applied_sha256 = sidecar.get("applied_sha256")
            if not applied_sha256 or current_sha256 != applied_sha256:
                blocked.append(symbol)
                continue
            write_json_atomic(sidecar_path, {**sidecar, "status": "rollback_in_progress"})
            restore_parquet_exact(backup_path, destination, backup_sha256)
            write_json_atomic(sidecar_path, {**sidecar, "status": "rolled_back"})
        restored.append(symbol)
    print(
        json.dumps(
            {
                "restored": len(restored),
                "already_restored": len(already_restored),
                "missing_backup": sorted(missing),
                "blocked": sorted(blocked),
            },
            sort_keys=True,
        )
    )
    return 0 if not missing and not blocked else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
