"""Restore bronze parquet saved by ``repair-legacy-basis`` before it mutated them.

Bronze is the system of record and the repair overwrites rows in place, so the
pre-repair bytes exist only in the batch's ``backup/`` directory. Restoring
verifies both the backup checksum and the active data-lake root before writing —
the same contract the repair enforces.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Sequence
from pathlib import Path

from clients.bronze_client import BronzeClient
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
    wanted = {t.upper() for t in args.tickers} if args.tickers else None
    restored: list[str] = []
    missing: list[str] = []
    for sidecar_path in sorted((args.output_dir / "symbols").glob("*.json")):
        sidecar = json.loads(sidecar_path.read_text())
        symbol = sidecar.get("symbol")
        # "in_progress" means the repair recorded its write-ahead intent and then died
        # before its terminal sidecar — bronze may or may not have been mutated, and
        # only this restore can tell the difference. Skipping it would strand exactly
        # the mutation that most needs undoing. Restoring an unmutated symbol just
        # rewrites identical bytes.
        if sidecar.get("status") not in ("done", "in_progress"):
            continue
        if wanted is not None and symbol not in wanted:
            continue
        backup_path = Path(sidecar.get("backup_path", ""))
        if not backup_path.is_file():
            missing.append(symbol)
            continue
        payload = backup_path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != sidecar["backup_sha256"]:
            raise ValueError(f"backup checksum mismatch for {symbol}: refusing to restore")
        destination = bronze.symbol_path(symbol)
        temporary = destination.with_name(f".{destination.name}.rollback.{os.getpid()}.tmp")
        try:
            temporary.write_bytes(payload)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        restored.append(symbol)
    print(json.dumps({"restored": len(restored), "missing_backup": sorted(missing)}, sort_keys=True))
    return 0 if not missing else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
