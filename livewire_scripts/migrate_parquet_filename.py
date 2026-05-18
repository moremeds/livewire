"""Migrate parquet filenames from data.parquet to 1d.parquet.

Usage:
    python scripts/migrate_parquet_filename.py                    # Migrate
    python scripts/migrate_parquet_filename.py --dry-run          # Preview only
    python scripts/migrate_parquet_filename.py --dir /custom/path # Custom directory
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def migrate_parquet_files(
    root_dir: Path,
    dry_run: bool = False,
) -> dict[str, int]:
    """Rename all data.parquet → 1d.parquet under *root_dir*.

    Returns ``{"renamed": N, "skipped": N, "errors": N}``.

    Raises ``RuntimeError`` if both ``data.parquet`` and ``1d.parquet``
    exist in the same directory (split-brain state).
    """
    stats = {"renamed": 0, "skipped": 0, "errors": 0}

    if not root_dir.exists():
        return stats

    for old_path in sorted(root_dir.rglob("data.parquet")):
        new_path = old_path.with_name("1d.parquet")

        if new_path.exists():
            raise RuntimeError(
                f"split-brain: both data.parquet and 1d.parquet exist in {old_path.parent}. "
                "Manual investigation required."
            )

        if dry_run:
            print(f"[DRY RUN] Would rename: {old_path} → {new_path}")
            stats["renamed"] += 1
        else:
            os.rename(old_path, new_path)
            print(f"Renamed: {old_path} → {new_path}")
            stats["renamed"] += 1

    return stats


def main():
    default_warehouse = Path.home() / "market-warehouse" / "data-lake"
    parser = argparse.ArgumentParser(
        description="Migrate parquet filenames from data.parquet to 1d.parquet"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview renames without executing"
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help=f"Root directory to migrate (default: {default_warehouse}/bronze and bronze-delisted)",
    )
    args = parser.parse_args()

    if args.dir:
        stats = migrate_parquet_files(args.dir, dry_run=args.dry_run)
        print(f"\nMigration complete: {stats}")
    else:
        total = {"renamed": 0, "skipped": 0, "errors": 0}
        for subdir in ("bronze", "bronze-delisted"):
            target = default_warehouse / subdir
            if target.exists():
                print(f"\n--- Migrating {target} ---")
                stats = migrate_parquet_files(target, dry_run=args.dry_run)
                for k in total:
                    total[k] += stats[k]
        print(f"\nTotal: {total}")


if __name__ == "__main__":
    main()
