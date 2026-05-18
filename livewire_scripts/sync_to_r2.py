#!/usr/bin/env python3
"""Sync bronze Parquet files to/from Cloudflare R2 (S3-compatible).

Usage:
    python scripts/sync_to_r2.py --upload          # Push local bronze → R2
    python scripts/sync_to_r2.py --download        # Pull R2 → local bronze
    python scripts/sync_to_r2.py --upload --dry-run # Show what would be uploaded
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

PARQUET_FILES_TO_SYNC = ("1d.parquet", "1h.parquet", "5m.parquet")

logger = logging.getLogger("mdw.sync_to_r2")


def _get_s3_client():
    """Create a boto3 S3 client configured for Cloudflare R2."""
    import boto3

    endpoint_url = os.environ["R2_ENDPOINT_URL"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )


def _get_bucket() -> str:
    return os.getenv("R2_BUCKET", "market-data")


def upload(bronze_dir: Path, prefix: str = "bronze", dry_run: bool = False) -> int:
    """Upload local bronze Parquet files to R2.

    Returns the number of files uploaded.
    """
    if not bronze_dir.exists():
        logger.warning("Bronze dir %s does not exist, nothing to upload", bronze_dir)
        return 0

    s3 = _get_s3_client()
    bucket = _get_bucket()
    uploaded = 0

    for parquet_filename in PARQUET_FILES_TO_SYNC:
        for parquet_file in bronze_dir.rglob(parquet_filename):
            rel_path = parquet_file.relative_to(bronze_dir.parent)
            s3_key = str(rel_path).replace("\\", "/")  # Windows compat

            if dry_run:
                logger.info("[DRY RUN] Would upload %s → s3://%s/%s", parquet_file, bucket, s3_key)
            else:
                logger.info("Uploading %s → s3://%s/%s", parquet_file, bucket, s3_key)
                s3.upload_file(str(parquet_file), bucket, s3_key)

            uploaded += 1

    logger.info("Upload complete: %d files %s", uploaded, "(dry run)" if dry_run else "")
    return uploaded


def download(bronze_dir: Path, prefix: str = "bronze", dry_run: bool = False) -> int:
    """Download Parquet files from R2 to local bronze directory.

    Returns the number of files downloaded.
    """
    s3 = _get_s3_client()
    bucket = _get_bucket()
    downloaded = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]
            if not any(s3_key.endswith(name) for name in PARQUET_FILES_TO_SYNC):
                continue

            local_path = bronze_dir.parent / s3_key.replace("/", os.sep)

            if dry_run:
                logger.info("[DRY RUN] Would download s3://%s/%s → %s", bucket, s3_key, local_path)
            else:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info("Downloading s3://%s/%s → %s", bucket, s3_key, local_path)
                s3.download_file(bucket, s3_key, str(local_path))

            downloaded += 1

    logger.info("Download complete: %d files %s", downloaded, "(dry run)" if dry_run else "")
    return downloaded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync bronze Parquet to/from Cloudflare R2")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--upload", action="store_true", help="Push local bronze → R2")
    group.add_argument("--download", action="store_true", help="Pull R2 → local bronze")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be synced")
    parser.add_argument(
        "--data-lake",
        type=Path,
        default=Path(os.getenv("MDW_DATA_LAKE", str(Path.home() / "market-warehouse" / "data-lake"))),
        help="Data lake root directory",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bronze_dir = args.data_lake / "bronze"

    if args.upload:
        count = upload(bronze_dir, dry_run=args.dry_run)
    else:
        count = download(bronze_dir, dry_run=args.dry_run)

    return 0 if count >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
