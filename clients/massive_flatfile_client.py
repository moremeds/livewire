"""Massive S3 flat-file client for whole-market minute aggregate downloads.

Polygon publishes per-day gzipped CSVs at:
  s3://flatfiles/us_stocks_sip/minute_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz

Each file contains 1m bars for all U.S. equities on that trading day.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger("livewire.massive_flatfile")

S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET = "flatfiles"
S3_PREFIX = "us_stocks_sip/minute_aggs_v1"


def require_flatfile_credentials() -> None:
    missing = [name for name in ("MASSIVE_S3_ACCESS_KEY", "MASSIVE_S3_SECRET_KEY") if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required Massive S3 credentials: {', '.join(missing)}")


def _s3_key_for_date(d: date) -> str:
    return f"{S3_PREFIX}/{d.year}/{d.month:02d}/{d.isoformat()}.csv.gz"


class FlatfileObjectStatus(StrEnum):
    AVAILABLE = "available"
    NOT_FOUND = "not_found"
    FORBIDDEN = "forbidden"
    TRANSIENT_ERROR = "transient_error"


@dataclass(frozen=True)
class FlatfileObjectInfo:
    date: date
    key: str
    status: FlatfileObjectStatus
    size_bytes: int | None = None
    etag: str | None = None
    error: str | None = None


def normalize_object_key(key: str) -> str:
    """Return an object key without a leading bucket-name prefix."""
    prefix = f"{S3_BUCKET}/"
    return key[len(prefix) :] if key.startswith(prefix) else key


class MassiveFlatfileClient:
    """S3 client for Massive whole-market minute aggregate flat files."""

    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        *,
        _s3_client: Any | None = None,
    ):
        if _s3_client is not None:
            self._s3 = _s3_client
            return
        import boto3  # pragma: no cover

        ak = access_key or os.environ["MASSIVE_S3_ACCESS_KEY"]  # pragma: no cover
        sk = secret_key or os.environ["MASSIVE_S3_SECRET_KEY"]  # pragma: no cover
        session = boto3.Session(aws_access_key_id=ak, aws_secret_access_key=sk)
        self._s3 = session.client(  # pragma: no cover
            "s3",
            endpoint_url=S3_ENDPOINT,
            region_name="us-east-1",
            config=Config(signature_version="s3v4"),
        )

    def close(self) -> None:
        pass

    def __enter__(self) -> MassiveFlatfileClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def inspect_date(self, d: date) -> FlatfileObjectInfo:
        key = _s3_key_for_date(d)
        try:
            result = self._s3.head_object(Bucket=S3_BUCKET, Key=key)
            return FlatfileObjectInfo(
                date=d,
                key=key,
                status=FlatfileObjectStatus.AVAILABLE,
                size_bytes=int(result["ContentLength"]),
                etag=str(result.get("ETag", "")).strip('"') or None,
            )
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = (
                FlatfileObjectStatus.NOT_FOUND
                if code in {"404", "NoSuchKey", "NotFound"}
                else FlatfileObjectStatus.FORBIDDEN
                if code in {"401", "403", "AccessDenied", "Forbidden"}
                else FlatfileObjectStatus.TRANSIENT_ERROR
            )
            return FlatfileObjectInfo(date=d, key=key, status=status, error=code)

    def list_objects(self, prefix: str = S3_PREFIX) -> list[dict[str, Any]]:
        """List every object under *prefix* using pagination."""
        items: list[dict[str, Any]] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": S3_BUCKET, "Prefix": normalize_object_key(prefix), "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            result = self._s3.list_objects_v2(**kwargs)
            items.extend(result.get("Contents", []))
            if not result.get("IsTruncated"):
                return items
            token = result["NextContinuationToken"]

    def download_date_to_path(self, d: date, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open("wb") as fh:
                self._s3.download_fileobj(S3_BUCKET, _s3_key_for_date(d), fh)
        except Exception:
            destination.unlink(missing_ok=True)
            raise
        return destination
