"""Polygon S3 flat file client for minute aggregate bulk downloads.

Polygon publishes per-day gzipped CSVs at:
  s3://flatfiles/us_stocks_sip/minute_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz

Each file contains 1m bars for ALL U.S. equities on that trading day.
This client downloads, parses, filters to target tickers, and deletes
the local CSV after successful parse.
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger("livewire.massive_flatfile")

S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET = "flatfiles"
S3_PREFIX = "us_stocks_sip/minute_aggs_v1"


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


def parse_flatfile_csv(
    csv_gz_bytes: bytes,
    *,
    target_tickers: set[str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Parse a gzipped Polygon minute aggregate CSV.

    Returns {ticker: [row_dicts]} where each row dict has keys matching
    IntradayBronzeClient's expected schema (bar_timestamp, symbol_id=0,
    open, high, low, close, volume). symbol_id is set to 0 here —
    the caller assigns the real ID before merging.
    """
    raw = gzip.decompress(csv_gz_bytes)
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))

    result: dict[str, list[dict[str, Any]]] = {}
    for row in reader:
        ticker = row["ticker"]
        if target_tickers is not None and ticker not in target_tickers:
            continue

        ns = int(row["window_start"])
        ts = datetime.fromtimestamp(ns / 1_000_000_000, tz=UTC)

        bar = {
            "bar_timestamp": ts,
            "symbol_id": 0,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
        }
        result.setdefault(ticker, []).append(bar)

    return result


class MassiveFlatfileClient:
    """S3 client for Polygon minute aggregate flat files.

    Downloads each day's CSV to a temp file, parses it, then deletes
    the temp file — no large allocations held across dates.
    """

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

    def download_date(
        self,
        d: date,
        *,
        target_tickers: set[str] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Download one day's minute aggregates, parse, and clean up.

        Downloads the gzipped CSV to a temp file, parses it into per-ticker
        row dicts, then deletes the temp file regardless of success/failure.
        """
        key = _s3_key_for_date(d)
        log.info("Downloading s3://%s/%s", S3_BUCKET, key)

        tmp_path: Path | None = None
        try:
            fd, tmp_name = tempfile.mkstemp(suffix=".csv.gz", prefix=f"polygon_{d}_")
            tmp_path = Path(tmp_name)
            with os.fdopen(fd, "wb") as fh:
                self._s3.download_fileobj(S3_BUCKET, key, fh)

            csv_gz_bytes = tmp_path.read_bytes()
            result = parse_flatfile_csv(csv_gz_bytes, target_tickers=target_tickers)
            log.info(
                "Parsed %s: %d tickers, %d total bars",
                d.isoformat(),
                len(result),
                sum(len(rows) for rows in result.values()),
            )
            return result
        finally:
            if tmp_path is not None and tmp_path.exists():
                tmp_path.unlink()
                log.debug("Deleted temp file %s", tmp_path)

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
        with destination.open("wb") as fh:
            self._s3.download_fileobj(S3_BUCKET, _s3_key_for_date(d), fh)
        return destination
