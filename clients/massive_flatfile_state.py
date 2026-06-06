"""Durable resume state for full-market Massive flat-file ingestion."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any


class MassiveFlatfileState:
    def __init__(self, cursor_dir: Path):
        self.cursor_dir = cursor_dir
        self.manifest_path = cursor_dir / "massive_flatfile_manifest.jsonl"
        self.state_path = cursor_dir / "massive_flatfile_state.json"
        self.data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            data = {}
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed Massive flat-file state: {self.state_path}") from exc
        data.setdefault("raw_completed", [])
        data.setdefault("publish_scopes", {})
        return data

    def record(self, event: str, **fields: Any) -> None:
        self.cursor_dir.mkdir(parents=True, exist_ok=True)
        payload = {"ts": datetime.now(UTC).isoformat(), "event": event, **fields}
        with self.manifest_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True) + "\n")

    def save(self) -> None:
        self.cursor_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.state_path)

    def mark_raw_completed(self, day: date | str) -> None:
        value = day.isoformat() if hasattr(day, "isoformat") else str(day)
        completed = set(self.data.setdefault("raw_completed", []))
        completed.add(value)
        self.data["raw_completed"] = sorted(completed)
        self.record("raw_completed", date=value)
        self.save()

    def raw_completed(self, day: date | str) -> bool:
        value = day.isoformat() if hasattr(day, "isoformat") else str(day)
        return value in self.data.get("raw_completed", [])

    def mark_raw_failed(self, day: date | str, error: str) -> None:
        value = day.isoformat() if hasattr(day, "isoformat") else str(day)
        self.record("raw_failed", date=value, error=error)

    def mark_raw_unavailable(self, day: date | str, error: str) -> None:
        value = day.isoformat() if hasattr(day, "isoformat") else str(day)
        self.record("raw_unavailable", date=value, error=error)

    def set_discovery(self, *, earliest: date, latest: date, object_count: int, compressed_bytes: int) -> None:
        self.data["discovery"] = {
            "earliest": earliest.isoformat(),
            "latest": latest.isoformat(),
            "object_count": object_count,
            "compressed_bytes": compressed_bytes,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self.record("discovery_completed", **self.data["discovery"])
        self.save()

    def _scope(self, scope: str) -> dict[str, Any]:
        scopes = self.data.setdefault("publish_scopes", {})
        return scopes.setdefault(scope, {"buckets_completed": [], "tickers_completed": {}})

    def bucket_completed(self, scope: str, bucket: int) -> bool:
        return bucket in self._scope(scope)["buckets_completed"]

    def ticker_completed(self, scope: str, bucket: int, ticker: str) -> bool:
        return ticker in self._scope(scope)["tickers_completed"].get(str(bucket), [])

    def mark_ticker_completed(self, scope: str, bucket: int, ticker: str) -> None:
        per_bucket = self._scope(scope)["tickers_completed"].setdefault(str(bucket), [])
        if ticker not in per_bucket:
            per_bucket.append(ticker)
            per_bucket.sort()
            self.record("ticker_completed", scope=scope, bucket=bucket, ticker=ticker)
            self.save()

    def mark_bucket_completed(self, scope: str, bucket: int) -> None:
        completed = self._scope(scope)["buckets_completed"]
        if bucket not in completed:
            completed.append(bucket)
            completed.sort()
            self.record("bucket_completed", scope=scope, bucket=bucket)
            self.save()

    def reset_publish_scope(self, scope: str) -> None:
        if self.data.setdefault("publish_scopes", {}).pop(scope, None) is not None:
            self.record("publish_scope_reset", scope=scope)
            self.save()
