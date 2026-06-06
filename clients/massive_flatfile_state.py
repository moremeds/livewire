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
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return {"raw_completed": [], "buckets_completed": [], "tickers_completed": {}}

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
