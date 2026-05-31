"""Per-ticker tag registry — the single source of truth for index membership.

Stores per-ticker tags (sp500, ndx100, r2k, interest, delisted) with status
and a capped append-only changelog. Lives at ~/market-warehouse/registry.json.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

log = logging.getLogger(__name__)

_CHANGELOG_CAP = 500


@dataclass
class RegistryEntry:
    tags: set[str] = field(default_factory=set)
    status: str = "active"
    added_at: str | None = None
    last_verified: str | None = None
    delisted_at: str | None = None
    earliest_available: str | None = None
    earliest_source: str | None = None


@dataclass(frozen=True)
class ChangelogEntry:
    date: str
    type: str
    ticker: str
    from_tags: list[str]
    to_tags: list[str]


class TagRegistry:
    def __init__(self, path: Path):
        self._path = Path(path)
        self._entries: dict[str, RegistryEntry] = {}
        self.changelog: list[ChangelogEntry] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            with self._path.open() as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            log.warning("Corrupted registry at %s, starting fresh", self._path)
            return
        for ticker, entry_data in data.get("tickers", {}).items():
            self._entries[ticker] = RegistryEntry(
                tags=set(entry_data.get("tags", [])),
                status=entry_data.get("status", "active"),
                added_at=entry_data.get("added_at"),
                last_verified=entry_data.get("last_verified"),
                delisted_at=entry_data.get("delisted_at"),
                earliest_available=entry_data.get("earliest_available"),
                earliest_source=entry_data.get("earliest_source"),
            )
        for cl in data.get("changelog", []):
            self.changelog.append(
                ChangelogEntry(
                    date=cl["date"],
                    type=cl["type"],
                    ticker=cl["ticker"],
                    from_tags=cl.get("from_tags", []),
                    to_tags=cl.get("to_tags", []),
                )
            )

    def save(self) -> None:
        import tempfile

        self._path.parent.mkdir(parents=True, exist_ok=True)
        tickers_data = {}
        for ticker, entry in sorted(self._entries.items()):
            d = {
                "tags": sorted(entry.tags),
                "status": entry.status,
                "added_at": entry.added_at,
                "last_verified": entry.last_verified,
            }
            if entry.delisted_at is not None:
                d["delisted_at"] = entry.delisted_at
            if entry.earliest_available is not None:
                d["earliest_available"] = entry.earliest_available
            if entry.earliest_source is not None:
                d["earliest_source"] = entry.earliest_source
            tickers_data[ticker] = d
        trimmed = self.changelog[-_CHANGELOG_CAP:]
        data = {
            "version": 1,
            "updated_at": datetime.now(UTC).isoformat(),
            "tickers": tickers_data,
            "changelog": [
                {
                    "date": cl.date,
                    "type": cl.type,
                    "ticker": cl.ticker,
                    "from_tags": cl.from_tags,
                    "to_tags": cl.to_tags,
                }
                for cl in trimmed
            ],
        }
        fd, tmp = tempfile.mkstemp(dir=str(self._path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, str(self._path))
        except BaseException:  # pragma: no cover
            os.unlink(tmp)
            raise
        self.changelog = trimmed

    def get(self, ticker: str) -> RegistryEntry | None:
        return self._entries.get(ticker)

    def all_tickers(self) -> set[str]:
        return set(self._entries.keys())

    def set_tags(self, ticker: str, tags: set[str], status: str = "active") -> None:
        today = date.today().isoformat()
        existing = self._entries.get(ticker)
        self._entries[ticker] = RegistryEntry(
            tags=set(tags),
            status=status,
            added_at=existing.added_at if existing else today,
            last_verified=today,
            delisted_at=existing.delisted_at if existing else None,
            earliest_available=existing.earliest_available if existing else None,
            earliest_source=existing.earliest_source if existing else None,
        )

    def add_tag(self, ticker: str, tag: str) -> None:
        entry = self._entries.get(ticker)
        if entry:
            entry.tags.add(tag)
        else:
            self.set_tags(ticker, {tag})

    def remove_tag(self, ticker: str, tag: str) -> None:
        entry = self._entries.get(ticker)
        if entry:
            entry.tags.discard(tag)

    def by_tag(self, tag: str, active_only: bool = True) -> set[str]:
        result: set[str] = set()
        for ticker, entry in self._entries.items():
            if tag in entry.tags:
                if active_only and entry.status != "active":
                    continue
                result.add(ticker)
        return result

    def by_tags(self, tags: set[str], active_only: bool = True) -> set[str]:
        result: set[str] = set()
        for ticker, entry in self._entries.items():
            if tags.issubset(entry.tags):
                if active_only and entry.status != "active":
                    continue
                result.add(ticker)
        return result

    def mark_delisted(self, ticker: str, delisted_at: str | None = None) -> None:
        entry = self._entries.get(ticker)
        if entry:
            entry.status = "delisted"
            entry.delisted_at = delisted_at or date.today().isoformat()

    def set_earliest(self, ticker: str, date_str: str, source: str = "ib") -> None:
        entry = self._entries.get(ticker)
        if entry:
            entry.earliest_available = date_str
            entry.earliest_source = source

    def log_change(self, type_: str, ticker: str, from_tags: list[str], to_tags: list[str]) -> None:
        self.changelog.append(
            ChangelogEntry(
                date=date.today().isoformat(),
                type=type_,
                ticker=ticker,
                from_tags=from_tags,
                to_tags=to_tags,
            )
        )
