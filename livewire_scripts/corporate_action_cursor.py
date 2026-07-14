"""Scope-safe durable cursor state for corporate-action reconciliation."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from clients.symbol_paths import canonical_symbol

_SCHEMA_VERSION = 1
_NEW_YORK = ZoneInfo("America/New_York")


def _boolean(payload: dict, key: str) -> bool:
    value = payload[key]
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be boolean")
    return value


@dataclass(frozen=True)
class CursorIdentity:
    schema_version: int
    root: str
    ticker_sha256: str
    ticker_count: int
    full_reconcile: bool
    dry_run: bool


@dataclass
class CorporateActionCursor:
    path: Path
    identity: CursorIdentity
    started_at: datetime
    started_on_ny: date
    completed: set[str]
    run_completed_at: datetime | None = None

    @classmethod
    def from_json(cls, path: Path) -> CorporateActionCursor:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            identity = CursorIdentity(
                schema_version=int(payload["schema_version"]),
                root=str(payload["root"]),
                ticker_sha256=str(payload["ticker_sha256"]),
                ticker_count=int(payload["ticker_count"]),
                full_reconcile=_boolean(payload, "full_reconcile"),
                dry_run=_boolean(payload, "dry_run"),
            )
            completed_at = payload.get("run_completed_at")
            return cls(
                path=path,
                identity=identity,
                started_at=datetime.fromisoformat(payload["started_at"]),
                started_on_ny=date.fromisoformat(payload["started_on_ny"]),
                completed={str(ticker) for ticker in payload["completed"]},
                run_completed_at=None if completed_at is None else datetime.fromisoformat(completed_at),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"malformed corporate-action cursor: {path}") from exc

    def mark_completed(self, ticker: str, *, now: datetime) -> None:
        del now
        self.completed.add(canonical_symbol(ticker))
        self._save()

    def mark_run_completed(self, *, now: datetime) -> None:
        self.run_completed_at = now
        self._save()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        payload = {
            "completed": sorted(self.completed),
            "dry_run": self.identity.dry_run,
            "full_reconcile": self.identity.full_reconcile,
            "root": self.identity.root,
            "run_completed_at": None if self.run_completed_at is None else self.run_completed_at.isoformat(),
            "schema_version": self.identity.schema_version,
            "started_at": self.started_at.isoformat(),
            "started_on_ny": self.started_on_ny.isoformat(),
            "ticker_count": self.identity.ticker_count,
            "ticker_sha256": self.identity.ticker_sha256,
        }
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


def build_identity(
    root: Path,
    tickers: list[str],
    *,
    full_reconcile: bool,
    dry_run: bool,
) -> CursorIdentity:
    normalized = sorted({canonical_symbol(ticker) for ticker in tickers})
    ticker_sha256 = hashlib.sha256("\n".join(normalized).encode()).hexdigest()
    return CursorIdentity(
        schema_version=_SCHEMA_VERSION,
        root=str(root.resolve()),
        ticker_sha256=ticker_sha256,
        ticker_count=len(normalized),
        full_reconcile=full_reconcile,
        dry_run=dry_run,
    )


def default_cursor_path(root: Path, identity: CursorIdentity) -> Path:
    scope = f"{identity.ticker_sha256}|{int(identity.full_reconcile)}|{int(identity.dry_run)}"
    scope_id = hashlib.sha256(scope.encode()).hexdigest()[:20]
    return root / "cursors" / "corporate_actions" / f"{scope_id}.json"


def open_cursor(
    path: Path,
    identity: CursorIdentity,
    *,
    resume: bool,
    now: datetime,
) -> CorporateActionCursor:
    if resume and path.exists():
        cursor = CorporateActionCursor.from_json(path)
        if cursor.identity != identity:
            raise ValueError(f"corporate-action cursor is incompatible with this run: {path}")
        if cursor.run_completed_at is not None:
            raise ValueError(f"corporate-action cursor is already complete: {path}")
        return cursor

    cursor = CorporateActionCursor(
        path=path,
        identity=identity,
        started_at=now,
        started_on_ny=now.astimezone(_NEW_YORK).date(),
        completed=set(),
    )
    cursor._save()
    return cursor
