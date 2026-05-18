"""Source-agnostic JSONL telemetry primitives for the Livewire pipeline.

See: docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_VALID_SOURCES = {"ib", "uw", "massive"}

_logger = logging.getLogger("mdw.telemetry")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_default_path() -> Optional[Path]:
    raw = os.environ.get(
        "MDW_TELEMETRY_PATH",
        str(Path.home() / "market-warehouse" / "logs" / "telemetry.jsonl"),
    )
    if raw.strip().lower() in {"none", "off", "disabled", ""}:
        return None
    return Path(raw).expanduser()


class BaseTelemetry:
    """Append-only JSONL emitter with disabled-when-broken fallback."""

    _WARN_RATE_LIMIT_SECONDS = 60

    def __init__(self, source: str, jsonl_path: Optional[Path]):
        if source not in _VALID_SOURCES:
            raise ValueError(f"source must be one of {_VALID_SOURCES}, got {source!r}")
        self.source = source
        self.jsonl_path = jsonl_path
        self._disabled = False
        self._started = False
        self._last_warn_at = 0.0

    def start(self) -> None:
        if self._started:
            return
        if self.jsonl_path is None:
            self._disabled = True
            self._started = True
            return
        if not self.jsonl_path.parent.is_dir():
            _logger.warning(
                "telemetry path %s unusable (parent dir missing); disabling",
                self.jsonl_path,
            )
            self._disabled = True
            self._started = True
            return
        self._started = True
        self._emit({"event": "telemetry_started"})

    def stop(self) -> None:
        if not self._started or self._disabled:
            self._started = False
            return
        self._emit({"event": "telemetry_stopped"})
        self._started = False

    def _emit(self, record: dict) -> None:
        if self._disabled or not self._started:
            return
        record = dict(record)
        record.setdefault("ts", _utc_iso())
        record.setdefault("source", self.source)
        line = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
        try:
            self._do_write(line)
        except OSError as exc:
            now = time.monotonic()
            if now - self._last_warn_at > self._WARN_RATE_LIMIT_SECONDS:
                _logger.warning("telemetry write failed: %s (rate-limited)", exc)
                self._last_warn_at = now

    def _do_write(self, line: str) -> None:
        with self.jsonl_path.open("a", encoding="utf-8") as fh:
            fh.write(line)


_FARM_STATE_BY_CODE = {
    2104: "ok",
    2105: "broken",
    2106: "ok",
    2107: "inactive",
    2158: "ok",
}


def _parse_farm_name(error_string: str) -> Optional[str]:
    """Extract trailing ':farmname' suffix if present."""
    if not error_string or ":" not in error_string:
        return None
    tail = error_string.rsplit(":", 1)[-1].strip()
    if not tail or " " in tail:
        return None
    return tail


class ConnectionTelemetry(BaseTelemetry):
    """IB-specific telemetry: subscribes to ib_async error/connected/disconnected events."""

    def __init__(self, *, ib, jsonl_path: Optional[Path], source: str = "ib"):
        super().__init__(source=source, jsonl_path=jsonl_path)
        self._ib = ib
        self._attached = False

    def start(self) -> None:
        super().start()
        if self._disabled or self._attached:
            return
        self._ib.errorEvent.connect(self._on_error)
        self._ib.connectedEvent.connect(self._on_connected)
        self._ib.disconnectedEvent.connect(self._on_disconnected)
        self._attached = True

    def stop(self) -> None:
        if self._attached:
            try:
                self._ib.errorEvent.disconnect(self._on_error)
                self._ib.connectedEvent.disconnect(self._on_connected)
                self._ib.disconnectedEvent.disconnect(self._on_disconnected)
            except Exception as exc:  # pragma: no cover - defensive
                _logger.warning("telemetry detach failed: %s", exc)
            self._attached = False
        super().stop()

    def _on_error(self, reqId, errorCode, errorString, contract):
        if errorCode in _FARM_STATE_BY_CODE:
            self._emit({
                "event": "farm_state",
                "code": int(errorCode),
                "state": _FARM_STATE_BY_CODE[errorCode],
                "farm": _parse_farm_name(errorString),
                "message": str(errorString),
            })
        else:
            self._emit({
                "event": "ib_error",
                "code": int(errorCode),
                "req_id": int(reqId),
                "message": str(errorString),
            })

    def _on_connected(self):
        self._emit({"event": "connected"})

    def _on_disconnected(self):
        self._emit({"event": "disconnected"})
