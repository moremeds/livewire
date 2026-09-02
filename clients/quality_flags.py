"""Quality-flag emit paths: sidecar JSON, audit JSONL, alert email.

Three independent emit paths; any failing alone does not sink the others.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from clients import ledger
from clients.quality_detector import QualityFlag
from livewire_scripts.paths import log_dir

_logger = logging.getLogger("livewire.quality")

_VALID_SOURCES = {"ib", "uw", "massive"}


def _sidecar_path(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(parquet_path.suffix + ".meta.json")


def write_sidecar(parquet_path: Path, flags: list[QualityFlag], metadata: dict) -> bool:
    """Write <parquet>.meta.json atomically. Returns True on success."""
    sidecar = _sidecar_path(parquet_path)
    payload = dict(metadata)
    payload["parquet_path"] = str(parquet_path)
    payload["flags"] = [asdict(f) for f in flags]
    try:
        fd, tmp_path = tempfile.mkstemp(prefix=".sidecar_", suffix=".tmp", dir=str(sidecar.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True, default=str)
            os.replace(tmp_path, sidecar)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:  # pragma: no cover - best-effort cleanup
                pass
            raise
    except OSError as exc:
        _logger.warning("sidecar write failed for %s: %s", parquet_path, exc)
        return False
    return True


def _utc_iso() -> str:
    from datetime import datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_audit_path() -> Path:
    raw = os.environ.get(
        "MDW_QUALITY_AUDIT_PATH",
        str(log_dir() / "quality_audit.jsonl"),
    )
    return Path(raw).expanduser()


def append_audit(
    flag: QualityFlag,
    *,
    source: str,
    ticker: str,
    timeframe: str,
    parquet_path: Path,
) -> bool:
    """Append one JSON line to the central audit JSONL. Raises on invalid source."""
    if source not in _VALID_SOURCES:
        raise ValueError(f"source must be one of {_VALID_SOURCES}, got {source!r}")
    record = {
        "ts": _utc_iso(),
        "source": source,
        "ticker": ticker,
        "timeframe": timeframe,
        "parquet_path": str(parquet_path),
        "category": flag.category,
        "severity": flag.severity,
        "detail": flag.detail,
    }
    line = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
    audit = _resolve_audit_path()
    try:
        audit.parent.mkdir(parents=True, exist_ok=True)
        with audit.open("a", encoding="utf-8") as fh:
            fh.write(line)
    except OSError as exc:
        _logger.warning("audit append failed: %s", exc)
        return False
    return True


_SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}
_RATE_LIMIT_CACHE: dict[tuple[str, str, str], float] = {}

_REPO_ROOT = Path(__file__).resolve().parent.parent
_EMAIL_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ops.py"


def _resolve_threshold() -> str:
    return os.environ.get("MDW_ALERT_SEVERITY_THRESHOLD", "warning").lower()


def _resolve_rate_limit_seconds() -> int:
    raw = os.environ.get("MDW_ALERT_RATE_LIMIT_SECONDS", "300")
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 300


def alert_on_flag(
    flag: QualityFlag,
    *,
    source: str,
    ticker: str,
    severity_threshold: str | None = None,
) -> bool:
    """Spawn Nodemailer email if severity meets threshold. Returns True if email sent."""
    threshold = (severity_threshold or _resolve_threshold()).lower()
    if _SEVERITY_ORDER.get(flag.severity, 0) < _SEVERITY_ORDER.get(threshold, 1):
        return False

    key = (source, ticker, flag.category)
    now = time.monotonic()
    rl = _resolve_rate_limit_seconds()
    last = _RATE_LIMIT_CACHE.get(key, 0.0)
    if rl > 0 and (now - last) < rl:
        _logger.info("alert rate-limited: %s/%s/%s", source, ticker, flag.category)
        return False
    _RATE_LIMIT_CACHE[key] = now

    payload = {
        "source": source,
        "ticker": ticker,
        "category": flag.category,
        "severity": flag.severity,
        "detail": flag.detail,
        "ts": flag.ts,
    }
    cmd = [
        sys.executable,
        str(_EMAIL_SCRIPT),
        "send-alert",
        "--mode",
        "flag-alert",
        "--payload",
        json.dumps(payload),
    ]
    started = datetime.now(UTC)
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
    except (subprocess.SubprocessError, OSError) as exc:
        _logger.error("alert spawn failed: %s", exc)
        _record_failed_alert(payload, started, 1, {"error": str(exc)})
        return False
    if result.returncode != 0:
        _logger.error(
            "alert send returned %s: %s",
            result.returncode,
            (result.stderr or b"").decode("utf-8", "replace"),
        )
        _record_failed_alert(
            payload,
            started,
            int(result.returncode),
            {"stderr": (result.stderr or b"").decode("utf-8", "replace")},
        )
        return False
    return True


def _record_failed_alert(payload: dict, started: datetime, exit_code: int, receipt: dict) -> None:
    try:
        run = os.environ["LW_RUN_ID"]
        ledger.emit(
            "executions",
            [
                {
                    "evidence_hash": None,
                    "script": "send_alert",
                    "attempt": 1,
                    "args_json": json.dumps(payload, sort_keys=True),
                    "release_sha": os.environ.get("LW_RELEASE_SHA", "unknown"),
                    "started": started,
                    "ended": datetime.now(UTC),
                    "exit_code": exit_code,
                    "receipt_json": json.dumps(receipt, sort_keys=True),
                    "run_id": run,
                }
            ],
            run_id=run,
        )
    except Exception as exc:  # pragma: no cover - alert failure remains non-fatal
        _logger.error("could not record failed alert: %s", exc)
