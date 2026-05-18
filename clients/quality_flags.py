"""Quality-flag emit paths: sidecar JSON, audit JSONL, alert email.

Three independent emit paths; any failing alone does not sink the others.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

from clients.quality_detector import QualityFlag

_logger = logging.getLogger("mdw.quality")

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
        fd, tmp_path = tempfile.mkstemp(
            prefix=".sidecar_", suffix=".tmp", dir=str(sidecar.parent)
        )
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
