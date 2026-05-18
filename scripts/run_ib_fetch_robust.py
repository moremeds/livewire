#!/usr/bin/env python
"""Productized orchestrator: per-ticker process isolation for IB fetches.

Replaces /tmp/orchestrate_ib_fetch.sh. Spawns one subprocess per ticker,
enforces hard timeout + retry budget + cooldown. Recognizes ok-noop for
backfill mode (no older history available, exit 0 = success).

See: docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_DIR = Path.home() / "market-warehouse" / "data-lake" / "bronze"
DEFAULT_LOG_DIR = Path.home() / "market-warehouse" / "logs"

_logger = logging.getLogger("mdw.orchestrator")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Robust per-ticker IB fetch orchestrator")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--preset", type=Path, help="Preset JSON file with .tickers array")
    src.add_argument("--tickers", nargs="+", help="Explicit ticker list")
    p.add_argument("--mode", choices=["seed", "backfill"], required=True)
    p.add_argument(
        "--asset-class",
        default="equity",
        choices=["equity", "volatility", "futures", "cmdty", "fx"],
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=_env_int("MDW_ORCHESTRATOR_TIMEOUT_SECONDS", 300),
    )
    p.add_argument(
        "--max-attempts",
        type=int,
        default=_env_int("MDW_ORCHESTRATOR_MAX_ATTEMPTS", 3),
    )
    p.add_argument(
        "--cooldown",
        type=int,
        default=_env_int("MDW_ORCHESTRATOR_COOLDOWN_SECONDS", 60),
    )
    p.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    p.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    return p.parse_args(argv)


def load_tickers(*, preset_path: Optional[Path], explicit: Optional[list[str]]) -> list[str]:
    if explicit:
        return list(explicit)
    if preset_path is None:
        return []
    payload = json.loads(preset_path.read_text())
    return list(payload.get("tickers") or [])


def _bronze_path_for(
    bronze_dir: Path | str,
    asset_class: str,
    ticker: str,
    timeframe: str = "1d",
) -> Path:
    return Path(bronze_dir) / f"asset_class={asset_class}" / f"symbol={ticker}" / f"{timeframe}.parquet"


def _is_already_done(parquet_path: Path, mode: str) -> bool:
    """seed: skip if exists. backfill: skip if missing (no prior bronze to extend)."""
    if mode == "seed":
        return parquet_path.exists()
    if mode == "backfill":
        return not parquet_path.exists()
    return False  # pragma: no cover - argparse choices prevent other values
