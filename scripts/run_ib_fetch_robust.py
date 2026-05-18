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
import subprocess
import sys
import time
from dataclasses import dataclass
from enum import Enum
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


class OutcomeCategory(str, Enum):
    OK = "ok"
    OK_NOOP = "ok-noop"
    SKIP = "skip"
    FAIL = "fail"
    TIMEOUT = "timeout"


@dataclass
class TickerOutcome:
    ticker: str
    code: OutcomeCategory
    attempts_used: int
    elapsed_seconds: float
    rows_before: int
    rows_after: int
    note: str = ""


def _build_worker_cmd(ticker: str, mode: str, asset_class: str) -> list[str]:
    """Construct the subprocess args for fetch_ib_historical."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "fetch_ib_historical.py"),
        "--tickers",
        ticker,
        "--asset-class",
        asset_class,
        "--batch-size",
        "1",
        "--max-concurrent",
        "1",
    ]
    if mode == "seed":
        cmd += ["--years", "0"]
    elif mode == "backfill":
        cmd += ["--backfill"]
    return cmd


def _count_rows(parquet_path: Path) -> int:
    if not parquet_path.exists():
        return 0
    try:
        import duckdb

        escaped = str(parquet_path).replace("'", "''")
        return duckdb.connect().execute(
            f"select count(*) from read_parquet('{escaped}')"
        ).fetchone()[0]
    except Exception as exc:  # pragma: no cover - unknown row count is non-fatal
        _logger.warning("row count failed for %s: %s", parquet_path, exc)
        return 0


def run_one_ticker(
    *,
    ticker: str,
    mode: str,
    asset_class: str,
    bronze_dir: Path,
    timeout: int,
    max_attempts: int,
    cooldown: int,
) -> TickerOutcome:
    parquet = _bronze_path_for(bronze_dir, asset_class, ticker)
    if _is_already_done(parquet, mode):
        return TickerOutcome(ticker, OutcomeCategory.SKIP, 0, 0.0, 0, 0)

    cmd = _build_worker_cmd(ticker, mode, asset_class)
    rows_before = _count_rows(parquet)
    start = time.monotonic()

    attempts = 0
    last_was_timeout = False
    while attempts < max_attempts:
        attempts += 1
        try:
            result = subprocess.run(cmd, timeout=timeout, capture_output=True)
            last_was_timeout = False
        except subprocess.TimeoutExpired:
            last_was_timeout = True
            _logger.warning("[%s] attempt %d timeout after %ss", ticker, attempts, timeout)
            if attempts < max_attempts:
                time.sleep(cooldown)
            continue

        if result.returncode == 0:
            rows_after = _count_rows(parquet)
            elapsed = time.monotonic() - start
            if parquet.exists() and rows_after > rows_before:
                return TickerOutcome(
                    ticker,
                    OutcomeCategory.OK,
                    attempts,
                    elapsed,
                    rows_before,
                    rows_after,
                    note=f"rows +{rows_after - rows_before}",
                )
            return TickerOutcome(
                ticker,
                OutcomeCategory.OK,
                attempts,
                elapsed,
                rows_before,
                rows_after,
            )
        _logger.warning("[%s] attempt %d exit=%d", ticker, attempts, result.returncode)
        if attempts < max_attempts:
            time.sleep(cooldown)

    elapsed = time.monotonic() - start
    code = OutcomeCategory.TIMEOUT if last_was_timeout else OutcomeCategory.FAIL
    return TickerOutcome(ticker, code, attempts, elapsed, rows_before, rows_before)
