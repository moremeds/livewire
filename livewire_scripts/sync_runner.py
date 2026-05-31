#!/usr/bin/env python3
"""Daily sync runner — routine warehouse catch-up.

Replaces tools/run_daily_backfill.sh with a testable Python module.
Runs Massive equity daily + intraday, FRED rates, CBOE volatility,
IB vol intraday, and optional Postgres rebuild.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

logger = logging.getLogger("livewire.sync_runner")

EQUITY_PRESETS = ("presets/sp500.json", "presets/ndx100.json", "presets/r2k.json")
VOL_PRESET = "presets/volatility-intraday.json"
VOL_DAILY_PRESET = "presets/volatility.json"
EQUITY_INTRADAY_TIMEFRAMES = ("1m", "5m", "1h")
VOL_INTRADAY_TIMEFRAMES = ("30m",)


@dataclass(frozen=True)
class SyncConfig:
    python_bin: str
    ingest_script: Path
    store_script: Path
    log_dir: Path
    equity_presets: tuple[str, ...]
    vol_preset: str
    vol_daily_preset: str
    intraday_days: int
    intraday_concurrent: int
    target_date: str | None


def build_config(repo_root: Path | None = None) -> SyncConfig:
    root = repo_root or _PROJECT_ROOT
    warehouse = Path(
        os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))
    )
    return SyncConfig(
        python_bin=os.getenv("MDW_PYTHON_BIN", sys.executable),
        ingest_script=root / "scripts" / "livewire_ingest.py",
        store_script=root / "scripts" / "livewire_store.py",
        log_dir=Path(os.getenv("MDW_LOG_DIR", str(warehouse / "logs"))),
        equity_presets=tuple(str(root / p) for p in EQUITY_PRESETS),
        vol_preset=str(root / VOL_PRESET),
        vol_daily_preset=str(root / VOL_DAILY_PRESET),
        intraday_days=int(os.getenv("MDW_DAILY_BACKFILL_INTRADAY_DAYS", "7")),
        intraday_concurrent=int(
            os.getenv("MDW_DAILY_BACKFILL_INTRADAY_CONCURRENT", "20")
        ),
        target_date=os.getenv("MDW_DAILY_BACKFILL_TARGET_DATE") or None,
    )


def load_tickers(preset_path: str) -> list[str]:
    with open(preset_path, encoding="utf-8") as fh:
        payload = json.load(fh)
    return sorted(str(t).upper() for t in payload.get("tickers", []))


def ticker_union(presets: Sequence[str]) -> list[str]:
    all_tickers: set[str] = set()
    for preset in presets:
        all_tickers.update(load_tickers(preset))
    return sorted(all_tickers)


def latest_complete_trading_day() -> str:
    from livewire_scripts.daily_update import (
        is_trading_day,
        previous_trading_day,
        session_close_time,
    )

    et_now = datetime.now(ZoneInfo("America/New_York"))
    today = et_now.date()
    if not is_trading_day(today):
        return previous_trading_day(today).isoformat()
    close_time = session_close_time(today)
    close_dt = et_now.replace(
        hour=close_time.hour,
        minute=close_time.minute,
        second=0,
        microsecond=0,
    )
    if et_now >= close_dt + timedelta(minutes=30):
        return today.isoformat()
    return previous_trading_day(today).isoformat()


def _format_command(cmd: Sequence[str], limit: int = 24) -> str:
    parts = list(cmd)
    if len(parts) <= limit:
        return " ".join(parts)
    return " ".join(parts[:limit]) + f" ... [{len(parts) - limit} more args]"


def run_phase(
    label: str,
    command: list[str],
    log_dir: Path,
    *,
    allow_completed_summary: bool = False,
    runner: callable = subprocess.run,
) -> int:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{label}.log"
    logger.info("CMD %s: %s", label, _format_command(command))

    with log_file.open("a", encoding="utf-8") as fh:
        result = runner(
            command,
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    if result.returncode != 0:
        if allow_completed_summary:
            try:
                content = log_file.read_text(encoding="utf-8")
                if "Daily Update Complete" in content:
                    logger.warning(
                        "%s exited %d after completed summary; continuing",
                        label,
                        result.returncode,
                    )
                    return 0
            except FileNotFoundError:
                pass
        logger.warning("%s exited with code %d", label, result.returncode)

    return result.returncode


def _derive_vol_1h(
    vol_preset: str,
    *,
    warehouse_dir: Path | None = None,
) -> int:
    """Derive 1h bars from 30m for all tickers in the vol preset."""
    from clients.intraday_bronze_client import IntradayBronzeClient
    from clients.timeframe_aggregator import aggregate_bars

    tickers = load_tickers(vol_preset)
    wh = warehouse_dir or Path(
        os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))
    )
    bronze_dir = wh / "data-lake" / "bronze" / "asset_class=volatility"
    derived = 0

    for ticker in tickers:
        bronze_30m = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="30m")
        rows = bronze_30m.read_symbol_rows(ticker)
        if not rows:
            continue
        agg = aggregate_bars(rows, source_tf="30m", target_tf="1h")
        if agg:
            bronze_1h = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe="1h")
            bronze_1h.merge_ticker_rows(ticker, agg, overwrite_existing=True)
            derived += 1

    logger.info("Derived 1h from 30m for %d/%d vol tickers", derived, len(tickers))
    return derived


def run_sync(
    config: SyncConfig,
    *,
    runner: callable = subprocess.run,
    trading_day_fn: callable = latest_complete_trading_day,
) -> int:
    failures: list[str] = []
    target_date = config.target_date or trading_day_fn()
    equity_tickers = ticker_union(config.equity_presets)

    logger.info("=" * 60)
    logger.info("DAILY BACKFILL START")
    logger.info(
        "Target: %s | Intraday: %d days | Concurrent: %d",
        target_date,
        config.intraday_days,
        config.intraday_concurrent,
    )
    logger.info("=" * 60)

    py = config.python_bin
    ingest = str(config.ingest_script)
    store = str(config.store_script)

    # Phase 1: Equity daily via Massive
    rc = run_phase(
        "daily_backfill_equity_union",
        [
            py,
            ingest,
            "daily",
            "--asset-class",
            "equity",
            "--source",
            "massive",
            "--tickers",
            *equity_tickers,
            "--target-date",
            target_date,
            "--force",
        ],
        config.log_dir,
        allow_completed_summary=True,
        runner=runner,
    )
    if rc != 0:
        failures.append("equity_daily")

    # Phase 2: FRED Treasury rates
    rc = run_phase(
        "daily_backfill_fred_rates",
        [py, ingest, "fred-rates"],
        config.log_dir,
        runner=runner,
    )
    if rc != 0:
        failures.append("fred_rates")

    # Phase 3: CBOE volatility daily
    rc = run_phase(
        "daily_backfill_volatility_cboe",
        [py, ingest, "cboe-vol", "--preset", config.vol_daily_preset],
        config.log_dir,
        runner=runner,
    )
    if rc != 0:
        failures.append("cboe_volatility")

    # Phase 4: Equity intraday via Massive
    for tf in EQUITY_INTRADAY_TIMEFRAMES:
        rc = run_phase(
            f"daily_backfill_intraday_{tf}_equity",
            [
                py,
                ingest,
                "intraday-backfill",
                "--tickers",
                *equity_tickers,
                "--timeframe",
                tf,
                "--source",
                "massive",
                "--asset-class",
                "equity",
                "--days",
                str(config.intraday_days),
                "--max-concurrent",
                str(config.intraday_concurrent),
            ],
            config.log_dir,
            runner=runner,
        )
        if rc != 0:
            failures.append(f"intraday_{tf}")

    # Phase 5: Volatility intraday via IB
    vol_tickers = load_tickers(config.vol_preset)
    for tf in VOL_INTRADAY_TIMEFRAMES:
        rc = run_phase(
            f"daily_backfill_intraday_{tf}_volatility",
            [
                py,
                ingest,
                "intraday-backfill",
                "--tickers",
                *vol_tickers,
                "--timeframe",
                tf,
                "--source",
                "ib",
                "--asset-class",
                "volatility",
                "--days",
                str(config.intraday_days),
            ],
            config.log_dir,
            runner=runner,
        )
        if rc != 0:
            failures.append(f"vol_intraday_{tf}")

    # Phase 5b: Derive 1h from 30m locally
    _derive_vol_1h(config.vol_preset)

    # Phase 6: Postgres rebuild (conditional)
    if os.getenv("MDW_POSTGRES_DSN"):
        for suffix, ac_args in [
            (
                "equity",
                [
                    "--asset-class",
                    "equity",
                    "--timeframe",
                    "all",
                    "--include-reliability",
                ],
            ),
            ("volatility", ["--asset-class", "volatility", "--timeframe", "1d"]),
        ]:
            rc = run_phase(
                f"daily_backfill_postgres_{suffix}",
                [py, store, "rebuild-postgres", *ac_args],
                config.log_dir,
                runner=runner,
            )
            if rc != 0:
                failures.append(f"postgres_{suffix}")
    else:
        logger.info("Postgres rebuild skipped — MDW_POSTGRES_DSN not set")

    logger.info("=" * 60)
    if failures:
        logger.warning("DAILY BACKFILL COMPLETE with failures: %s", ", ".join(failures))
        return 1
    logger.info("DAILY BACKFILL COMPLETE")
    logger.info("=" * 60)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Daily sync runner — routine warehouse catch-up"
    )
    parser.add_argument("--target-date", type=str, default=None)
    parser.add_argument("--intraday-days", type=int, default=None)
    parser.add_argument("--intraday-concurrent", type=int, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = build_config()
    overrides: dict = {}
    if args.target_date:
        overrides["target_date"] = args.target_date
    if args.intraday_days is not None:
        overrides["intraday_days"] = args.intraday_days
    if args.intraday_concurrent is not None:
        overrides["intraday_concurrent"] = args.intraday_concurrent
    if overrides:
        config = replace(config, **overrides)

    return run_sync(config)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
