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
import time
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from livewire_scripts.paths import data_lake_dir, log_dir

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.daily_outcomes import SUMMARY_PREFIX, parse_last_summary_json

logger = logging.getLogger("livewire.sync_runner")

EQUITY_PRESETS = ("presets/sp500.json", "presets/ndx100.json", "presets/r2k.json")
VOL_PRESET = "presets/volatility-intraday.json"
# Distinct so the digest and failure summary can name a stall as a stall.
TIMEOUT_EXIT_CODE = 124
VOL_DAILY_PRESET = "presets/volatility.json"
EQUITY_INTRADAY_TIMEFRAMES = ("1m", "5m", "1h")
VOL_INTRADAY_TIMEFRAMES = ("30m", "5m")


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
    target_date: str | None


def build_config(repo_root: Path | None = None) -> SyncConfig:
    root = repo_root or _PROJECT_ROOT
    return SyncConfig(
        python_bin=os.getenv("MDW_PYTHON_BIN", sys.executable),
        ingest_script=root / "scripts" / "livewire_ingest.py",
        store_script=root / "scripts" / "livewire_store.py",
        log_dir=log_dir(),
        equity_presets=tuple(str(root / p) for p in EQUITY_PRESETS),
        vol_preset=str(root / VOL_PRESET),
        vol_daily_preset=str(root / VOL_DAILY_PRESET),
        intraday_days=int(os.getenv("MDW_DAILY_BACKFILL_INTRADAY_DAYS", "7")),
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


def phase_timeout_seconds() -> int:
    """Hard per-phase wall-clock budget.

    There was no timeout anywhere on this path, despite the wrapper's docstring
    claiming `daily-backfill` owns "activity-based stall detection" — it does
    not. A wedged IB call in the volatility phase blocked the phase forever;
    launchd will not start a second instance while the first lives, so the
    nightly job silently stopped running until an operator noticed.
    """
    return int(os.getenv("MDW_SYNC_PHASE_TIMEOUT_SECONDS", str(6 * 60 * 60)))


def run_phase(
    label: str,
    command: list[str],
    log_dir: Path,
    *,
    allow_completed_summary: bool = False,
    runner: callable = subprocess.run,
    timeout: int | None = None,
) -> int:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{label}.log"
    offset = log_file.stat().st_size if log_file.exists() else 0
    logger.info("CMD %s: %s", label, _format_command(command))

    budget = phase_timeout_seconds() if timeout is None else timeout
    with log_file.open("a", encoding="utf-8") as fh:
        try:
            result = runner(
                command,
                stdout=fh,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
                timeout=budget,
            )
        except subprocess.TimeoutExpired:
            logger.error("%s exceeded its %ds budget and was killed", label, budget)
            return TIMEOUT_EXIT_CODE

    if result.returncode != 0:
        if allow_completed_summary:
            try:
                with log_file.open(encoding="utf-8") as fh:
                    fh.seek(offset)
                    content = fh.read()
                summary = parse_last_summary_json(content)
                if summary and int(summary.get("updated", 0)) > 0 and int(summary.get("errors", 0)) == 0:
                    logger.warning(
                        "%s exited %d after successful this-run summary; continuing",
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
    lake = warehouse_dir / "data-lake" if warehouse_dir is not None else data_lake_dir()
    bronze_dir = lake / "bronze" / "asset_class=volatility"
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
    from clients.massive_flatfile_client import require_flatfile_credentials

    try:
        require_flatfile_credentials()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 2
    failures: list[str] = []
    phase_results: list[dict] = []
    target_date = config.target_date or trading_day_fn()
    equity_tickers = ticker_union(config.equity_presets)

    def _phase(label: str, command: list[str], **kwargs) -> int:
        start = time.monotonic()
        rc = run_phase(label, command, config.log_dir, runner=runner, **kwargs)
        phase_results.append({"label": label, "exit": rc, "duration_s": round(time.monotonic() - start, 1)})
        return rc

    logger.info("=" * 60)
    logger.info("DAILY BACKFILL START")
    logger.info(
        "Target: %s | Intraday: %d days",
        target_date,
        config.intraday_days,
    )
    logger.info("=" * 60)

    py = config.python_bin
    ingest = str(config.ingest_script)
    store = str(config.store_script)

    # Phase 1: Equity daily via Massive
    rc = _phase(
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
        allow_completed_summary=True,
    )
    if rc != 0:
        failures.append("equity_daily")

    # Phase 2: FRED Treasury rates
    rc = _phase("daily_backfill_fred_rates", [py, ingest, "fred-rates"])
    if rc != 0:
        failures.append("fred_rates")

    # Phase 3: CBOE volatility daily
    rc = _phase(
        "daily_backfill_volatility_cboe",
        [py, ingest, "cboe-vol", "--preset", config.vol_daily_preset],
    )
    if rc != 0:
        failures.append("cboe_volatility")

    # Phase 3b: Full-universe equity daily via Massive day_aggs flat files.
    # This lane owns the ~20K SIP daily universe (and publishes 1d for symbols
    # that only had intraday files); it went stale when nothing re-ran it.
    day_aggs_days = int(os.getenv("MDW_DAILY_BACKFILL_DAY_AGGS_DAYS", "7"))
    rc = _phase(
        "daily_backfill_equity_day_aggs",
        [
            py,
            ingest,
            "flatfile-ingest-daily",
            "catch-up",
            "--days",
            str(day_aggs_days),
            "--workers",
            str(int(os.getenv("MDW_FLATFILE_DAILY_WORKERS", "4"))),
        ],
    )
    if rc != 0:
        failures.append("equity_day_aggs")

    # Phase 4: Full-market equity intraday via Massive flat files
    rc = _phase(
        "daily_backfill_intraday_equity_flatfiles",
        [
            py,
            ingest,
            "flatfile-ingest",
            "catch-up",
            "--days",
            str(config.intraday_days),
            "--workers",
            str(int(os.getenv("MDW_FLATFILE_WORKERS", "4"))),
        ],
    )
    if rc != 0:
        failures.append("intraday_equity_flatfiles")

    # Phase 5: Volatility intraday via IB
    vol_tickers = load_tickers(config.vol_preset)
    for tf in VOL_INTRADAY_TIMEFRAMES:
        rc = _phase(
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
        )
        if rc != 0:
            failures.append(f"vol_intraday_{tf}")

    # Phase 5b: Derive 1h from 30m locally.
    # Wrapped because this ran outside the phase harness: any OSError or
    # ArrowInvalid propagated out of run_sync, so Postgres never ran AND the
    # SUMMARY_JSON line below was never printed — leaving the wrapper to scrape
    # the last log line and the nightly digest with no phase table at all.
    try:
        _derive_vol_1h(config.vol_preset)
    except Exception as exc:  # noqa: BLE001 - one lane must not kill the summary
        logger.error("vol_1h_derive failed: %s", exc)
        failures.append("vol_1h_derive")

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
            rc = _phase(
                f"daily_backfill_postgres_{suffix}",
                [py, store, "rebuild-postgres", *ac_args],
            )
            if rc != 0:
                failures.append(f"postgres_{suffix}")
    else:
        logger.info("Postgres rebuild skipped — MDW_POSTGRES_DSN not set")

    # Machine-readable per-phase summary for the nightly digest / watchdog.
    print(
        SUMMARY_PREFIX
        + json.dumps(
            {
                "job": "daily_backfill",
                "target_date": str(target_date),
                "phases": phase_results,
                "failed": [p["label"] for p in phase_results if p["exit"] != 0],
            },
            separators=(",", ":"),
        )
    )

    logger.info("=" * 60)
    if failures:
        logger.warning("DAILY BACKFILL COMPLETE with failures: %s", ", ".join(failures))
        return 1
    logger.info("DAILY BACKFILL COMPLETE")
    logger.info("=" * 60)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Daily sync runner — routine warehouse catch-up")
    parser.add_argument("--target-date", type=str, default=None)
    parser.add_argument("--intraday-days", type=int, default=None)
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
    if overrides:
        config = replace(config, **overrides)

    return run_sync(config)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
