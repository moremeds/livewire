#!/usr/bin/env python3
"""Full warehouse backfill runner — replaces tools/run_backfill_all.sh.

Runs equity daily seed + backfill, FRED rates, equity intraday via Massive,
CBOE volatility daily, IB volatility intraday, and optional Postgres rebuild.
Includes activity-based stall detection and retry-until-done logic.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

logger = logging.getLogger("livewire.backfill_runner")

EQUITY_PRESETS = ("presets/sp500.json", "presets/ndx100.json", "presets/r2k.json")
VOL_PRESET = "presets/volatility-intraday.json"
VOL_DAILY_PRESET = "presets/volatility.json"
EQUITY_INTRADAY_TIMEFRAMES = ("1m", "5m", "1h")
VOL_INTRADAY_TIMEFRAMES = ("30m", "5m")


@dataclass(frozen=True)
class BackfillConfig:
    python_bin: str
    ingest_script: Path
    store_script: Path
    log_dir: Path
    cursor_dir: Path
    equity_presets: tuple[str, ...]
    vol_preset: str
    vol_daily_preset: str
    stall_timeout: int
    stall_cooldown: int
    success_cooldown: int
    no_progress_cooldown: int
    poll_interval: float
    max_stale: int
    batch_size: int
    max_concurrent: int


def build_config(repo_root: Path | None = None) -> BackfillConfig:
    root = repo_root or _PROJECT_ROOT
    warehouse = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
    return BackfillConfig(
        python_bin=os.getenv("MDW_PYTHON_BIN", sys.executable),
        ingest_script=root / "scripts" / "livewire_ingest.py",
        store_script=root / "scripts" / "livewire_store.py",
        log_dir=Path(os.getenv("MDW_LOG_DIR", str(warehouse / "logs"))),
        cursor_dir=Path(os.getenv("MDW_CURSOR_DIR", str(warehouse / "cursors"))),
        equity_presets=tuple(str(root / p) for p in EQUITY_PRESETS),
        vol_preset=str(root / VOL_PRESET),
        vol_daily_preset=str(root / VOL_DAILY_PRESET),
        stall_timeout=int(os.getenv("MDW_BACKFILL_STALL_TIMEOUT", "600")),
        stall_cooldown=int(
            os.getenv(
                "MDW_BACKFILL_STALL_COOLDOWN",
                os.getenv("MDW_BACKFILL_COOLDOWN", "300"),
            )
        ),
        success_cooldown=int(os.getenv("MDW_BACKFILL_SUCCESS_COOLDOWN", "0")),
        no_progress_cooldown=int(os.getenv("MDW_BACKFILL_NO_PROGRESS_COOLDOWN", "30")),
        poll_interval=float(os.getenv("MDW_BACKFILL_POLL_INTERVAL", "30")),
        max_stale=int(os.getenv("MDW_BACKFILL_MAX_STALE", "3")),
        batch_size=int(os.getenv("MDW_BACKFILL_BATCH_SIZE", "5")),
        max_concurrent=int(os.getenv("MDW_BACKFILL_MAX_CONCURRENT", "10")),
    )


def preset_info(preset_path: str) -> tuple[str, int]:
    with open(preset_path, encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload["name"], len(payload.get("tickers", []))


def cursor_completed(cursor_file: Path) -> int:
    try:
        with cursor_file.open(encoding="utf-8") as fh:
            return len(json.load(fh).get("completed", []))
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return 0


def gap_aware_completed(
    cursor_file: Path,
    total: int,
    preset_path: str | None,
    warehouse_dir: Path | None = None,
    bronze_dir: Path | None = None,
    as_of: date | None = None,
) -> int:
    """Count completed tickers, cross-checking with gap analysis when cursor is full.

    Returns cursor count when below *total*. When cursor count >= total,
    validates against bronze parquet dates and TagRegistry.earliest_available.
    Falls back to cursor count if registry is unavailable.
    """
    cursor_count = cursor_completed(cursor_file)
    if cursor_count < total:
        return cursor_count

    wh = warehouse_dir or Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
    registry_path = wh / "registry.json"
    if not registry_path.exists():
        logger.debug("No registry at %s — falling back to cursor count", registry_path)
        return cursor_count

    try:
        import clients.bronze_client as _bc
        from clients.tag_registry import TagRegistry
        from livewire_scripts.check_gaps import compute_gaps

        registry = TagRegistry(registry_path)
        bdir = bronze_dir or (wh / "data-lake" / "bronze" / "asset_class=equity")
        bronze = _bc.BronzeClient(bronze_dir=bdir)
        all_dates = bronze.get_trade_dates_by_symbol()

        tickers: list[str]
        if preset_path:
            with open(preset_path, encoding="utf-8") as fh:
                tickers = json.load(fh).get("tickers", [])
        else:
            tickers = sorted(all_dates.keys())

        n_no_bounds = 0
        n_with_gaps = 0
        for ticker in tickers:
            entry = registry.get(ticker)
            earliest = entry.earliest_available if entry else None
            if earliest is None:
                n_no_bounds += 1
                continue
            bronze_dates = set(all_dates.get(ticker, []))
            report = compute_gaps(ticker, earliest, bronze_dates, as_of=as_of)
            if not report.complete:
                n_with_gaps += 1

        if n_no_bounds > 0:
            logger.warning(
                "Gap check: %d/%d tickers lack earliest_available bounds",
                n_no_bounds,
                len(tickers),
            )
        if n_with_gaps > 0:
            logger.info(
                "Gap check: %d/%d tickers still have data gaps despite cursor completion",
                n_with_gaps,
                len(tickers),
            )
            return total - n_with_gaps

    except Exception:
        logger.warning("Gap analysis failed — falling back to cursor count", exc_info=True)

    return cursor_count


def make_gap_completed_fn(
    total: int,
    preset_path: str | None,
) -> callable:
    """Return a completed_fn that wraps cursor_completed with gap analysis."""

    def _fn(cursor_file: Path) -> int:
        return gap_aware_completed(cursor_file, total, preset_path)

    return _fn


def _file_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


def _kill_process(proc: subprocess.Popen) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait()


def run_preset(
    label: str,
    cursor_file: Path,
    command: list[str],
    log_dir: Path,
    *,
    stall_timeout: float,
    poll_interval: float,
    popen_fn: callable = subprocess.Popen,
    sleep_fn: callable = time.sleep,
    clock_fn: callable = time.monotonic,
    mtime_fn: callable = _file_mtime,
    completed_fn: callable = cursor_completed,
    kill_fn: callable = _kill_process,
) -> tuple[int, int]:
    """Run a command with activity-based stall detection.

    Returns (exit_code, completed_delta). exit_code is -1 for stall kills.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{label}.log"
    start_completed = completed_fn(cursor_file)
    logger.info("START %s — cursor: %d", label, start_completed)

    fh = log_file.open("a", encoding="utf-8")
    try:
        proc = popen_fn(
            command,
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )

        last_mtime = max(mtime_fn(cursor_file), mtime_fn(log_file))
        last_check = clock_fn()

        while proc.poll() is None:
            sleep_fn(poll_interval)
            current_mtime = max(mtime_fn(cursor_file), mtime_fn(log_file))

            if current_mtime != last_mtime:
                last_mtime = current_mtime
                last_check = clock_fn()
            else:
                stall = clock_fn() - last_check
                if stall >= stall_timeout:
                    logger.warning(
                        "STALL %s — no activity for %ds, killing",
                        label,
                        int(stall),
                    )
                    kill_fn(proc)
                    end_completed = completed_fn(cursor_file)
                    return -1, end_completed - start_completed
    finally:
        fh.close()

    exit_code = proc.wait()
    end_completed = completed_fn(cursor_file)
    delta = end_completed - start_completed
    logger.info(
        "EXIT %s — code=%d, completed: %d → %d (+%d)",
        label,
        exit_code,
        start_completed,
        end_completed,
        delta,
    )
    return exit_code, delta


def run_until_done(
    label: str,
    cursor_file: Path,
    total: int,
    command: list[str],
    config: BackfillConfig,
    *,
    popen_fn: callable = subprocess.Popen,
    sleep_fn: callable = time.sleep,
    clock_fn: callable = time.monotonic,
    mtime_fn: callable = _file_mtime,
    completed_fn: callable = cursor_completed,
    kill_fn: callable = _kill_process,
) -> int:
    """Retry a preset until all tickers complete or max_stale no-progress rounds."""
    stale_count = 0

    while True:
        completed = completed_fn(cursor_file)
        if completed >= total:
            logger.info("COMPLETE %s — %d/%d done", label, completed, total)
            return completed

        logger.info(
            "ATTEMPT %s — %d/%d done, %d remaining (stale=%d/%d)",
            label,
            completed,
            total,
            total - completed,
            stale_count,
            config.max_stale,
        )

        before = completed
        exit_code, _delta = run_preset(
            label,
            cursor_file,
            command,
            config.log_dir,
            stall_timeout=config.stall_timeout,
            poll_interval=config.poll_interval,
            popen_fn=popen_fn,
            sleep_fn=sleep_fn,
            clock_fn=clock_fn,
            mtime_fn=mtime_fn,
            completed_fn=completed_fn,
            kill_fn=kill_fn,
        )

        completed = completed_fn(cursor_file)
        if completed >= total:
            logger.info("COMPLETE %s — %d/%d done", label, completed, total)
            return completed

        if exit_code == 0:
            if completed > before:
                logger.info("PROGRESS %s — %d → %d", label, before, completed)
                stale_count = 0
                sleep_fn(config.success_cooldown)
            else:
                stale_count += 1
                logger.info(
                    "NO PROGRESS %s — still %d/%d (stale %d/%d)",
                    label,
                    completed,
                    total,
                    stale_count,
                    config.max_stale,
                )
                sleep_fn(config.no_progress_cooldown)
        else:
            stale_count += 1
            logger.info(
                "FAIL %s — exit=%d, stale %d/%d",
                label,
                exit_code,
                stale_count,
                config.max_stale,
            )
            sleep_fn(config.stall_cooldown)

        if stale_count >= config.max_stale:
            logger.warning(
                "GIVING UP %s — %d/%d done after %d stale rounds",
                label,
                completed,
                total,
                stale_count,
            )
            return completed


def _run_equity_intraday(
    config: BackfillConfig,
    *,
    runner: callable = subprocess.run,
    popen_fn: callable = subprocess.Popen,
    sleep_fn: callable = time.sleep,
    clock_fn: callable = time.monotonic,
    mtime_fn: callable = _file_mtime,
    completed_fn: callable = cursor_completed,
    kill_fn: callable = _kill_process,
) -> int:
    """Full-market equity intraday via Massive flat files."""
    del popen_fn, sleep_fn, clock_fn, mtime_fn, completed_fn, kill_fn
    result = runner(
        [
            config.python_bin,
            str(config.ingest_script),
            "flatfile-ingest",
            "backfill",
            "--workers",
            str(int(os.getenv("MDW_FLATFILE_WORKERS", "4"))),
        ],
        check=False,
    )
    return result.returncode


def _derive_vol_1h(
    vol_preset: str,
    *,
    warehouse_dir: Path | None = None,
) -> int:
    """Derive 1h bars from 30m for all tickers in the vol preset."""
    from clients.intraday_bronze_client import IntradayBronzeClient
    from clients.timeframe_aggregator import aggregate_bars

    with open(vol_preset, encoding="utf-8") as fh:
        tickers = json.load(fh).get("tickers", [])

    wh = warehouse_dir or Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
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


def _run_volatility_lanes(
    config: BackfillConfig,
    *,
    runner: callable = subprocess.run,
    popen_fn: callable = subprocess.Popen,
    sleep_fn: callable = time.sleep,
    clock_fn: callable = time.monotonic,
    mtime_fn: callable = _file_mtime,
    completed_fn: callable = cursor_completed,
    kill_fn: callable = _kill_process,
) -> int:
    """Phase 7-8: CBOE daily volatility + IB volatility intraday."""
    py = config.python_bin
    ingest = str(config.ingest_script)
    log_dir = config.log_dir
    inject = dict(
        popen_fn=popen_fn,
        sleep_fn=sleep_fn,
        clock_fn=clock_fn,
        mtime_fn=mtime_fn,
        completed_fn=completed_fn,
        kill_fn=kill_fn,
    )

    # Phase 7: CBOE volatility daily
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "volatility_cboe.log"
    with log_file.open("a", encoding="utf-8") as fh:
        result = runner(
            [py, ingest, "cboe-vol", "--preset", config.vol_daily_preset],
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if result.returncode != 0:
        logger.warning("CBOE volatility sync exited %d", result.returncode)

    # Phase 8: IB volatility intraday
    name, total = preset_info(config.vol_preset)
    for tf in VOL_INTRADAY_TIMEFRAMES:
        cursor_file = config.cursor_dir / f"cursor_intraday_{tf}_{name}.json"
        gap_fn = make_gap_completed_fn(total, config.vol_preset)
        run_until_done(
            f"intraday_{tf}_{name}",
            cursor_file,
            total,
            [
                py,
                ingest,
                "intraday-backfill",
                "--preset",
                config.vol_preset,
                "--timeframe",
                tf,
                "--source",
                "ib",
                "--asset-class",
                "volatility",
                "--skip-existing",
            ],
            config,
            **{**inject, "completed_fn": gap_fn},
        )

    # Phase 8b: Derive 1h from 30m locally
    _derive_vol_1h(config.vol_preset)
    return 0


def run_backfill(
    config: BackfillConfig,
    *,
    runner: callable = subprocess.run,
    popen_fn: callable = subprocess.Popen,
    sleep_fn: callable = time.sleep,
    clock_fn: callable = time.monotonic,
    mtime_fn: callable = _file_mtime,
    completed_fn: callable = cursor_completed,
    kill_fn: callable = _kill_process,
) -> int:
    from clients.massive_flatfile_client import require_flatfile_credentials

    try:
        require_flatfile_credentials()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 2
    logger.info("=" * 60)
    logger.info("BACKFILL RUNNER START")
    logger.info(
        "Stall timeout: %ds | Poll: %.0fs | Max stale: %d",
        config.stall_timeout,
        config.poll_interval,
        config.max_stale,
    )
    logger.info("=" * 60)

    py = config.python_bin
    ingest = str(config.ingest_script)
    store = str(config.store_script)
    inject = dict(
        popen_fn=popen_fn,
        sleep_fn=sleep_fn,
        clock_fn=clock_fn,
        mtime_fn=mtime_fn,
        completed_fn=completed_fn,
        kill_fn=kill_fn,
    )

    # Phase 1: Normal fetch for each preset
    for preset_path in config.equity_presets:
        name, total = preset_info(preset_path)
        cursor_file = config.log_dir / f"cursor_{name}.json"
        logger.info("── PHASE 1: Normal fetch %s (%d tickers) ──", name, total)
        gap_fn = make_gap_completed_fn(total, preset_path)
        run_until_done(
            f"normal_{name}",
            cursor_file,
            total,
            [
                py,
                ingest,
                "historical",
                "--preset",
                preset_path,
                "--years",
                "0",
                "--skip-existing",
                "--batch-size",
                str(config.batch_size),
                "--max-concurrent",
                str(config.max_concurrent),
            ],
            config,
            **{**inject, "completed_fn": gap_fn},
        )

    # Phase 2: Backfill older data for each preset
    for preset_path in config.equity_presets:
        name, total = preset_info(preset_path)
        cursor_file = config.log_dir / f"cursor_backfill_{name}.json"
        logger.info("── PHASE 2: Backfill %s (%d tickers) ──", name, total)
        gap_fn = make_gap_completed_fn(total, preset_path)
        run_until_done(
            f"backfill_{name}",
            cursor_file,
            total,
            [
                py,
                ingest,
                "historical",
                "--preset",
                preset_path,
                "--backfill",
                "--source",
                "auto",
                "--batch-size",
                str(config.batch_size),
                "--max-concurrent",
                str(config.max_concurrent),
            ],
            config,
            **{**inject, "completed_fn": gap_fn},
        )

    # Phase 3: FRED Treasury rates
    logger.info("── PHASE 3: FRED Treasury rates ──")
    config.log_dir.mkdir(parents=True, exist_ok=True)
    log_file = config.log_dir / "backfill_fred_rates.log"
    with log_file.open("a", encoding="utf-8") as fh:
        result = runner(
            [py, ingest, "fred-rates"],
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if result.returncode != 0:
        logger.warning("FRED rates exited %d", result.returncode)

    # Phases 4-8: Equity intraday and volatility lanes in parallel
    logger.info("Starting equity intraday and volatility lanes in parallel")
    with ThreadPoolExecutor(max_workers=2) as pool:
        equity_future = pool.submit(_run_equity_intraday, config, runner=runner, **inject)
        vol_future = pool.submit(_run_volatility_lanes, config, runner=runner, **inject)
        equity_status = equity_future.result()
        vol_status = vol_future.result()

    if equity_status != 0 or vol_status != 0:
        logger.warning(
            "Parallel lanes: equity=%d volatility=%d",
            equity_status,
            vol_status,
        )
        return 1

    # Phase 9: Postgres rebuild
    if os.getenv("MDW_POSTGRES_DSN"):
        logger.info("── PHASE 9: Postgres analytical rebuild ──")
        config.log_dir.mkdir(parents=True, exist_ok=True)
        for ac, tf_arg in [("equity", "all"), ("volatility", "1d")]:
            log_f = config.log_dir / f"backfill_postgres_{ac}.log"
            with log_f.open("a", encoding="utf-8") as fh:
                pg_result = runner(
                    [
                        py,
                        store,
                        "rebuild-postgres",
                        "--asset-class",
                        ac,
                        "--timeframe",
                        tf_arg,
                    ]
                    + (["--include-reliability"] if ac == "equity" else []),
                    stdout=fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                )
            if pg_result.returncode != 0:
                logger.warning("Postgres %s rebuild exited %d", ac, pg_result.returncode)
    else:
        logger.info("Postgres rebuild skipped — MDW_POSTGRES_DSN not set")

    logger.info("=" * 60)
    logger.info("ALL DONE")
    logger.info("=" * 60)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Full warehouse backfill runner")
    parser.add_argument(
        "--stall-timeout",
        type=int,
        default=None,
        help="Seconds of no activity before killing a subprocess",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=None,
        help="Seconds between activity checks",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = build_config()
    overrides: dict = {}
    if args.stall_timeout is not None:
        overrides["stall_timeout"] = args.stall_timeout
    if args.poll_interval is not None:
        overrides["poll_interval"] = args.poll_interval
    if overrides:
        config = replace(config, **overrides)

    return run_backfill(config)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
