"""Daily coverage report + auto-recovery for the market data warehouse.

For each of the three timeframes (1d, 1h, 5m), counts how many symbols have
bars current as-of the target trading day. If coverage drops below the
threshold (default 95%), triggers a targeted backfill via fetch_ib_historical
and re-checks. Sends an email alert when post-recovery coverage is still
incomplete; logs INFO only when recovery is fully successful.

Spec: docs/superpowers/specs/2026-04-06-multi-timeframe-design.md § 17 Layer 2.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
from rich.console import Console

from clients.intraday_bronze_client import INTRADAY_PARQUET_FILENAME
from livewire_scripts.daily_update import is_trading_day, previous_trading_day

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"
_LOG_DIR = _WAREHOUSE_DIR / "logs"
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
_INGEST_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ingest.py"
_OPS_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ops.py"

TIMEFRAMES: tuple[str, ...] = ("1d", "1h", "5m")
DEFAULT_THRESHOLD = float(os.getenv("MDW_COVERAGE_ALERT_THRESHOLD", "0.95"))
DEFAULT_SAFETY_CAP = 100


@dataclass
class CoverageResult:
    timeframe: str
    total: int
    present: int
    missing_symbols: list[str] = field(default_factory=list)

    @property
    def ratio(self) -> float:
        return 1.0 if self.total == 0 else self.present / self.total


@dataclass
class RecoveryOutcome:
    timeframe: str
    attempted: list[str]
    recovered: int
    still_missing: list[str]
    aborted: bool = False
    reason: str = ""


def _filename_for(tf: str) -> str:
    return "1d.parquet" if tf == "1d" else INTRADAY_PARQUET_FILENAME[tf]


def _glob_for(tf: str, bronze_root: Path) -> str:
    pattern = bronze_root / "asset_class=equity" / "symbol=*" / _filename_for(tf)
    return str(pattern).replace("'", "''")


def _list_symbols(tf: str, bronze_root: Path) -> set[str]:
    bronze_dir = bronze_root / "asset_class=equity"
    if not bronze_dir.exists():
        return set()
    fname = _filename_for(tf)
    return {
        p.parent.name.split("=", 1)[1]
        for p in bronze_dir.glob(f"symbol=*/{fname}")
    }


def compute_coverage(
    target_date: date,
    bronze_root: Path | None = None,
) -> dict[str, CoverageResult]:
    """Return per-timeframe coverage as-of *target_date*."""
    bronze_root = bronze_root or _DATA_LAKE / "bronze"
    results: dict[str, CoverageResult] = {}

    # Universe = union of symbols across all timeframes (so a missing tf shows up
    # as missing rather than silently passing because the file doesn't exist).
    universe = set()
    for tf in TIMEFRAMES:
        universe |= _list_symbols(tf, bronze_root)
    universe_size = len(universe)

    con = duckdb.connect(":memory:")
    try:
        for tf in TIMEFRAMES:
            symbols_at_tf = _list_symbols(tf, bronze_root)
            if not symbols_at_tf:
                results[tf] = CoverageResult(
                    timeframe=tf,
                    total=universe_size,
                    present=0,
                    missing_symbols=sorted(universe),
                )
                continue

            ts_col = "trade_date" if tf == "1d" else "CAST(bar_timestamp AS DATE)"
            sql = f"""
                SELECT symbol, MAX({ts_col}) AS latest
                FROM read_parquet('{_glob_for(tf, bronze_root)}', hive_partitioning=true)
                GROUP BY symbol
            """
            rows = con.execute(sql).fetchall()
            present_symbols = {sym for sym, latest in rows if latest >= target_date}
            missing = sorted(universe - present_symbols)
            results[tf] = CoverageResult(
                timeframe=tf,
                total=universe_size,
                present=len(present_symbols),
                missing_symbols=missing,
            )
    finally:
        con.close()

    return results


def format_one_liner(target_date: date, results: dict[str, CoverageResult]) -> str:
    """Return the spec § 17 single-line summary."""
    parts = []
    for tf in TIMEFRAMES:
        r = results[tf]
        parts.append(f"{tf}={r.present}/{r.total} ({r.ratio:.2%})")
    return f"{target_date} coverage: " + " ".join(parts)


def format_missing_blocks(results: dict[str, CoverageResult], max_listed: int = 10) -> list[str]:
    """Return per-timeframe 'missing:' lines for the log file."""
    blocks: list[str] = []
    for tf in TIMEFRAMES:
        r = results[tf]
        if not r.missing_symbols:
            continue
        head = ", ".join(r.missing_symbols[:max_listed])
        suffix = ""
        if len(r.missing_symbols) > max_listed:
            suffix = f", ... ({len(r.missing_symbols)} total)"
        blocks.append(f"  {tf} missing: {head}{suffix}")
    return blocks


def write_coverage_log(target_date: date, line: str, missing_blocks: Iterable[str]) -> Path:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _LOG_DIR / f"coverage_{target_date:%Y-%m-%d}.log"
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        for block in missing_blocks:
            fh.write(block + "\n")
    return log_path


def auto_recover(
    timeframe: str,
    missing_symbols: list[str],
    bronze_root: Path | None = None,
    target_date: date | None = None,
    safety_cap: int = DEFAULT_SAFETY_CAP,
) -> RecoveryOutcome:
    """Trigger a targeted backfill subprocess and re-check coverage."""
    if not missing_symbols:
        return RecoveryOutcome(timeframe=timeframe, attempted=[], recovered=0, still_missing=[])

    if len(missing_symbols) > safety_cap:
        return RecoveryOutcome(
            timeframe=timeframe,
            attempted=list(missing_symbols),
            recovered=0,
            still_missing=list(missing_symbols),
            aborted=True,
            reason=f"safety_cap (>{safety_cap} missing symbols)",
        )

    if timeframe == "1d":
        cmd = [
            sys.executable,
            str(_INGEST_SCRIPT),
            "historical",
            "--tickers", *missing_symbols,
        ]
    else:
        cmd = [
            sys.executable,
            str(_INGEST_SCRIPT),
            "intraday-backfill",
            "--timeframe", timeframe,
            "--tickers", *missing_symbols,
        ]
    console.print(
        f"[cyan]Auto-recover {timeframe}: launching backfill for "
        f"{len(missing_symbols)} symbols[/cyan]"
    )
    subprocess.run(cmd, check=False)

    target = target_date or date.today()
    rechecked = compute_coverage(target, bronze_root=bronze_root)[timeframe]
    still_missing = [s for s in missing_symbols if s in rechecked.missing_symbols]
    recovered = len(missing_symbols) - len(still_missing)
    return RecoveryOutcome(
        timeframe=timeframe,
        attempted=list(missing_symbols),
        recovered=recovered,
        still_missing=still_missing,
    )


def _send_alert(
    target_date: date,
    outcomes: list[RecoveryOutcome],
    log_path: Path,
) -> None:
    """Send the coverage email via the existing failure-email script."""
    summary_lines = []
    for o in outcomes:
        if o.aborted:
            summary_lines.append(
                f"{o.timeframe}: ABORTED — {o.reason}; {len(o.still_missing)} missing"
            )
        else:
            summary_lines.append(
                f"{o.timeframe}: recovered {o.recovered}/{len(o.attempted)}, "
                f"{len(o.still_missing)} still missing"
            )
    error_summary = "coverage_report: " + "; ".join(summary_lines)
    cmd = [
        sys.executable,
        str(_OPS_SCRIPT),
        "send-alert",
        "--run-date", target_date.isoformat(),
        "--log-file", str(log_path),
        "--error-summary", error_summary,
        "--repo-root", str(_REPO_ROOT),
        "--job-name", "coverage_report",
    ]
    subprocess.run(cmd, check=False)


def _resolve_target_date(force: bool, override: date | None) -> date | None:
    if override is not None:
        return override
    today = date.today()
    if is_trading_day(today):
        return today
    if force:
        return previous_trading_day(today)
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily coverage report + auto-recovery")
    parser.add_argument(
        "--target-date",
        type=date.fromisoformat,
        help="Target trading day (YYYY-MM-DD). Defaults to today if a trading day.",
    )
    parser.add_argument(
        "--no-recover",
        action="store_true",
        help="Report coverage only — skip auto-recovery subprocess.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Coverage ratio below which auto-recovery fires (default {DEFAULT_THRESHOLD}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run on a non-trading day (uses the previous trading day).",
    )
    args = parser.parse_args()

    target = _resolve_target_date(args.force, args.target_date)
    if target is None:
        console.print(
            f"[yellow]{date.today()} is not a trading day. Use --force or --target-date.[/yellow]"
        )
        return

    console.print(f"\n[bold]Coverage Report[/bold]  target_date={target}")
    results = compute_coverage(target)
    line = format_one_liner(target, results)
    console.print(line)
    blocks = format_missing_blocks(results)
    for block in blocks:
        console.print(block)
    log_path = write_coverage_log(target, line, blocks)

    if args.no_recover:
        return

    # Decide which timeframes need recovery
    outcomes: list[RecoveryOutcome] = []
    for tf in TIMEFRAMES:
        r = results[tf]
        if r.ratio >= args.threshold:
            continue
        outcome = auto_recover(
            timeframe=tf,
            missing_symbols=r.missing_symbols,
            target_date=target,
        )
        outcomes.append(outcome)

    if not outcomes:
        log.info("Coverage above threshold for all timeframes — no recovery needed")
        return

    # Append recovery outcome lines to the same log
    with log_path.open("a", encoding="utf-8") as fh:
        for o in outcomes:
            if o.aborted:
                fh.write(f"  {o.timeframe} recovery ABORTED: {o.reason}\n")
            else:
                fh.write(
                    f"  {o.timeframe} recovery: recovered {o.recovered}/"
                    f"{len(o.attempted)}, still_missing={len(o.still_missing)}\n"
                )

    needs_email = any(o.aborted or o.still_missing for o in outcomes)
    if needs_email:
        _send_alert(target, outcomes, log_path)
    else:
        console.print("[green]All timeframes recovered — INFO log only, no email[/green]")


if __name__ == "__main__":
    main()
