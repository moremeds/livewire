"""Daily coverage report + auto-recovery for Livewire.

For each tracked timeframe (1d, 1m, 1h, 5m), counts how many symbols have
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
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

import pyarrow.parquet as pq
from rich.console import Console

from clients.intraday_bronze_client import INTRADAY_PARQUET_FILENAME
from clients.symbol_paths import decode_symbol
from livewire_scripts.daily_update import is_trading_day, previous_trading_day
from livewire_scripts.paths import data_lake_dir, log_dir

log = logging.getLogger(__name__)
console = Console()

_DATA_LAKE: Path | None = None
_LOG_DIR: Path | None = None
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
_INGEST_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ingest.py"
_OPS_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ops.py"

TIMEFRAMES: tuple[str, ...] = ("1d", "1m", "1h", "5m", "30m")
DEFAULT_THRESHOLD = float(os.getenv("MDW_COVERAGE_ALERT_THRESHOLD", "0.95"))
DEFAULT_SAFETY_CAP = 100


def _resolved_data_lake() -> Path:
    return _DATA_LAKE or data_lake_dir()


def _resolved_log_dir() -> Path:
    return _LOG_DIR or log_dir()


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


def _symbol_from_parquet_path(path: Path) -> str:
    return decode_symbol(path.parent.name.split("=", 1)[1])


def _raw_symbols_for_date(target_date: date, bronze_root: Path) -> set[str]:
    path = (
        bronze_root.parent
        / "raw"
        / "massive"
        / "us_stocks_sip"
        / "minute_aggs_v1"
        / f"date={target_date.isoformat()}"
        / "_symbols.parquet"
    )
    if not path.exists():
        return set()
    return set(pq.read_table(path, columns=["ticker"]).column("ticker").to_pylist())


def _latest_date_in_parquet(path: Path, column_name: str) -> date | None:
    """Return the latest value in *column_name* using parquet footer statistics.

    Reads only the file's footer (row-group min/max stats) instead of the full
    column — the whole-column read is what made coverage take hours over ~13K
    symbols. Falls back to a full column read only when stats are unavailable.
    """
    md = pq.ParquetFile(path).metadata
    col_idx = next(
        (i for i in range(md.num_columns) if md.schema.column(i).path == column_name),
        None,
    )
    if col_idx is not None:
        max_value = None
        for rg_idx in range(md.num_row_groups):
            stats = md.row_group(rg_idx).column(col_idx).statistics
            if stats is None or not stats.has_min_max:
                max_value = None
                break
            candidate = stats.max
            if max_value is None or candidate > max_value:
                max_value = candidate
        if max_value is not None:
            if isinstance(max_value, datetime):
                return max_value.date()
            if isinstance(max_value, date):
                return max_value
            if isinstance(max_value, (str, bytes)):
                raw = max_value.decode() if isinstance(max_value, bytes) else max_value
                return date.fromisoformat(raw[:10])
    # Fallback: stats unavailable -> full column read (rare).
    table = pq.read_table(path, columns=[column_name])
    values = table.column(column_name).to_pylist()
    if not values:
        return None
    dates = [value.date() if isinstance(value, datetime) else value for value in values]
    return max(dates)


def compute_coverage(
    target_date: date,
    bronze_root: Path | None = None,
) -> dict[str, CoverageResult]:
    """Return per-timeframe coverage as-of *target_date*.

    Denominator is the **active bronze universe for that timeframe** — the
    symbols we actually carry — not the full provider SIP set. A symbol counts
    as present if it is current through *target_date* OR it is absent from the
    day's raw traded set (it simply did not trade; no-trade is not missing).
    """
    bronze_root = bronze_root or _resolved_data_lake() / "bronze"
    results: dict[str, CoverageResult] = {}

    # The provider's exact target-day traded set, used only to exclude
    # instruments that did not trade from the "missing" count.
    traded_today = _raw_symbols_for_date(target_date, bronze_root)

    for tf in TIMEFRAMES:
        parquet_paths = sorted((bronze_root / "asset_class=equity").glob(f"symbol=*/{_filename_for(tf)}"))
        universe = {_symbol_from_parquet_path(path) for path in parquet_paths}

        column_name = "trade_date" if tf == "1d" else "bar_timestamp"
        latest_by_symbol = {
            _symbol_from_parquet_path(path): latest
            for path in parquet_paths
            if (latest := _latest_date_in_parquet(path, column_name)) is not None
        }
        present_symbols = {
            symbol
            for symbol in universe
            if (latest_by_symbol.get(symbol) or date.min) >= target_date
            or (traded_today and symbol not in traded_today)
        }
        missing = sorted(universe - present_symbols)
        results[tf] = CoverageResult(
            timeframe=tf,
            total=len(universe),
            present=len(present_symbols),
            missing_symbols=missing,
        )

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
    resolved_log_dir = _resolved_log_dir()
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = resolved_log_dir / f"coverage_{target_date:%Y-%m-%d}.log"
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

    effective_target = target_date or datetime.now(UTC).date()

    if timeframe == "1d":
        cmd = [
            sys.executable,
            str(_INGEST_SCRIPT),
            "daily",
            "--source",
            "massive",
            "--force",
            "--target-date",
            effective_target.isoformat(),
            "--tickers",
            *missing_symbols,
        ]
    else:
        cmd = [
            sys.executable,
            str(_INGEST_SCRIPT),
            "flatfile-ingest",
            "repair",
            "--dates",
            effective_target.isoformat(),
        ]
    console.print(f"[cyan]Auto-recover {timeframe}: launching backfill for {len(missing_symbols)} symbols[/cyan]")
    subprocess.run(cmd, check=False)

    rechecked = compute_coverage(effective_target, bronze_root=bronze_root)[timeframe]
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
            summary_lines.append(f"{o.timeframe}: ABORTED — {o.reason}; {len(o.still_missing)} missing")
        else:
            summary_lines.append(
                f"{o.timeframe}: recovered {o.recovered}/{len(o.attempted)}, {len(o.still_missing)} still missing"
            )
    error_summary = "coverage_report: " + "; ".join(summary_lines)
    cmd = [
        sys.executable,
        str(_OPS_SCRIPT),
        "send-alert",
        "--run-date",
        target_date.isoformat(),
        "--log-file",
        str(log_path),
        "--error-summary",
        error_summary,
        "--repo-root",
        str(_REPO_ROOT),
        "--job-name",
        "coverage_report",
    ]
    subprocess.run(cmd, check=False)


def _resolve_target_date(force: bool, override: date | None) -> date | None:
    if override is not None:
        return override
    today = datetime.now(UTC).date()
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
        console.print(f"[yellow]{date.today()} is not a trading day. Use --force or --target-date.[/yellow]")
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
