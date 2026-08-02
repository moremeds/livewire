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
import json
import logging
import os
import subprocess
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

import pyarrow.parquet as pq
from rich.console import Console

from clients.intraday_bronze_client import INTRADAY_PARQUET_FILENAME
from clients.symbol_paths import decode_symbol
from livewire_scripts.daily_update import _et_today, is_trading_day, previous_trading_day
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

# Threads for the per-file footer pass. Measured 2026-08-02 over the 13,270
# equity `1d` files, filesystem cache warm: 1 -> 154.0s, 8 -> 34.9s,
# 16 -> 29.2s, 32 -> 25.2s. 16 takes 5.3x off the wall clock; past that the
# curve flattens and only the file-descriptor pressure keeps growing.
#
# This is why coverage had to be parallel at all: five timeframes at ~150-300s
# each does not fit the 600s budget `_spawn_post_success_quality` gives it, and
# hasn't since the universe reached ~20K symbols. It timed out every night from
# 2026-07-07, so coverage logs stop at 2026-06-17 and every weekly report since
# has been an empty "No coverage logs found" stub.
FOOTER_READ_WORKERS = 16


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

    if not traded_today:
        # Be loud about it: with no raw partition the no-trade exemption
        # silently switches off and the same code reports a different, stricter
        # denominator with nothing in the log saying which one ran.
        log.warning(
            "No raw minute partition for %s — coverage runs without the no-trade "
            "exemption and intraday denominators fall back to files on disk.",
            target_date,
        )

    for tf in TIMEFRAMES:
        parquet_paths = sorted((bronze_root / "asset_class=equity").glob(f"symbol=*/{_filename_for(tf)}"))
        on_disk = {_symbol_from_parquet_path(path) for path in parquet_paths}

        # For intraday, the provider's traded set is the honest denominator.
        # Globbing files on disk made the denominator self-defining: a symbol
        # with no 5m.parquet was not in the 5m universe, so a symbol that
        # silently stopped receiving intraday — or never got a file at all —
        # could never be counted missing, and coverage read 100% forever.
        universe = on_disk if (tf == "1d" or not traded_today) else set(traded_today)

        column_name = "trade_date" if tf == "1d" else "bar_timestamp"
        # Threaded: the pass is one small footer read per file, so it is bound by
        # I/O rather than the GIL — pyarrow releases it for the read and the parse.
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=FOOTER_READ_WORKERS) as pool:
            latest_dates = pool.map(partial(_latest_date_in_parquet, column_name=column_name), parquet_paths)
            latest_by_symbol = {
                _symbol_from_parquet_path(path): latest
                for path, latest in zip(parquet_paths, latest_dates, strict=True)
                if latest is not None
            }
        # Logged so the next time this outgrows its budget it is measurable rather
        # than a bare timeout. It outgrew the old one silently for four weeks.
        log.info("%s: read %d footers in %.1fs", tf, len(parquet_paths), time.monotonic() - started)
        present_symbols = {
            symbol
            for symbol in universe
            if (latest_by_symbol.get(symbol) or date.min) >= target_date
            or (tf == "1d" and traded_today and symbol not in traded_today)
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


# Daily-only asset classes. They were in no denominator at any timeframe, so
# VIX going stale for a week was invisible to coverage. There is no recovery
# path for these (CBOE/FRED/IB own them), so this reports and alerts only.
NON_EQUITY_ASSET_CLASSES: tuple[str, ...] = ("volatility", "futures", "rates")


def compute_non_equity_coverage(
    target_date: date,
    bronze_root: Path | None = None,
) -> dict[str, CoverageResult]:
    """Return per-asset-class 1d freshness for the non-equity universes.

    No no-trade exemption: these are small, continuously-quoted universes
    (~14 volatility indices, a handful of futures, 4 Treasury series). A stale
    one is a real gap, not an instrument that happened not to print.
    """
    bronze_root = bronze_root or _resolved_data_lake() / "bronze"
    results: dict[str, CoverageResult] = {}
    for asset_class in NON_EQUITY_ASSET_CLASSES:
        paths = sorted((bronze_root / f"asset_class={asset_class}").glob("symbol=*/1d.parquet"))
        universe = {_symbol_from_parquet_path(p) for p in paths}
        present = {
            _symbol_from_parquet_path(p)
            for p in paths
            if (latest := _latest_date_in_parquet(p, "trade_date")) is not None and latest >= target_date
        }
        results[asset_class] = CoverageResult(
            timeframe=asset_class,
            total=len(universe),
            present=len(present),
            missing_symbols=sorted(universe - present),
        )
    return results


def format_non_equity_line(target_date: date, results: dict[str, CoverageResult]) -> str:
    parts = [f"{ac}={results[ac].present}/{results[ac].total}" for ac in NON_EQUITY_ASSET_CLASSES]
    return f"{target_date} non-equity 1d: " + " ".join(parts)


MISSING_JSON_PREFIX = "MISSING_JSON "


def format_missing_json(results: dict[str, CoverageResult]) -> str:
    """One machine-readable line carrying the COMPLETE missing lists.

    The human `missing:` blocks are truncated to 10 names for readability, and
    the weekly report parsed only those — so a symbol whose name sorted after
    the first ten could never be detected as a persistent gap no matter how
    many consecutive days it was absent.
    """
    payload = {tf: results[tf].missing_symbols for tf in TIMEFRAMES if results[tf].missing_symbols}
    return MISSING_JSON_PREFIX + json.dumps(payload, separators=(",", ":"), sort_keys=True)


def parse_missing_json(text: str) -> dict[str, list[str]] | None:
    """Return the last well-formed MISSING_JSON payload in *text*, or None."""
    result = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith(MISSING_JSON_PREFIX):
            continue
        try:
            result = json.loads(stripped[len(MISSING_JSON_PREFIX) :])
        except json.JSONDecodeError:
            continue
    return result


def write_coverage_log(
    target_date: date,
    line: str,
    missing_blocks: Iterable[str],
    results: dict[str, CoverageResult] | None = None,
) -> Path:
    resolved_log_dir = _resolved_log_dir()
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = resolved_log_dir / f"coverage_{target_date:%Y-%m-%d}.log"
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        for block in missing_blocks:
            fh.write(block + "\n")
        if results is not None:
            fh.write(format_missing_json(results) + "\n")
    return log_path


def auto_recover(
    timeframe: str,
    missing_symbols: list[str],
    bronze_root: Path | None = None,
    target_date: date | None = None,
    safety_cap: int = DEFAULT_SAFETY_CAP,
) -> RecoveryOutcome:
    """Trigger a targeted backfill subprocess and re-check coverage.

    The cap used to abort outright, which made it self-perpetuating: recovery
    only ran below the alert ratio AND below the cap, so a large outage landed
    in a dead zone where coverage was bad enough to alarm but too bad to act,
    and the same symbols were dropped and re-emailed every night forever.
    1d recovery now runs in cap-sized batches instead.

    The cap never applied to intraday in the first place: that branch passes no
    symbols at all — it republishes the whole day's flat file — so its cost is
    date-shaped, not symbol-shaped, and refusing to run because 101 symbols are
    missing was measuring the wrong quantity.
    """
    if not missing_symbols:
        return RecoveryOutcome(timeframe=timeframe, attempted=[], recovered=0, still_missing=[])

    effective_target = target_date or datetime.now(UTC).date()

    if timeframe == "1d":
        batches = [missing_symbols[i : i + safety_cap] for i in range(0, len(missing_symbols), safety_cap)]
        if len(batches) > 1:
            console.print(
                f"[cyan]Auto-recover 1d: {len(missing_symbols)} symbols in {len(batches)} "
                f"batch(es) of up to {safety_cap}[/cyan]"
            )
        for batch in batches:
            subprocess.run(
                [
                    sys.executable,
                    str(_INGEST_SCRIPT),
                    "daily",
                    "--source",
                    "massive",
                    "--force",
                    "--target-date",
                    effective_target.isoformat(),
                    "--tickers",
                    *batch,
                ],
                check=False,
            )
    else:
        console.print(
            f"[cyan]Auto-recover {timeframe}: republishing {effective_target} "
            f"({len(missing_symbols)} symbols missing)[/cyan]"
        )
        subprocess.run(
            [
                sys.executable,
                str(_INGEST_SCRIPT),
                "flatfile-ingest",
                "repair",
                "--dates",
                effective_target.isoformat(),
            ],
            check=False,
        )

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
    """Resolve the session to measure: the most recently *completed* one.

    This used to be `datetime.now(UTC).date()`. The scheduled job runs at 06:00
    UTC — 02:00 ET — so on a weekday that resolved to a session that had not yet
    opened, and every symbol read as missing. `coverage_2026-06-17.log` is the
    artifact: `1d=0/20396 (0.00%)` on every timeframe. A 0% reading then fired
    auto-recovery across the whole universe, which is why `coverage report`
    exhausted its 600s budget night after night and no coverage log has been
    written since 2026-06-17 — leaving the digest's Coverage line permanently
    "(not found)".

    `daily_update._et_today` already encodes exactly this ET-close boundary for
    the ingest lane. Coverage measures what that lane ingested, so it must
    resolve to the same session rather than keep a second, wrong calendar.
    """
    if override is not None:
        return override
    target = _et_today()
    if is_trading_day(target):
        return target
    if force:
        return previous_trading_day(target)
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
    # Non-equity was in no denominator at all: a stale VIX, a stale DGS10 or a
    # stale futures contract could never register as missing.
    non_equity = compute_non_equity_coverage(target)
    non_equity_line = format_non_equity_line(target, non_equity)
    console.print(non_equity_line)
    stale_non_equity = {ac: r.missing_symbols for ac, r in non_equity.items() if r.missing_symbols}
    for asset_class, symbols in stale_non_equity.items():
        console.print(f"  [yellow]{asset_class} stale:[/yellow] {', '.join(symbols)}")

    log_path = write_coverage_log(target, line, [*blocks, non_equity_line], results)

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
        if stale_non_equity:
            # No recovery path exists for these — CBOE/FRED/IB own them — so
            # reporting is all we can honestly do, but silence was worse.
            _send_alert(
                target,
                [
                    RecoveryOutcome(
                        timeframe=asset_class,
                        attempted=symbols,
                        recovered=0,
                        still_missing=symbols,
                        aborted=True,
                        reason="no recovery path for this asset class",
                    )
                    for asset_class, symbols in stale_non_equity.items()
                ],
                log_path,
            )
            return
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
