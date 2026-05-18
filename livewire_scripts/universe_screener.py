#!/usr/bin/env python3
"""IB Scanner-based universe builder for Livewire.

Runs multiple IB scanner sweeps to discover ~1000 U.S. equities by market cap,
volume, and turnover. Compares against current bronze parquet, handles a grace
period for removals, archives delisted tickers, triggers historical backfill for
new additions, and writes presets/screened-universe.json.

Usage:
    source ~/market-warehouse/.venv/bin/activate

    # Normal daily run:
    python scripts/universe_screener.py

    # Dry-run — report changes without modifying anything:
    python scripts/universe_screener.py --dry-run

    # Force run even if already ran today or it's not a trading day:
    python scripts/universe_screener.py --force
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from rich.console import Console

# Add project root to path so clients module is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients import BronzeClient
from clients.ib_client import IBClient
from livewire_scripts.daily_update import is_trading_day

# ── Constants ───────────────────────────────────────────────────────────────

TARGET_SIZE = 1000
GRACE_DAYS = 3
MAX_REMOVALS = 50
EMAIL_THRESHOLD = 10
_SCANNER_THROTTLE_SECONDS = 1.0

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"

_SCRIPT_DIR = Path(__file__).resolve().parent
_INGEST_SCRIPT = PROJECT_ROOT / "scripts" / "livewire_ingest.py"
_OPS_SCRIPT = PROJECT_ROOT / "scripts" / "livewire_ops.py"
_PRESET_PATH = PROJECT_ROOT / "presets" / "screened-universe.json"
_CORE_ETFS_PATH = PROJECT_ROOT / "presets" / "core-etfs.json"
_STATE_PATH = _WAREHOUSE_DIR / "logs" / "screener_state.json"
_LOG_DIR = _WAREHOUSE_DIR / "logs"

console = Console()
log = logging.getLogger(__name__)

# ── Pure helper functions ───────────────────────────────────────────────────


def compare_universes(
    current: set[str], scanned: set[str]
) -> tuple[set[str], set[str]]:
    """Compare current bronze universe against newly scanned universe.

    Returns:
        (additions, removals) — tickers to add and tickers to remove.
    """
    additions = scanned - current
    removals = current - scanned
    return additions, removals


def load_core_etfs() -> set[str]:
    """Load the core ETF ticker list from presets/core-etfs.json.

    Returns empty set if the file doesn't exist (graceful degradation).
    """
    if not _CORE_ETFS_PATH.exists():
        return set()
    with _CORE_ETFS_PATH.open() as f:
        data = json.load(f)
    return set(data.get("tickers", []))


def load_screener_state(path: Path) -> dict | None:
    """Load JSON state file. Returns None if file doesn't exist."""
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def save_screener_state(path: Path, state: dict) -> None:
    """Write state JSON. Creates parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def update_absent_counts(
    absent_counts: dict[str, int],
    removals: set[str],
    scanned: set[str],
) -> dict[str, int]:
    """Update the per-ticker absent day count.

    - Increments count for tickers in ``removals``.
    - Removes tickers that appear in ``scanned`` (they're back).
    - Carries forward others unchanged.
    """
    result = dict(absent_counts)

    # Remove tickers that have reappeared in the scan
    for ticker in list(result.keys()):
        if ticker in scanned:
            del result[ticker]

    # Increment absent count for newly removed tickers
    for ticker in removals:
        result[ticker] = result.get(ticker, 0) + 1

    return result


def get_removals_after_grace(
    absent_counts: dict[str, int], grace_days: int = GRACE_DAYS
) -> set[str]:
    """Return tickers that have been absent for >= grace_days consecutive days."""
    return {ticker for ticker, count in absent_counts.items() if count >= grace_days}


def write_universe_preset(path: Path, tickers: list[str]) -> None:
    """Write sorted preset JSON with name, description, generated_at, tickers."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "name": "screened-universe",
        "description": (
            "IB Scanner-based U.S. equity universe (~1000 tickers) "
            "by market cap, volume, and turnover."
        ),
        "generated_at": datetime.now(UTC).isoformat(),
        "tickers": sorted(tickers),
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def log_changes(
    log_dir: Path,
    run_date: date,
    additions: set[str],
    removals: set[str],
) -> Path:
    """Write a dated log file with additions and removals. Returns the path."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"screener_{run_date.isoformat()}.log"

    lines = [
        f"Universe screener run: {run_date.isoformat()}",
        f"Additions ({len(additions)}): {', '.join(sorted(additions)) or 'none'}",
        f"Removals ({len(removals)}): {', '.join(sorted(removals)) or 'none'}",
    ]
    log_path.write_text("\n".join(lines) + "\n")
    return log_path


# ── IB Scanner ─────────────────────────────────────────────────────────────


async def run_scanner_sweeps(ib) -> set[str]:
    """Run multiple IB scanner sweeps and return a deduplicated set of tickers.

    IB caps each scan at 50 results, so we use **tight price bands** to force
    non-overlapping result sets within each scan type.  Five scan types × eight
    price bands = 40 sweeps → ~1,000+ unique tickers.

    ``marketCapAbove`` is avoided because IB returns empty results for that
    filter outside trading hours.
    """
    from ib_async import ScannerSubscription  # lazy import

    scan_codes = [
        "MOST_ACTIVE",       # Share volume — broad coverage
        "MOST_ACTIVE_USD",   # Dollar volume — large-cap biased
        "TOP_TRADE_COUNT",   # Trade frequency — high turnover
        "HOT_BY_VOLUME",     # Volume vs avg — momentum / news-driven
        "TOP_PERC_GAIN",     # Price movers — captures unusual activity
    ]

    # Tight price bands prevent overlap within each scan type
    price_bands: list[tuple[float, float]] = [
        (200.0, 1e6),
        (100.0, 200.0),
        (50.0, 100.0),
        (30.0, 50.0),
        (20.0, 30.0),
        (15.0, 20.0),
        (10.0, 15.0),
        (5.0, 10.0),
    ]

    symbols: set[str] = set()

    for scan_code in scan_codes:
        for above_price, below_price in price_bands:
            sub = ScannerSubscription(
                instrument="STK",
                locationCode="STK.US.MAJOR",
                scanCode=scan_code,
                numberOfRows=50,
            )
            sub.abovePrice = above_price
            sub.belowPrice = below_price
            try:
                results = await ib.reqScannerDataAsync(sub)
            except Exception as exc:
                log.warning("Scanner %s $%.0f-$%.0f failed: %s", scan_code, above_price, below_price, exc)
                results = []
            if results:
                for item in results:
                    symbols.add(item.contractDetails.contract.symbol)
            # IB limits concurrent scanner subscriptions; throttle to avoid error 162
            await asyncio.sleep(_SCANNER_THROTTLE_SECONDS)

    return symbols


# ── Alert ──────────────────────────────────────────────────────────────────


def _send_screener_alert(
    run_date: date,
    additions: set[str],
    removals: set[str],
) -> None:
    """Send an email alert via the existing Nodemailer CLI."""
    error_summary = (
        f"universe_screener: {len(additions)} additions, {len(removals)} removals "
        f"on {run_date.isoformat()}."
    )
    cmd = [
        sys.executable,
        str(_OPS_SCRIPT),
        "send-alert",
        "--run-date", run_date.isoformat(),
        "--error-summary", error_summary,
        "--repo-root", str(PROJECT_ROOT),
        "--job-name", "universe_screener",
    ]
    subprocess.run(cmd, check=False)


# ── Main entry point ────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for the universe screener."""
    parser = argparse.ArgumentParser(description="IB Scanner universe screener")
    parser.add_argument(
        "--dry-run", action="store_true", help="Report changes without modifying anything"
    )
    parser.add_argument(
        "--force", action="store_true", help="Run even if already ran today or not a trading day"
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("MDW_IB_HOST", "127.0.0.1"),
        help="IB Gateway host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MDW_IB_PORT", "4001")),
        help="IB Gateway port",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    today = date.today()

    # ── Trading day check ──────────────────────────────────────────────
    if not args.force and not is_trading_day(today):
        log.info("Not a trading day (%s), skipping. Use --force to override.", today)
        sys.exit(0)

    # ── Idempotency check ──────────────────────────────────────────────
    state = load_screener_state(_STATE_PATH)
    if not args.force and state is not None and state.get("run_date") == today.isoformat():
        log.info("Already ran today (%s), skipping. Use --force to override.", today)
        sys.exit(0)

    # ── Determine current bronze universe ──────────────────────────────
    bronze_dir = _DATA_LAKE / "bronze" / "asset_class=equity"
    with BronzeClient(bronze_dir=bronze_dir) as bronze:
        current_universe = bronze.get_existing_symbols()

    log.info("Current bronze universe: %d tickers", len(current_universe))

    # ── Run IB scanner sweeps ──────────────────────────────────────────
    with IBClient() as ib_client:
        ib_client.connect(host=args.host, port=args.port)
        scanned_universe = ib_client.ib.run(run_scanner_sweeps(ib_client.ib))
    log.info("Scanned universe: %d tickers", len(scanned_universe))

    # ── Compare universes (excluding core ETFs from removal logic) ─────
    core_etfs = load_core_etfs()
    log.info("Core ETFs (always included): %d tickers", len(core_etfs))

    # Union scanner output with core ETFs BEFORE the comparison
    full_scanned = scanned_universe | core_etfs

    # Exclude core ETFs from the removal-eligible set on BOTH sides
    current_excl_core = current_universe - core_etfs
    scanned_excl_core = full_scanned - core_etfs

    additions = full_scanned - current_universe  # may include core ETFs not yet in bronze
    candidate_removals = current_excl_core - scanned_excl_core  # core ETFs CANNOT be here

    # ── Load prior absent counts from state ────────────────────────────
    prior_absent = state.get("absent_counts", {}) if state is not None else {}
    # Defensive: if a previous (buggy) run added a core ETF here, drop it
    prior_absent = {k: v for k, v in prior_absent.items() if k not in core_etfs}

    # ── Update absent counts ───────────────────────────────────────────
    new_absent = update_absent_counts(prior_absent, candidate_removals, scanned_excl_core)

    # ── Bootstrap mode: first run (no state file) skips all removals ──
    bootstrap_mode = state is None
    if bootstrap_mode:
        log.info("Bootstrap mode: first run — skipping all removals.")
        confirmed_removals: set[str] = set()
    else:
        confirmed_removals = get_removals_after_grace(new_absent, grace_days=GRACE_DAYS)

    # ── Max removals cap: if too many, abort removals ──────────────────
    if len(confirmed_removals) > MAX_REMOVALS:
        log.warning(
            "Removal count (%d) exceeds MAX_REMOVALS (%d) — aborting removals as a safety measure.",
            len(confirmed_removals),
            MAX_REMOVALS,
        )
        confirmed_removals = set()

    log.info(
        "Changes: +%d additions, -%d removals (grace: %d absent candidates)",
        len(additions),
        len(confirmed_removals),
        len(candidate_removals),
    )

    # ── Log changes ────────────────────────────────────────────────────
    log_path = log_changes(_LOG_DIR, today, additions, confirmed_removals)
    log.info("Change log written to: %s", log_path)

    if args.dry_run:
        log.info("Dry run — no files modified.")
        return

    # ── Archive confirmed removals to bronze-delisted ──────────────────
    delisted_base = _DATA_LAKE / "bronze-delisted" / "asset_class=equity"
    for ticker in sorted(confirmed_removals):
        src = bronze_dir / f"symbol={ticker}"
        dst = delisted_base / f"symbol={ticker}"
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            log.info("Archived %s to bronze-delisted/", ticker)

    # ── Compute new universe and write preset ──────────────────────────
    new_universe = (current_universe | additions) - confirmed_removals
    # Belt-and-suspenders: ensure core ETFs are always present
    new_universe = new_universe | core_etfs
    write_universe_preset(_PRESET_PATH, list(new_universe))
    log.info("Preset written: %s (%d tickers)", _PRESET_PATH, len(new_universe))

    # ── Save state ─────────────────────────────────────────────────────
    save_screener_state(
        _STATE_PATH,
        {
            "run_date": today.isoformat(),
            "universe": sorted(new_universe),
            "absent_counts": new_absent,
        },
    )

    # ── Trigger backfill for new additions ─────────────────────────────
    if additions:
        python_bin = sys.executable
        cmd = [
            python_bin,
            str(_INGEST_SCRIPT),
            "historical",
            "--tickers",
            *sorted(additions),
            "--years", "0",
        ]
        log.info("Triggering backfill for %d new tickers: %s", len(additions), sorted(additions))
        subprocess.run(cmd, check=False)

    # ── Send alert if significant changes ──────────────────────────────
    total_changes = len(additions) + len(confirmed_removals)
    if total_changes >= EMAIL_THRESHOLD:
        _send_screener_alert(today, additions, confirmed_removals)


if __name__ == "__main__":
    main()
