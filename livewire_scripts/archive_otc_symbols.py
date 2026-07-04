"""Archive genuinely-inactive equity symbols to bronze-delisted.

Identifies equity tickers that are (a) absent from the Massive **minute_aggs**
whole-market SIP universe over a recent lookback window AND (b) whose own latest
bronze 1d bar is older than a staleness threshold, then moves their entire
symbol directories to bronze-delisted so they are excluded from future syncs,
backfills, and coverage tracking.

Why minute_aggs (not day_aggs): the day_aggs universe excludes warrants, units,
rights, and many preferred/special share classes, while bronze is fed by the
whole-market minute_aggs lane which includes them. Differencing bronze against
day_aggs therefore flags hundreds of *actively-trading* instruments as OTC. The
minute_aggs `_symbols.parquet` set is the lane that actually feeds bronze, and
the staleness guard is a second, data-driven safety net: a symbol is only
archived when its own most-recent bar confirms it has stopped trading.

Usage:
    python scripts/livewire_store.py archive-otc --dry-run
    python scripts/livewire_store.py archive-otc
    python scripts/livewire_store.py archive-otc --universe-days 20 --staleness-days 30
    python scripts/livewire_store.py archive-otc --as-of 2026-07-02
"""

from __future__ import annotations

import argparse
import shutil
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

_DEFAULT_WAREHOUSE = Path.home() / "market-warehouse"
_DEFAULT_UNIVERSE_DAYS = 20
_DEFAULT_STALENESS_DAYS = 30


def load_active_universe(
    minute_base: Path,
    universe_days: int,
    as_of: str | None = None,
) -> tuple[set[str], list[str]]:
    """Return (tickers, dates_used) from the minute_aggs SIP universe.

    Unions the ``_symbols.parquet`` ticker set across the last *universe_days*
    available date partitions at or before *as_of* (latest available when
    *as_of* is None). Thinly-traded instruments do not print every day, so a
    single day is not a reliable "active" set — the window captures them.
    """
    date_dirs = sorted(minute_base.glob("date=*"))
    if as_of is not None:
        date_dirs = [d for d in date_dirs if d.name.removeprefix("date=") <= as_of]
    if not date_dirs:
        raise FileNotFoundError(f"No minute_aggs raw data found under {minute_base}")

    window = date_dirs[-universe_days:]
    tickers: set[str] = set()
    dates_used: list[str] = []
    for d in window:
        symbols_file = d / "_symbols.parquet"
        if symbols_file.exists():
            tbl = pq.read_table(symbols_file, columns=["ticker"])
            tickers.update(tbl.column("ticker").to_pylist())
            dates_used.append(d.name.removeprefix("date="))
    if not dates_used:
        raise FileNotFoundError(
            f"No _symbols.parquet found in the last {universe_days} minute_aggs dates"
        )
    return tickers, dates_used


def latest_1d_date(sym_dir: Path) -> str | None:
    """Return the max ``trade_date`` (ISO string) from ``sym_dir/1d.parquet``.

    Returns None when the file is missing, unreadable, or empty. ``trade_date``
    may be stored as a date or a string; both stringify to ``YYYY-MM-DD``.
    """
    f = sym_dir / "1d.parquet"
    try:
        values = pq.read_table(str(f), columns=["trade_date"]).column("trade_date").to_pylist()
    except Exception:
        return None
    if not values:
        return None
    return max(str(v) for v in values)


def find_archivable_symbols(
    bronze_equity: Path,
    active_universe: set[str],
    staleness_cutoff: str,
) -> list[str]:
    """Return symbols eligible for archival, sorted.

    A symbol qualifies only when it is absent from *active_universe* AND its
    latest bronze 1d bar is strictly older than *staleness_cutoff* (ISO date).
    Symbols whose latest date cannot be determined are conservatively skipped —
    we never archive without positive evidence of staleness.
    """
    out: list[str] = []
    for sym_dir in sorted(bronze_equity.glob("symbol=*/")):
        sym = sym_dir.name.removeprefix("symbol=")
        if sym in active_universe:
            continue
        latest = latest_1d_date(sym_dir)
        if latest is not None and latest < staleness_cutoff:
            out.append(sym)
    return out


def archive_symbol(
    sym: str,
    bronze_equity: Path,
    delisted_equity: Path,
    dry_run: bool,
) -> str:
    """Move symbol=X/ from bronze to bronze-delisted.

    Returns one of: ``"archived"``, ``"skipped_exists"``, ``"dry_run"``.
    """
    dst = delisted_equity / f"symbol={sym}"
    if dst.exists():
        return "skipped_exists"
    if dry_run:
        return "dry_run"
    delisted_equity.mkdir(parents=True, exist_ok=True)
    shutil.move(str(bronze_equity / f"symbol={sym}"), str(dst))
    return "archived"


def _staleness_cutoff(dates_used: list[str], staleness_days: int) -> str:
    """ISO cutoff = latest universe date minus *staleness_days* calendar days."""
    reference = date.fromisoformat(max(dates_used))
    return (reference - timedelta(days=staleness_days)).isoformat()


def run_archive(
    bronze_equity: Path,
    delisted_equity: Path,
    minute_base: Path,
    as_of: str | None,
    universe_days: int,
    staleness_days: int,
    dry_run: bool,
) -> dict[str, int]:
    """Identify and archive inactive tickers. Returns stats dict."""
    active_universe, dates_used = load_active_universe(minute_base, universe_days, as_of)
    cutoff = _staleness_cutoff(dates_used, staleness_days)
    print(
        f"Active universe: {len(active_universe)} tickers "
        f"({len(dates_used)} minute_aggs days, {dates_used[0]}..{dates_used[-1]})"
    )
    print(f"Staleness cutoff: latest 1d bar must be < {cutoff}")

    candidates = find_archivable_symbols(bronze_equity, active_universe, cutoff)
    print(f"Archivable (inactive + stale) tickers: {len(candidates)}")

    if not candidates:
        print("Nothing to archive.")
        return {"archived": 0, "skipped_exists": 0, "dry_run": 0}

    stats: dict[str, int] = {"archived": 0, "skipped_exists": 0, "dry_run": 0}
    for sym in candidates:
        result = archive_symbol(sym, bronze_equity, delisted_equity, dry_run)
        stats[result] += 1
        if result == "skipped_exists":
            print(f"  SKIP (already in delisted): {sym}")

    verb = "Would archive" if dry_run else "Archived"
    n = stats["dry_run"] if dry_run else stats["archived"]
    print(f"\n{verb} {n} tickers, skipped {stats['skipped_exists']} already in bronze-delisted")
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Archive inactive equity symbols (absent from recent SIP + stale) to bronze-delisted"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without moving files")
    parser.add_argument(
        "--as-of",
        default=None,
        metavar="YYYY-MM-DD",
        help="End of the minute_aggs universe window (default: latest available)",
    )
    parser.add_argument(
        "--universe-days",
        type=int,
        default=_DEFAULT_UNIVERSE_DAYS,
        help=f"Minute_aggs lookback window in trading days (default: {_DEFAULT_UNIVERSE_DAYS})",
    )
    parser.add_argument(
        "--staleness-days",
        type=int,
        default=_DEFAULT_STALENESS_DAYS,
        help=f"Archive only if latest 1d bar is older than this many days (default: {_DEFAULT_STALENESS_DAYS})",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=_DEFAULT_WAREHOUSE,
        help="Path to market-warehouse root (default: ~/market-warehouse)",
    )
    args = parser.parse_args(argv)

    warehouse = args.warehouse
    minute_base = warehouse / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1"
    bronze_equity = warehouse / "data-lake" / "bronze" / "asset_class=equity"
    delisted_equity = warehouse / "data-lake" / "bronze-delisted" / "asset_class=equity"

    if not bronze_equity.is_dir():
        print(f"ERROR: bronze equity dir not found: {bronze_equity}")
        return 1

    run_archive(
        bronze_equity=bronze_equity,
        delisted_equity=delisted_equity,
        minute_base=minute_base,
        as_of=args.as_of,
        universe_days=args.universe_days,
        staleness_days=args.staleness_days,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
