"""Universe sync — fetch live index constituents, detect dead tickers, update registry and presets.

Usage:
    source ~/market-warehouse/.venv/bin/activate
    python scripts/livewire_ingest.py universe-sync              # Full sync
    python scripts/livewire_ingest.py universe-sync --dry-run    # Report only
    python scripts/livewire_ingest.py universe-sync --skip-dead  # Skip Polygon dead-ticker check
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.tag_registry import TagRegistry
from clients.universe_client import (
    UniverseFetchError,
    check_tickers_bulk,
    fetch_ndx100,
    fetch_r2k,
    fetch_sp500,
)
from livewire_scripts.paths import data_lake_dir, warehouse_dir

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR: Path | None = None
_DATA_LAKE: Path | None = None
_PRESET_DIR = PROJECT_ROOT / "presets"

INDEX_TAGS = ("sp500", "ndx100", "r2k")
INDEX_HIERARCHY = ["sp500", "ndx100", "r2k"]

MIN_EXPECTED_CONSTITUENTS = {
    "sp500": 400,
    "ndx100": 80,
    "r2k": 1500,
}


@dataclass(frozen=True)
class Movement:
    type: str
    ticker: str
    from_tags: list[str]
    to_tags: list[str]


def compute_movements(
    live: dict[str, set[str]],
    existing: dict[str, set[str]],
) -> list[Movement]:
    movements: list[Movement] = []
    all_tickers: set[str] = set()
    for s in live.values():
        all_tickers |= s
    for s in existing.values():
        all_tickers |= s

    for ticker in sorted(all_tickers):
        live_in = [idx for idx in INDEX_HIERARCHY if ticker in live.get(idx, set())]
        was_in = [idx for idx in INDEX_HIERARCHY if ticker in existing.get(idx, set())]

        if live_in == was_in:
            continue

        if not was_in and live_in:
            movements.append(Movement("add", ticker, from_tags=[], to_tags=live_in))
        elif was_in and not live_in:
            movements.append(Movement("remove", ticker, from_tags=was_in, to_tags=[]))
        elif was_in and live_in:
            was_set = set(was_in)
            live_set = set(live_in)
            added = live_set - was_set
            dropped = was_set - live_set
            if added and not dropped:
                move_type = "promotion"
            elif dropped and not added:
                move_type = "demotion"
            elif added and dropped:
                was_best = min(INDEX_HIERARCHY.index(t) for t in was_in)
                live_best = min(INDEX_HIERARCHY.index(t) for t in live_in)
                move_type = "promotion" if live_best < was_best else "demotion"
            else:  # pragma: no cover
                move_type = "move"
            movements.append(Movement(move_type, ticker, from_tags=was_in, to_tags=live_in))

    return movements


def apply_sync(registry: TagRegistry, movements: list[Movement]) -> None:
    for move in movements:
        for tag in move.from_tags:
            registry.remove_tag(move.ticker, tag)
        for tag in move.to_tags:
            registry.add_tag(move.ticker, tag)
        registry.log_change(move.type, move.ticker, move.from_tags, move.to_tags)


def update_preset_tickers(
    path: Path,
    tickers: set[str],
    name: str | None = None,
    description: str | None = None,
) -> None:
    if path.exists():
        with path.open() as f:
            data = json.load(f)
        data["tickers"] = sorted(tickers)
    else:
        data = {
            "name": name or path.stem,
            "description": description or "",
            "tickers": sorted(tickers),
            "pairs": [],
            "groups": {},
            "source": "universe-sync",
        }
    with path.open("w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def _archive_delisted(ticker: str, data_lake: Path) -> bool:
    src = data_lake / "bronze" / "asset_class=equity" / f"symbol={ticker}"
    if not src.exists():
        return False
    dst = data_lake / "bronze-delisted" / "asset_class=equity" / f"symbol={ticker}"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    log.info("Archived %s to bronze-delisted/", ticker)
    return True


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Sync index constituents and update registry")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without modifying")
    parser.add_argument("--skip-dead", action="store_true", help="Skip Polygon dead-ticker check")
    parser.add_argument(
        "--indexes",
        nargs="+",
        choices=INDEX_TAGS,
        default=list(INDEX_TAGS),
        help="Which indexes to refresh. Defaults to all of them.",
    )
    parser.add_argument(
        "--interests",
        nargs="*",
        default=None,
        help="Tickers to add to the interests preset",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    registry_path = (_WAREHOUSE_DIR or warehouse_dir()) / "registry.json"
    registry = TagRegistry(registry_path)

    # ── Fetch live constituents ──────────────────────────────────────────
    log.info("Fetching live index constituents...")
    # Canonical order, not the order the flag was typed in: INDEX_HIERARCHY is
    # ordered and compute_movements reads promotions and demotions off it.
    selected = tuple(idx for idx in INDEX_TAGS if idx in set(args.indexes))
    live: dict[str, set[str]] = {}
    for idx, fetcher, label in [
        ("sp500", fetch_sp500, "S&P 500"),
        ("ndx100", fetch_ndx100, "Nasdaq-100"),
        ("r2k", fetch_r2k, "Russell 2000"),
    ]:
        if idx not in selected:
            log.info("%s: skipped (not in --indexes)", label)
            continue
        try:
            tickers = fetcher()
            live[idx] = tickers
            log.info("%s: %d constituents", label, len(tickers))
        except UniverseFetchError as exc:
            log.error("Failed to fetch %s: %s", label, exc)
            sys.exit(1)

    # ── Sanity check ────────────────────────────────────────────────────
    for idx, tickers in live.items():
        min_expected = MIN_EXPECTED_CONSTITUENTS.get(idx, 0)
        if len(tickers) < min_expected:
            log.error(
                "SAFETY: %s returned only %d tickers (expected >= %d). Aborting to prevent mass removal.",
                idx,
                len(tickers),
                min_expected,
            )
            sys.exit(1)

    # ── Build existing state from registry or presets ────────────────────
    # `selected`, NOT INDEX_TAGS. An index present in `existing` but absent from
    # `live` has every one of its members read as a removal -- r2k alone is 1886
    # tickers -- and MIN_EXPECTED_CONSTITUENTS cannot catch it, because that guard
    # only inspects indexes that were actually fetched. The two dicts must cover
    # the same set of indexes or compute_movements is comparing against nothing.
    existing: dict[str, set[str]] = {}
    for idx in selected:
        existing[idx] = registry.by_tag(idx, active_only=False)
        if not existing[idx]:
            preset_path = _PRESET_DIR / f"{idx}.json"
            if preset_path.exists():
                with preset_path.open() as f:
                    existing[idx] = set(json.load(f).get("tickers", []))

    # ── Compute movements ───────────────────────────────────────────────
    movements = compute_movements(live, existing)

    # ── Display movements ───────────────────────────────────────────────
    if movements:
        table = Table(title="Universe Changes")
        table.add_column("Type", style="bold")
        table.add_column("Ticker")
        table.add_column("From")
        table.add_column("To")
        for m in movements:
            style = {
                "add": "green",
                "remove": "red",
                "promotion": "cyan",
                "demotion": "yellow",
                "move": "blue",
            }.get(m.type, "white")
            table.add_row(
                m.type.upper(),
                m.ticker,
                ", ".join(m.from_tags) or "-",
                ", ".join(m.to_tags) or "-",
                style=style,
            )
        console.print(table)
        console.print(
            f"\n[bold]{len(movements)} changes[/bold] "
            f"({sum(1 for m in movements if m.type == 'add')} adds, "
            f"{sum(1 for m in movements if m.type == 'remove')} removes, "
            f"{sum(1 for m in movements if m.type == 'promotion')} promotions, "
            f"{sum(1 for m in movements if m.type == 'demotion')} demotions)"
        )
    else:
        console.print("[green]No changes — all presets are current.[/green]")

    if args.dry_run:
        log.info("Dry run — no files modified.")
        return

    # ── Apply movements to registry ─────────────────────────────────────
    apply_sync(registry, movements)

    # ── Seed registry for tickers not yet tracked ───────────────────────
    for idx, tickers in live.items():
        for ticker in tickers:
            if registry.get(ticker) is None:
                registry.set_tags(ticker, {idx}, status="active")
            elif idx not in registry.get(ticker).tags:
                registry.add_tag(ticker, idx)

    # ── Dead ticker check via Polygon ───────────────────────────────────
    if not args.skip_dead and os.environ.get("MASSIVE_API_KEY"):
        removed_tickers = [m.ticker for m in movements if m.type == "remove"]
        orphan_tickers = [
            t
            for t in registry.all_tickers()
            if registry.get(t) and not registry.get(t).tags & set(INDEX_TAGS) and registry.get(t).status == "active"
        ]
        check_list = list(set(removed_tickers + orphan_tickers))
        if check_list:
            log.info(
                "Checking %d tickers via Polygon for delisted status...",
                len(check_list),
            )
            statuses = check_tickers_bulk(check_list)
            for ticker, status in statuses.items():
                if status.list_date:
                    registry.set_earliest(ticker, status.list_date, source="polygon")
                if not status.active:
                    log.info("DELISTED: %s (delisted_utc=%s)", ticker, status.delisted_utc)
                    registry.mark_delisted(ticker, delisted_at=status.delisted_utc)
                    lake = _DATA_LAKE or (_WAREHOUSE_DIR / "data-lake" if _WAREHOUSE_DIR else data_lake_dir())
                    _archive_delisted(ticker, lake)
    elif not args.skip_dead:
        log.info("MASSIVE_API_KEY not set — skipping dead-ticker check")

    # ── Handle interests ────────────────────────────────────────────────
    if args.interests is not None:
        for ticker in args.interests:
            registry.add_tag(ticker.upper(), "interest")
        interest_tickers = registry.by_tag("interest")
        update_preset_tickers(
            _PRESET_DIR / "interests.json",
            interest_tickers,
            name="interests",
            description="Personal watchlist",
        )
        log.info("Interests preset: %d tickers", len(interest_tickers))

    # ── Update preset tickers arrays ────────────────────────────────────
    # `selected` again, and for a second reason. This loop reads the REGISTRY,
    # not `live` -- but the registry was only tagged for the indexes that were
    # fetched, so an unselected index resolves to the empty set and the write
    # TRUNCATES its preset. Measured 2026-09-02 on the first real apply:
    # `Updated r2k.json: 0 tickers`, 1886 gone. The compute_movements guard above
    # does not cover this; it is a separate write path, and the movements table
    # was correct while the file was being emptied.
    for idx in selected:
        active_tickers = registry.by_tag(idx, active_only=True)
        preset_path = _PRESET_DIR / f"{idx}.json"
        if preset_path.exists():
            update_preset_tickers(preset_path, active_tickers)
            log.info("Updated %s: %d tickers", preset_path.name, len(active_tickers))

    # ── Save registry ───────────────────────────────────────────────────
    registry.save()
    log.info("Registry saved to %s", registry_path)


if __name__ == "__main__":  # pragma: no cover
    main()
