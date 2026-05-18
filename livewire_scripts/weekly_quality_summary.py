"""Weekly markdown quality summary aggregating 7 daily coverage logs.

Reads ~/market-warehouse/logs/coverage_YYYY-MM-DD.log files written by
coverage_report.py and emits a markdown report at
~/market-warehouse/logs/quality_weekly_YYYY-WW.md.

Spec: docs/superpowers/specs/2026-04-06-multi-timeframe-design.md § 17 Layer 2.

Self-skips on non-Sunday unless --force is passed, so the entrypoint can
call it unconditionally every day.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_LOG_DIR = _WAREHOUSE_DIR / "logs"

_TIMEFRAMES: tuple[str, ...] = ("1d", "1h", "5m")

# Match the leading line written by coverage_report.format_one_liner
# Example: "2026-04-06 coverage: 1d=1166/1166 (100.00%) 1h=1162/1166 (99.66%) 5m=1158/1166 (99.31%)"
_HEADER_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) coverage:\s+"
    r"1d=(\d+)/(\d+).*?\s+"
    r"1h=(\d+)/(\d+).*?\s+"
    r"5m=(\d+)/(\d+)"
)

# Match a per-timeframe missing block, e.g. "  5m missing: NEWA, RECENT_IPO, ... (8 total)"
_MISSING_RE = re.compile(r"^\s+(1d|1h|5m) missing:\s*(.+?)\s*$")
_TOTAL_SUFFIX_RE = re.compile(r",\s*\.\.\.\s*\((\d+) total\)\s*$")


@dataclass
class CoverageEntry:
    day: date
    totals: dict[str, tuple[int, int]] = field(default_factory=dict)  # tf -> (present, total)
    missing: dict[str, list[str]] = field(default_factory=dict)       # tf -> sorted symbols


def parse_coverage_log(path: Path) -> CoverageEntry | None:
    """Parse a single coverage_YYYY-MM-DD.log file. Returns None if unparseable."""
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    header: CoverageEntry | None = None
    for line in lines:
        m = _HEADER_RE.match(line)
        if m:
            day = date.fromisoformat(m.group(1))
            header = CoverageEntry(
                day=day,
                totals={
                    "1d": (int(m.group(2)), int(m.group(3))),
                    "1h": (int(m.group(4)), int(m.group(5))),
                    "5m": (int(m.group(6)), int(m.group(7))),
                },
            )
            break

    if header is None:
        return None

    for line in lines:
        m = _MISSING_RE.match(line)
        if not m:
            continue
        tf, symbols_part = m.group(1), m.group(2)
        # Strip the "... (N total)" suffix if present — we only have a sample
        symbols_part = _TOTAL_SUFFIX_RE.sub("", symbols_part)
        symbols = [s.strip() for s in symbols_part.split(",") if s.strip()]
        header.missing.setdefault(tf, []).extend(symbols)

    return header


def load_week(start_day: date) -> list[CoverageEntry]:
    """Load up to 7 daily coverage logs starting at *start_day* (Monday)."""
    entries: list[CoverageEntry] = []
    for offset in range(7):
        d = start_day + timedelta(days=offset)
        path = _LOG_DIR / f"coverage_{d:%Y-%m-%d}.log"
        entry = parse_coverage_log(path)
        if entry is not None:
            entries.append(entry)
    return entries


def detect_churn(entries: list[CoverageEntry]) -> tuple[list[str], list[str]]:
    """Return ``(added, removed)`` symbols this week.

    A symbol is *added* if it appears in the latest entry but is missing from
    the first day's universe. *Removed* requires absence (in the missing list)
    on at least 3 consecutive days at any single timeframe — the same
    threshold the spec uses for "persistent gaps", which represents a real
    delisting candidate rather than transient absence.
    """
    if len(entries) < 2:
        return [], []

    first, last = entries[0], entries[-1]

    # Universe per entry = all symbols that have a max present count for 1d
    # We don't have explicit symbol lists in the log, so we use the missing-set
    # delta as a proxy: "added" symbols are ones whose first-day 1d total < last-day total.
    first_total = first.totals.get("1d", (0, 0))[1]
    last_total = last.totals.get("1d", (0, 0))[1]

    added: list[str] = []
    if last_total > first_total:
        added.append(f"<{last_total - first_total} new symbols>")

    removed: list[str] = []
    persistent = detect_persistent_gaps(entries)
    for sym, by_tf in persistent.items():
        if any(streak >= 3 for streak in by_tf.values()):
            removed.append(sym)

    return sorted(set(added)), sorted(set(removed))


def detect_persistent_gaps(entries: list[CoverageEntry]) -> dict[str, dict[str, int]]:
    """Return ``{symbol: {timeframe: max_consecutive_missing_days}}``.

    Only symbols with at least one ``≥3`` streak at some timeframe are kept.
    """
    if not entries:
        return {}

    # Track running streaks per (symbol, timeframe)
    running: dict[tuple[str, str], int] = {}
    best: dict[tuple[str, str], int] = {}

    all_seen: set[str] = set()
    for entry in entries:
        for tf in _TIMEFRAMES:
            for sym in entry.missing.get(tf, []):
                all_seen.add(sym)

    for entry in entries:
        for tf in _TIMEFRAMES:
            missing_today = set(entry.missing.get(tf, []))
            for sym in all_seen:
                key = (sym, tf)
                if sym in missing_today:
                    running[key] = running.get(key, 0) + 1
                    if running[key] > best.get(key, 0):
                        best[key] = running[key]
                else:
                    running[key] = 0

    result: dict[str, dict[str, int]] = {}
    for (sym, tf), streak in best.items():
        if streak >= 3:
            result.setdefault(sym, {})[tf] = streak
    return result


def render_markdown(week_label: str, entries: list[CoverageEntry]) -> str:
    """Render the weekly markdown report."""
    if not entries:
        return f"# Weekly Quality Report — {week_label}\n\nNo coverage logs found for this week.\n"

    lines: list[str] = []
    lines.append(f"# Weekly Quality Report — {week_label}")
    lines.append("")
    lines.append("## Coverage trend (per timeframe)")
    lines.append("| Day        | 1d        | 1h        | 5m        |")
    lines.append("|------------|-----------|-----------|-----------|")
    for entry in entries:
        cells = []
        for tf in _TIMEFRAMES:
            present, total = entry.totals.get(tf, (0, 0))
            cells.append(f"{present}/{total}")
        lines.append(f"| {entry.day} | {cells[0]:9s} | {cells[1]:9s} | {cells[2]:9s} |")
    lines.append("")

    added, removed = detect_churn(entries)
    lines.append("## Symbol churn this week")
    if added:
        lines.append(f"- Added ({len(added)}): " + ", ".join(added))
    if removed:
        lines.append(f"- Removed ({len(removed)}): " + ", ".join(removed))
    if not added and not removed:
        lines.append("- No churn detected")
    lines.append("")

    persistent = detect_persistent_gaps(entries)
    lines.append("## Persistent gaps")
    if not persistent:
        lines.append("None — every symbol recovered within 2 consecutive days.")
    else:
        lines.append("Symbols with ≥3 consecutive days of missing bars at any timeframe:")
        for sym in sorted(persistent):
            for tf in _TIMEFRAMES:
                streak = persistent[sym].get(tf)
                if streak:
                    lines.append(
                        f"- {sym} ({tf}): missing {streak} of last {len(entries)} days"
                    )
    lines.append("")
    return "\n".join(lines)


def write_summary(markdown: str, week_iso: tuple[int, int]) -> Path:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    year, week = week_iso
    out_path = _LOG_DIR / f"quality_weekly_{year}-{week:02d}.md"
    out_path.write_text(markdown, encoding="utf-8")
    return out_path


def _iso_week_start(year: int, week: int) -> date:
    """Return the Monday of ISO week *week* in *year*."""
    return date.fromisocalendar(year, week, 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly quality summary report")
    parser.add_argument(
        "--week",
        type=str,
        help="ISO week 'YYYY-WW' (default: current week)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Render even if today is not Sunday",
    )
    args = parser.parse_args()

    today = date.today()
    if not args.force and today.isoweekday() != 7:
        console.print(
            f"[dim]{today} is not a Sunday — skipping. Pass --force to render anyway.[/dim]"
        )
        return

    if args.week:
        year_str, week_str = args.week.split("-", 1)
        year, week = int(year_str), int(week_str)
    else:
        year, week, _ = today.isocalendar()

    start = _iso_week_start(year, week)
    entries = load_week(start)
    week_label = f"Week {week} of {year}"
    markdown = render_markdown(week_label, entries)
    out_path = write_summary(markdown, (year, week))
    console.print(f"[green]Wrote {out_path}[/green]")


if __name__ == "__main__":
    main()
