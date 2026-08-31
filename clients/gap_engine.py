"""Diff expected coverage against actual bronze, and classify what is missing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from clients.coverage_denominator import ExpectedSeries

DATE_COLUMN = "trade_date"


@dataclass(frozen=True)
class Finding:
    symbol: str
    asset_class: str
    timeframe: str
    gap: str  # "G1" | "G2" | "G3"
    sessions: tuple[date, ...]
    heal_by_days: int
    tier: str  # "A" | "B"


def actual_sessions(bronze_root: Path, series: ExpectedSeries) -> set[date]:
    """Sessions actually present on disk. A missing file is an empty set, not an error."""
    path = (
        bronze_root
        / f"asset_class={series.asset_class}"
        / f"symbol={series.symbol}"
        / f"{series.timeframe}.parquet"
    )
    if not path.exists():
        return set()
    table = pq.read_table(path, columns=[DATE_COLUMN])
    return {value.as_py() for value in table.column(DATE_COLUMN)}


def _finding(
    series: ExpectedSeries, gap: str, sessions: tuple[date, ...], massive_floor: date
) -> Finding:
    """Tier follows the source split in section 6.1 of the spec.

    Inside the rolling Massive window the repair is unattended (Tier A). Below
    the floor the only source is IB, which is 2FA-gated and never auto-retries
    (CLAUDE.md:764), so it is a decision, not an automatic repair (Tier B).
    """
    heal_by_days = (min(sessions) - massive_floor).days
    return Finding(
        symbol=series.symbol,
        asset_class=series.asset_class,
        timeframe=series.timeframe,
        gap=gap,
        sessions=sessions,
        heal_by_days=heal_by_days,
        tier="A" if heal_by_days >= 0 else "B",
    )


def classify(
    series: ExpectedSeries, present: set[date], massive_floor: date
) -> list[Finding]:
    expected = set(series.sessions)
    missing = tuple(sorted(expected - present))
    if not missing:
        return []
    if not present:
        return [_finding(series, "G3", missing, massive_floor)]

    newest_present = max(present)
    tail = tuple(d for d in missing if d > newest_present)
    interior = tuple(d for d in missing if d < newest_present)

    findings: list[Finding] = []
    if tail:
        findings.append(_finding(series, "G1", tail, massive_floor))
    if interior:
        findings.append(_finding(series, "G2", interior, massive_floor))
    return findings
