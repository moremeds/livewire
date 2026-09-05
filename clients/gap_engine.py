"""Diff expected coverage against actual bronze, and classify what is missing."""

from __future__ import annotations

import fcntl
import json
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from clients import constants
from clients.coverage_denominator import ExpectedSeries

DATE_COLUMN = "trade_date"

# Massive's flat-file/REST entitlement is a ROLLING window, measured 2026-07-29:
# 2021-07-27 -> 403, 2021-07-28 -> OK, i.e. exactly 1827 days (5.00y) before the
# probe date. Deriving it from `as_of` keeps tier routing and heal_by ordering
# true as the window rolls; a constant silently rots one day per day.
MASSIVE_WINDOW_DAYS = int(constants.declared("massive_window_days"))

# Repair source per asset class, from CLAUDE.md. An IB-sourced lane can never be
# Tier A: IB is 2FA-gated and never auto-retries, so its repair is a decision,
# not an unattended action. Anything not listed here has no known repair path and
# must not be silently assumed repairable -- see `repair_source`.
IB_SOURCED = frozenset({"futures", "cmdty"})
# Only equity rides Massive's rolling window. Yahoo (fx), CBOE (volatility) and
# FRED (rates) serve deep history, so their repair path has no expiry date.
FLOORED_ASSET_CLASSES = frozenset({"equity"})
REPAIR_SOURCE = {
    "equity": "massive",
    "fx": "yahoo",
    "volatility": "cboe",
    "rates": "fred",
    "futures": "ib",
    "cmdty": "ib",
}


def massive_floor_for(as_of: date) -> date:
    """The rolling Massive entitlement floor as of a given scan date."""
    return as_of - timedelta(days=MASSIVE_WINDOW_DAYS)


def repair_source(asset_class: str) -> str:
    """Fail closed: an unmapped asset class has no known repair path."""
    try:
        return REPAIR_SOURCE[asset_class]
    except KeyError:
        raise ValueError(
            f"asset_class {asset_class!r} has no mapped repair source; known: {sorted(REPAIR_SOURCE)}"
        ) from None


@dataclass(frozen=True)
class Finding:
    symbol: str
    asset_class: str
    timeframe: str
    gap: str  # "G1" tail | "G3" nothing on disk | "G14" left the tape
    sessions: tuple[date, ...]
    # Days of headroom above the rolling Massive floor. None when the repair
    # source has no rolling window (Yahoo/CBOE/FRED/IB), i.e. no expiry date.
    heal_by_days: int | None
    tier: str  # "A" | "B"
    source: str


def actual_sessions(bronze_root: Path, series: ExpectedSeries) -> set[date]:
    """Sessions actually present on disk. A missing file is an empty set, not an error."""
    path = bronze_root / f"asset_class={series.asset_class}" / f"symbol={series.symbol}" / f"{series.timeframe}.parquet"
    if not path.exists():
        return set()
    table = pq.read_table(path, columns=[DATE_COLUMN])
    return {value.as_py() for value in table.column(DATE_COLUMN)}


def _finding(series: ExpectedSeries, gap: str, sessions: tuple[date, ...], massive_floor: date) -> Finding:
    """Tier follows the repair source, not the severity of the gap.

    Tier A means "repairable unattended". That is a property of the source:
    - IB-sourced lanes (futures, cmdty) are 2FA-gated and never auto-retry, so
      they are always Tier B regardless of how recent the gap is.
    - Equity rides Massive's rolling window: inside it Tier A, below it only IB
      can serve the bar, so Tier B.
    - Yahoo/CBOE/FRED serve deep history, so there is no floor and no expiry.
    """
    source = repair_source(series.asset_class)
    if series.asset_class in IB_SOURCED:
        return Finding(
            symbol=series.symbol,
            asset_class=series.asset_class,
            timeframe=series.timeframe,
            gap=gap,
            sessions=sessions,
            heal_by_days=None,
            tier="B",
            source=source,
        )
    if series.asset_class not in FLOORED_ASSET_CLASSES:
        return Finding(
            symbol=series.symbol,
            asset_class=series.asset_class,
            timeframe=series.timeframe,
            gap=gap,
            sessions=sessions,
            heal_by_days=None,
            tier="A",
            source=source,
        )
    heal_by_days = (min(sessions) - massive_floor).days
    return Finding(
        symbol=series.symbol,
        asset_class=series.asset_class,
        timeframe=series.timeframe,
        gap=gap,
        sessions=sessions,
        heal_by_days=heal_by_days,
        tier="A" if heal_by_days >= 0 else "B",
        source=source,
    )


def _terminus_finding(series: ExpectedSeries, sessions: tuple[date, ...]) -> Finding:
    """Always Tier B, in every cell, with no heal-by.

    Spec section 9.3 rule 4: a tier is a claim about a store. No store carries
    bars for an instrument that is not printing, so the rolling-window arithmetic
    that produces `heal_by_days` has nothing to measure and would sort a job that
    can never run to the front of the repair queue.
    """
    return Finding(
        symbol=series.symbol,
        asset_class=series.asset_class,
        timeframe=series.timeframe,
        gap="G14",
        sessions=sessions,
        heal_by_days=None,
        tier="B",
        source=repair_source(series.asset_class),
    )


def classify(
    series: ExpectedSeries,
    present: set[date],
    massive_floor: date,
    terminus: date | None = None,
    unconfirmed: bool = False,
) -> list[Finding]:
    """Classify what is missing.

    `terminus` is a confirmed departure from the tape; `unconfirmed` says the
    suffix test fired but a gate withheld, which downgrades the tier without
    changing the gap class. They are mutually exclusive by construction -- see
    clients.terminus.TerminusVerdict.
    """
    expected = set(series.sessions)
    missing = tuple(sorted(expected - present))
    if not missing:
        return []
    findings: list[Finding] = []
    if terminus is not None:
        # An instrument that left the tape cannot be repaired from any source, so
        # its terminal sessions are one Tier B finding. Sessions BEFORE the
        # terminus are a different fact: the instrument was still printing then,
        # so those bars exist at the provider and are repairable. Returning only
        # the terminus finding silently discarded them -- an ingestion outage that
        # happened to precede a delisting became unrepairable by classification.
        terminal = tuple(d for d in missing if d >= terminus)
        missing = tuple(d for d in missing if d < terminus)
        if terminal:
            findings.append(_terminus_finding(series, terminal))
        if not missing:
            return findings
    repairable: list[Finding] = []
    if not present:
        repairable.append(_finding(series, "G3", missing, massive_floor))
    else:
        # ponytail: tail only. G2 (interior) and G13 (head) produced zero true
        # findings out of 501 on the first production run, and interior absence
        # within bar files alone is the circular question that made the 5m scan
        # flag 96.6% of the universe. Reinstate either only with a measurement
        # asking for it -- the taxonomy still names them (spec section 3).
        newest_present = max(present)
        tail = tuple(d for d in missing if d > newest_present)
        if tail:
            repairable.append(_finding(series, "G1", tail, massive_floor))
    if unconfirmed:
        # Tier A means "repairable unattended". A qualifying absence run nobody
        # could explain may be an instrument that is not printing at all, so the
        # nightly job would fetch nothing forever. The gap CLASS stays honest --
        # it really is a tail or a missing file -- and only the tier records that
        # a human has to look. heal_by_days goes with it: no store carries bars
        # for an instrument that may be gone, so there is nothing to expire.
        repairable = [replace(f, tier="B", heal_by_days=None) for f in repairable]
    return findings + repairable


UnresolvedKey = tuple[str, str, str, date]


def _unresolved_key(symbol: str, asset_class: str, timeframe: str, session: date) -> UnresolvedKey:
    """Ledger identity is the full series, not just (symbol, session).

    Keyed on the symbol alone, marking one timeframe unresolved silently
    suppressed every other timeframe for that symbol.
    """
    return (symbol, asset_class, timeframe, session)


def load_unresolved(path: Path) -> set[UnresolvedKey]:
    if not Path(path).exists():
        return set()
    entries = json.loads(Path(path).read_text())
    keys: set[UnresolvedKey] = set()
    for entry in entries:
        # Deliberately strict. Defaulting a missing asset_class/timeframe would
        # suppress every timeframe for the symbol — the exact over-broad
        # suppression this key was widened to prevent.
        missing = [f for f in ("symbol", "asset_class", "timeframe", "session") if f not in entry]
        if missing:
            raise ValueError(f"unresolved ledger entry {entry!r} is missing {missing}")
        keys.add(
            _unresolved_key(
                entry["symbol"],
                entry["asset_class"],
                entry["timeframe"],
                date.fromisoformat(entry["session"]),
            )
        )
    return keys


def record_unresolved(
    path: Path,
    symbol: str,
    session: date,
    reason: str,
    as_of: date,
    asset_class: str = "equity",
    timeframe: str = "1d",
) -> None:
    """Record a permanently unsourceable session so it is never retried again.

    Serialized with flock: this is a read-modify-write on a file the scheduled
    scan and an operator can both touch, and the repo already settles that
    question the same way in shepherd_repair.py.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "symbol": symbol,
        "asset_class": asset_class,
        "timeframe": timeframe,
        "session": session.isoformat(),
        "reason": reason,
        "as_of": as_of.isoformat(),
    }
    with open(path.with_suffix(path.suffix + ".lock"), "w") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            entries = json.loads(path.read_text()) if path.exists() else []
            if entry not in entries:
                entries.append(entry)
            path.write_text(json.dumps(entries, indent=2, sort_keys=True))
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def suppress_unresolved(findings: list[Finding], unresolved: set[UnresolvedKey]) -> list[Finding]:
    """Drop sessions already recorded unresolved; drop findings left with none."""
    kept: list[Finding] = []
    for finding in findings:
        sessions = tuple(
            s
            for s in finding.sessions
            if _unresolved_key(finding.symbol, finding.asset_class, finding.timeframe, s) not in unresolved
        )
        if sessions:
            kept.append(replace(finding, sessions=sessions))
    return kept
