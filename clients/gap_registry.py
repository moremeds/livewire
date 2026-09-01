"""Coverage is a set of registry rows, not a set of detector scripts.

A row without a test is rejected: see section 4.5 of
docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# G1..G12 are the spec taxonomy. G13 (head gap) and G14 (terminus: the symbol
# left the tape) were added by this engine. G2 and G13 are named but not emitted
# -- see clients/gap_engine.classify.
VALID_GAPS = {f"G{n}" for n in range(1, 15)}
VALID_TIERS = {"A", "B"}
# Only checks the engine actually dispatches. `scan` runs denominator_diff for
# every row regardless of this field, so a row naming anything else would
# silently produce G1/G2/G3 while claiming to run some other detector. Adding a
# check must be an explicit act here, paired with real dispatch.
VALID_CHECKS = {"denominator_diff"}
# The denominator uses trading_calendar.trading_dates_in_range, which is XNYS.
# FX trades ~24/5, CME futures keep their own sessions and FRED publishes on its
# own schedule, so for those a bar expected on an XNYS holiday is not expected at
# all and its absence is invisible. That is a KNOWN, DEFERRED limitation --
# recorded here so a new asset class cannot inherit the blind spot silently. Add
# to this set only together with a calendar that is actually right for it.
XNYS_CALENDAR_ASSET_CLASSES = {
    "equity",
    "volatility",
    "rates",
    "fx",
    "cmdty",
    "futures",
}
REQUIRED_FIELDS = (
    "id",
    "gap",
    "asset_class",
    "timeframe",
    "universe",
    "check",
    "tier",
    "since",
    "test",
)


class RegistryError(ValueError):
    """A registry row that would silently weaken coverage."""


@dataclass(frozen=True)
class RegistryRow:
    id: str
    gap: tuple[str, ...]  # a check emits a family, e.g. denominator_diff -> G1/G2/G3
    asset_class: str
    timeframe: str
    universe: tuple[str, ...]
    check: str
    tier: str
    since: str
    test: str
    params: dict[str, Any] = field(default_factory=dict)


def load_registry(path: Path) -> list[RegistryRow]:
    raw_rows = json.loads(Path(path).read_text())
    rows: list[RegistryRow] = []
    for raw in raw_rows:
        row_id = raw.get("id", "<no id>")
        for name in REQUIRED_FIELDS:
            if not raw.get(name):
                raise RegistryError(f"row {row_id}: missing required field {name!r}")
        for gap_id in raw["gap"]:
            if gap_id not in VALID_GAPS:
                raise RegistryError(f"row {row_id}: unknown gap id {gap_id!r}")
        if raw["tier"] not in VALID_TIERS:
            raise RegistryError(f"row {row_id}: unknown tier {raw['tier']!r}")
        if raw["check"] not in VALID_CHECKS:
            raise RegistryError(f"row {row_id}: unknown check {raw['check']!r}")
        if raw["asset_class"] not in XNYS_CALENDAR_ASSET_CLASSES:
            raise RegistryError(
                f"row {row_id}: asset_class {raw['asset_class']!r} has no calendar "
                "mapping; the denominator is XNYS-only, so add a real calendar "
                "before adding this asset class"
            )
        rows.append(
            RegistryRow(
                id=raw["id"],
                gap=tuple(raw["gap"]),
                asset_class=raw["asset_class"],
                timeframe=raw["timeframe"],
                universe=tuple(raw["universe"]),
                check=raw["check"],
                tier=raw["tier"],
                since=raw["since"],
                test=raw["test"],
                params=raw.get("params", {}),
            )
        )
    return rows
