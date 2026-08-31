"""Coverage is a set of registry rows, not a set of detector scripts.

A row without a test is rejected: see section 4.5 of
docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

VALID_GAPS = {f"G{n}" for n in range(1, 13)}
VALID_TIERS = {"A", "B"}
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
