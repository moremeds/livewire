# Livewire Gap Engine — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the disk-glob coverage denominator with one registry-driven engine that computes `expected − actual` from presets and the trading calendar, so a symbol that never landed becomes detectable.

**Architecture:** Three new library modules under `clients/` (denominator, registry, engine) plus one CLI entry under `livewire_scripts/`. The engine emits findings; Tier A findings become a manifest handed to the existing `shepherd_repair` executor; Tier B findings are written to a decision queue that nothing consumes in Phase 1. No model calls, no Helium dependency.

**Tech Stack:** Python 3.13, pyarrow (parquet), pytest, ruff, `uv` for all execution.

**Spec:** `docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md`

## Global Constraints

- **Python `>=3.13`**, run everything through `uv run` — never bare `python`/`pip`.
- **No new dependencies.** Available: `boto3`, `duckdb>=1.5`, `httpx`, `ib-async`, `lxml`, `prefect`, `pyarrow>=23.0`, `requests`, `rich`. There is **no `pyyaml`, no `pandas`, no `polars`** — the registry is therefore **JSON**, matching `presets/*.json`, and parquet is read with `pyarrow`.
- **The denominator is never derived from disk contents.** Deriving expectation from what already exists is the defect this plan removes (`livewire_scripts/coverage_report.py:274,379`).
- **Tests use real tickers and real frozen dates.** No placeholder symbols, no invented prices. 2026-08-26, 2026-08-27 and 2026-08-28 are known real XNYS sessions (verified: `MUNJ` bronze holds exactly those three rows).
- **Nothing in this plan writes the data lake** except through the existing `shepherd_repair` executor.
- Lint/format: `ruff>=0.12`. Tests: `pytest>=9.0`, flat layout `tests/test_<module>.py`.
- Every commit message: no AI attribution trailers.

## Scope

**In:** the denominator, the registry mechanism, G1/G2/G3 detection with `heal_by` ordering, the unresolved ledger, the Tier A/Tier B split, and scheduling.

**Deferred to follow-on plans, with reason:**

| Deferred                                                  | Reason it cannot be in this plan                                                                                                                 |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Retiring the 10 ENGINE scripts (spec §15)                 | The engine must exist and run clean before anything is retired; retiring first leaves no detector at all.                                        |
| G12 silent-mispricing row                                 | Threshold calibration needs the 2026-07-16 replay set, whose availability is unknown (spec §11.4). That investigation must not block the engine. |
| `rebuild-silver` precondition gate + `INCOMPLETE_HISTORY` | Independent subsystem — changes silver publication semantics and the PIT manifest, not the detection engine.                                     |

---

Also deferred: the **one-time IB deep-history backfill to 1995-01-01**
(spec §13.1). It is an attended, 2FA-gated operational run bounded by IB pacing,
not code, and it needs the engine's hole list as its input — so it follows.

**Deviation from the spec, stated explicitly:** spec §10 lists Tier A repair
execution as a Phase 1 deliverable. This plan stops at _producing_ the repair
manifest and defers _executing_ it (follow-on 1). Reason: detection correctness
is verifiable read-only, and no unattended mutation should be scheduled before
the denominator has been observed to be right on real data. If you disagree,
fold follow-on 1 back in — but do not schedule the executor before the
"denominator validity" acceptance criterion below has passed.

**Freshness SLA (spec §8.1) is expressed by schedule time, not by code.** The
scan's `--end` argument together with the launchd `StartCalendarInterval` in
Task 6 encodes "equity 1d must be complete by T+1 06:00 HKT". No separate SLA
evaluator is built; one would duplicate the scheduler.

---

### Task 1: Coverage denominator

**Files:**

- Create: `clients/coverage_denominator.py`
- Test: `tests/test_coverage_denominator.py`

**Interfaces:**

- Consumes: `clients.ingestion_common.load_preset(path) -> tuple[str, list[str], dict[str, str]]`; `clients.trading_calendar.trading_dates_in_range(start: date, end: date) -> list[date]`
- Produces: `ExpectedSeries(symbol: str, asset_class: str, timeframe: str, sessions: tuple[date, ...])` and `build_denominator(preset_paths: list[Path], asset_class: str, timeframe: str, start: date, end: date, as_of: date) -> list[ExpectedSeries]`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_coverage_denominator.py
from datetime import date
from pathlib import Path

from clients.coverage_denominator import build_denominator

PRESETS = Path(__file__).resolve().parents[1] / "presets"


def test_denominator_does_not_depend_on_disk():
    """A preset symbol with no parquet file must still be expected.

    This is the whole point: coverage_report.py globs the disk, so a symbol
    that never landed is invisible to it.
    """
    series = build_denominator(
        [PRESETS / "volatility.json"],
        asset_class="volatility",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 8, 28),
        as_of=date(2026, 8, 31),
    )
    assert series, "volatility preset must yield expected series"
    # every expected series carries the three real XNYS sessions in range
    for s in series:
        assert s.sessions == (date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))


def test_expired_futures_contract_is_not_expected():
    """ES_202506 has expired as of 2026-08-31; it must leave the denominator."""
    fresh = build_denominator(
        [PRESETS / "futures-active.json"],
        asset_class="futures",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 8, 28),
        as_of=date(2026, 8, 31),
    )
    symbols = {s.symbol for s in fresh}
    expired = {s for s in symbols if s.endswith("_202506")}
    assert not expired, f"expired contracts still expected: {expired}"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_coverage_denominator.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.coverage_denominator'`

- [ ] **Step 3: Write the minimal implementation**

```python
# clients/coverage_denominator.py
"""Expected coverage = presets x trading calendar x timeframe.

The denominator must never be derived from what is already on disk: a symbol
that never landed has to stay visible. See section 4 of
docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from clients.ingestion_common import load_preset
from clients.trading_calendar import trading_dates_in_range


@dataclass(frozen=True)
class ExpectedSeries:
    symbol: str
    asset_class: str
    timeframe: str
    sessions: tuple[date, ...]


def _contract_expiry(symbol: str) -> date | None:
    """Month-start expiry for a composite futures ticker (ES_202506), else None."""
    _root, _sep, expiry = symbol.partition("_")
    if len(expiry) != 6 or not expiry.isdigit():
        return None
    return date(int(expiry[:4]), int(expiry[4:]), 1)


def build_denominator(
    preset_paths: list[Path],
    asset_class: str,
    timeframe: str,
    start: date,
    end: date,
    as_of: date,
) -> list[ExpectedSeries]:
    sessions = tuple(trading_dates_in_range(start, end))
    current_month = as_of.replace(day=1)
    out: list[ExpectedSeries] = []
    for preset_path in preset_paths:
        _name, tickers, _exchange_map = load_preset(preset_path)
        for ticker in tickers:
            expiry = _contract_expiry(ticker)
            if expiry is not None and expiry < current_month:
                continue
            out.append(ExpectedSeries(ticker, asset_class, timeframe, sessions))
    return out
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_coverage_denominator.py -v`
Expected: PASS (2 passed)

If `test_expired_futures_contract_is_not_expected` passes vacuously because `presets/futures-active.json` holds no `_202506` contract, change the assertion to a contract root that is actually present and already expired — check with `uv run python -c "from clients.ingestion_common import load_preset; print(load_preset('presets/futures-active.json')[1])"`.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check clients/coverage_denominator.py tests/test_coverage_denominator.py
git add clients/coverage_denominator.py tests/test_coverage_denominator.py
git commit -m "feat: coverage denominator from presets and trading calendar"
```

---

### Task 2: Actual-state reader and G1/G2/G3 classification

**Files:**

- Create: `clients/gap_engine.py`
- Test: `tests/test_gap_engine.py`

**Interfaces:**

- Consumes: `ExpectedSeries` from Task 1
- Produces: `Finding(symbol, asset_class, timeframe, gap, sessions, heal_by_days, tier)`; `actual_sessions(bronze_root: Path, series: ExpectedSeries) -> set[date]`; `classify(series: ExpectedSeries, present: set[date], massive_floor: date) -> list[Finding]`

- [ ] **Step 1: Confirm the bronze date column name**

Run: `grep -n "_BASE_COLUMNS" clients/bronze_client.py | head -3` then read that assignment.
The implementation below assumes the column is `trade_date` (documented for rates bronze at `CLAUDE.md:104`). If equity bronze uses a different name, use the real one in Step 3 — do not guess.

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_gap_engine.py
from datetime import date

from clients.coverage_denominator import ExpectedSeries
from clients.gap_engine import classify

SESSIONS = (date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))
FLOOR = date(2021, 7, 12)  # measured Massive entitlement floor, docs/audits/2026-07-11


def _series() -> ExpectedSeries:
    return ExpectedSeries("MUNJ", "equity", "1d", SESSIONS)


def test_missing_file_is_g3():
    findings = classify(_series(), present=set(), massive_floor=FLOOR)
    assert [f.gap for f in findings] == ["G3"]
    assert findings[0].sessions == SESSIONS


def test_missing_latest_session_is_g1():
    present = {date(2026, 8, 26), date(2026, 8, 27)}
    findings = classify(_series(), present=present, massive_floor=FLOOR)
    assert [f.gap for f in findings] == ["G1"]
    assert findings[0].sessions == (date(2026, 8, 28),)


def test_missing_middle_session_is_g2():
    present = {date(2026, 8, 26), date(2026, 8, 28)}
    findings = classify(_series(), present=present, massive_floor=FLOOR)
    assert [f.gap for f in findings] == ["G2"]
    assert findings[0].sessions == (date(2026, 8, 27),)


def test_complete_series_yields_nothing():
    assert classify(_series(), present=set(SESSIONS), massive_floor=FLOOR) == []


def test_heal_by_is_headroom_above_the_massive_floor():
    findings = classify(_series(), present=set(), massive_floor=FLOOR)
    # earliest missing session is 2026-08-26; headroom is days above the floor
    assert findings[0].heal_by_days == (date(2026, 8, 26) - FLOOR).days


def test_session_below_the_floor_has_negative_headroom():
    old = ExpectedSeries("MUNJ", "equity", "1d", (date(2019, 3, 1),))
    findings = classify(old, present=set(), massive_floor=FLOOR)
    assert findings[0].heal_by_days < 0, "pre-floor sessions are IB-only"


def test_tier_follows_the_massive_window():
    """Inside the window Massive repairs unattended; below it only IB can, and
    IB is 2FA-gated, so it is a decision rather than an automatic repair."""
    inside = classify(_series(), present=set(), massive_floor=FLOOR)
    assert inside[0].tier == "A"

    old = ExpectedSeries("MUNJ", "equity", "1d", (date(2019, 3, 1),))
    below = classify(old, present=set(), massive_floor=FLOOR)
    assert below[0].tier == "B", "pre-floor gaps must not claim unattended repair"
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_gap_engine.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.gap_engine'`

- [ ] **Step 4: Write the minimal implementation**

```python
# clients/gap_engine.py
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
    gap: str                      # "G1" | "G2" | "G3"
    sessions: tuple[date, ...]
    heal_by_days: int
    tier: str                     # "A" | "B"


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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_gap_engine.py -v`
Expected: PASS (6 passed)

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check clients/gap_engine.py tests/test_gap_engine.py
git add clients/gap_engine.py tests/test_gap_engine.py
git commit -m "feat: gap classification with Massive-window headroom"
```

---

### Task 3: Registry loader with the mandatory-test rule

**Files:**

- Create: `clients/gap_registry.py`
- Create: `registry/gaps.json`
- Test: `tests/test_gap_registry.py`

**Interfaces:**

- Produces: `RegistryRow(id, gap, asset_class, timeframe, universe, check, params, tier, since, test)`; `load_registry(path: Path) -> list[RegistryRow]`; `RegistryError`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_gap_registry.py
import json
from pathlib import Path

import pytest

from clients.gap_registry import RegistryError, load_registry

REPO = Path(__file__).resolve().parents[1]

VALID_ROW = {
    "id": "g3-equity-sp500-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "equity",
    "timeframe": "1d",
    "universe": ["sp500"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3",
}


def test_shipped_registry_loads():
    rows = load_registry(REPO / "registry" / "gaps.json")
    assert rows, "shipped registry must not be empty"
    assert all(row.test for row in rows)


def test_row_without_test_is_rejected(tmp_path):
    """Spec 4.5: a row without a test is not coverage, it is a claim."""
    row = dict(VALID_ROW)
    del row["test"]
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="test"):
        load_registry(path)


def test_unknown_gap_id_is_rejected(tmp_path):
    row = dict(VALID_ROW, gap=["G99"])
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="G99"):
        load_registry(path)


def test_unknown_tier_is_rejected(tmp_path):
    row = dict(VALID_ROW, tier="Z")
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps([row]))
    with pytest.raises(RegistryError, match="tier"):
        load_registry(path)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_gap_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.gap_registry'`

- [ ] **Step 3: Write the implementation**

```python
# clients/gap_registry.py
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
    gap: tuple[str, ...]          # a check emits a family, e.g. denominator_diff -> G1/G2/G3
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
```

- [ ] **Step 4: Create the shipped registry with the first real rows**

```json
[
  {
    "id": "g1-g2-g3-equity-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "equity",
    "timeframe": "1d",
    "universe": ["sp500", "ndx100"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3"
  },
  {
    "id": "g1-g2-g3-rates-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "rates",
    "timeframe": "1d",
    "universe": ["interests"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3"
  },
  {
    "id": "g1-g2-g3-fx-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "fx",
    "timeframe": "1d",
    "universe": ["fx-pairs"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3"
  },
  {
    "id": "g1-g2-g3-cmdty-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "cmdty",
    "timeframe": "1d",
    "universe": ["cmdty-metals"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3"
  },
  {
    "id": "g1-g2-g3-volatility-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "volatility",
    "timeframe": "1d",
    "universe": ["volatility"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3"
  },
  {
    "id": "g1-g2-g3-futures-daily",
    "gap": ["G1", "G2", "G3"],
    "asset_class": "futures",
    "timeframe": "1d",
    "universe": ["futures-active"],
    "check": "denominator_diff",
    "params": {},
    "tier": "A",
    "since": "2026-08-31",
    "test": "tests/test_gap_engine.py::test_missing_file_is_g3"
  }
]
```

`fx` and `cmdty` appear here deliberately: `coverage_report.py:363` omits both.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_gap_registry.py -v`
Expected: PASS (4 passed)

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check clients/gap_registry.py tests/test_gap_registry.py
git add clients/gap_registry.py registry/gaps.json tests/test_gap_registry.py
git commit -m "feat: gap registry with mandatory test binding per row"
```

---

### Task 4: Unresolved ledger

**Files:**

- Modify: `clients/gap_engine.py` (append)
- Modify: `tests/test_gap_engine.py` (append)

**Interfaces:**

- Produces: `load_unresolved(path: Path) -> set[tuple[str, date]]`; `record_unresolved(path: Path, symbol: str, session: date, reason: str, as_of: date) -> None`; `suppress_unresolved(findings: list[Finding], unresolved: set[tuple[str, date]]) -> list[Finding]`

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_gap_engine.py
from clients.gap_engine import load_unresolved, record_unresolved, suppress_unresolved


def test_unresolved_sessions_are_not_re_reported(tmp_path):
    """Cause 5: the same unsourceable symbols must not be re-litigated every round."""
    ledger = tmp_path / "unresolved.json"
    record_unresolved(
        ledger, "MUNJ", date(2026, 8, 27), reason="delisted, no source", as_of=date(2026, 8, 31)
    )
    findings = classify(_series(), present={date(2026, 8, 26), date(2026, 8, 28)}, massive_floor=FLOOR)
    kept = suppress_unresolved(findings, load_unresolved(ledger))
    assert kept == [], "a recorded unresolved session must not re-report"


def test_unresolved_ledger_keeps_the_reason(tmp_path):
    ledger = tmp_path / "unresolved.json"
    record_unresolved(
        ledger, "MUNJ", date(2026, 8, 27), reason="delisted, no source", as_of=date(2026, 8, 31)
    )
    assert ("MUNJ", date(2026, 8, 27)) in load_unresolved(ledger)
    assert "delisted, no source" in ledger.read_text()


def test_partially_unresolved_finding_keeps_its_other_sessions(tmp_path):
    ledger = tmp_path / "unresolved.json"
    record_unresolved(ledger, "MUNJ", date(2026, 8, 27), reason="x", as_of=date(2026, 8, 31))
    findings = classify(_series(), present=set(), massive_floor=FLOOR)
    kept = suppress_unresolved(findings, load_unresolved(ledger))
    assert kept and kept[0].sessions == (date(2026, 8, 26), date(2026, 8, 28))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_gap_engine.py -k unresolved -v`
Expected: FAIL — `ImportError: cannot import name 'load_unresolved'`

- [ ] **Step 3: Write the implementation**

```python
# append to clients/gap_engine.py
import json
from dataclasses import replace


def load_unresolved(path: Path) -> set[tuple[str, date]]:
    if not Path(path).exists():
        return set()
    entries = json.loads(Path(path).read_text())
    return {
        (entry["symbol"], date.fromisoformat(entry["session"])) for entry in entries
    }


def record_unresolved(
    path: Path, symbol: str, session: date, reason: str, as_of: date
) -> None:
    """Record a permanently unsourceable session so it is never retried again."""
    path = Path(path)
    entries = json.loads(path.read_text()) if path.exists() else []
    entry = {
        "symbol": symbol,
        "session": session.isoformat(),
        "reason": reason,
        "as_of": as_of.isoformat(),
    }
    if entry not in entries:
        entries.append(entry)
    path.write_text(json.dumps(entries, indent=2, sort_keys=True))


def suppress_unresolved(
    findings: list[Finding], unresolved: set[tuple[str, date]]
) -> list[Finding]:
    """Drop sessions already recorded unresolved; drop findings left with none."""
    kept: list[Finding] = []
    for finding in findings:
        sessions = tuple(
            s for s in finding.sessions if (finding.symbol, s) not in unresolved
        )
        if sessions:
            kept.append(replace(finding, sessions=sessions))
    return kept
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_gap_engine.py -v`
Expected: PASS (9 passed)

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check clients/gap_engine.py tests/test_gap_engine.py
git add clients/gap_engine.py tests/test_gap_engine.py
git commit -m "feat: unresolved ledger so unsourceable sessions stop recurring"
```

---

### Task 5: `gap-scan` CLI — Tier A manifest and Tier B queue

**Files:**

- Create: `livewire_scripts/gap_scan.py`
- Modify: `scripts/livewire_quality.py` (the `COMMANDS` dict at line 18)
- Test: `tests/test_gap_scan.py`

**Interfaces:**

- Consumes: `build_denominator`, `actual_sessions`, `classify`, `load_unresolved`, `suppress_unresolved`, `load_registry`
- Produces: `scan(bronze_root, registry_path, presets_dir, start, end, as_of, massive_floor, unresolved_path) -> list[Finding]` (sorted by `heal_by_days` ascending); `write_tier_a_manifest(findings, path) -> None`; `write_decision_requests(findings, path) -> None`; `main(argv) -> int`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_gap_scan.py
import json
from datetime import date
from pathlib import Path

from clients.gap_engine import Finding
from livewire_scripts.gap_scan import (
    write_decision_requests,
    write_tier_a_manifest,
)

FLOOR = date(2021, 7, 12)


def _finding(symbol: str, session: date, tier: str = "A") -> Finding:
    return Finding(
        symbol=symbol,
        asset_class="equity",
        timeframe="1d",
        gap="G2",
        sessions=(session,),
        heal_by_days=(session - FLOOR).days,
        tier=tier,
    )


def test_tier_a_manifest_is_ordered_by_heal_by(tmp_path):
    """Sessions nearest the rolling Massive floor lose the cheap repair path first."""
    urgent = _finding("MUNJ", date(2021, 8, 2))
    relaxed = _finding("AAPL", date(2026, 8, 27))
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([relaxed, urgent], path)
    manifest = json.loads(path.read_text())
    assert [entry["symbol"] for entry in manifest["repairs"]] == ["MUNJ", "AAPL"]


def test_tier_b_uses_the_triage_breaks_verdict_vocabulary(tmp_path):
    """Spec 15: adopt the existing vocabulary rather than inventing a schema."""
    path = tmp_path / "decisions.json"
    write_decision_requests([_finding("MUNJ", date(2026, 8, 27), tier="B")], path)
    requests = json.loads(path.read_text())
    assert requests[0]["verdict"] == "inconclusive"
    assert requests[0]["symbol"] == "MUNJ"


def test_tier_b_findings_never_enter_the_repair_manifest(tmp_path):
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([_finding("MUNJ", date(2026, 8, 27), tier="B")], path)
    assert json.loads(path.read_text())["repairs"] == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_gap_scan.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'livewire_scripts.gap_scan'`

- [ ] **Step 3: Write the implementation**

```python
# livewire_scripts/gap_scan.py
"""Detect coverage gaps from the denominator and route them by tier.

Tier A becomes a manifest for the existing shepherd_repair executor.
Tier B becomes a decision request that nothing consumes in Phase 1 — its
queue depth is the measurement that decides whether an agent lane is worth
building.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from clients.coverage_denominator import build_denominator
from clients.gap_engine import (
    Finding,
    actual_sessions,
    classify,
    load_unresolved,
    suppress_unresolved,
)
from clients.gap_registry import load_registry

MASSIVE_FLOOR = date(2021, 7, 12)  # measured, docs/audits/2026-07-11-daily-bronze-repair.md:58


def scan(
    bronze_root: Path,
    registry_path: Path,
    presets_dir: Path,
    start: date,
    end: date,
    as_of: date,
    massive_floor: date = MASSIVE_FLOOR,
    unresolved_path: Path | None = None,
) -> list[Finding]:
    unresolved = load_unresolved(unresolved_path) if unresolved_path else set()
    findings: list[Finding] = []
    for row in load_registry(registry_path):
        preset_paths = [presets_dir / f"{name}.json" for name in row.universe]
        for series in build_denominator(
            preset_paths, row.asset_class, row.timeframe, start, end, as_of
        ):
            present = actual_sessions(bronze_root, series)
            findings.extend(classify(series, present, massive_floor))
    return sorted(
        suppress_unresolved(findings, unresolved), key=lambda f: f.heal_by_days
    )


def _entry(finding: Finding) -> dict:
    return {
        "symbol": finding.symbol,
        "asset_class": finding.asset_class,
        "timeframe": finding.timeframe,
        "gap": finding.gap,
        "sessions": [session.isoformat() for session in finding.sessions],
        "heal_by_days": finding.heal_by_days,
    }


def write_tier_a_manifest(findings: list[Finding], path: Path) -> None:
    repairs = [
        _entry(f)
        for f in sorted(
            (f for f in findings if f.tier == "A"), key=lambda f: f.heal_by_days
        )
    ]
    Path(path).write_text(json.dumps({"repairs": repairs}, indent=2))


def write_decision_requests(findings: list[Finding], path: Path) -> None:
    """Tier B queue. Verdict vocabulary is triage_breaks.py's, not a new one."""
    requests = [
        dict(_entry(f), verdict="inconclusive")
        for f in findings
        if f.tier == "B"
    ]
    Path(path).write_text(json.dumps(requests, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="livewire_quality.py gap-scan")
    parser.add_argument("--bronze-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, default=Path("registry/gaps.json"))
    parser.add_argument("--presets-dir", type=Path, default=Path("presets"))
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument("--unresolved", type=Path, default=None)
    parser.add_argument("--manifest-out", type=Path, required=True)
    parser.add_argument("--decisions-out", type=Path, required=True)
    args = parser.parse_args(argv)

    findings = scan(
        args.bronze_root,
        args.registry,
        args.presets_dir,
        args.start,
        args.end,
        args.as_of,
        unresolved_path=args.unresolved,
    )
    write_tier_a_manifest(findings, args.manifest_out)
    write_decision_requests(findings, args.decisions_out)
    print(
        json.dumps(
            {
                "findings": len(findings),
                "tier_a": sum(1 for f in findings if f.tier == "A"),
                "tier_b": sum(1 for f in findings if f.tier == "B"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Register the subcommand**

Add one row to the `COMMANDS` dict in `scripts/livewire_quality.py` (starts at line 18), keeping the existing alphabetical grouping style:

```python
    "gap-scan": "livewire_scripts.gap_scan",
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_gap_scan.py -v`
Expected: PASS (3 passed)

- [ ] **Step 6: Run it against the real lake, read-only**

Run:

```bash
uv run python scripts/livewire_quality.py gap-scan \
  --bronze-root /Volumes/DATA_LAKE/livewire/data-lake/bronze \
  --start 2026-08-01 --end 2026-08-28 --as-of 2026-08-31 \
  --manifest-out /tmp/gap-manifest.json \
  --decisions-out /tmp/gap-decisions.json
```

Expected: a JSON summary line. **Neither output file is fed to `shepherd_repair` in this task** — Phase 1 stops at producing the manifest. Inspect `/tmp/gap-manifest.json` and confirm the first entries have the smallest `heal_by_days`.

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check livewire_scripts/gap_scan.py tests/test_gap_scan.py
git add livewire_scripts/gap_scan.py scripts/livewire_quality.py tests/test_gap_scan.py
git commit -m "feat: gap-scan CLI routing findings to repair manifest and decision queue"
```

---

### Task 6: Schedule the scan and the denominator refresh

**Files:**

- Create: `launchd/com.livewire.gap-scan.plist.example`
- Create: `launchd/com.livewire.universe-refresh.plist.example`
- Modify: `CLAUDE.md` (the scheduled-jobs section)

**Interfaces:**

- Consumes: the `gap-scan` subcommand from Task 5; the existing `universe_sync.py` and `shepherd_universe.py` entry points.

- [ ] **Step 1: Confirm the entry points for the two universe scripts**

Run: `grep -n "def main\|__main__" livewire_scripts/universe_sync.py livewire_scripts/shepherd_universe.py`
and `grep -n "universe" scripts/livewire.py scripts/livewire_ingest.py`
Use whichever router already exposes them; do not add a second invocation path.

- [ ] **Step 2: Copy the existing plist shape**

Run: `cat launchd/com.livewire.coverage.plist.example`
Reuse its structure verbatim — same `EnvironmentVariables`, `StandardOutPath`/`StandardErrorPath` conventions and working directory. Only the label, arguments and `StartCalendarInterval` change.

- [ ] **Step 3: Write `launchd/com.livewire.gap-scan.plist.example`**

Label `com.livewire.gap-scan`, invoking the `gap-scan` subcommand with `--bronze-root`, `--manifest-out`, `--decisions-out` and `--unresolved`, scheduled after the existing `com.livewire.coverage` job (11:00 UTC) so it reads a settled lake.

- [ ] **Step 4: Write `launchd/com.livewire.universe-refresh.plist.example`**

Label `com.livewire.universe-refresh`, invoking `universe_sync` then `shepherd_universe` through the router confirmed in Step 1, weekly. This closes the drift mechanism found by the audit: both scripts exist and neither is scheduled, so the denominator silently goes stale (spec §9.2).

- [ ] **Step 5: Document both jobs in `CLAUDE.md`**

Add them to the scheduled-jobs list alongside `com.livewire.coverage`, stating that `gap-scan` is read-only in Phase 1 and writes two artifacts but mutates nothing.

- [ ] **Step 6: Verify the plists parse**

Run: `plutil -lint launchd/com.livewire.gap-scan.plist.example launchd/com.livewire.universe-refresh.plist.example`
Expected: `OK` for both.

- [ ] **Step 7: Commit**

```bash
git add launchd/com.livewire.gap-scan.plist.example launchd/com.livewire.universe-refresh.plist.example CLAUDE.md
git commit -m "feat: schedule gap-scan and the universe refresh that keeps the denominator true"
```

---

## Acceptance for Phase 1 core

Run before opening the PR:

- [ ] `uv run pytest tests/test_coverage_denominator.py tests/test_gap_engine.py tests/test_gap_registry.py tests/test_gap_scan.py -v` — all pass
- [ ] `uv run pytest` — full suite still green (no regression against the existing ~2,387 tests)
- [ ] `uv run ruff check .` — clean
- [ ] **Denominator validity (spec §11.2):** add a never-ingested real symbol to a preset copy, run `gap-scan` against it, confirm a `G3` finding appears. This is the criterion the 2026-08 `MUNJ` case did not prove.
- [ ] **Ordering:** confirm `/tmp/gap-manifest.json` is sorted ascending by `heal_by_days`.
- [ ] **No mutation:** `gap-scan` wrote only its two output files; the lake is byte-identical.

## Follow-on plans (do not start here)

1. **Tier A execution** — feed the manifest to `shepherd_repair`, verify, and prove one repair end-to-end.
2. **Detector convergence** — dispositions for the 10 ENGINE scripts (spec §15), respecting the `weekly_quality_summary` → `coverage_report` log dependency.
3. **G12 silent mispricing** — the replay-set investigation and threshold calibration.
4. **Silver publication contract** — `rebuild-silver` precondition gate, `INCOMPLETE_HISTORY`, `known_holes[]`. _(The `reason` half is cut out into 6.)_
5. **L4 — delisting terminus.** Prerequisite for Task 1's delisted branch, so it goes first. Run the existing producer chain (`universe_screener` → `TagRegistry.mark_delisted`) so `registry.json` and `bronze-delisted/` hold rows, *then* add the delisted branch to `build_denominator`. Against an empty registry the branch is untestable — spec §4.3.
6. **L5-min — carry `reason` into the silver manifest.** `rebuild_silver.py:472` already computes it and writes it only to the optional `--failure-output`. Adding it to the revision manifest is spec §7's "additive only", so Apex needs no coordinated change. Independent PR, does not touch the gap engine. The precondition gate and `INCOMPLETE_HISTORY` stay in 4.
7. **L2 — G11 negative evidence.** A missing corporate-action event recorded once with reason and as-of, never retried: Task 4's ledger semantics with an action rather than a bar as the object. G11 appears in no plan today.
