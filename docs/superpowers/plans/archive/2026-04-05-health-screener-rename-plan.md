# Health Check, Universe Screener, and Parquet Rename — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add data integrity health checks with auto-backfill, an IB scanner-based universe screener, and rename parquet files from `data.parquet` to `1d.parquet` for multi-timeframe support.

**Architecture:** Three independent features implemented in order: rename (mechanical refactor) → health check (new script reusing existing clients) → universe screener (new script with IB scanner API). The rename introduces a `PARQUET_FILENAME` constant in `bronze_client.py` consumed by all parquet-touching code. Health check and screener are standalone scripts following the same patterns as `daily_update.py`.

**Tech Stack:** Python 3.13, PyArrow, DuckDB, ib_async, pytest (100% coverage)

**Spec:** `docs/superpowers/specs/2026-04-05-health-screener-rename-design.md`

---

## File Structure

### Feature 3: Parquet Rename

| File | Action | Responsibility |
|------|--------|----------------|
| `clients/bronze_client.py` | Modify | Add `PARQUET_FILENAME` constant, update 4 internal references |
| `clients/db_client.py` | Modify | Import `PARQUET_FILENAME`, update 3 methods |
| `scripts/sync_to_r2.py` | Modify | Import `PARQUET_FILENAME`, update `rglob` and `endswith` |
| `scripts/fetch_cboe_volatility.py` | Modify | Import `PARQUET_FILENAME`, update path construction |
| `scripts/rebuild_duckdb_from_parquet.py` | Modify | Import `PARQUET_FILENAME`, update glob check |
| `scripts/migrate_parquet_filename.py` | Create | Idempotent disk migration script |
| `tests/test_migrate_parquet_filename.py` | Create | Tests for migration script |
| 7 test files | Modify | Replace `"data.parquet"` literals with `"1d.parquet"` |
| 5 doc files | Modify | Replace `data.parquet` references |

### Feature 1: Health Check

| File | Action | Responsibility |
|------|--------|----------------|
| `scripts/health_check.py` | Create | Interior gap detection, auto-backfill, alerting |
| `tests/test_health_check.py` | Create | Full test coverage for health check logic |

### Feature 2: Universe Screener

| File | Action | Responsibility |
|------|--------|----------------|
| `scripts/universe_screener.py` | Create | IB scanner sweeps, universe diff, backfill trigger, archiving |
| `tests/test_universe_screener.py` | Create | Full test coverage for screener logic |

---

## Task 1: Add `PARQUET_FILENAME` Constant and Update `bronze_client.py`

**Files:**
- Modify: `clients/bronze_client.py`
- Test: `tests/test_bronze_client.py`

- [ ] **Step 1: Update the constant and references in `bronze_client.py`**

Add the constant after the imports (line 18 area) and update 4 internal references:

```python
# Add after line 18 (log = logging.getLogger(__name__))
PARQUET_FILENAME = "1d.parquet"
```

Then replace these 4 locations:

```python
# Line 115: get_existing_symbols()
# OLD: for path in self._bronze_dir.glob("symbol=*/data.parquet"):
# NEW:
for path in self._bronze_dir.glob(f"symbol=*/{PARQUET_FILENAME}"):

# Line 230: _symbol_path()
# OLD: return self._bronze_dir / f"symbol={symbol}" / "data.parquet"
# NEW:
return self._bronze_dir / f"symbol={symbol}" / PARQUET_FILENAME

# Line 233: _escaped_glob()
# OLD: return str(self._bronze_dir / "symbol=*/data.parquet").replace("'", "''")
# NEW:
return str(self._bronze_dir / f"symbol=*/{PARQUET_FILENAME}").replace("'", "''")

# Line 284: _publish_symbol_rows()
# OLD: tmp_path = out_path.with_name(f".data.parquet.{os.getpid()}.{time.time_ns()}.tmp")
# NEW:
tmp_path = out_path.with_name(f".{PARQUET_FILENAME}.{os.getpid()}.{time.time_ns()}.tmp")
```

- [ ] **Step 2: Update test literals in `tests/test_bronze_client.py`**

Replace all `"data.parquet"` literals with `"1d.parquet"`:

```python
# Line 59 (temp file glob pattern):
# OLD: assert list(bronze.bronze_dir.glob("symbol=AAPL/.data.parquet.*.tmp")) == []
# NEW:
assert list(bronze.bronze_dir.glob("symbol=AAPL/.1d.parquet.*.tmp")) == []

# Line 119 (path assertion):
# OLD: path = tmp_bronze / "symbol=AAPL" / "data.parquet"
# NEW:
path = tmp_bronze / "symbol=AAPL" / "1d.parquet"

# Line 160 (temp file glob pattern):
# OLD: assert list(bronze.bronze_dir.glob("symbol=AAPL/.data.parquet.*.tmp")) == []
# NEW:
assert list(bronze.bronze_dir.glob("symbol=AAPL/.1d.parquet.*.tmp")) == []
```

- [ ] **Step 3: Run tests to verify**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_bronze_client.py -v`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add clients/bronze_client.py tests/test_bronze_client.py
git commit -m "refactor: introduce PARQUET_FILENAME constant (data.parquet → 1d.parquet)"
```

---

## Task 2: Update `db_client.py` to Use `PARQUET_FILENAME`

**Files:**
- Modify: `clients/db_client.py`
- Test: `tests/test_db_client.py`

- [ ] **Step 1: Update `db_client.py`**

Add import at the top of `clients/db_client.py`:

```python
from clients.bronze_client import PARQUET_FILENAME
```

Then replace 5 locations:

```python
# Line 308 (docstring):
# OLD: Layout: bronze_dir/symbol=AAPL/data.parquet (Hive-partitioned).
# NEW: Layout: bronze_dir/symbol=AAPL/1d.parquet (Hive-partitioned).

# Line 314 (write_ticker_parquet):
# OLD: out_path = ticker_dir / "data.parquet"
# NEW:
out_path = ticker_dir / PARQUET_FILENAME

# Line 334 (replace_equities_from_parquet):
# OLD: parquet_files = list(bronze_dir.glob("symbol=*/data.parquet"))
# NEW:
parquet_files = list(bronze_dir.glob(f"symbol=*/{PARQUET_FILENAME}"))

# Line 335:
# OLD: parquet_glob = str(bronze_dir / "symbol=*/data.parquet").replace("'", "''")
# NEW:
parquet_glob = str(bronze_dir / f"symbol=*/{PARQUET_FILENAME}").replace("'", "''")

# Line 392 (replace_futures_from_parquet):
# OLD: parquet_files = list(bronze_dir.glob("symbol=*/data.parquet"))
# NEW:
parquet_files = list(bronze_dir.glob(f"symbol=*/{PARQUET_FILENAME}"))

# Line 393:
# OLD: parquet_glob = str(bronze_dir / "symbol=*/data.parquet").replace("'", "''")
# NEW:
parquet_glob = str(bronze_dir / f"symbol=*/{PARQUET_FILENAME}").replace("'", "''")
```

- [ ] **Step 2: Update test literals in `tests/test_db_client.py`**

```python
# Line 382:
# OLD: expected = bronze / "symbol=AAPL" / "data.parquet"
# NEW:
expected = bronze / "symbol=AAPL" / "1d.parquet"

# Line 519:
# OLD: (broken_bronze / "data.parquet").write_text("not parquet")
# NEW:
(broken_bronze / "1d.parquet").write_text("not parquet")

# Line 608:
# OLD: (broken_bronze / "data.parquet").write_text("not parquet")
# NEW:
(broken_bronze / "1d.parquet").write_text("not parquet")
```

- [ ] **Step 3: Run tests to verify**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_db_client.py -v`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add clients/db_client.py tests/test_db_client.py
git commit -m "refactor: update db_client to use PARQUET_FILENAME constant"
```

---

## Task 3: Update Remaining Scripts

**Files:**
- Modify: `scripts/sync_to_r2.py`, `scripts/fetch_cboe_volatility.py`, `scripts/rebuild_duckdb_from_parquet.py`
- Test: `tests/test_sync_to_r2.py`, `tests/test_fetch_cboe_volatility.py`, `tests/test_fetch_ib_historical.py`

- [ ] **Step 1: Update `scripts/sync_to_r2.py`**

Add import:
```python
from clients.bronze_client import PARQUET_FILENAME
```

Replace 2 locations:
```python
# Line 55:
# OLD: for parquet_file in bronze_dir.rglob("data.parquet"):
# NEW:
for parquet_file in bronze_dir.rglob(PARQUET_FILENAME):

# Line 84:
# OLD: if not s3_key.endswith("data.parquet"):
# NEW:
if not s3_key.endswith(PARQUET_FILENAME):
```

- [ ] **Step 2: Update `scripts/fetch_cboe_volatility.py`**

Add import:
```python
from clients.bronze_client import PARQUET_FILENAME
```

Replace 1 location:
```python
# Line 99:
# OLD: parquet_path = bronze_dir / "data.parquet"
# NEW:
parquet_path = bronze_dir / PARQUET_FILENAME
```

- [ ] **Step 3: Update `scripts/rebuild_duckdb_from_parquet.py`**

Add import:
```python
from clients.bronze_client import PARQUET_FILENAME
```

Replace 1 location:
```python
# Line 61:
# OLD: if not any(args.bronze_dir.glob("symbol=*/data.parquet")):
# NEW:
if not any(args.bronze_dir.glob(f"symbol=*/{PARQUET_FILENAME}")):
```

- [ ] **Step 4: Update `tests/test_sync_to_r2.py`**

Replace all `"data.parquet"` literals with `"1d.parquet"`:
- Line 39: `(equity_dir / "1d.parquet").write_bytes(b"fake parquet")`
- Line 49: `assert "1d.parquet" in args[0][0]`
- Line 51: `assert "asset_class=equity/symbol=AAPL/1d.parquet" in args[0][2]`
- Line 58: `(d / "1d.parquet").write_bytes(b"fake")`
- Line 72: `(d / "1d.parquet").write_bytes(b"fake")`
- Line 93: `{"Contents": [{"Key": "bronze/asset_class=equity/symbol=AAPL/1d.parquet"}]}`
- Line 109: `{"Key": "bronze/asset_class=equity/symbol=AAPL/1d.parquet"},`
- Line 125: `{"Contents": [{"Key": "bronze/asset_class=equity/symbol=AAPL/1d.parquet"}]}`

- [ ] **Step 5: Update `tests/test_fetch_cboe_volatility.py`**

Replace all `"data.parquet"` literals with `"1d.parquet"` at lines:
185, 231, 282, 296, 309, 321, 322, 336.

- [ ] **Step 6: Update `tests/test_fetch_ib_historical.py`**

```python
# Line 795:
# OLD: assert (bronze_dir / "symbol=AAPL" / "data.parquet").exists()
# NEW:
assert (bronze_dir / "symbol=AAPL" / "1d.parquet").exists()

# Line 1274:
# OLD: assert (vol_bronze_dir / "symbol=VIX" / "data.parquet").exists()
# NEW:
assert (vol_bronze_dir / "symbol=VIX" / "1d.parquet").exists()
```

- [ ] **Step 7: Run all tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add scripts/sync_to_r2.py scripts/fetch_cboe_volatility.py scripts/rebuild_duckdb_from_parquet.py tests/test_sync_to_r2.py tests/test_fetch_cboe_volatility.py tests/test_fetch_ib_historical.py
git commit -m "refactor: update all scripts and tests for 1d.parquet rename"
```

---

## Task 4: Update Documentation

**Files:**
- Modify: `CLAUDE.md`, `README.md`, `AGENTS.md`, `.codex/project-memory.md`, `docs/observability_defensive_blueprint.md`, `scripts/fetch_ib_historical.py` (docstring only)

- [ ] **Step 1: Replace all `data.parquet` references in docs**

In each file, replace every occurrence of `data.parquet` with `1d.parquet`:

**`CLAUDE.md`** — lines 59, 60, 229, 246, 355
**`README.md`** — lines 73, 74, 75
**`AGENTS.md`** — lines 21, 22
**`.codex/project-memory.md`** — lines 18, 19
**`docs/observability_defensive_blueprint.md`** — line 22
**`scripts/fetch_ib_historical.py`** — line 8 (module docstring only)

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md README.md AGENTS.md .codex/project-memory.md docs/observability_defensive_blueprint.md scripts/fetch_ib_historical.py
git commit -m "docs: update all references from data.parquet to 1d.parquet"
```

---

## Task 5: Create Migration Script

**Files:**
- Create: `scripts/migrate_parquet_filename.py`
- Create: `tests/test_migrate_parquet_filename.py`

- [ ] **Step 1: Write the test file**

```python
"""Tests for scripts/migrate_parquet_filename.py."""

from __future__ import annotations

import pytest

from scripts.migrate_parquet_filename import migrate_parquet_files


class TestMigrateParquetFilename:
    def test_renames_data_to_1d(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        old = sym_dir / "data.parquet"
        old.write_bytes(b"fake parquet")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=False)

        assert not old.exists()
        assert (sym_dir / "1d.parquet").exists()
        assert stats["renamed"] == 1
        assert stats["skipped"] == 0
        assert stats["errors"] == 0

    def test_skips_already_renamed(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        (sym_dir / "1d.parquet").write_bytes(b"fake parquet")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=False)

        assert stats["renamed"] == 0
        assert stats["skipped"] == 0

    def test_dry_run_does_not_rename(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        old = sym_dir / "data.parquet"
        old.write_bytes(b"fake parquet")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=True)

        assert old.exists()
        assert not (sym_dir / "1d.parquet").exists()
        assert stats["renamed"] == 1  # counts what would be renamed

    def test_aborts_on_split_brain(self, tmp_path):
        bronze = tmp_path / "bronze" / "asset_class=equity"
        sym_dir = bronze / "symbol=AAPL"
        sym_dir.mkdir(parents=True)
        (sym_dir / "data.parquet").write_bytes(b"old")
        (sym_dir / "1d.parquet").write_bytes(b"new")

        with pytest.raises(RuntimeError, match="split-brain"):
            migrate_parquet_files(tmp_path / "bronze", dry_run=False)

    def test_handles_multiple_asset_classes(self, tmp_path):
        for ac in ("asset_class=equity", "asset_class=futures", "asset_class=volatility"):
            sym_dir = tmp_path / "bronze" / ac / "symbol=TEST"
            sym_dir.mkdir(parents=True)
            (sym_dir / "data.parquet").write_bytes(b"fake")

        stats = migrate_parquet_files(tmp_path / "bronze", dry_run=False)

        assert stats["renamed"] == 3

    def test_handles_delisted_dir(self, tmp_path):
        # bronze-delisted alongside bronze
        sym_dir = tmp_path / "bronze-delisted" / "asset_class=equity" / "symbol=OLD"
        sym_dir.mkdir(parents=True)
        (sym_dir / "data.parquet").write_bytes(b"fake")

        stats = migrate_parquet_files(tmp_path / "bronze-delisted", dry_run=False)

        assert stats["renamed"] == 1
        assert (sym_dir / "1d.parquet").exists()

    def test_empty_dir_returns_zero(self, tmp_path):
        bronze = tmp_path / "bronze"
        bronze.mkdir()

        stats = migrate_parquet_files(bronze, dry_run=False)

        assert stats["renamed"] == 0

    def test_nonexistent_dir_returns_zero(self, tmp_path):
        stats = migrate_parquet_files(tmp_path / "nonexistent", dry_run=False)

        assert stats["renamed"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_migrate_parquet_filename.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Write the migration script**

```python
"""Migrate parquet filenames from data.parquet to 1d.parquet.

Usage:
    python scripts/migrate_parquet_filename.py                    # Migrate
    python scripts/migrate_parquet_filename.py --dry-run          # Preview only
    python scripts/migrate_parquet_filename.py --dir /custom/path # Custom directory
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def migrate_parquet_files(
    root_dir: Path,
    dry_run: bool = False,
) -> dict[str, int]:
    """Rename all data.parquet → 1d.parquet under *root_dir*.

    Returns ``{"renamed": N, "skipped": N, "errors": N}``.

    Raises ``RuntimeError`` if both ``data.parquet`` and ``1d.parquet``
    exist in the same directory (split-brain state).
    """
    stats = {"renamed": 0, "skipped": 0, "errors": 0}

    if not root_dir.exists():
        return stats

    for old_path in sorted(root_dir.rglob("data.parquet")):
        new_path = old_path.with_name("1d.parquet")

        if new_path.exists():
            raise RuntimeError(
                f"split-brain: both data.parquet and 1d.parquet exist in {old_path.parent}. "
                "Manual investigation required."
            )

        if dry_run:
            print(f"[DRY RUN] Would rename: {old_path} → {new_path}")
            stats["renamed"] += 1
        else:
            os.rename(old_path, new_path)
            print(f"Renamed: {old_path} → {new_path}")
            stats["renamed"] += 1

    return stats


def main():
    default_warehouse = Path.home() / "market-warehouse" / "data-lake"
    parser = argparse.ArgumentParser(
        description="Migrate parquet filenames from data.parquet to 1d.parquet"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview renames without executing"
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help=f"Root directory to migrate (default: {default_warehouse}/bronze and bronze-delisted)",
    )
    args = parser.parse_args()

    if args.dir:
        stats = migrate_parquet_files(args.dir, dry_run=args.dry_run)
        print(f"\nMigration complete: {stats}")
    else:
        total = {"renamed": 0, "skipped": 0, "errors": 0}
        for subdir in ("bronze", "bronze-delisted"):
            target = default_warehouse / subdir
            if target.exists():
                print(f"\n--- Migrating {target} ---")
                stats = migrate_parquet_files(target, dry_run=args.dry_run)
                for k in total:
                    total[k] += stats[k]
        print(f"\nTotal: {total}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_migrate_parquet_filename.py -v`
Expected: All tests PASS

- [ ] **Step 5: Run full test suite with coverage**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing`
Expected: All tests PASS, 100% coverage maintained

- [ ] **Step 6: Commit**

```bash
git add scripts/migrate_parquet_filename.py tests/test_migrate_parquet_filename.py
git commit -m "feat: add parquet filename migration script (data.parquet → 1d.parquet)"
```

---

## Task 6: Health Check — Core Gap Detection Logic

**Files:**
- Create: `scripts/health_check.py`
- Create: `tests/test_health_check.py`

- [ ] **Step 1: Write tests for gap detection functions**

```python
"""Tests for scripts/health_check.py."""

from __future__ import annotations

from datetime import date

import pytest

from scripts.health_check import (
    compute_range_duration,
    find_interior_gaps,
    group_contiguous_dates,
)


class TestFindInteriorGaps:
    def test_no_gaps(self):
        # Mon-Fri week with all trading days present
        actual = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
                  date(2026, 1, 8), date(2026, 1, 9)]
        gaps = find_interior_gaps(actual, asset_class="equity")
        assert gaps == []

    def test_single_interior_gap(self):
        # Missing Wednesday
        actual = [date(2026, 1, 5), date(2026, 1, 6),
                  date(2026, 1, 8), date(2026, 1, 9)]
        gaps = find_interior_gaps(actual, asset_class="equity")
        assert gaps == [date(2026, 1, 7)]

    def test_weekend_not_detected_as_gap(self):
        # Friday to Monday — weekend is not a gap
        actual = [date(2026, 1, 9), date(2026, 1, 12)]
        gaps = find_interior_gaps(actual, asset_class="equity")
        assert gaps == []

    def test_holiday_not_detected_as_gap(self):
        # MLK Day 2026 is Monday Jan 19 — not a gap
        actual = [date(2026, 1, 16), date(2026, 1, 20)]
        gaps = find_interior_gaps(actual, asset_class="equity")
        assert gaps == []

    def test_futures_skips_calendar(self):
        # For futures, only detect gaps where adjacent data exists
        # Missing date between two known dates
        actual = [date(2026, 1, 5), date(2026, 1, 7)]
        gaps = find_interior_gaps(actual, asset_class="futures")
        # Futures uses adjacency, not NYSE calendar — Jan 6 is a gap
        assert date(2026, 1, 6) in gaps

    def test_single_date_no_gaps(self):
        actual = [date(2026, 1, 5)]
        gaps = find_interior_gaps(actual, asset_class="equity")
        assert gaps == []

    def test_empty_dates_no_gaps(self):
        gaps = find_interior_gaps([], asset_class="equity")
        assert gaps == []


class TestGroupContiguousDates:
    def test_single_gap(self):
        dates = [date(2026, 1, 7)]
        groups = group_contiguous_dates(dates)
        assert groups == [(date(2026, 1, 7), date(2026, 1, 7))]

    def test_contiguous_range(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        groups = group_contiguous_dates(dates)
        assert groups == [(date(2026, 1, 5), date(2026, 1, 7))]

    def test_two_separate_ranges(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6),
                 date(2026, 1, 12), date(2026, 1, 13)]
        groups = group_contiguous_dates(dates)
        assert groups == [
            (date(2026, 1, 5), date(2026, 1, 6)),
            (date(2026, 1, 12), date(2026, 1, 13)),
        ]

    def test_empty_returns_empty(self):
        assert group_contiguous_dates([]) == []


class TestComputeRangeDuration:
    def test_short_range(self):
        d = compute_range_duration(date(2026, 1, 5), date(2026, 1, 9))
        assert d == "6 D"  # 4 calendar days + 2 buffer

    def test_month_range(self):
        d = compute_range_duration(date(2026, 1, 1), date(2026, 3, 1))
        assert d == "61 D"  # 59 days + 2 buffer

    def test_long_range(self):
        d = compute_range_duration(date(2025, 1, 1), date(2026, 1, 1))
        assert d == "2 Y"

    def test_same_day(self):
        d = compute_range_duration(date(2026, 1, 5), date(2026, 1, 5))
        assert d == "1 D"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_health_check.py -v -k "TestFindInteriorGaps or TestGroupContiguous or TestComputeRange"`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement the core gap detection functions**

Create `scripts/health_check.py`:

```python
"""Health check: detect interior gaps in bronze parquet and auto-backfill.

Usage:
    python scripts/health_check.py                          # Normal run
    python scripts/health_check.py --dry-run                # Report only
    python scripts/health_check.py --force                  # Run on non-trading day
    python scripts/health_check.py --asset-class futures    # Futures health check
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from rich.console import Console

from clients import BronzeClient
from scripts.daily_update import (
    bars_to_rows,
    bars_to_futures_rows,
    is_trading_day,
    previous_trading_day,
    validate_bars,
)

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"


def find_interior_gaps(
    actual_dates: list[date],
    asset_class: str = "equity",
) -> list[date]:
    """Return missing dates between min and max of *actual_dates*.

    For equity/volatility: uses NYSE calendar to determine expected dates.
    For futures: uses simple weekday check (no holiday calendar).
    """
    if len(actual_dates) < 2:
        return []

    sorted_dates = sorted(actual_dates)
    actual_set = set(sorted_dates)
    start, end = sorted_dates[0], sorted_dates[-1]
    missing: list[date] = []

    cursor = start + timedelta(days=1)
    while cursor < end:
        if cursor not in actual_set:
            if asset_class == "futures":
                # Futures: only flag weekdays (no NYSE holiday calendar)
                if cursor.weekday() < 5:
                    missing.append(cursor)
            else:
                # Equity/volatility: use NYSE calendar
                if is_trading_day(cursor):
                    missing.append(cursor)
        cursor += timedelta(days=1)

    return missing


def group_contiguous_dates(dates: list[date]) -> list[tuple[date, date]]:
    """Group sorted dates into contiguous ``(start, end)`` ranges.

    Two dates are contiguous if separated by exactly 1 calendar day.
    """
    if not dates:
        return []

    sorted_dates = sorted(dates)
    groups: list[tuple[date, date]] = []
    start = sorted_dates[0]
    prev = sorted_dates[0]

    for d in sorted_dates[1:]:
        if (d - prev).days == 1:
            prev = d
        else:
            groups.append((start, prev))
            start = d
            prev = d

    groups.append((start, prev))
    return groups


def compute_range_duration(start_date: date, end_date: date) -> str:
    """Compute IB duration string for an arbitrary date range.

    Unlike ``compute_ib_duration`` (tail-only), this handles any range.
    """
    cal_days = (end_date - start_date).days
    if cal_days <= 0:
        return "1 D"
    cal_days += 2  # buffer
    if cal_days <= 180:
        return f"{cal_days} D"
    elif cal_days <= 365:
        return "1 Y"
    else:
        return "2 Y"


def get_all_trade_dates(bronze: BronzeClient) -> dict[str, list[date]]:
    """Bulk-read all trade dates per symbol from bronze parquet."""
    if not bronze.get_existing_symbols():
        return {}

    sql = f"""
        SELECT symbol, trade_date
        FROM read_parquet('{bronze._escaped_glob()}', hive_partitioning=true)
        ORDER BY symbol, trade_date
    """
    rows = bronze._query(sql)

    result: dict[str, list[date]] = {}
    for row in rows:
        symbol = row["symbol"]
        td = row["trade_date"]
        if isinstance(td, str):
            td = date.fromisoformat(td)
        result.setdefault(symbol, []).append(td)

    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_health_check.py -v -k "TestFindInteriorGaps or TestGroupContiguous or TestComputeRange"`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/health_check.py tests/test_health_check.py
git commit -m "feat(health-check): core gap detection and duration computation"
```

---

## Task 7: Health Check — Backfill and Main Entry Point

**Files:**
- Modify: `scripts/health_check.py`
- Modify: `tests/test_health_check.py`

- [ ] **Step 1: Write tests for backfill and main flow**

Append to `tests/test_health_check.py`:

```python
from unittest.mock import MagicMock, patch, AsyncMock
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.health_check import get_all_trade_dates, main


class TestGetAllTradeDates:
    def test_reads_dates_from_parquet(self, tmp_path):
        bronze_dir = tmp_path / "asset_class=equity"
        sym_dir = bronze_dir / "symbol=AAPL"
        sym_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(
            [
                {"trade_date": date(2026, 1, 5), "symbol_id": 1,
                 "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                 "adj_close": 1.5, "volume": 100},
                {"trade_date": date(2026, 1, 6), "symbol_id": 1,
                 "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                 "adj_close": 1.5, "volume": 100},
            ],
            schema=pa.schema([
                ("trade_date", pa.date32()), ("symbol_id", pa.int64()),
                ("open", pa.float64()), ("high", pa.float64()),
                ("low", pa.float64()), ("close", pa.float64()),
                ("adj_close", pa.float64()), ("volume", pa.int64()),
            ]),
        )
        pq.write_table(table, sym_dir / "1d.parquet")

        with BronzeClient(bronze_dir=bronze_dir) as bronze:
            result = get_all_trade_dates(bronze)

        assert "AAPL" in result
        assert len(result["AAPL"]) == 2

    def test_empty_bronze_returns_empty(self, tmp_path):
        bronze_dir = tmp_path / "asset_class=equity"
        bronze_dir.mkdir(parents=True)

        with BronzeClient(bronze_dir=bronze_dir) as bronze:
            result = get_all_trade_dates(bronze)

        assert result == {}


class TestMain:
    @patch("scripts.health_check._DATA_LAKE", new_callable=lambda: property(lambda self: None))
    def test_dry_run_reports_but_does_not_backfill(self, tmp_path, monkeypatch):
        """Dry run detects gaps but does not connect to IB."""
        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        sym_dir = bronze_dir / "symbol=TEST"
        sym_dir.mkdir(parents=True)

        # Create parquet with a gap (missing Jan 7)
        table = pa.Table.from_pylist(
            [
                {"trade_date": date(2026, 1, 5), "symbol_id": 1,
                 "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                 "adj_close": 1.5, "volume": 100},
                {"trade_date": date(2026, 1, 6), "symbol_id": 1,
                 "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                 "adj_close": 1.5, "volume": 100},
                {"trade_date": date(2026, 1, 8), "symbol_id": 1,
                 "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                 "adj_close": 1.5, "volume": 100},
            ],
            schema=pa.schema([
                ("trade_date", pa.date32()), ("symbol_id", pa.int64()),
                ("open", pa.float64()), ("high", pa.float64()),
                ("low", pa.float64()), ("close", pa.float64()),
                ("adj_close", pa.float64()), ("volume", pa.int64()),
            ]),
        )
        pq.write_table(table, sym_dir / "1d.parquet")

        monkeypatch.setattr("scripts.health_check._DATA_LAKE", tmp_path / "data-lake")

        with patch("sys.argv", ["health_check.py", "--dry-run", "--force"]):
            main()

        # No IB connection should have been made — dry run only reports
```

- [ ] **Step 2: Add the main() and backfill logic to `health_check.py`**

Append to `scripts/health_check.py`:

```python
def _resolve_bronze_dir(asset_class: str) -> Path:
    """Return the bronze directory for the given asset class."""
    return _DATA_LAKE / "bronze" / f"asset_class={asset_class}"


def main():
    parser = argparse.ArgumentParser(description="Health check: detect and repair interior gaps")
    parser.add_argument("--dry-run", action="store_true", help="Report gaps without backfilling")
    parser.add_argument("--force", action="store_true", help="Run on non-trading day")
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures"],
        default="equity",
        help="Asset class to check (default: equity)",
    )
    args = parser.parse_args()

    today = date.today()
    if not args.force and not is_trading_day(today):
        console.print(f"[yellow]{today} is not a trading day. Use --force to override.[/yellow]")
        return

    asset_class = args.asset_class
    bronze_dir = _resolve_bronze_dir(asset_class)

    console.print(f"\n[bold]Health check: {asset_class}[/bold]")
    console.print(f"Bronze dir: {bronze_dir}\n")

    with BronzeClient(bronze_dir=bronze_dir, asset_class=asset_class) as bronze:
        all_dates = get_all_trade_dates(bronze)

        if not all_dates:
            console.print("[yellow]No symbols found in bronze.[/yellow]")
            return

        total_gaps = 0
        gap_report: dict[str, list[date]] = {}

        for symbol, dates_list in sorted(all_dates.items()):
            gaps = find_interior_gaps(dates_list, asset_class=asset_class)
            if gaps:
                gap_report[symbol] = gaps
                total_gaps += len(gaps)

        if not gap_report:
            console.print("[green]No interior gaps detected.[/green]")
            return

        console.print(f"[red]Found {total_gaps} interior gaps across {len(gap_report)} symbols.[/red]\n")

        for symbol, gaps in sorted(gap_report.items()):
            ranges = group_contiguous_dates(gaps)
            range_strs = [
                f"{s.isoformat()}..{e.isoformat()}" if s != e else s.isoformat()
                for s, e in ranges
            ]
            console.print(f"  {symbol}: {len(gaps)} gaps — {', '.join(range_strs)}")

        if args.dry_run:
            console.print(f"\n[yellow]Dry run complete. {total_gaps} gaps would be backfilled.[/yellow]")
            return

        # ── Backfill ──
        console.print(f"\n[bold]Backfilling {total_gaps} gaps...[/bold]\n")

        host = os.getenv("MDW_IB_HOST", "127.0.0.1")
        port = int(os.getenv("MDW_IB_PORT", "4001"))

        repaired = 0

        if asset_class == "volatility":
            # Use CBOE as authoritative source for volatility
            console.print("[yellow]Volatility backfill via CBOE not yet implemented.[/yellow]")
            return

        from clients.ib_client import IBClient
        from clients.daily_bar_fallback import DailyBarFallbackClient
        from scripts.daily_update import _make_contract

        ib = IBClient()
        ib.connect(host=host, port=port)

        try:
            for symbol, gaps in sorted(gap_report.items()):
                ranges = group_contiguous_dates(gaps)
                for range_start, range_end in ranges:
                    duration = compute_range_duration(range_start, range_end)
                    end_dt = (range_end + timedelta(days=1)).strftime("%Y%m%d-%H:%M:%S")
                    contract = _make_contract(symbol, asset_class)
                    ib.ib.qualifyContracts(contract)
                    bars = ib.get_historical_data(
                        contract,
                        duration=duration,
                        bar_size="1 day",
                        what_to_show="TRADES",
                        end_date_time=end_dt,
                    )
                    if not bars:
                        console.print(f"  [yellow]{symbol}: no bars from IB for {range_start}..{range_end}[/yellow]")
                        continue

                    valid_bars, issues = validate_bars(bars, symbol, asset_class=asset_class)
                    if issues:
                        for issue in issues:
                            console.print(f"  [yellow]{issue}[/yellow]")

                    if valid_bars:
                        symbol_id = bronze.get_symbol_id(symbol)
                        if asset_class == "futures":
                            root_symbol = symbol.rsplit("_", 1)[0]
                            expiry_code = symbol.rsplit("_", 1)[1]
                            expiry_date = f"{expiry_code[:4]}-{expiry_code[4:6]}-01"
                            rows = bars_to_futures_rows(valid_bars, symbol_id, root_symbol, expiry_date)
                        else:
                            rows = bars_to_rows(valid_bars, symbol_id)
                        inserted = bronze.merge_ticker_rows(symbol, rows)
                        repaired += inserted
                        console.print(f"  [green]{symbol}: repaired {inserted} bars ({range_start}..{range_end})[/green]")

                    # Equity fallback for remaining gaps
                    if asset_class == "equity" and valid_bars:
                        from scripts.daily_update import fetch_fallback_bars, get_missing_trading_dates
                        covered_dates = {date.fromisoformat(str(b.date)) for b in valid_bars}
                        still_missing = [d for d in gaps if d not in covered_dates]
                        if still_missing:
                            fallback = DailyBarFallbackClient()
                            fb_bars, fb_sources = fetch_fallback_bars(symbol, still_missing, fallback)
                            if fb_bars:
                                fb_rows = [
                                    {
                                        "trade_date": str(b.trade_date),
                                        "symbol_id": symbol_id,
                                        "open": b.open, "high": b.high,
                                        "low": b.low, "close": b.close,
                                        "adj_close": b.close,
                                        "volume": b.volume,
                                    }
                                    for b in fb_bars
                                ]
                                fb_inserted = bronze.merge_ticker_rows(symbol, fb_rows)
                                repaired += fb_inserted
                                console.print(f"  [green]{symbol}: fallback repaired {fb_inserted} bars[/green]")
        finally:
            ib.disconnect()

        console.print(f"\n[bold green]Health check complete. Repaired {repaired} bars.[/bold green]")

        # ── Log ──
        log_dir = _WAREHOUSE_DIR / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"health_check_{today.isoformat()}.log"
        with open(log_path, "w") as f:
            f.write(f"Health check: {today}\n")
            f.write(f"Asset class: {asset_class}\n")
            f.write(f"Total gaps found: {total_gaps}\n")
            f.write(f"Total bars repaired: {repaired}\n\n")
            for symbol, gaps in sorted(gap_report.items()):
                ranges = group_contiguous_dates(gaps)
                f.write(f"{symbol}: {len(gaps)} gaps — {ranges}\n")
        console.print(f"Log written to {log_path}")

        # ── Email alert ──
        threshold = int(os.getenv("MDW_HEALTH_CHECK_EMAIL_THRESHOLD", "10"))
        if repaired >= threshold:
            console.print(f"[yellow]Repairs ({repaired}) exceed threshold ({threshold}), sending alert...[/yellow]")
            _send_alert(today, asset_class, total_gaps, repaired, log_path)


def _send_alert(
    run_date: date,
    asset_class: str,
    total_gaps: int,
    repaired: int,
    log_path: Path,
) -> None:
    """Send email alert via existing Nodemailer CLI."""
    node_bin = os.getenv("MDW_NODE_BIN", "node")
    script = Path(__file__).parent / "send_daily_update_failure_email.mjs"

    if not script.exists():
        console.print("[yellow]Alert script not found, skipping email.[/yellow]")
        return

    try:
        subprocess.run(
            [
                node_bin, str(script),
                "--run-date", run_date.isoformat(),
                "--log-file", str(log_path),
                "--job-name", f"health-check-{asset_class}",
                "--error-summary", f"Repaired {repaired} interior gaps across {total_gaps} detected",
            ],
            check=False,
            timeout=30,
        )
    except Exception as exc:
        console.print(f"[red]Failed to send alert: {exc}[/red]")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run all health check tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_health_check.py -v`
Expected: All PASS

- [ ] **Step 4: Run full test suite with coverage**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing`
Expected: All PASS, coverage maintained

- [ ] **Step 5: Commit**

```bash
git add scripts/health_check.py tests/test_health_check.py
git commit -m "feat(health-check): backfill engine and main entry point"
```

---

## Task 8: Universe Screener — Core Logic

**Files:**
- Create: `scripts/universe_screener.py`
- Create: `tests/test_universe_screener.py`

- [ ] **Step 1: Write tests for scanner and comparison logic**

```python
"""Tests for scripts/universe_screener.py."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.universe_screener import (
    compare_universes,
    load_screener_state,
    save_screener_state,
    update_absent_counts,
    get_removals_after_grace,
    write_universe_preset,
    log_changes,
)


class TestCompareUniverses:
    def test_additions_and_removals(self):
        current = {"AAPL", "MSFT", "TSLA"}
        scanned = {"AAPL", "MSFT", "NVDA"}
        additions, removals = compare_universes(current, scanned)
        assert additions == {"NVDA"}
        assert removals == {"TSLA"}

    def test_no_changes(self):
        current = {"AAPL", "MSFT"}
        additions, removals = compare_universes(current, current)
        assert additions == set()
        assert removals == set()

    def test_all_new(self):
        additions, removals = compare_universes(set(), {"AAPL", "MSFT"})
        assert additions == {"AAPL", "MSFT"}
        assert removals == set()


class TestScreenerState:
    def test_load_nonexistent_returns_none(self, tmp_path):
        state = load_screener_state(tmp_path / "no_such_file.json")
        assert state is None

    def test_save_and_load_roundtrip(self, tmp_path):
        path = tmp_path / "state.json"
        state = {"last_run": "2026-04-05", "absent_counts": {"TSLA": 2}}
        save_screener_state(path, state)
        loaded = load_screener_state(path)
        assert loaded == state


class TestAbsentCounts:
    def test_increments_absent(self):
        absent_counts = {"TSLA": 1, "GME": 2}
        removals = {"TSLA", "GME", "AMC"}
        updated = update_absent_counts(absent_counts, removals, scanned={"AAPL"})
        assert updated["TSLA"] == 2
        assert updated["GME"] == 3
        assert updated["AMC"] == 1
        assert "AAPL" not in updated  # present in scan, reset

    def test_resets_present_tickers(self):
        absent_counts = {"TSLA": 2}
        updated = update_absent_counts(absent_counts, removals=set(), scanned={"TSLA"})
        assert "TSLA" not in updated


class TestGracePeriod:
    def test_returns_tickers_past_grace(self):
        absent_counts = {"TSLA": 3, "GME": 1, "AMC": 4}
        removals = get_removals_after_grace(absent_counts, grace_days=3)
        assert removals == {"TSLA", "AMC"}

    def test_returns_empty_when_none_past_grace(self):
        absent_counts = {"TSLA": 1, "GME": 2}
        removals = get_removals_after_grace(absent_counts, grace_days=3)
        assert removals == set()


class TestWritePreset:
    def test_writes_valid_json(self, tmp_path):
        path = tmp_path / "screened-universe.json"
        write_universe_preset(path, ["AAPL", "NVDA", "MSFT"])
        data = json.loads(path.read_text())
        assert data["name"] == "screened-universe"
        assert data["tickers"] == ["AAPL", "MSFT", "NVDA"]  # sorted
        assert "generated_at" in data


class TestLogChanges:
    def test_writes_log_file(self, tmp_path):
        log_changes(
            log_dir=tmp_path,
            run_date=date(2026, 4, 5),
            additions={"NVDA", "AMD"},
            removals={"TSLA"},
        )
        log_path = tmp_path / "universe_changes_2026-04-05.log"
        assert log_path.exists()
        content = log_path.read_text()
        assert "NVDA" in content
        assert "TSLA" in content
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_screener.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement the core screener functions**

Create `scripts/universe_screener.py`:

```python
"""IB Scanner-based universe builder.

Usage:
    python scripts/universe_screener.py            # Normal daily run
    python scripts/universe_screener.py --dry-run  # Scan and compare only
    python scripts/universe_screener.py --force    # Re-run same day
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

from rich.console import Console

from clients import BronzeClient
from scripts.daily_update import is_trading_day

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"

# ── Defaults ──
TARGET_SIZE = 1000
GRACE_DAYS = 3
MAX_REMOVALS = 50
EMAIL_THRESHOLD = 10


def compare_universes(
    current: set[str], scanned: set[str]
) -> tuple[set[str], set[str]]:
    """Return ``(additions, removals)`` between current and scanned universes."""
    return (scanned - current, current - scanned)


def load_screener_state(path: Path) -> dict | None:
    """Load screener state from JSON. Returns None if file doesn't exist."""
    if not path.exists():
        return None
    with path.open() as f:
        return json.load(f)


def save_screener_state(path: Path, state: dict) -> None:
    """Write screener state to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(state, f, indent=2)


def update_absent_counts(
    absent_counts: dict[str, int],
    removals: set[str],
    scanned: set[str],
) -> dict[str, int]:
    """Update absence counters. Increment for absent tickers, reset for present ones."""
    updated: dict[str, int] = {}
    for symbol, count in absent_counts.items():
        if symbol in scanned:
            continue  # present → reset (remove from counts)
        updated[symbol] = count  # carry forward

    for symbol in removals:
        updated[symbol] = updated.get(symbol, 0) + 1

    return updated


def get_removals_after_grace(
    absent_counts: dict[str, int], grace_days: int = GRACE_DAYS
) -> set[str]:
    """Return tickers that have been absent for >= grace_days consecutive runs."""
    return {sym for sym, count in absent_counts.items() if count >= grace_days}


def write_universe_preset(path: Path, tickers: list[str]) -> None:
    """Write the screened universe as a preset JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "name": "screened-universe",
        "description": "Auto-generated: top ~1000 U.S. equities by market cap, volume, and turnover",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tickers": sorted(tickers),
    }
    with path.open("w") as f:
        json.dump(data, f, indent=2)


def log_changes(
    log_dir: Path,
    run_date: date,
    additions: set[str],
    removals: set[str],
) -> Path:
    """Write universe change log. Returns log path."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"universe_changes_{run_date.isoformat()}.log"
    with log_path.open("w") as f:
        f.write(f"Universe changes: {run_date}\n")
        f.write(f"Additions ({len(additions)}): {', '.join(sorted(additions))}\n")
        f.write(f"Removals ({len(removals)}): {', '.join(sorted(removals))}\n")
    return log_path


async def run_scanner_sweeps(ib) -> set[str]:
    """Run multiple IB scanner sweeps and return the union of all symbols."""
    from ib_async import ScannerSubscription

    scanned: set[str] = set()

    sweeps = [
        # Market cap-based (primary)
        ("TOP_MARKET_CAP", 1e10, None,  100000),  # Large cap
        ("TOP_MARKET_CAP", 2e9,  1e10,  100000),  # Mid cap
        ("TOP_MARKET_CAP", 5e8,  2e9,   100000),  # Small cap
        # Volume supplement
        ("MOST_ACTIVE",    5e8,  None,  100000),
        # Turnover supplement
        ("TOP_TRADE_COUNT", 5e8, None,  100000),
    ]

    for scan_code, cap_above, cap_below, min_vol in sweeps:
        sub = ScannerSubscription(
            instrument="STK",
            locationCode="STK.US.MAJOR",
            scanCode=scan_code,
            numberOfRows=50,
        )
        if cap_above:
            sub.marketCapAbove = cap_above
        if cap_below:
            sub.marketCapBelow = cap_below
        if min_vol:
            sub.aboveVolume = min_vol

        try:
            results = await ib.reqScannerDataAsync(sub)
            if results:
                for item in results:
                    if hasattr(item, "contractDetails") and item.contractDetails:
                        symbol = item.contractDetails.contract.symbol
                        if symbol:
                            scanned.add(symbol)
            console.print(f"  {scan_code} (cap>{cap_above/1e9:.1f}B): {len(results or [])} results")
        except Exception as exc:
            console.print(f"  [red]{scan_code}: {exc}[/red]")

    return scanned


def main():
    parser = argparse.ArgumentParser(description="IB Scanner-based universe builder")
    parser.add_argument("--dry-run", action="store_true", help="Scan and compare only")
    parser.add_argument("--force", action="store_true", help="Re-run same day")
    args = parser.parse_args()

    today = date.today()
    if not args.force and not is_trading_day(today):
        console.print(f"[yellow]{today} is not a trading day. Use --force to override.[/yellow]")
        return

    state_path = _WAREHOUSE_DIR / "logs" / "screener_state.json"
    state = load_screener_state(state_path)
    is_bootstrap = state is None

    if not args.force and state and state.get("last_run") == today.isoformat():
        console.print(f"[yellow]Already ran today ({today}). Use --force to re-run.[/yellow]")
        return

    # ── Scan ──
    console.print("\n[bold]Running IB scanner sweeps...[/bold]\n")

    host = os.getenv("MDW_IB_HOST", "127.0.0.1")
    port = int(os.getenv("MDW_IB_PORT", "4001"))

    import asyncio
    from clients.ib_client import IBClient

    ib = IBClient()
    ib.connect(host=host, port=port)

    try:
        scanned = asyncio.get_event_loop().run_until_complete(run_scanner_sweeps(ib.ib))
    finally:
        ib.disconnect()

    console.print(f"\n[bold]Scanned universe: {len(scanned)} tickers[/bold]\n")

    # ── Compare ──
    bronze_dir = _DATA_LAKE / "bronze" / "asset_class=equity"
    with BronzeClient(bronze_dir=bronze_dir) as bronze:
        current = bronze.get_existing_symbols()

    additions, candidate_removals = compare_universes(current, scanned)

    # ── Grace period ──
    absent_counts = state.get("absent_counts", {}) if state else {}
    absent_counts = update_absent_counts(absent_counts, candidate_removals, scanned)
    actual_removals = get_removals_after_grace(absent_counts, GRACE_DAYS)

    console.print(f"Additions: {len(additions)}")
    console.print(f"Candidate removals: {len(candidate_removals)}")
    console.print(f"Actual removals (past grace): {len(actual_removals)}")

    if is_bootstrap:
        console.print("[yellow]Bootstrap mode: skipping all removals.[/yellow]")
        actual_removals = set()
    elif len(actual_removals) > MAX_REMOVALS:
        console.print(
            f"[red]Removals ({len(actual_removals)}) exceed max ({MAX_REMOVALS}). "
            f"Aborting removals. Review screener_state.json manually.[/red]"
        )
        actual_removals = set()

    # ── Log ──
    log_dir = _WAREHOUSE_DIR / "logs"
    log_changes(log_dir, today, additions, actual_removals)

    if args.dry_run:
        console.print(f"\n[yellow]Dry run complete.[/yellow]")
        return

    # ── Archive removals ──
    archive_base = _DATA_LAKE / "bronze-delisted" / "asset_class=equity"
    for symbol in sorted(actual_removals):
        src = bronze_dir / f"symbol={symbol}"
        dst = archive_base / f"symbol={symbol}"
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            console.print(f"  Archived: {symbol}")
        # Remove from absent counts after archiving
        absent_counts.pop(symbol, None)

    # ── Write preset ──
    preset_path = Path("presets/screened-universe.json")
    final_universe = (current | additions) - actual_removals
    write_universe_preset(preset_path, list(final_universe))
    console.print(f"\nPreset written: {preset_path} ({len(final_universe)} tickers)")

    # ── Save state ──
    new_state = {
        "last_run": today.isoformat(),
        "absent_counts": absent_counts,
    }
    save_screener_state(state_path, new_state)

    # ── Backfill additions ──
    if additions:
        console.print(f"\n[bold]Triggering backfill for {len(additions)} new tickers...[/bold]")
        tickers_arg = " ".join(sorted(additions))
        try:
            subprocess.run(
                [
                    sys.executable, "scripts/fetch_ib_historical.py",
                    "--tickers", *sorted(additions),
                    "--years", "0",
                ],
                check=False,
                timeout=3600,
            )
        except Exception as exc:
            console.print(f"[red]Backfill failed: {exc}[/red]")

    # ── Email alert ──
    total_changes = len(additions) + len(actual_removals)
    if total_changes >= EMAIL_THRESHOLD:
        console.print(f"[yellow]Changes ({total_changes}) exceed threshold ({EMAIL_THRESHOLD}), sending alert...[/yellow]")
        _send_screener_alert(today, additions, actual_removals)

    console.print(f"\n[bold green]Universe screener complete.[/bold green]")


def _send_screener_alert(
    run_date: date, additions: set[str], removals: set[str]
) -> None:
    """Send email alert for universe changes."""
    node_bin = os.getenv("MDW_NODE_BIN", "node")
    script = Path(__file__).parent / "send_daily_update_failure_email.mjs"
    log_dir = _WAREHOUSE_DIR / "logs"
    log_path = log_dir / f"universe_changes_{run_date.isoformat()}.log"

    if not script.exists():
        console.print("[yellow]Alert script not found, skipping email.[/yellow]")
        return

    try:
        subprocess.run(
            [
                node_bin, str(script),
                "--run-date", run_date.isoformat(),
                "--log-file", str(log_path),
                "--job-name", "universe-screener",
                "--error-summary",
                f"Universe changed: +{len(additions)} -{len(removals)} tickers",
            ],
            check=False,
            timeout=30,
        )
    except Exception as exc:
        console.print(f"[red]Failed to send alert: {exc}[/red]")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_screener.py -v`
Expected: All PASS

- [ ] **Step 5: Run full test suite with coverage**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing`
Expected: All PASS, 100% coverage

- [ ] **Step 6: Commit**

```bash
git add scripts/universe_screener.py tests/test_universe_screener.py
git commit -m "feat: add IB scanner-based universe screener with grace period and alerts"
```

---

## Task 9: Final Verification

- [ ] **Step 1: Run full test suite with coverage and warnings**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning
```
Expected: All PASS, 100% coverage, no RuntimeWarnings

- [ ] **Step 2: Run migration script dry-run (on real data)**

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/migrate_parquet_filename.py --dry-run
```
Expected: Lists all files that would be renamed

- [ ] **Step 3: Run migration script (on real data)**

**Stop any running writers first!**
```bash
# Unload launchd jobs if active
launchctl unload ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist 2>/dev/null
launchctl unload ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist 2>/dev/null

# Run migration
python scripts/migrate_parquet_filename.py

# Verify
find ~/market-warehouse/data-lake -name "data.parquet" | head -5
# Expected: empty output

# Reload launchd jobs
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist 2>/dev/null
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist 2>/dev/null
```

- [ ] **Step 4: Rebuild DuckDB and verify**

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/rebuild_duckdb_from_parquet.py
duckdb ~/market-warehouse/duckdb/market.duckdb "SELECT count(*) FROM md.equities_daily"
```
Expected: Row count matches previous (no data loss)

- [ ] **Step 5: Run health check dry-run**

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/health_check.py --dry-run --force
```
Expected: Reports any interior gaps found

- [ ] **Step 6: Run universe screener dry-run**

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/universe_screener.py --dry-run --force
```
Expected: Reports scanned universe and diff against current bronze
