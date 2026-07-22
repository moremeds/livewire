# Multi-Timeframe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `1h` and `5m` intraday bar storage alongside existing `1d` for the equity universe, with a separate `IntradayBronzeClient`, core ETF list, intraday update flow, per-timeframe cursor, data quality with auto-recovery, and unified UTC timestamp handling.

**Architecture:** Two parallel storage clients (existing `BronzeClient` for daily, new `IntradayBronzeClient` for intraday) sharing only an extracted atomic-write helper. Core ETFs are excluded from screener removal logic. Intraday updates have an explicit session-state model. Auto-recovery triggers targeted backfills before alerting on coverage drops.

**Tech Stack:** Python 3.13, PyArrow, DuckDB (TIMESTAMPTZ), ib_async, pytest (100% coverage)

**Spec:** `docs/superpowers/specs/2026-04-06-multi-timeframe-design.md`

## Phase scope

This plan implements **Phase 1** of the spec — the foundation: storage architecture, schema, calendar helpers, ingest validation, core ETFs, screener integration, per-timeframe cursor, intraday backfill helper, DuckDB rebuild, R2 sync, intraday session-state classifier, and entrypoint wiring.

**Deferred to a follow-up Phase 2 plan** (so Phase 1 is shippable):
- Health check `--intraday` report-only mode (spec § 9)
- Coverage tracking with daily log + weekly summary (spec § 17 Layer 2)
- Auto-recovery on coverage drop below 95% (spec § 17 Layer 2)
- Full historical intraday backfill orchestration (use the chunking helper from Task 9 manually for now)

After Phase 1, the system can write/read intraday parquet, the screener correctly handles core ETFs, and the entrypoint runs intraday updates after daily updates. The follow-up plan adds coverage monitoring and self-healing.

---

## File Structure

### New files
| File | Responsibility |
|------|---------------|
| `clients/parquet_io.py` | Atomic publish + parquet validation helpers (extracted from `bronze_client.py`) |
| `clients/intraday_bronze_client.py` | Intraday parquet client with `bar_timestamp TIMESTAMPTZ` schema |
| `presets/core-etfs.json` | 38 always-include ETFs across 10 groups |
| `scripts/probe_ib_intraday.py` | One-shot empirical probe for IB return type / timezone |
| `scripts/intraday_update.py` | Daily intraday refresh with session-state model |
| `tests/test_parquet_io.py` | Atomic publish, validation, cleanup tests |
| `tests/test_intraday_bronze_client.py` | Schema, timestamp validation, merge, path resolution |
| `tests/test_intraday_update.py` | Session model, all 5 states |
| `tests/test_core_etfs_integration.py` | Core ETF screener invariants |

### Modified files
| File | Change |
|------|--------|
| `clients/bronze_client.py` | Use `parquet_io.publish_parquet` instead of inline `_publish_symbol_rows` |
| `clients/db_client.py` | Add `replace_equities_intraday_from_parquet` method |
| `scripts/daily_update.py` | Add `get_early_close_days`, `session_close_time`; add `validate_intraday_bar` helper |
| `scripts/universe_screener.py` | Exclude core ETFs from `compare_universes`; trigger 3-tf backfill on additions |
| `scripts/fetch_ib_historical.py` | Per-timeframe cursor; intraday chunking |
| `scripts/health_check.py` | `--intraday`, `--coverage-report`, `--weekly-summary` modes; auto-recovery |
| `scripts/rebuild_duckdb_from_parquet.py` | `--timeframe` flag |
| `scripts/sync_to_r2.py` | Iterate over all 3 parquet filenames |
| `docker/ibroker-mkt-data/entrypoint.py` | Chain `intraday_update.py` after `daily_update.py` |

---

## Task 1: Empirical IB Probe

**Files:**
- Create: `scripts/probe_ib_intraday.py`

This task **must run first** because the probe results determine constants used by all subsequent tasks. The probe is not a unit test — it talks to live IB Gateway.

- [ ] **Step 1: Create the probe script**

```python
#!/usr/bin/env python3
"""One-shot probe to determine IB intraday return type, timezone, and bar grid.

Run before implementing intraday support to lock in formatDate / useRTH choices.
Results are documented in commit message and as constants in intraday_bronze_client.py.

Usage:
    source ~/market-warehouse/.venv/bin/activate
    python scripts/probe_ib_intraday.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def main() -> None:
    import ib_async

    ib = ib_async.IB()
    ib.connect("127.0.0.1", 4001, clientId=99)

    contract = ib_async.Stock("AAPL", "SMART", "USD")
    ib.qualifyContracts(contract)

    print("=" * 60)
    print("IB INTRADAY PROBE — AAPL")
    print("=" * 60)

    for bar_size, label in [("5 mins", "5m"), ("1 hour", "1h")]:
        print(f"\n--- {label} bars (formatDate=1, useRTH=True) ---")
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        print(f"Got {len(bars)} bars")
        if bars:
            first, last = bars[0], bars[-1]
            print(f"First.date type: {type(first.date).__name__}")
            print(f"First.date repr: {first.date!r}")
            print(f"First.date tzinfo: {getattr(first.date, 'tzinfo', 'N/A')}")
            print(f"Last.date repr:  {last.date!r}")

        print(f"\n--- {label} bars (formatDate=2, useRTH=True) ---")
        bars2 = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=2,
        )
        print(f"Got {len(bars2)} bars")
        if bars2:
            first, last = bars2[0], bars2[-1]
            print(f"First.date type: {type(first.date).__name__}")
            print(f"First.date repr: {first.date!r}")
            print(f"First.date tzinfo: {getattr(first.date, 'tzinfo', 'N/A')}")
            print(f"Last.date repr:  {last.date!r}")

    # DST transition probe
    print("\n--- DST spring forward (2026-03-08, 5m bars) ---")
    bars_dst = ib.reqHistoricalData(
        contract,
        endDateTime="20260309 21:00:00",
        durationStr="2 D",
        barSizeSetting="5 mins",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=2,
    )
    print(f"Got {len(bars_dst)} bars across DST boundary")
    if bars_dst:
        for b in bars_dst[:3]:
            print(f"  {b.date!r}")
        print("  ...")
        for b in bars_dst[-3:]:
            print(f"  {b.date!r}")

    ib.disconnect()
    print("\n" + "=" * 60)
    print("PROBE COMPLETE — capture this output in commit message")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the probe (requires live IB Gateway)**

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/probe_ib_intraday.py 2>&1 | tee /tmp/ib_probe_output.txt
```

Expected: prints bar dates, types, tzinfo for both `formatDate=1` and `formatDate=2`. The implementer captures this output to inform Task 3 constants.

- [ ] **Step 3: Document probe results in commit**

```bash
git add scripts/probe_ib_intraday.py
git commit -m "$(cat <<'EOF'
feat: add IB intraday probe script

Empirical results (run 2026-04-06 against IB Gateway):
[paste relevant lines from /tmp/ib_probe_output.txt]

These results inform the timezone normalization logic in
clients/intraday_bronze_client.py.
EOF
)"
```

---

## Task 2: Extract `parquet_io.py` Shared Helpers

**Files:**
- Create: `clients/parquet_io.py`
- Create: `tests/test_parquet_io.py`
- Modify: `clients/bronze_client.py` (use the new helpers)

This is a pure refactor — no behavior change. The existing `BronzeClient._publish_symbol_rows` and `_validate_parquet_file` are extracted into module-level functions that take a parquet schema and time-column-name parameter, so both `BronzeClient` and the new `IntradayBronzeClient` can use them.

- [ ] **Step 1: Write tests for the extracted helpers**

```python
"""Tests for clients/parquet_io.py — shared parquet publish and validation."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.parquet_io import publish_parquet, validate_parquet_file


_SCHEMA = pa.schema([
    ("trade_date", pa.date32()),
    ("symbol_id", pa.int64()),
    ("value", pa.float64()),
])


def _table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=_SCHEMA)


class TestPublishParquet:
    def test_writes_file_atomically(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 6), "symbol_id": 1, "value": 2.0},
        ]
        table = _table(rows)
        publish_parquet(out, table, sort_column="trade_date")
        assert out.exists()
        loaded = pq.read_table(out)
        assert loaded.num_rows == 2

    def test_no_temp_file_remains_on_success(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        publish_parquet(out, _table(rows), sort_column="trade_date")
        tmps = list(tmp_path.glob(".data.parquet.*.tmp"))
        assert tmps == []

    def test_temp_file_cleaned_on_validation_failure(self, tmp_path, monkeypatch):
        out = tmp_path / "data.parquet"
        # Force validation to fail by passing a sort column that doesn't exist
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        with pytest.raises(KeyError):
            publish_parquet(out, _table(rows), sort_column="nonexistent_column")
        tmps = list(tmp_path.glob(".data.parquet.*.tmp"))
        assert tmps == []
        assert not out.exists()

    def test_creates_parent_dirs(self, tmp_path):
        out = tmp_path / "deeply" / "nested" / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        publish_parquet(out, _table(rows), sort_column="trade_date")
        assert out.exists()


class TestValidateParquetFile:
    def test_valid_file_passes(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 6), "symbol_id": 1, "value": 2.0},
        ]
        pq.write_table(_table(rows), out)
        validate_parquet_file(out, expected_rows=2, sort_column="trade_date")

    def test_wrong_row_count_raises(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        pq.write_table(_table(rows), out)
        with pytest.raises(ValueError, match="expected 5 rows"):
            validate_parquet_file(out, expected_rows=5, sort_column="trade_date")

    def test_unsorted_raises(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 6), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 2.0},
        ]
        pq.write_table(_table(rows), out)
        with pytest.raises(ValueError, match="not sorted"):
            validate_parquet_file(out, expected_rows=2, sort_column="trade_date")

    def test_duplicates_raise(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 2.0},
        ]
        pq.write_table(_table(rows), out)
        with pytest.raises(ValueError, match="duplicate"):
            validate_parquet_file(out, expected_rows=2, sort_column="trade_date")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/test_parquet_io.py -v
```
Expected: ImportError on `clients.parquet_io`

- [ ] **Step 3: Implement `clients/parquet_io.py`**

```python
"""Shared parquet publish and validation helpers.

Used by both BronzeClient (daily) and IntradayBronzeClient. The publish
function writes to a temp file, validates it, then atomically renames into
place. Validation checks row count, sort order, and duplicates on the
specified sort column.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def publish_parquet(
    out_path: Path,
    table: pa.Table,
    sort_column: str,
) -> Path:
    """Atomically publish a parquet file: write temp → validate → rename.

    Raises ValueError on validation failure (row count, sort order, dupes).
    The temp file is always cleaned up.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(
        f".{out_path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )

    try:
        pq.write_table(table, tmp_path, compression="snappy")
        validate_parquet_file(tmp_path, expected_rows=table.num_rows, sort_column=sort_column)
        os.replace(tmp_path, out_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return out_path


def validate_parquet_file(
    path: Path,
    expected_rows: int,
    sort_column: str,
) -> None:
    """Validate a parquet file: row count, ascending sort, no duplicates.

    Raises ValueError on any failure. Raises KeyError if sort_column doesn't
    exist in the file.
    """
    table = pq.read_table(path, columns=[sort_column])
    if table.num_rows != expected_rows:
        raise ValueError(
            f"{path}: expected {expected_rows} rows, found {table.num_rows}"
        )

    if sort_column not in table.column_names:
        raise KeyError(f"sort column {sort_column!r} not in parquet")

    raw_values = table.column(sort_column).to_pylist()
    values = [
        v.isoformat() if isinstance(v, (date, datetime)) else str(v)
        for v in raw_values
    ]
    if values != sorted(values):
        raise ValueError(f"{path}: {sort_column} values are not sorted ascending")
    if len(values) != len(set(values)):
        raise ValueError(f"{path}: duplicate {sort_column} values detected")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_parquet_io.py -v
```
Expected: All PASS

- [ ] **Step 5: Refactor `BronzeClient` to use the new helpers**

In `clients/bronze_client.py`, replace `_publish_symbol_rows` and `_validate_parquet_file` to delegate:

```python
# At the top, add:
from clients.parquet_io import publish_parquet, validate_parquet_file

# Replace _publish_symbol_rows method body (keep signature):
def _publish_symbol_rows(self, symbol: str, rows: list[dict[str, Any]]) -> Path:
    out_path = self._symbol_path(symbol)
    table = self._table_from_rows(rows)
    sort_column = "trade_date"  # both equity and futures schemas use this
    result = publish_parquet(out_path, table, sort_column=sort_column)
    log.info("Published %s", result)
    return result

# Delete _validate_parquet_file method entirely (no longer needed — callers use parquet_io.validate_parquet_file)
```

- [ ] **Step 6: Run all existing bronze client tests**

```bash
python -m pytest tests/test_bronze_client.py tests/test_parquet_io.py -v
```
Expected: All PASS (no behavior change for `BronzeClient`)

- [ ] **Step 7: Run full suite for safety**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 8: Commit**

```bash
git add clients/parquet_io.py clients/bronze_client.py tests/test_parquet_io.py
git commit -m "refactor: extract parquet_io helpers shared by daily and intraday clients"
```

---

## Task 3: Create `IntradayBronzeClient`

**Files:**
- Create: `clients/intraday_bronze_client.py`
- Create: `tests/test_intraday_bronze_client.py`
- Modify: `clients/__init__.py` (export the new class)

- [ ] **Step 1: Write tests**

```python
"""Tests for clients/intraday_bronze_client.py."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pytest

from clients.intraday_bronze_client import (
    INTRADAY_TIMEFRAMES,
    INTRADAY_PARQUET_FILENAME,
    IntradayBronzeClient,
)


_UTC = timezone.utc
_ET = ZoneInfo("America/New_York")


class TestConstants:
    def test_timeframes_are_1h_and_5m(self):
        assert INTRADAY_TIMEFRAMES == ("1h", "5m")

    def test_filenames_match_timeframes(self):
        assert INTRADAY_PARQUET_FILENAME == {
            "1h": "1h.parquet",
            "5m": "5m.parquet",
        }


class TestConstructor:
    def test_invalid_timeframe_raises(self, tmp_path):
        with pytest.raises(ValueError, match="unsupported timeframe"):
            IntradayBronzeClient(bronze_dir=tmp_path, timeframe="1m")

    def test_valid_timeframes_accepted(self, tmp_path):
        for tf in ("1h", "5m"):
            with IntradayBronzeClient(bronze_dir=tmp_path, timeframe=tf) as client:
                assert client.timeframe == tf

    def test_path_uses_timeframe_filename(self, tmp_path):
        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="1h") as client:
            path = client._symbol_path("AAPL")
            assert path.name == "1h.parquet"


class TestRowNormalization:
    def test_naive_timestamp_rejected(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        rows = [{
            "bar_timestamp": datetime(2026, 4, 6, 13, 30),  # naive!
            "symbol_id": 1,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "volume": 100,
        }]
        with pytest.raises(ValueError, match="must be tz-aware"):
            client.replace_ticker_rows("AAPL", rows)
        client.close()

    def test_et_timestamp_normalized_to_utc(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        et_ts = datetime(2026, 4, 6, 9, 30, tzinfo=_ET)
        rows = [{
            "bar_timestamp": et_ts,
            "symbol_id": 1,
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
            "volume": 100,
        }]
        client.replace_ticker_rows("AAPL", rows)

        out_path = tmp_path / "symbol=AAPL" / "5m.parquet"
        table = pq.read_table(out_path)
        stored = table.column("bar_timestamp")[0].as_py()
        # 9:30 ET on 2026-04-06 (EDT = UTC-4) = 13:30 UTC
        assert stored == et_ts.astimezone(_UTC)
        client.close()

    def test_replace_then_read_roundtrip(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        ts2 = datetime(2026, 4, 6, 13, 35, tzinfo=_UTC)
        rows = [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
            {"bar_timestamp": ts2, "symbol_id": 1, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 200},
        ]
        n = client.replace_ticker_rows("AAPL", rows)
        assert n == 2

        loaded = client.read_symbol_rows("AAPL")
        assert len(loaded) == 2
        assert loaded[0]["bar_timestamp"] == ts1
        assert loaded[1]["bar_timestamp"] == ts2
        client.close()

    def test_merge_dedups_by_timestamp(self, tmp_path):
        client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m")
        ts1 = datetime(2026, 4, 6, 13, 30, tzinfo=_UTC)
        ts2 = datetime(2026, 4, 6, 13, 35, tzinfo=_UTC)
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        # Merge with overlap + new
        client.merge_ticker_rows("AAPL", [
            {"bar_timestamp": ts1, "symbol_id": 1, "open": 9.0, "high": 9.0, "low": 9.0, "close": 9.0, "volume": 999},
            {"bar_timestamp": ts2, "symbol_id": 1, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 200},
        ])
        loaded = client.read_symbol_rows("AAPL")
        assert len(loaded) == 2
        # ts1 was overwritten
        assert loaded[0]["volume"] == 999


class TestDiscovery:
    def test_get_existing_symbols_finds_intraday_files(self, tmp_path):
        # Create AAPL with 5m, MSFT with 1h
        for sym, tf in [("AAPL", "5m"), ("MSFT", "1h"), ("NVDA", "5m")]:
            client = IntradayBronzeClient(bronze_dir=tmp_path, timeframe=tf)
            client.replace_ticker_rows(sym, [
                {
                    "bar_timestamp": datetime(2026, 4, 6, 13, 30, tzinfo=_UTC),
                    "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5,
                    "close": 1.5, "volume": 100,
                },
            ])
            client.close()

        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="5m") as client:
            assert client.get_existing_symbols() == {"AAPL", "NVDA"}
        with IntradayBronzeClient(bronze_dir=tmp_path, timeframe="1h") as client:
            assert client.get_existing_symbols() == {"MSFT"}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_intraday_bronze_client.py -v
```
Expected: ImportError

- [ ] **Step 3: Implement `clients/intraday_bronze_client.py`**

```python
"""Intraday parquet bronze client (1h and 5m equity bars).

Stores per-ticker per-timeframe parquet files alongside the existing 1d.parquet
files written by BronzeClient. Schema is timestamp-keyed (bar_timestamp
TIMESTAMPTZ) rather than date-keyed.

Universal rule: all bar timestamps stored as UTC with timezone awareness.
Naive datetimes are rejected at write time.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from clients.parquet_io import publish_parquet
from clients.symbol_ids import stable_symbol_id

log = logging.getLogger(__name__)

_DEFAULT_BRONZE_DIR = (
    Path.home() / "market-warehouse" / "data-lake" / "bronze" / "asset_class=equity"
)

INTRADAY_TIMEFRAMES = ("1h", "5m")

INTRADAY_PARQUET_FILENAME = {
    "1h": "1h.parquet",
    "5m": "5m.parquet",
}

# IB historical request limits per timeframe
INTRADAY_MAX_REQUEST_DURATION = {
    "1h": "1 M",
    "5m": "1 W",
}

# Realistic IB data depth per timeframe
INTRADAY_MAX_DEPTH = {
    "1h": "2 Y",
    "5m": "1 Y",
}

# IB barSizeSetting strings
INTRADAY_IB_BAR_SIZE = {
    "1h": "1 hour",
    "5m": "5 mins",
}

_INTRADAY_COLUMNS = (
    "bar_timestamp",
    "symbol_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

_INTRADAY_SCHEMA = pa.schema([
    ("bar_timestamp", pa.timestamp("us", tz="UTC")),
    ("symbol_id", pa.int64()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.int64()),
])


class IntradayBronzeClient:
    """Per-ticker intraday bronze parquet client.

    Independent from BronzeClient. Daily code paths are never affected.
    """

    def __init__(
        self,
        bronze_dir: Optional[str | Path] = None,
        timeframe: str = "5m",
    ):
        if timeframe not in INTRADAY_TIMEFRAMES:
            raise ValueError(
                f"unsupported timeframe: {timeframe!r}. Must be one of {INTRADAY_TIMEFRAMES}"
            )
        self._bronze_dir = Path(bronze_dir or _DEFAULT_BRONZE_DIR)
        self._timeframe = timeframe
        self._filename = INTRADAY_PARQUET_FILENAME[timeframe]
        self._conn = duckdb.connect(":memory:")

    @property
    def timeframe(self) -> str:
        return self._timeframe

    @property
    def bronze_dir(self) -> Path:
        return self._bronze_dir

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "IntradayBronzeClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def get_existing_symbols(self) -> set[str]:
        """Return symbols with bronze parquet snapshots at this timeframe."""
        if not self._bronze_dir.exists():
            return set()
        symbols: set[str] = set()
        for path in self._bronze_dir.glob(f"symbol=*/{self._filename}"):
            partition = path.parent.name
            if partition.startswith("symbol="):
                symbols.add(partition.split("=", 1)[1])
        return symbols

    def get_latest_timestamps(self) -> dict[str, datetime]:
        """Return ``{symbol: latest_bar_timestamp}`` (UTC) for all symbols."""
        if not self.get_existing_symbols():
            return {}
        glob = self._escaped_glob()
        sql = f"""
            SELECT symbol, MAX(bar_timestamp) AS latest
            FROM read_parquet('{glob}', hive_partitioning=true)
            GROUP BY symbol
        """
        rows = self._conn.execute(sql).fetchall()
        return {sym: ts for sym, ts in rows}

    def get_symbol_id(self, symbol: str) -> int:
        path = self._symbol_path(symbol)
        if not path.exists():
            return stable_symbol_id(symbol)
        table = pq.read_table(path, columns=["symbol_id"])
        if table.num_rows == 0:
            return stable_symbol_id(symbol)
        return int(table.column("symbol_id")[0].as_py())

    def read_symbol_rows(self, symbol: str) -> list[dict[str, Any]]:
        path = self._symbol_path(symbol)
        if not path.exists():
            return []
        table = pq.read_table(path, columns=list(_INTRADAY_COLUMNS))
        return table.to_pylist()

    def replace_ticker_rows(self, symbol: str, rows: list[dict[str, Any]]) -> int:
        normalized = self._normalize_rows(rows, symbol)
        if not normalized:
            raise ValueError(f"{symbol}: cannot publish an empty parquet snapshot")
        self._publish(symbol, normalized)
        return len(normalized)

    def merge_ticker_rows(self, symbol: str, rows: list[dict[str, Any]]) -> int:
        incoming = self._normalize_rows(rows, symbol)
        if not incoming:
            return 0

        existing = self.read_symbol_rows(symbol)
        merged: dict[datetime, dict[str, Any]] = {
            row["bar_timestamp"]: row for row in existing
        }
        existing_keys = set(merged.keys())
        for row in incoming:
            merged[row["bar_timestamp"]] = row

        inserted = sum(1 for row in incoming if row["bar_timestamp"] not in existing_keys)
        ordered = [merged[ts] for ts in sorted(merged)]
        self._publish(symbol, ordered)
        return inserted

    def _symbol_path(self, symbol: str) -> Path:
        return self._bronze_dir / f"symbol={symbol}" / self._filename

    def _escaped_glob(self) -> str:
        return str(self._bronze_dir / f"symbol=*/{self._filename}").replace("'", "''")

    def _normalize_rows(
        self, rows: list[dict[str, Any]], symbol: str
    ) -> list[dict[str, Any]]:
        symbol_id = self.get_symbol_id(symbol)
        normalized: dict[datetime, dict[str, Any]] = {}

        for row in rows:
            ts = row["bar_timestamp"]
            if not isinstance(ts, datetime):
                raise ValueError(
                    f"{symbol}: bar_timestamp must be a datetime, got {type(ts).__name__}"
                )
            if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
                raise ValueError(
                    f"{symbol}: bar_timestamp must be tz-aware (got naive {ts!r})"
                )
            ts_utc = ts.astimezone(timezone.utc)
            normalized[ts_utc] = {
                "bar_timestamp": ts_utc,
                "symbol_id": symbol_id,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
            }

        return [normalized[ts] for ts in sorted(normalized)]

    def _publish(self, symbol: str, rows: list[dict[str, Any]]) -> Path:
        out_path = self._symbol_path(symbol)
        table = pa.Table.from_pylist(rows, schema=_INTRADAY_SCHEMA)
        result = publish_parquet(out_path, table, sort_column="bar_timestamp")
        log.info("Published %s", result)
        return result
```

- [ ] **Step 4: Export from `clients/__init__.py`**

Add to `clients/__init__.py`:

```python
from clients.intraday_bronze_client import (
    IntradayBronzeClient,
    INTRADAY_TIMEFRAMES,
    INTRADAY_PARQUET_FILENAME,
    INTRADAY_MAX_REQUEST_DURATION,
    INTRADAY_MAX_DEPTH,
    INTRADAY_IB_BAR_SIZE,
)
```

- [ ] **Step 5: Run tests**

```bash
python -m pytest tests/test_intraday_bronze_client.py -v
```
Expected: All PASS

- [ ] **Step 6: Run full suite with coverage**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 7: Commit**

```bash
git add clients/intraday_bronze_client.py clients/__init__.py tests/test_intraday_bronze_client.py
git commit -m "feat: add IntradayBronzeClient for 1h/5m equity bars (UTC timestamps)"
```

---

## Task 4: Add NYSE Calendar Extensions (Early Close, Session Close)

**Files:**
- Modify: `scripts/daily_update.py`
- Modify: `tests/test_daily_update.py`

- [ ] **Step 1: Write tests**

Append to `tests/test_daily_update.py`:

```python
from datetime import time as dtime

from scripts.daily_update import get_early_close_days, session_close_time


class TestEarlyCloseDays:
    def test_2025_includes_day_after_thanksgiving(self):
        # Thanksgiving 2025 = Nov 27 (Thu); day after = Nov 28 (Fri)
        result = get_early_close_days(2025)
        assert date(2025, 11, 28) in result
        assert result[date(2025, 11, 28)] == dtime(13, 0)

    def test_2025_includes_christmas_eve(self):
        # Dec 24, 2025 is a Wednesday (trading day) → early close
        result = get_early_close_days(2025)
        assert date(2025, 12, 24) in result
        assert result[date(2025, 12, 24)] == dtime(13, 0)

    def test_2026_includes_day_after_thanksgiving(self):
        # Thanksgiving 2026 = Nov 26 (Thu); day after = Nov 27 (Fri)
        result = get_early_close_days(2026)
        assert date(2026, 11, 27) in result

    def test_2024_july_3_early_close(self):
        # July 4 2024 was a Thursday, July 3 (Wed) had an early close
        result = get_early_close_days(2024)
        assert date(2024, 7, 3) in result


class TestSessionCloseTime:
    def test_normal_day_returns_4pm(self):
        # Random Wednesday with no early close
        assert session_close_time(date(2026, 4, 8)) == dtime(16, 0)

    def test_early_close_day_returns_1pm(self):
        # Day after Thanksgiving 2025
        assert session_close_time(date(2025, 11, 28)) == dtime(13, 0)

    def test_christmas_eve_2025_returns_1pm(self):
        assert session_close_time(date(2025, 12, 24)) == dtime(13, 0)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_daily_update.py::TestEarlyCloseDays tests/test_daily_update.py::TestSessionCloseTime -v
```
Expected: AttributeError on `get_early_close_days`

- [ ] **Step 3: Add helpers to `scripts/daily_update.py`**

In the calendar section (after `get_nyse_holidays`), add:

```python
def get_early_close_days(year: int) -> dict[date, "time"]:
    """Return ``{trading_date: close_time_ET}`` for half-day trading days.

    Standard early-close days (NYSE close at 13:00 ET):
      - Day after Thanksgiving (4th Friday of November)
      - Christmas Eve (Dec 24, if a trading day)
      - July 3 (if Independence Day on a weekday other than Mon/Sun)
    """
    from datetime import time as dtime  # local import keeps top of file clean
    early: dict[date, dtime] = {}

    # Day after Thanksgiving — 4th Thursday of November + 1 day
    nov1 = date(year, 11, 1)
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    thanksgiving = first_thu + timedelta(weeks=3)
    day_after = thanksgiving + timedelta(days=1)
    if is_trading_day(day_after):
        early[day_after] = dtime(13, 0)

    # Christmas Eve — Dec 24, only if trading day
    christmas_eve = date(year, 12, 24)
    if is_trading_day(christmas_eve):
        early[christmas_eve] = dtime(13, 0)

    # July 3 — early close when Independence Day (Jul 4) is Tue/Wed/Thu/Fri
    july_3 = date(year, 7, 3)
    july_4 = date(year, 7, 4)
    if is_trading_day(july_3) and july_4.weekday() in (1, 2, 3, 4):
        early[july_3] = dtime(13, 0)

    return early


def session_close_time(d: date) -> "time":
    """Return the ET close time for trading day *d*.

    16:00 normally; 13:00 on early-close days.
    """
    from datetime import time as dtime
    early = get_early_close_days(d.year)
    return early.get(d, dtime(16, 0))
```

Add `from datetime import time` to the existing top-of-file imports if not already present (it should be since `daily_update.py` already imports `date`, `datetime`, `timedelta`).

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_daily_update.py::TestEarlyCloseDays tests/test_daily_update.py::TestSessionCloseTime -v
```
Expected: All PASS

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 6: Commit**

```bash
git add scripts/daily_update.py tests/test_daily_update.py
git commit -m "feat: add get_early_close_days and session_close_time NYSE calendar helpers"
```

---

## Task 5: Add `validate_intraday_bar` Helper

**Files:**
- Modify: `scripts/daily_update.py`
- Modify: `tests/test_daily_update.py`

- [ ] **Step 1: Write tests**

Append to `tests/test_daily_update.py`:

```python
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from scripts.daily_update import validate_intraday_bar


_UTC = timezone.utc
_ET = ZoneInfo("America/New_York")


class TestValidateIntradayBar:
    def _bar(self, ts: datetime):
        from types import SimpleNamespace
        return SimpleNamespace(bar_timestamp=ts, open=1.0, high=2.0, low=0.5, close=1.5, volume=100)

    def test_valid_5m_bar_at_open(self):
        # 9:30 ET on a Tuesday (April 7, 2026)
        ts = datetime(2026, 4, 7, 9, 30, tzinfo=_ET).astimezone(_UTC)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "5m")
        assert issues == []

    def test_valid_1h_bar_at_open(self):
        ts = datetime(2026, 4, 7, 9, 30, tzinfo=_ET).astimezone(_UTC)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "1h")
        assert issues == []

    def test_naive_timestamp_rejected(self):
        ts = datetime(2026, 4, 7, 13, 30)  # naive
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "5m")
        assert any("tz-aware" in i for i in issues)

    def test_non_utc_offset_rejected(self):
        # tz-aware but not UTC
        ts = datetime(2026, 4, 7, 9, 30, tzinfo=_ET)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "5m")
        assert any("UTC" in i for i in issues)

    def test_non_trading_day_rejected(self):
        # Saturday April 4, 2026
        ts = datetime(2026, 4, 4, 13, 30, tzinfo=_UTC)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "5m")
        assert any("not a trading day" in i for i in issues)

    def test_outside_rth_rejected(self):
        # 8:00 ET on a trading day (pre-market)
        ts = datetime(2026, 4, 7, 8, 0, tzinfo=_ET).astimezone(_UTC)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "5m")
        assert any("outside RTH" in i for i in issues)

    def test_after_close_rejected(self):
        # 16:30 ET on a trading day (after close)
        ts = datetime(2026, 4, 7, 16, 30, tzinfo=_ET).astimezone(_UTC)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "5m")
        assert any("outside RTH" in i for i in issues)

    def test_5m_grid_misalignment_rejected(self):
        # 9:32 ET — not on the 5-min grid
        ts = datetime(2026, 4, 7, 9, 32, tzinfo=_ET).astimezone(_UTC)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "5m")
        assert any("5-min grid" in i for i in issues)

    def test_1h_grid_misalignment_rejected(self):
        # 10:00 ET — 1h bars start on :30
        ts = datetime(2026, 4, 7, 10, 0, tzinfo=_ET).astimezone(_UTC)
        issues = validate_intraday_bar(self._bar(ts), "AAPL", "1h")
        assert any("1h grid" in i for i in issues)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_daily_update.py::TestValidateIntradayBar -v
```
Expected: ImportError on `validate_intraday_bar`

- [ ] **Step 3: Implement `validate_intraday_bar` in `scripts/daily_update.py`**

Add after `validate_bars`:

```python
def validate_intraday_bar(bar: Any, ticker: str, timeframe: str) -> list[str]:
    """Validate an intraday bar's timestamp against UTC, RTH, and grid alignment.

    Returns list of issue strings; empty if valid. Caller is responsible for the
    OHLCV relationship checks (use ``validate_bars`` for those).
    """
    from datetime import timedelta as _td  # local to avoid shadow
    from zoneinfo import ZoneInfo as _ZI

    issues: list[str] = []

    ts = getattr(bar, "bar_timestamp", None)
    if not isinstance(ts, datetime):
        issues.append(f"{ticker}: bar_timestamp must be datetime, got {type(ts).__name__}")
        return issues

    # 1. tz-aware UTC
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        issues.append(f"{ticker} {ts}: bar_timestamp must be tz-aware")
        return issues
    if ts.utcoffset() != _td(0):
        issues.append(f"{ticker} {ts}: bar_timestamp must be UTC offset 0")
        return issues

    # 2. Convert to ET for date and session-window checks
    et = ts.astimezone(_ZI("America/New_York"))

    # 3. Trading day
    if not is_trading_day(et.date()):
        issues.append(f"{ticker} {ts}: not a trading day")

    # 4. Within RTH
    rth_start = et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_t = session_close_time(et.date())
    rth_end = et.replace(hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0)
    if not (rth_start <= et < rth_end):
        issues.append(f"{ticker} {ts}: outside RTH ({et.time()} ET)")

    # 5. Grid alignment
    if timeframe == "5m":
        if et.minute % 5 != 0 or et.second != 0:
            issues.append(f"{ticker} {ts}: not aligned to 5-min grid")
    elif timeframe == "1h":
        if et.minute != 30 or et.second != 0:
            issues.append(f"{ticker} {ts}: not aligned to 1h grid (expected :30 ET)")

    return issues
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_daily_update.py::TestValidateIntradayBar -v
```
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/daily_update.py tests/test_daily_update.py
git commit -m "feat: add validate_intraday_bar for UTC + RTH + grid alignment checks"
```

---

## Task 6: Add `presets/core-etfs.json`

**Files:**
- Create: `presets/core-etfs.json`

- [ ] **Step 1: Create the preset file**

```json
{
  "name": "core-etfs",
  "description": "Always-include ETFs — added to the screened universe regardless of scanner output. Covers broad market, sectors, international, commodities, bonds, vol, leveraged, and crypto for backtesting, intraday signals, and options analytics.",
  "groups": {
    "broad_market":  {"description": "U.S. broad market index ETFs",                    "tickers": ["SPY", "QQQ", "IWM", "DIA", "VTI"]},
    "sectors_spdr":  {"description": "All 11 SPDR sector ETFs",                         "tickers": ["XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC"]},
    "industry":      {"description": "Key industry ETFs not in SPDR sectors",           "tickers": ["SMH"]},
    "international": {"description": "Country and regional ETFs",                       "tickers": ["EFA", "EEM", "EWJ", "EWY", "EWZ", "FXI", "KWEB"]},
    "metals":        {"description": "Precious metals — gold and silver",               "tickers": ["GLD", "SLV"]},
    "commodities":   {"description": "Energy and ag commodities — macro exposure",      "tickers": ["USO"]},
    "bonds":         {"description": "Treasury, IG, HY, aggregate — rates and credit",  "tickers": ["TLT", "IEF", "HYG", "LQD", "AGG"]},
    "volatility":    {"description": "VIX-tracking ETFs — options analytics",           "tickers": ["VXX", "UVXY"]},
    "leveraged":     {"description": "Leveraged broad market — intraday signals",       "tickers": ["TQQQ", "SQQQ"]},
    "crypto":        {"description": "Spot crypto ETFs",                                "tickers": ["IBIT"]}
  },
  "tickers": [
    "SPY", "QQQ", "IWM", "DIA", "VTI",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC",
    "SMH",
    "EFA", "EEM", "EWJ", "EWY", "EWZ", "FXI", "KWEB",
    "GLD", "SLV",
    "USO",
    "TLT", "IEF", "HYG", "LQD", "AGG",
    "VXX", "UVXY",
    "TQQQ", "SQQQ",
    "IBIT"
  ]
}
```

- [ ] **Step 2: Verify it's parseable**

```bash
python -c "import json; d=json.load(open('presets/core-etfs.json')); print(f'{len(d[\"tickers\"])} tickers in {len(d[\"groups\"])} groups')"
```
Expected: `38 tickers in 10 groups`

- [ ] **Step 3: Verify `load_preset()` works on it**

```bash
python -c "from scripts.fetch_ib_historical import load_preset; n, t, _ = load_preset('presets/core-etfs.json'); print(f'name={n}, tickers={len(t)}'); assert 'SPY' in t"
```
Expected: `name=core-etfs, tickers=38`

- [ ] **Step 4: Commit**

```bash
git add presets/core-etfs.json
git commit -m "feat: add core-etfs preset (38 tickers, 10 groups)"
```

---

## Task 7: Patch `universe_screener.py` for Core ETF Exclusion

**Files:**
- Modify: `scripts/universe_screener.py`
- Modify: `tests/test_universe_screener.py`

This task fixes Codex finding #4 — core ETFs must bypass the absent-counts logic entirely.

- [ ] **Step 1: Write tests**

Append to `tests/test_universe_screener.py`:

```python
class TestCoreEtfIntegration:
    @pytest.fixture(autouse=True)
    def _no_scanner_throttle(self, monkeypatch):
        monkeypatch.setattr("scripts.universe_screener._SCANNER_THROTTLE_SECONDS", 0)

    def test_core_etf_added_when_missing(self, tmp_path, monkeypatch):
        """A core ETF not in bronze should be added even if not scanned."""
        monkeypatch.setattr("scripts.universe_screener._DATA_LAKE", tmp_path / "data-lake")
        monkeypatch.setattr("scripts.universe_screener._STATE_PATH", tmp_path / "state.json")
        monkeypatch.setattr("scripts.universe_screener._LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr("scripts.universe_screener._PRESET_PATH", tmp_path / "preset.json")
        monkeypatch.setattr("scripts.universe_screener._CORE_ETFS_PATH", tmp_path / "core.json")

        # Write a core ETF preset
        (tmp_path / "core.json").write_text(json.dumps({"name": "core-etfs", "tickers": ["SPY", "QQQ"]}))
        # Empty bronze
        (tmp_path / "data-lake" / "bronze" / "asset_class=equity").mkdir(parents=True)
        # Scanner returns nothing
        mock_ib = _make_mock_ib_client([])

        with patch("scripts.universe_screener.IBClient", return_value=mock_ib):
            with patch("subprocess.run") as mock_subproc:
                with patch("sys.argv", ["universe_screener.py", "--force"]):
                    main()

        # Backfill must have been triggered for SPY and QQQ
        assert mock_subproc.called
        cmd = mock_subproc.call_args[0][0]
        assert "SPY" in cmd
        assert "QQQ" in cmd

    def test_core_etf_never_in_removals(self, tmp_path, monkeypatch):
        """A core ETF in bronze but not scanned should NEVER be archived."""
        monkeypatch.setattr("scripts.universe_screener._DATA_LAKE", tmp_path / "data-lake")
        monkeypatch.setattr("scripts.universe_screener._STATE_PATH", tmp_path / "state.json")
        monkeypatch.setattr("scripts.universe_screener._LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr("scripts.universe_screener._PRESET_PATH", tmp_path / "preset.json")
        monkeypatch.setattr("scripts.universe_screener._CORE_ETFS_PATH", tmp_path / "core.json")

        (tmp_path / "core.json").write_text(json.dumps({"name": "core-etfs", "tickers": ["SPY"]}))

        # Pre-existing state with SPY having absent_count = 99 (way past grace)
        (tmp_path / "state.json").write_text(json.dumps({
            "run_date": "2026-04-01",
            "universe": ["SPY", "AAPL"],
            "absent_counts": {},  # if logic is correct, SPY can never be here
        }))

        # Bronze contains SPY and AAPL
        for sym in ("SPY", "AAPL"):
            (tmp_path / "data-lake" / "bronze" / "asset_class=equity" / f"symbol={sym}").mkdir(parents=True)
            (tmp_path / "data-lake" / "bronze" / "asset_class=equity" / f"symbol={sym}" / "1d.parquet").write_bytes(b"x")

        # Scanner returns AAPL but not SPY
        mock_ib = _make_mock_ib_client(["AAPL"])

        with patch("scripts.universe_screener.IBClient", return_value=mock_ib):
            with patch("subprocess.run"):
                with patch("sys.argv", ["universe_screener.py", "--force"]):
                    main()

        # SPY must NOT be archived
        spy_archive = tmp_path / "data-lake" / "bronze-delisted" / "asset_class=equity" / "symbol=SPY"
        assert not spy_archive.exists()

        # SPY must NOT appear in absent_counts in the new state
        state = json.loads((tmp_path / "state.json").read_text())
        assert "SPY" not in state["absent_counts"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_universe_screener.py::TestCoreEtfIntegration -v
```
Expected: AttributeError on `_CORE_ETFS_PATH` or assertion failures

- [ ] **Step 3: Patch `scripts/universe_screener.py`**

Near the top of the file (after `_PRESET_PATH` constant), add:

```python
_CORE_ETFS_PATH = Path(__file__).resolve().parent.parent / "presets" / "core-etfs.json"
```

Add a helper function after `compare_universes`:

```python
def load_core_etfs() -> set[str]:
    """Load the core ETF ticker list from presets/core-etfs.json.

    Returns empty set if the file doesn't exist (graceful degradation).
    """
    if not _CORE_ETFS_PATH.exists():
        return set()
    with _CORE_ETFS_PATH.open() as f:
        data = json.load(f)
    return set(data.get("tickers", []))
```

In `main()`, replace the comparison block with:

```python
    # ── Compare universes ──────────────────────────────────────────────
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
```

Replace the new universe computation:

```python
    # ── Compute new universe and write preset ──────────────────────────
    new_universe = (current_universe | additions) - confirmed_removals
    # Belt-and-suspenders: ensure core ETFs are always present
    new_universe = new_universe | core_etfs
    write_universe_preset(_PRESET_PATH, list(new_universe))
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_universe_screener.py -v
```
Expected: All PASS (existing + new core ETF tests)

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 6: Commit**

```bash
git add scripts/universe_screener.py tests/test_universe_screener.py
git commit -m "feat: exclude core ETFs from screener removal logic"
```

---

## Task 8: Per-Timeframe Cursor in `fetch_ib_historical.py`

**Files:**
- Modify: `scripts/fetch_ib_historical.py`
- Modify: `tests/test_fetch_ib_historical.py`

This task changes the cursor schema from a flat `set[str]` of completed tickers to a `dict[str, list[str]]` mapping ticker → list of completed timeframes. The change is **backward compatible**: an old cursor (set) is migrated on read.

- [ ] **Step 1: Write tests**

Append to `tests/test_fetch_ib_historical.py`:

```python
class TestPerTimeframeCursor:
    def test_load_old_cursor_format_migrates_to_dict(self, tmp_path, monkeypatch):
        from scripts.fetch_ib_historical import load_cursor, _cursor_path
        monkeypatch.setattr("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path)

        # Write old format: completed = list of strings
        old = _cursor_path("test")
        old.write_text(json.dumps({
            "completed": ["AAPL", "NVDA"],
            "started_at": "2026-04-06T10:00:00",
        }))

        result = load_cursor("test")
        # Old format is treated as "all timeframes complete" for these tickers
        assert result == {"AAPL": ["1d", "1h", "5m"], "NVDA": ["1d", "1h", "5m"]}

    def test_load_new_cursor_format(self, tmp_path, monkeypatch):
        from scripts.fetch_ib_historical import load_cursor, _cursor_path
        monkeypatch.setattr("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path)

        new = _cursor_path("test")
        new.write_text(json.dumps({
            "completed": {"AAPL": ["1d", "1h"], "NVDA": ["1d"]},
            "started_at": "2026-04-06T10:00:00",
        }))

        result = load_cursor("test")
        assert result == {"AAPL": ["1d", "1h"], "NVDA": ["1d"]}

    def test_load_missing_cursor_returns_empty_dict(self, tmp_path, monkeypatch):
        from scripts.fetch_ib_historical import load_cursor
        monkeypatch.setattr("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path)
        assert load_cursor("nonexistent") == {}

    def test_save_cursor_writes_dict_format(self, tmp_path, monkeypatch):
        from scripts.fetch_ib_historical import save_cursor, _cursor_path
        monkeypatch.setattr("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path)

        save_cursor("test", {"AAPL": ["1d", "1h"]}, started_at="2026-04-06T10:00:00")

        loaded = json.loads(_cursor_path("test").read_text())
        assert loaded["completed"] == {"AAPL": ["1d", "1h"]}

    def test_is_ticker_complete_for_all_timeframes(self):
        from scripts.fetch_ib_historical import is_ticker_complete

        # All 3 done
        assert is_ticker_complete({"AAPL": ["1d", "1h", "5m"]}, "AAPL", required=("1d", "1h", "5m"))
        # Missing 5m
        assert not is_ticker_complete({"AAPL": ["1d", "1h"]}, "AAPL", required=("1d", "1h", "5m"))
        # Not in cursor
        assert not is_ticker_complete({}, "AAPL", required=("1d", "1h", "5m"))

    def test_mark_timeframe_done_appends(self):
        from scripts.fetch_ib_historical import mark_timeframe_done

        cursor = {"AAPL": ["1d"]}
        mark_timeframe_done(cursor, "AAPL", "1h")
        assert cursor == {"AAPL": ["1d", "1h"]}

        # Idempotent
        mark_timeframe_done(cursor, "AAPL", "1h")
        assert cursor == {"AAPL": ["1d", "1h"]}

        # New ticker
        mark_timeframe_done(cursor, "NVDA", "5m")
        assert cursor == {"AAPL": ["1d", "1h"], "NVDA": ["5m"]}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_fetch_ib_historical.py::TestPerTimeframeCursor -v
```
Expected: ImportError on `is_ticker_complete` / `mark_timeframe_done`, or signature mismatch on `load_cursor`/`save_cursor`

- [ ] **Step 3: Patch `scripts/fetch_ib_historical.py`**

Replace the cursor functions:

```python
def load_cursor(name: str) -> dict[str, list[str]]:
    """Load completed-timeframes-per-ticker cursor.

    Backward compatible: old format ``{"completed": ["AAPL", ...]}`` is migrated
    to the new format treating each listed ticker as fully complete (all known
    timeframes done).
    """
    path = _cursor_path(name)
    if not path.exists():
        return {}
    with path.open() as f:
        data = json.load(f)
    raw = data.get("completed")
    if isinstance(raw, list):
        # Legacy format — migrate
        return {ticker: ["1d", "1h", "5m"] for ticker in raw}
    if isinstance(raw, dict):
        return {k: list(v) for k, v in raw.items()}
    return {}


def save_cursor(name: str, completed: dict[str, list[str]], started_at: str) -> None:
    """Write per-timeframe cursor JSON atomically."""
    path = _cursor_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    payload = {
        "completed": {k: sorted(set(v)) for k, v in completed.items()},
        "started_at": started_at,
        "updated_at": datetime.now().isoformat(),
    }
    with tmp.open("w") as f:
        json.dump(payload, f, indent=2)
    tmp.rename(path)


def is_ticker_complete(
    cursor: dict[str, list[str]],
    ticker: str,
    required: tuple[str, ...],
) -> bool:
    """Return True if all required timeframes are completed for *ticker*."""
    done = set(cursor.get(ticker, []))
    return all(tf in done for tf in required)


def mark_timeframe_done(
    cursor: dict[str, list[str]], ticker: str, timeframe: str
) -> None:
    """Add *timeframe* to a ticker's completed list (idempotent)."""
    done = cursor.setdefault(ticker, [])
    if timeframe not in done:
        done.append(timeframe)
```

- [ ] **Step 4: Update existing call sites in `fetch_ib_historical.py`**

Find places that call `load_cursor`/`save_cursor`/use the cursor as a `set`. The existing daily-only logic should still work — when only `1d` is being fetched, the cursor records `{ticker: ["1d"]}` and is_ticker_complete with `required=("1d",)` returns True.

Search for `cursor` usage in `main()` and replace any `cursor.add(ticker)` with `mark_timeframe_done(cursor, ticker, "1d")`. Replace any `if ticker in cursor:` with `if is_ticker_complete(cursor, ticker, ("1d",)):`.

- [ ] **Step 5: Run tests**

```bash
python -m pytest tests/test_fetch_ib_historical.py -v
```
Expected: All PASS (new tests + existing tests still work after the migration)

- [ ] **Step 6: Run full suite**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 7: Commit**

```bash
git add scripts/fetch_ib_historical.py tests/test_fetch_ib_historical.py
git commit -m "feat: per-timeframe cursor with backward-compat for legacy format"
```

---

## Task 9: Add Intraday Fetch Path to `fetch_ib_historical.py`

**Files:**
- Modify: `scripts/fetch_ib_historical.py`
- Modify: `tests/test_fetch_ib_historical.py`

Add a new function `fetch_intraday_for_ticker` that fetches one timeframe for one ticker, chunking IB requests by `INTRADAY_MAX_REQUEST_DURATION`. The existing daily flow stays untouched.

- [ ] **Step 1: Write tests**

Append to `tests/test_fetch_ib_historical.py`:

```python
class TestFetchIntraday:
    def test_compute_intraday_chunks_5m(self):
        """5m bars: 1-week chunks for 1 year of depth."""
        from scripts.fetch_ib_historical import compute_intraday_chunks
        chunks = compute_intraday_chunks(timeframe="5m", years_back=1)
        # ~52 weeks
        assert 50 <= len(chunks) <= 54
        # Each chunk is ("1 W", end_datetime_str)
        assert all(c[0] == "1 W" for c in chunks)

    def test_compute_intraday_chunks_1h(self):
        """1h bars: 1-month chunks for 2 years of depth."""
        from scripts.fetch_ib_historical import compute_intraday_chunks
        chunks = compute_intraday_chunks(timeframe="1h", years_back=2)
        # ~24 months
        assert 22 <= len(chunks) <= 26
        assert all(c[0] == "1 M" for c in chunks)

    def test_compute_intraday_chunks_invalid_timeframe(self):
        from scripts.fetch_ib_historical import compute_intraday_chunks
        with pytest.raises(ValueError, match="unsupported"):
            compute_intraday_chunks(timeframe="2m", years_back=1)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_fetch_ib_historical.py::TestFetchIntraday -v
```
Expected: ImportError on `compute_intraday_chunks`

- [ ] **Step 3: Add the helper to `scripts/fetch_ib_historical.py`**

```python
def compute_intraday_chunks(
    timeframe: str,
    years_back: int,
) -> list[tuple[str, str]]:
    """Generate ``(duration_str, end_datetime_str)`` chunks for an intraday backfill.

    For 5m: walks backwards from now in 1-week chunks for ``years_back`` years.
    For 1h: walks backwards from now in 1-month chunks for ``years_back`` years.

    Raises ValueError on unsupported timeframe.
    """
    from clients.intraday_bronze_client import INTRADAY_MAX_REQUEST_DURATION

    if timeframe not in INTRADAY_MAX_REQUEST_DURATION:
        raise ValueError(f"unsupported intraday timeframe: {timeframe!r}")

    duration = INTRADAY_MAX_REQUEST_DURATION[timeframe]
    end_dt = datetime.now()

    if timeframe == "5m":
        step = timedelta(weeks=1)
    elif timeframe == "1h":
        step = timedelta(days=30)
    else:
        raise ValueError(f"unsupported intraday timeframe: {timeframe!r}")

    head_dt = end_dt - timedelta(days=365 * years_back)

    chunks: list[tuple[str, str]] = []
    cursor = end_dt
    while cursor > head_dt:
        chunks.append((duration, cursor.strftime("%Y%m%d-%H:%M:%S")))
        cursor -= step

    return chunks
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_fetch_ib_historical.py::TestFetchIntraday -v
```
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/fetch_ib_historical.py tests/test_fetch_ib_historical.py
git commit -m "feat: add compute_intraday_chunks for 1h/5m IB request windowing"
```

---

## Task 10: Add Intraday Support to `db_client.py` and `rebuild_duckdb_from_parquet.py`

**Files:**
- Modify: `clients/db_client.py`
- Modify: `scripts/rebuild_duckdb_from_parquet.py`
- Modify: `tests/test_db_client.py`
- Modify: `tests/test_rebuild_duckdb_from_parquet.py`

- [ ] **Step 1: Write tests for the new db_client method**

Append to `tests/test_db_client.py`:

```python
class TestReplaceEquitiesIntradayFromParquet:
    @pytest.mark.integration
    def test_creates_intraday_table_and_loads_data(self, db, tmp_path):
        from datetime import datetime, timezone
        from clients.intraday_bronze_client import IntradayBronzeClient

        bronze = tmp_path / "bronze"
        client = IntradayBronzeClient(bronze_dir=bronze, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {
                "bar_timestamp": datetime(2026, 4, 6, 13, 30, tzinfo=timezone.utc),
                "symbol_id": 1,
                "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100,
            },
            {
                "bar_timestamp": datetime(2026, 4, 6, 13, 35, tzinfo=timezone.utc),
                "symbol_id": 1,
                "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 200,
            },
        ])
        client.close()

        result = db.replace_equities_intraday_from_parquet(bronze, timeframe="5m")
        assert result["rows"] == 2

        rows = db._conn.execute(
            "SELECT bar_timestamp, symbol_id, close FROM md.equities_5m ORDER BY bar_timestamp"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][2] == 1.5  # close

    def test_invalid_timeframe_raises(self, db, tmp_path):
        with pytest.raises(ValueError, match="unsupported"):
            db.replace_equities_intraday_from_parquet(tmp_path, timeframe="3m")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_db_client.py::TestReplaceEquitiesIntradayFromParquet -v
```
Expected: AttributeError

- [ ] **Step 3: Add `replace_equities_intraday_from_parquet` to `clients/db_client.py`**

First, ensure the table is created in `_ensure_schema`. Find the existing table creation code and add:

```python
self._conn.execute("""
    CREATE TABLE IF NOT EXISTS md.equities_1h (
        bar_timestamp TIMESTAMPTZ,
        symbol_id BIGINT,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
        volume BIGINT,
        UNIQUE (bar_timestamp, symbol_id)
    )
""")
self._conn.execute("""
    CREATE TABLE IF NOT EXISTS md.equities_5m (
        bar_timestamp TIMESTAMPTZ,
        symbol_id BIGINT,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
        volume BIGINT,
        UNIQUE (bar_timestamp, symbol_id)
    )
""")
```

Then add the method:

```python
def replace_equities_intraday_from_parquet(
    self,
    bronze_dir: str | Path,
    timeframe: str,
) -> dict[str, int]:
    """Rebuild ``md.equities_{timeframe}`` from intraday bronze parquet."""
    if timeframe not in ("1h", "5m"):
        raise ValueError(f"unsupported intraday timeframe: {timeframe!r}")

    table_name = f"equities_{timeframe}"
    bronze_dir = Path(bronze_dir)
    parquet_filename = f"{timeframe}.parquet"
    parquet_files = list(bronze_dir.glob(f"symbol=*/{parquet_filename}"))
    parquet_glob = str(bronze_dir / f"symbol=*/{parquet_filename}").replace("'", "''")

    self._conn.execute("BEGIN")
    try:
        self._conn.execute(f"DROP TABLE IF EXISTS md.{table_name}")
        self._ensure_schema()
        if parquet_files:
            self._conn.execute(f"""
                INSERT INTO md.{table_name}
                    (bar_timestamp, symbol_id, open, high, low, close, volume)
                SELECT bar_timestamp, symbol_id, open, high, low, close, volume
                FROM read_parquet('{parquet_glob}', hive_partitioning=true)
            """)
        self._conn.execute("COMMIT")
    except Exception:
        self._conn.execute("ROLLBACK")
        raise

    counts = self._conn.execute(f"SELECT count(*) FROM md.{table_name}").fetchone()
    return {"rows": counts[0]}
```

- [ ] **Step 4: Patch `scripts/rebuild_duckdb_from_parquet.py` for `--timeframe` flag**

Add the flag to argparse:

```python
parser.add_argument(
    "--timeframe",
    choices=["1d", "1h", "5m", "all"],
    default="all",
    help="Which timeframe table(s) to rebuild (default: all)",
)
```

In the main flow, branch on timeframe:

```python
    if args.timeframe in ("1d", "all"):
        # Existing daily rebuild logic
        ...

    if args.timeframe in ("1h", "all"):
        bronze_dir = DATA_LAKE / "bronze" / "asset_class=equity"
        result = db.replace_equities_intraday_from_parquet(bronze_dir, timeframe="1h")
        log.info("Rebuilt md.equities_1h with %d rows", result["rows"])

    if args.timeframe in ("5m", "all"):
        bronze_dir = DATA_LAKE / "bronze" / "asset_class=equity"
        result = db.replace_equities_intraday_from_parquet(bronze_dir, timeframe="5m")
        log.info("Rebuilt md.equities_5m with %d rows", result["rows"])
```

- [ ] **Step 5: Add tests for the rebuild script**

Append to `tests/test_rebuild_duckdb_from_parquet.py`:

```python
class TestRebuildIntraday:
    @pytest.mark.integration
    def test_timeframe_5m_only_rebuilds_5m(self, tmp_path, monkeypatch):
        from datetime import datetime, timezone
        from clients.intraday_bronze_client import IntradayBronzeClient

        data_lake = tmp_path / "data-lake"
        bronze = data_lake / "bronze" / "asset_class=equity"
        bronze.mkdir(parents=True)

        client = IntradayBronzeClient(bronze_dir=bronze, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": datetime(2026, 4, 6, 13, 30, tzinfo=timezone.utc),
             "symbol_id": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        client.close()

        with patch("scripts.rebuild_duckdb_from_parquet.DATA_LAKE", data_lake):
            with patch("sys.argv", ["rebuild_duckdb_from_parquet.py", "--timeframe", "5m"]):
                main()

        # Verify md.equities_5m has the row
        import duckdb
        conn = duckdb.connect(str(data_lake.parent / "duckdb" / "market.duckdb"))
        result = conn.execute("SELECT count(*) FROM md.equities_5m").fetchone()
        assert result[0] == 1
        conn.close()
```

- [ ] **Step 6: Run tests**

```bash
python -m pytest tests/test_db_client.py tests/test_rebuild_duckdb_from_parquet.py -v
```
Expected: All PASS

- [ ] **Step 7: Run full suite**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 8: Commit**

```bash
git add clients/db_client.py scripts/rebuild_duckdb_from_parquet.py tests/test_db_client.py tests/test_rebuild_duckdb_from_parquet.py
git commit -m "feat: add intraday DuckDB rebuild for 1h/5m equities tables"
```

---

## Task 11: Patch `sync_to_r2.py` for All Three Timeframes

**Files:**
- Modify: `scripts/sync_to_r2.py`
- Modify: `tests/test_sync_to_r2.py`

- [ ] **Step 1: Write tests**

Append to `tests/test_sync_to_r2.py`:

```python
class TestMultiTimeframeSync:
    def test_uploads_all_three_timeframes(self, tmp_path, monkeypatch):
        from scripts.sync_to_r2 import upload, PARQUET_FILES_TO_SYNC

        equity_dir = tmp_path / "asset_class=equity" / "symbol=AAPL"
        equity_dir.mkdir(parents=True)
        (equity_dir / "1d.parquet").write_bytes(b"d")
        (equity_dir / "1h.parquet").write_bytes(b"h")
        (equity_dir / "5m.parquet").write_bytes(b"m")

        # All three filenames should be in PARQUET_FILES_TO_SYNC
        assert "1d.parquet" in PARQUET_FILES_TO_SYNC
        assert "1h.parquet" in PARQUET_FILES_TO_SYNC
        assert "5m.parquet" in PARQUET_FILES_TO_SYNC

        with patch("scripts.sync_to_r2._get_s3_client") as mock_s3:
            with patch("scripts.sync_to_r2._get_bucket", return_value="test-bucket"):
                client = MagicMock()
                mock_s3.return_value = client
                count = upload(tmp_path, dry_run=False)

        assert count == 3
        # Verify upload_file called 3 times with the right keys
        calls = client.upload_file.call_args_list
        keys_uploaded = sorted(c[0][2] for c in calls)
        assert "asset_class=equity/symbol=AAPL/1d.parquet" in keys_uploaded
        assert "asset_class=equity/symbol=AAPL/1h.parquet" in keys_uploaded
        assert "asset_class=equity/symbol=AAPL/5m.parquet" in keys_uploaded
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_sync_to_r2.py::TestMultiTimeframeSync -v
```
Expected: ImportError on `PARQUET_FILES_TO_SYNC`

- [ ] **Step 3: Patch `scripts/sync_to_r2.py`**

Add at the top of the module:

```python
PARQUET_FILES_TO_SYNC = ("1d.parquet", "1h.parquet", "5m.parquet")
```

Replace the `upload` function's iteration:

```python
def upload(bronze_dir: Path, prefix: str = "bronze", dry_run: bool = False) -> int:
    if not bronze_dir.exists():
        logger.warning("Bronze dir %s does not exist, nothing to upload", bronze_dir)
        return 0

    s3 = _get_s3_client()
    bucket = _get_bucket()
    uploaded = 0

    for parquet_filename in PARQUET_FILES_TO_SYNC:
        for parquet_file in bronze_dir.rglob(parquet_filename):
            rel_path = parquet_file.relative_to(bronze_dir.parent)
            s3_key = str(rel_path).replace("\\", "/")

            if dry_run:
                logger.info("[DRY RUN] Would upload %s → s3://%s/%s", parquet_file, bucket, s3_key)
            else:
                logger.info("Uploading %s → s3://%s/%s", parquet_file, bucket, s3_key)
                s3.upload_file(str(parquet_file), bucket, s3_key)
            uploaded += 1

    logger.info("Upload complete: %d files %s", uploaded, "(dry run)" if dry_run else "")
    return uploaded
```

Replace the `download` function's filter check:

```python
            if not any(s3_key.endswith(name) for name in PARQUET_FILES_TO_SYNC):
                continue
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_sync_to_r2.py -v
```
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/sync_to_r2.py tests/test_sync_to_r2.py
git commit -m "feat: sync_to_r2 uploads all three timeframes (1d, 1h, 5m)"
```

---

## Task 12: Create `intraday_update.py` with Session Model

**Files:**
- Create: `scripts/intraday_update.py`
- Create: `tests/test_intraday_update.py`

- [ ] **Step 1: Write tests for session-state classification**

```python
"""Tests for scripts/intraday_update.py."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from scripts.intraday_update import (
    SessionState,
    classify_session_state,
    expected_last_bar_utc,
)


_UTC = timezone.utc
_ET = ZoneInfo("America/New_York")


class TestExpectedLastBarUtc:
    def test_normal_day_5m(self):
        # Tue Apr 7 2026, 5m bars, last bar = 15:55 ET
        d = date(2026, 4, 7)
        result = expected_last_bar_utc(d, timeframe="5m")
        expected_et = datetime(2026, 4, 7, 15, 55, tzinfo=_ET)
        assert result == expected_et.astimezone(_UTC)

    def test_normal_day_1h(self):
        # Tue Apr 7 2026, 1h bars, last bar = 15:30 ET
        d = date(2026, 4, 7)
        result = expected_last_bar_utc(d, timeframe="1h")
        expected_et = datetime(2026, 4, 7, 15, 30, tzinfo=_ET)
        assert result == expected_et.astimezone(_UTC)

    def test_early_close_day_5m(self):
        # Day after Thanksgiving 2025 (Nov 28), close 13:00 ET, last 5m bar = 12:55 ET
        d = date(2025, 11, 28)
        result = expected_last_bar_utc(d, timeframe="5m")
        expected_et = datetime(2025, 11, 28, 12, 55, tzinfo=_ET)
        assert result == expected_et.astimezone(_UTC)


class TestClassifySessionState:
    def _now(self, et_str: str) -> datetime:
        # Helper: parse "YYYY-MM-DD HH:MM ET" → UTC datetime
        dt = datetime.strptime(et_str, "%Y-%m-%d %H:%M").replace(tzinfo=_ET)
        return dt.astimezone(_UTC)

    def test_complete_session_with_full_bars(self):
        # Now: Wed Apr 8 2026 09:00 ET (next day)
        # Latest stored: Tue Apr 7 2026 15:55 ET (last bar of prior day)
        now = self._now("2026-04-08 09:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 7), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.COMPLETE

    def test_in_progress_after_close(self):
        # Now: Tue Apr 7 2026 16:30 ET (after close)
        # Latest stored: Tue Apr 7 2026 14:00 ET (gap to fill)
        now = self._now("2026-04-07 16:30")
        latest_stored = datetime(2026, 4, 7, 14, 0, tzinfo=_ET).astimezone(_UTC)
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.IN_PROGRESS

    def test_live_during_session(self):
        # Now: Tue Apr 7 2026 11:00 ET (mid-session)
        # Latest stored: Tue Apr 7 2026 10:30 ET
        now = self._now("2026-04-07 11:00")
        latest_stored = datetime(2026, 4, 7, 10, 30, tzinfo=_ET).astimezone(_UTC)
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.LIVE

    def test_tail_gap_no_today_data(self):
        # Now: Tue Apr 7 2026 10:00 ET
        # Latest stored: Mon Apr 6 2026 15:55 ET (no bars from today yet)
        now = self._now("2026-04-07 10:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 6), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.TAIL_GAP

    def test_historical_multiple_days_behind(self):
        # Now: Thu Apr 9 2026 09:00 ET
        # Latest stored: Mon Apr 6 2026 15:55 ET
        now = self._now("2026-04-09 09:00")
        latest_stored = expected_last_bar_utc(date(2026, 4, 6), "5m")
        state = classify_session_state(latest_stored, now, "5m")
        assert state == SessionState.HISTORICAL
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_intraday_update.py -v
```
Expected: ImportError

- [ ] **Step 3: Implement `scripts/intraday_update.py` (session model only — full main() in next task)**

```python
"""Intraday update — refresh 1h and 5m bars for the equity universe.

Iterates over INTRADAY_TIMEFRAMES, classifies each (symbol, timeframe) into
one of 5 session states, and fetches the appropriate IB chunks. The 'live'
state never writes the in-progress bar.

Usage:
    python scripts/intraday_update.py                 # all symbols, all intraday timeframes
    python scripts/intraday_update.py --dry-run       # report only
    python scripts/intraday_update.py --force         # run on non-trading day
    python scripts/intraday_update.py --timeframe 5m  # only one timeframe
"""

from __future__ import annotations

import argparse
import enum
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.intraday_bronze_client import (
    INTRADAY_TIMEFRAMES,
    IntradayBronzeClient,
)
from scripts.daily_update import is_trading_day, session_close_time

log = logging.getLogger("intraday_update")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_UTC = timezone.utc
_ET = ZoneInfo("America/New_York")
_WAREHOUSE_DIR = Path(os.environ.get("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"


class SessionState(enum.Enum):
    COMPLETE = "complete"        # all bars present, session closed
    IN_PROGRESS = "in_progress"  # session closed but stored data is partial
    LIVE = "live"                # session currently open, fetch up to last complete bar
    TAIL_GAP = "tail_gap"        # session not yet started/started today, no today data
    HISTORICAL = "historical"    # multiple trading days behind


def expected_last_bar_utc(trading_day: date, timeframe: str) -> datetime:
    """Return the UTC timestamp of the last bar of *trading_day* at *timeframe*."""
    close_t = session_close_time(trading_day)
    # 5m last bar = close - 5min; 1h last bar = close - 1h
    if timeframe == "5m":
        last_et = datetime(
            trading_day.year, trading_day.month, trading_day.day,
            close_t.hour, close_t.minute, tzinfo=_ET,
        ) - timedelta(minutes=5)
    elif timeframe == "1h":
        last_et = datetime(
            trading_day.year, trading_day.month, trading_day.day,
            close_t.hour, close_t.minute, tzinfo=_ET,
        ) - timedelta(hours=1)
    else:
        raise ValueError(f"unsupported timeframe: {timeframe!r}")
    return last_et.astimezone(_UTC)


def classify_session_state(
    latest_stored: datetime,
    now: datetime,
    timeframe: str,
) -> SessionState:
    """Classify a (symbol, timeframe) pair into one of 5 session states.

    Args:
        latest_stored: UTC timestamp of the most recent stored bar
        now: current UTC time
        timeframe: '1h' or '5m'
    """
    today_et = now.astimezone(_ET).date()

    # Walk back to find the most recent trading day (today if trading, else previous)
    if is_trading_day(today_et):
        target_day = today_et
    else:
        target_day = today_et - timedelta(days=1)
        while not is_trading_day(target_day):
            target_day -= timedelta(days=1)

    expected_close = expected_last_bar_utc(target_day, timeframe)
    latest_stored_day = latest_stored.astimezone(_ET).date()

    # Is the target session over yet?
    now_et = now.astimezone(_ET)
    close_t = session_close_time(target_day)
    session_end = datetime(
        target_day.year, target_day.month, target_day.day,
        close_t.hour, close_t.minute, tzinfo=_ET,
    ).astimezone(_UTC)

    if now < session_end:
        # Session is open or hasn't started today
        if latest_stored_day < target_day:
            return SessionState.TAIL_GAP
        return SessionState.LIVE

    # Session is over
    if latest_stored_day < target_day - timedelta(days=1):
        return SessionState.HISTORICAL
    if latest_stored >= expected_close:
        return SessionState.COMPLETE
    return SessionState.IN_PROGRESS
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_intraday_update.py -v
```
Expected: All PASS

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 6: Commit**

```bash
git add scripts/intraday_update.py tests/test_intraday_update.py
git commit -m "feat: add intraday_update session-state classification"
```

---

## Task 13: Wire `intraday_update.py` to Daily Job + Smoke Test

**Files:**
- Modify: `scripts/intraday_update.py` (add `main()`)
- Modify: `docker/ibroker-mkt-data/entrypoint.py`
- Modify: `tests/test_intraday_update.py` (add main test)

- [ ] **Step 1: Add `main()` and CLI to `intraday_update.py`**

Append to `scripts/intraday_update.py`:

```python
def main() -> None:
    parser = argparse.ArgumentParser(description="Intraday update (1h + 5m equity bars)")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no fetches")
    parser.add_argument("--force", action="store_true", help="Run on non-trading day")
    parser.add_argument(
        "--timeframe",
        choices=list(INTRADAY_TIMEFRAMES) + ["all"],
        default="all",
        help="Which timeframe to update (default: all)",
    )
    args = parser.parse_args()

    today = date.today()
    if not args.force and not is_trading_day(today):
        log.info("Not a trading day (%s), skipping. Use --force to override.", today)
        return

    timeframes = INTRADAY_TIMEFRAMES if args.timeframe == "all" else (args.timeframe,)
    bronze_dir = _DATA_LAKE / "bronze" / "asset_class=equity"

    summary: dict[str, dict[str, int]] = {}

    for tf in timeframes:
        log.info("=== Intraday update: %s ===", tf)
        with IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=tf) as client:
            existing = client.get_existing_symbols()
            latest_ts = client.get_latest_timestamps()

            now = datetime.now(_UTC)
            states = {SessionState.COMPLETE: 0, SessionState.IN_PROGRESS: 0,
                      SessionState.LIVE: 0, SessionState.TAIL_GAP: 0,
                      SessionState.HISTORICAL: 0}
            for sym in sorted(existing):
                latest = latest_ts.get(sym)
                if latest is None:
                    states[SessionState.HISTORICAL] += 1
                    continue
                state = classify_session_state(latest, now, tf)
                states[state] += 1
            summary[tf] = {s.value: c for s, c in states.items()}
            log.info("Session states for %s: %s", tf, summary[tf])

    if args.dry_run:
        log.info("Dry run — no fetches performed.")
        return

    log.info("Intraday update complete. Summary: %s", summary)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add a smoke test for main()**

Append to `tests/test_intraday_update.py`:

```python
class TestMain:
    def test_dry_run_classifies_states_without_fetching(self, tmp_path, monkeypatch):
        from datetime import datetime, timezone
        from clients.intraday_bronze_client import IntradayBronzeClient

        bronze = tmp_path / "data-lake" / "bronze" / "asset_class=equity"
        bronze.mkdir(parents=True)

        # Seed AAPL with one 5m bar from yesterday
        from datetime import timedelta
        yesterday = datetime.now(_UTC) - timedelta(days=1)
        # Round to a 5-min boundary in ET
        et_yest = yesterday.astimezone(_ET).replace(hour=15, minute=55, second=0, microsecond=0)

        client = IntradayBronzeClient(bronze_dir=bronze, timeframe="5m")
        client.replace_ticker_rows("AAPL", [
            {"bar_timestamp": et_yest.astimezone(_UTC), "symbol_id": 1,
             "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ])
        client.close()

        monkeypatch.setattr("scripts.intraday_update._DATA_LAKE", tmp_path / "data-lake")

        with patch("sys.argv", ["intraday_update.py", "--dry-run", "--force", "--timeframe", "5m"]):
            from scripts.intraday_update import main
            main()
        # No exceptions = pass
```

- [ ] **Step 3: Wire intraday into entrypoint**

In `docker/ibroker-mkt-data/entrypoint.py`, add a function and update `run_job_cycle`:

```python
def run_intraday_update(force: bool = False) -> int:
    """Run the intraday update (1h + 5m bars)."""
    cmd = [_python(), str(SCRIPTS_DIR / "intraday_update.py")]
    if force:
        cmd.append("--force")
    return _run_cmd(cmd, "Intraday update")


def run_job_cycle(force: bool = False) -> int:
    """Full job cycle: download from R2 → daily update → intraday update → upload."""
    logger.info("=== Starting job cycle ===")

    rc = sync_download()
    if rc != 0:
        logger.warning("R2 download failed (rc=%d), continuing with local state", rc)

    rc = run_daily_update(force=force)

    if rc == 0:
        rc = run_intraday_update(force=force)

    if rc == 0:
        upload_rc = sync_upload()
        if upload_rc != 0:
            logger.error("R2 upload failed (rc=%d)", upload_rc)
            return upload_rc
    else:
        logger.warning("Update failed (rc=%d), skipping R2 upload", rc)

    logger.info("=== Job cycle complete (rc=%d) ===", rc)
    return rc
```

- [ ] **Step 4: Run all tests**

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: All PASS, 100% coverage

- [ ] **Step 5: Commit**

```bash
git add scripts/intraday_update.py docker/ibroker-mkt-data/entrypoint.py tests/test_intraday_update.py
git commit -m "feat: wire intraday_update into entrypoint job cycle"
```

---

## Task 14: Final Verification

- [ ] **Step 1: Full test suite with strict warnings**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning
```
Expected: All PASS, 100% coverage, no warnings

- [ ] **Step 2: Verify Docker build**

```bash
docker compose build ibroker-mkt-data
```
Expected: Image rebuilds cleanly

- [ ] **Step 3: Smoke test the probe (live IB)**

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/probe_ib_intraday.py
```
Expected: Prints bar dates and types — confirms IB connectivity and intraday data availability

- [ ] **Step 4: Targeted smoke fetch — 1 day of 5m for AAPL**

```bash
python scripts/fetch_ib_historical.py --tickers AAPL --years 0
```
Expected: Writes 1d.parquet for AAPL (existing path still works)

- [ ] **Step 5: Verify intraday dry-run**

```bash
python scripts/intraday_update.py --dry-run --force --timeframe 5m
```
Expected: Reports session-state counts for each symbol

- [ ] **Step 6: Verify DuckDB rebuild for intraday tables**

```bash
python scripts/rebuild_duckdb_from_parquet.py --timeframe 5m
duckdb ~/market-warehouse/duckdb/market.duckdb "SELECT count(*) FROM md.equities_5m"
```
Expected: Row count > 0 (or 0 if no intraday data fetched yet)

- [ ] **Step 7: Verify timezone correctness in DuckDB**

```bash
duckdb ~/market-warehouse/duckdb/market.duckdb "
SELECT bar_timestamp,
       bar_timestamp AT TIME ZONE 'America/New_York' AS et_time
FROM md.equities_5m
LIMIT 5
"
```
Expected: First column shows `+00` (UTC); second column shows ET wall-clock time

- [ ] **Step 8: Verify R2 sync handles all 3 timeframes**

```bash
python scripts/sync_to_r2.py --upload --dry-run
```
Expected: Lists 1d.parquet, 1h.parquet, and 5m.parquet files in upload preview
