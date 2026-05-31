# CLI Consolidation & Maximum Coverage Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse 4 CLIs / 21 subcommands into a single `livewire` CLI with 4 commands, add 30m timeframe, bump all intraday depths to 5 years, fix R2 incremental sync, replace bash orchestrators with testable Python, and run a full warehouse backfill.

**Architecture:** Single dispatcher (`scripts/livewire.py`) routes to 4 verbs: `sync`, `backfill`, `check`, `publish`. Auto source selection removes `--source`/`--asset-class` ceremony for the common case. Shell orchestrators (`run_backfill_all.sh`, `run_daily_backfill.sh`) are replaced by a Python lane runner with stall detection. Shared code duplicated across `daily_update.py` and `fetch_ib_historical.py` (~160 lines, 6 functions) is extracted to `clients/`.

**Tech Stack:** Python 3.13, PyArrow, ib_async, boto3, rich, pytest (100% coverage gate)

---

## Current State → Target State

### Entry points: 21 → 4

```
BEFORE (4 scripts, 21 subcommands)          AFTER (1 script, 4 commands)
─────────────────────────────────────        ─────────────────────────────
livewire_ingest.py daily                     livewire sync
livewire_ingest.py historical                livewire backfill
livewire_ingest.py robust                    livewire backfill (internal retry)
livewire_ingest.py cboe-vol                  livewire sync (auto vol lane)
livewire_ingest.py fred-rates                livewire sync (auto rates lane)
livewire_ingest.py intraday-backfill         livewire backfill --timeframe T
livewire_ingest.py intraday-status           livewire check (folded in)
livewire_ingest.py probe-intraday            DELETED (one-shot, served its purpose)
livewire_ingest.py universe                  livewire check --universe
livewire_ingest.py backfill-all              livewire backfill (Python orchestrator)
livewire_ingest.py daily-backfill            livewire sync (Python orchestrator)

livewire_quality.py health                   livewire check
livewire_quality.py coverage                 livewire check
livewire_quality.py report                   livewire check --report
livewire_quality.py weekly                   livewire check --weekly
livewire_quality.py watchdog                 livewire sync --scheduled (internal)

livewire_ops.py run-daily-job                livewire sync --scheduled
livewire_ops.py send-alert                   (internal to sync --scheduled)

livewire_store.py rebuild-postgres           livewire publish postgres
livewire_store.py smoke-postgres             livewire publish postgres --smoke
livewire_store.py sync-r2                    livewire publish r2
livewire_store.py migrate-parquet            livewire publish --migrate
```

### Auto source selection (no more `--source` / `--asset-class` ceremony)

| Asset Class | Daily | Intraday | Source Logic |
|-------------|-------|----------|-------------|
| Equity | Massive (fast) → IB fallback | Massive (fast) → IB fallback | `MASSIVE_API_KEY` present → Massive; else IB |
| Volatility | CBOE API (free) | IB (VIX/SPX/NDX/RUT only) | Hardcoded per source |
| Futures | IB | IB | Only option |
| FX | IB | IB | Only option |
| Commodities | IB | IB | Only option |
| Rates | FRED API | N/A | Only option |

### Depth bumps (Massive Starter = 5yr all timeframes)

| Timeframe | Before | After | Multiplier |
|-----------|--------|-------|-----------|
| 1d | 10yr (IB inception) | Same | — |
| 1h | 2yr | **5yr** | 2.5x |
| 30m | N/A | **5yr** | New |
| 5m | 1yr | **5yr** | 5x |
| 1m | 5yr | Same | — |

### R2 sync fixes

| Issue | Before | After |
|-------|--------|-------|
| Re-uploads everything | Full upload every time | Incremental (size + mtime comparison) |
| Missing timeframes | Only 1d, 1h, 5m | All: 1d, 1h, 30m, 5m, 1m |
| No download-merge | Overwrites local files | Download only files not present locally (or `--force` to overwrite) |

---

## File Map

### New files

| File | Responsibility |
|------|---------------|
| `scripts/livewire.py` | Unified CLI dispatcher (4 commands) |
| `clients/ingestion_common.py` | Shared code extracted from daily_update + fetch_ib_historical |
| `livewire_scripts/sync_runner.py` | Python orchestrator replacing `run_daily_backfill.sh` |
| `livewire_scripts/backfill_runner.py` | Python orchestrator replacing `run_backfill_all.sh` |
| `tests/test_livewire_cli.py` | Unified CLI dispatch tests |
| `tests/test_ingestion_common.py` | Extracted shared code tests |
| `tests/test_sync_runner.py` | Sync orchestrator tests |
| `tests/test_backfill_runner.py` | Backfill orchestrator tests |

### Modified files

| File | Change |
|------|--------|
| `clients/intraday_bronze_client.py` | Add `30m` to all `INTRADAY_*` dicts |
| `clients/massive_client.py` | Add `30m` → `(30, "minute")` mapping |
| `livewire_scripts/backfill_intraday.py` | Bump `_DEFAULT_YEARS` for 1h and 5m; add 30m |
| `livewire_scripts/sync_to_r2.py` | Incremental sync via size+mtime; add 1m, 30m |
| `livewire_scripts/daily_update.py` | Import shared code from `clients/ingestion_common.py` |
| `livewire_scripts/fetch_ib_historical.py` | Import shared code from `clients/ingestion_common.py` |
| `livewire_scripts/run_daily_update_job.py` | Adapt to call sync_runner instead of subprocess chains |
| `presets/volatility-intraday.json` | Add 30m if we want vol 30m coverage |
| `clients/postgres_schema.py` | Add `equities_30m` table DDL (same schema as `equities_5m`) |
| `clients/postgres_client.py` | Add 30m to intraday rebuild logic |
| `livewire_scripts/health_check.py` | Add `"30m": 30` to `_BAR_SIZE_MINUTES` |
| `livewire_scripts/fetch_ib_historical.py` | Add `"30m"` to `compute_intraday_chunks._step_map` |
| `CLAUDE.md` | Update all CLI examples, add new commands |
| `launchd/*.plist.example` | Update to call `livewire sync --scheduled` |

### Deleted files (Phase 4 cleanup)

| File | Reason |
|------|--------|
| `scripts/livewire_ingest.py` | Replaced by `scripts/livewire.py` |
| `scripts/livewire_quality.py` | Replaced by `livewire check` |
| `scripts/livewire_ops.py` | Replaced by `livewire sync --scheduled` |
| `scripts/livewire_store.py` | Replaced by `livewire publish` |
| `tools/run_backfill_all.sh` | Replaced by `livewire_scripts/backfill_runner.py` |
| `tools/run_daily_backfill.sh` | Replaced by `livewire_scripts/sync_runner.py` |
| `livewire_scripts/probe_ib_intraday.py` | One-shot diagnostic, already served its purpose |
| `livewire_scripts/intraday_update.py` | Report-only classifier, folded into check |
| `tests/test_livewire_entrypoints.py` | Replaced by `tests/test_livewire_cli.py` |
| `tests/test_script_consolidation.py` | Replaced by `tests/test_livewire_cli.py` |
| `tests/test_intraday_update.py` | Folded into check tests |

---

## Phase 1: Foundation (no CLI change, fully backward compatible)

All changes in this phase work with the existing 4-script CLI. Ship independently.

### Task 1.1: Add 30m timeframe to intraday constants

**Files:**
- Modify: `clients/intraday_bronze_client.py:30-57`
- Modify: `clients/massive_client.py` (timeframe mapping)
- Modify: `livewire_scripts/backfill_intraday.py:64`
- Test: `tests/test_intraday_bronze_client.py`
- Test: `tests/test_backfill_intraday.py`

- [ ] **Step 1: Write failing test for 30m constants**

In `tests/test_intraday_bronze_client.py`, add:

```python
def test_30m_in_all_intraday_dicts():
    from clients.intraday_bronze_client import (
        INTRADAY_IB_BAR_SIZE,
        INTRADAY_MAX_DEPTH,
        INTRADAY_MAX_REQUEST_DURATION,
        INTRADAY_PARQUET_FILENAME,
        INTRADAY_TIMEFRAMES,
    )
    assert "30m" in INTRADAY_TIMEFRAMES
    assert INTRADAY_PARQUET_FILENAME["30m"] == "30m.parquet"
    assert INTRADAY_MAX_REQUEST_DURATION["30m"] == "1 M"
    assert INTRADAY_MAX_DEPTH["30m"] == "5 Y"
    assert INTRADAY_IB_BAR_SIZE["30m"] == "30 mins"
```

- [ ] **Step 2: Run test, confirm it fails**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_intraday_bronze_client.py::test_30m_in_all_intraday_dicts -v`
Expected: FAIL — `"30m" not in INTRADAY_TIMEFRAMES`

- [ ] **Step 3: Add 30m to all INTRADAY dicts**

In `clients/intraday_bronze_client.py`:

```python
INTRADAY_TIMEFRAMES = ("1m", "1h", "5m", "30m")

INTRADAY_PARQUET_FILENAME = {
    "1m": "1m.parquet",
    "1h": "1h.parquet",
    "5m": "5m.parquet",
    "30m": "30m.parquet",
}

INTRADAY_MAX_REQUEST_DURATION = {
    "1m": "1 D",
    "1h": "1 M",
    "5m": "1 W",
    "30m": "1 M",
}

INTRADAY_MAX_DEPTH = {
    "1m": "5 Y",
    "1h": "5 Y",
    "5m": "5 Y",
    "30m": "5 Y",
}

INTRADAY_IB_BAR_SIZE = {
    "1m": "1 min",
    "1h": "1 hour",
    "5m": "5 mins",
    "30m": "30 mins",
}
```

Note: `INTRADAY_MAX_DEPTH` for `1h` also bumped from `"2 Y"` to `"5 Y"`, and `5m` from `"1 Y"` to `"5 Y"`.

- [ ] **Step 3b: Update `compute_intraday_chunks` step map**

In `livewire_scripts/fetch_ib_historical.py:302`, the `_step_map` only has `{"1m", "5m", "1h"}`. Add 30m:

```python
_step_map = {"1m": timedelta(days=1), "5m": timedelta(weeks=1), "1h": timedelta(days=30), "30m": timedelta(days=30)}
```

30m uses the same 1 M request duration as 1h, so the step is the same `timedelta(days=30)`.

- [ ] **Step 3c: Update `_BAR_SIZE_MINUTES` in health_check.py**

In `livewire_scripts/health_check.py:36`, add 30m:

```python
_BAR_SIZE_MINUTES = {"1m": 1, "1h": 60, "5m": 5, "30m": 30}
```

- [ ] **Step 4: Add 30m to Massive client timeframe mapping**

In `clients/massive_client.py`, find the `_TIMEFRAME_MAP` (or equivalent mapping dict) and add:

```python
"30m": (30, "minute"),
```

- [ ] **Step 5: Add `equities_30m` to Postgres schema**

In `clients/postgres_schema.py`, add the 30m table DDL following the same pattern as `equities_5m`:

```python
CREATE TABLE IF NOT EXISTS {schema}.equities_30m (
    bar_timestamp TIMESTAMPTZ NOT NULL,
    symbol_id BIGINT NOT NULL REFERENCES {schema}.symbols(symbol_id),
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    PRIMARY KEY (bar_timestamp, symbol_id)
);
```

In `clients/postgres_client.py`, add `"30m"` to the set of intraday timeframes that `replace_equities_intraday_from_parquet()` handles.

- [ ] **Step 6: Add 30m to backfill default years**

In `livewire_scripts/backfill_intraday.py`, update:

```python
_DEFAULT_YEARS = {"1m": 5, "1h": 5, "5m": 5, "30m": 5}
```

- [ ] **Step 6: Run full test suite**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing`
Expected: All pass, 100% coverage

- [ ] **Step 7: Commit**

```bash
git add clients/intraday_bronze_client.py clients/massive_client.py livewire_scripts/backfill_intraday.py tests/test_intraday_bronze_client.py tests/test_backfill_intraday.py
git commit -m "feat: add 30m timeframe and bump 1h/5m depth to 5yr"
```

---

### Task 1.2: Extract shared ingestion code to clients/

Four functions truly duplicated between `daily_update.py` and `fetch_ib_historical.py`: `_make_contract`, `bars_to_rows`, `bars_to_futures_rows`, `bars_to_midpoint_rows`. ~90 lines total.

**Not extracted (different signatures despite same name):**
- `_run_quality_detection` — `daily_update`'s takes `reference_source`; `fetch_ib_historical`'s takes `ib_head_timestamp` + `timeframe`. These are genuinely different functions. Keep each module's own version.
- `load_preset` — `daily_update`'s returns `(name, tickers)` (2-tuple); `fetch_ib_historical`'s returns `(name, tickers, exchange_map)` (3-tuple); `fetch_cboe_volatility` has a third variant returning `list[str]`. Extract the 3-tuple version as canonical. Update `daily_update.py` callers to destructure with `_, _` or ignore the third element.
- `_resolve_fx_pair` / `_is_inverted_fx_pair` — only in `daily_update.py`. Extract alongside `make_contract` since FX logic is tightly coupled to contract creation.

**Files:**
- Create: `clients/ingestion_common.py`
- Modify: `livewire_scripts/daily_update.py`
- Modify: `livewire_scripts/fetch_ib_historical.py`
- Create: `tests/test_ingestion_common.py`

- [ ] **Step 1: Write failing test for extracted module**

Create `tests/test_ingestion_common.py`:

```python
"""Tests for shared ingestion code extracted from daily_update + fetch_ib_historical."""
import pytest
from unittest.mock import MagicMock
from ib_async import Stock, Future, Index, Forex


def test_make_contract_equity():
    from clients.ingestion_common import make_contract
    c = make_contract("AAPL", asset_class="equity")
    assert isinstance(c, Stock)
    assert c.symbol == "AAPL"


def test_make_contract_futures():
    from clients.ingestion_common import make_contract
    c = make_contract("ES_202506", asset_class="futures")
    assert isinstance(c, Future)
    assert c.symbol == "ES"


def test_make_contract_volatility():
    from clients.ingestion_common import make_contract
    c = make_contract("VIX", asset_class="volatility")
    assert isinstance(c, Index)


def test_make_contract_fx():
    from clients.ingestion_common import make_contract
    c = make_contract("EURUSD", asset_class="fx")
    assert isinstance(c, Forex)


def test_bars_to_rows():
    from clients.ingestion_common import bars_to_rows
    bar = MagicMock()
    bar.date = "2026-01-02"
    bar.open = 100.0
    bar.high = 105.0
    bar.low = 99.0
    bar.close = 103.0
    bar.volume = 1000
    rows = bars_to_rows([bar], symbol_id=42)
    assert len(rows) == 1
    assert rows[0]["trade_date"] == "2026-01-02"
    assert rows[0]["symbol_id"] == 42
    assert rows[0]["adj_close"] == 103.0


def test_load_preset_equity():
    import json, tempfile
    from pathlib import Path
    from clients.ingestion_common import load_preset
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"name": "test", "tickers": ["AAPL", "MSFT"]}, f)
        f.flush()
        name, tickers, exchange_map = load_preset(f.name)
    assert name == "test"
    assert tickers == ["AAPL", "MSFT"]
    assert exchange_map == {}
    Path(f.name).unlink()


def test_load_preset_futures():
    import json, tempfile
    from pathlib import Path
    from clients.ingestion_common import load_preset
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({
            "name": "futures-index",
            "asset_class": "futures",
            "contracts": [{"root": "ES", "exchange": "CME", "expiry": "202506"}],
        }, f)
        f.flush()
        name, tickers, exchange_map = load_preset(f.name)
    assert tickers == ["ES_202506"]
    assert exchange_map["ES_202506"] == "CME"
    Path(f.name).unlink()
```

- [ ] **Step 2: Run test, confirm it fails**

Run: `python -m pytest tests/test_ingestion_common.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.ingestion_common'`

- [ ] **Step 3: Create `clients/ingestion_common.py`**

Extract the 6 functions from `fetch_ib_historical.py` (the version with `exchange` param on `_make_contract` is the superset). Keep function signatures identical. Rename `_make_contract` → `make_contract` (public API now).

```python
"""Shared ingestion helpers — extracted from daily_update and fetch_ib_historical.

Canonical location for contract creation, bar conversion, preset loading,
and quality detection wiring used by multiple ingestion scripts.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from ib_async import Contract, Forex, Future, Index, Stock

from clients.quality_detector import _normalize_bars_for_detection, detect_all
from clients.quality_flags import alert_on_flag, append_audit, write_sidecar

log = logging.getLogger(__name__)

# Exchange maps (futures, volatility)
ROOT_EXCHANGE_MAP = {
    "ES": "CME", "NQ": "CME", "RTY": "CME",
    "YM": "CBOT", "ZB": "CBOT", "ZN": "CBOT", "ZF": "CBOT",
    "CL": "NYMEX", "NG": "NYMEX",
    "GC": "COMEX", "SI": "COMEX",
}

VOLATILITY_EXCHANGE_MAP = {
    "NDX": "NASDAQ",
    "RUT": "RUSSELL",
}

SUPPORTED_IB_FX_PAIRS = {
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
    "USDCHF", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
}

_FX_INVERTED = {"USDEUR", "USDGBP", "USDAUD", "USDNZD"}


def make_contract(
    ticker: str,
    asset_class: str = "equity",
    exchange: str | None = None,
) -> Contract:
    """Build an ib_async Contract for the given ticker and asset class."""
    if asset_class == "futures":
        parts = ticker.rsplit("_", 1)
        root = parts[0]
        expiry = parts[1] if len(parts) > 1 else ""
        exch = exchange or ROOT_EXCHANGE_MAP.get(root, "CME")
        return Future(root, expiry, exch)

    if asset_class == "volatility":
        exch = exchange or VOLATILITY_EXCHANGE_MAP.get(ticker, "CBOE")
        return Index(ticker, exch, "USD")

    if asset_class == "fx":
        canonical = ticker.replace("/", "").upper()
        if canonical in _FX_INVERTED:
            canonical = canonical[3:] + canonical[:3]
        pair = canonical[:3] + canonical[3:]
        return Forex(pair, exchange="IDEALPRO")

    if asset_class == "cmdty":
        return Contract(symbol=ticker, secType="CMDTY", exchange="SMART", currency="USD")

    return Stock(ticker, "SMART", "USD")


def bars_to_rows(bars: list, symbol_id: int) -> list[dict]:
    """Convert IB BarData to equity/volatility bronze row dicts."""
    return [
        {
            "trade_date": str(bar.date),
            "symbol_id": symbol_id,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "adj_close": bar.close,
            "volume": int(bar.volume),
        }
        for bar in bars
    ]


def bars_to_futures_rows(
    bars: list, contract_id: int, root_symbol: str, expiry_date: str
) -> list[dict]:
    """Convert IB BarData to futures bronze row dicts."""
    return [
        {
            "trade_date": str(bar.date),
            "contract_id": contract_id,
            "root_symbol": root_symbol,
            "expiry_date": expiry_date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "settlement": bar.close,
            "volume": int(bar.volume),
            "open_interest": 0,
        }
        for bar in bars
    ]


def bars_to_midpoint_rows(
    bars: list, symbol_id: int, *, invert: bool = False
) -> list[dict]:
    """Convert IB MIDPOINT BarData to bronze row dicts (FX/CMDTY)."""
    rows = []
    for bar in bars:
        o, h, l, c = bar.open, bar.high, bar.low, bar.close
        if invert and all(v > 0 for v in (o, h, l, c)):
            o, h, l, c = 1 / o, 1 / l, 1 / h, 1 / c
            if h < l:
                h, l = l, h
        rows.append({
            "trade_date": str(bar.date),
            "symbol_id": symbol_id,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "adj_close": c,
            "volume": int(bar.volume),
        })
    return rows


def load_preset(path: str | Path) -> tuple[str, list[str], dict[str, str]]:
    """Load a preset JSON file. Returns (name, tickers, exchange_map).

    Handles both equity presets (``{"tickers": [...]}```) and futures presets
    (``{"contracts": [{"root": "ES", "exchange": "CME", "expiry": "202506"}]}``).
    """
    data = json.loads(Path(path).read_text())
    name = data.get("name", Path(path).stem)

    if "contracts" in data:
        tickers = []
        exchange_map: dict[str, str] = {}
        for c in data["contracts"]:
            composite = f"{c['root']}_{c['expiry']}"
            tickers.append(composite)
            exchange_map[composite] = c.get("exchange", "CME")
        return name, tickers, exchange_map

    return name, data.get("tickers", []), {}


def run_quality_detection(
    ticker: str,
    rows: list[dict],
    bronze_path: Path,
    audit_path: Path,
    *,
    ib_head_timestamp: date | None = None,
    source: str = "ib",
) -> None:
    """Run quality detection on fetched bars and emit sidecar + audit JSONL."""
    if not rows:
        return
    normalized = _normalize_bars_for_detection(rows)
    flags = detect_all(
        ticker=ticker,
        bars=normalized,
        source=source,
        ib_head_timestamp=ib_head_timestamp,
    )
    if flags:
        sidecar_path = bronze_path.with_suffix(".parquet.meta.json")
        write_sidecar(sidecar_path, flags)
        for flag in flags:
            append_audit(audit_path, flag)
            alert_on_flag(flag)


def resolve_fx_pair(ticker: str) -> tuple[str, bool]:
    """Return (canonical_pair, is_inverted) for an FX ticker."""
    canonical = ticker.replace("/", "").upper()
    inverted = canonical in _FX_INVERTED
    if inverted:
        canonical = canonical[3:] + canonical[:3]
    return canonical, inverted
```

- [ ] **Step 4: Update `daily_update.py` to import from `ingestion_common`**

Replace the 4 duplicated function definitions with imports. Keep `_run_quality_detection` local (it has a different signature with `reference_source` kwarg):

```python
from clients.ingestion_common import (
    make_contract as _make_contract,
    bars_to_rows,
    bars_to_futures_rows,
    bars_to_midpoint_rows,
    load_preset as _load_preset_3,
    resolve_fx_pair as _resolve_fx_pair,
    SUPPORTED_IB_FX_PAIRS,
)
```

Delete the local definitions of `_make_contract`, `bars_to_rows`, `bars_to_futures_rows`, `bars_to_midpoint_rows`. Update the `load_preset` call site at line 743 to handle the 3-tuple:

```python
# Before: preset_name, preset_list = load_preset(args.preset)
# After:
preset_name, preset_list, _ = _load_preset_3(args.preset)
```

Keep `_run_quality_detection` local — its signature (`reference_source` kwarg) differs from `fetch_ib_historical`'s version (`ib_head_timestamp` + `timeframe`).

- [ ] **Step 5: Update `fetch_ib_historical.py` to import from `ingestion_common`**

Same pattern — replace local definitions with imports from `clients.ingestion_common`. Delete `ROOT_EXCHANGE_MAP` and `SUPPORTED_IB_FX_PAIRS` locals (now in `ingestion_common`).

- [ ] **Step 6: Update `clients/__init__.py` exports**

Add `ingestion_common` exports if any downstream consumers need them.

- [ ] **Step 7: Run full test suite**

Run: `python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing`
Expected: All pass, 100% coverage. This is a pure refactoring — no behavior change.

- [ ] **Step 8: Commit**

```bash
git add clients/ingestion_common.py clients/__init__.py livewire_scripts/daily_update.py livewire_scripts/fetch_ib_historical.py tests/test_ingestion_common.py
git commit -m "refactor: extract shared ingestion code to clients/ingestion_common"
```

---

### Task 1.3: Fix R2 incremental sync

**Files:**
- Modify: `livewire_scripts/sync_to_r2.py`
- Test: `tests/test_sync_to_r2.py`

- [ ] **Step 1: Write failing tests for incremental sync and new timeframes**

Add to `tests/test_sync_to_r2.py`:

```python
def test_parquet_files_to_sync_includes_all_timeframes():
    from livewire_scripts.sync_to_r2 import PARQUET_FILES_TO_SYNC
    assert "1m.parquet" in PARQUET_FILES_TO_SYNC
    assert "30m.parquet" in PARQUET_FILES_TO_SYNC
    assert "1d.parquet" in PARQUET_FILES_TO_SYNC
    assert "1h.parquet" in PARQUET_FILES_TO_SYNC
    assert "5m.parquet" in PARQUET_FILES_TO_SYNC


def test_upload_skips_unchanged_files(tmp_path, monkeypatch):
    """Upload should skip files whose size matches the remote object."""
    from livewire_scripts.sync_to_r2 import upload
    from unittest.mock import MagicMock

    bronze = tmp_path / "bronze" / "asset_class=equity" / "symbol=AAPL"
    bronze.mkdir(parents=True)
    parquet = bronze / "1d.parquet"
    parquet.write_bytes(b"x" * 100)

    s3 = MagicMock()
    # Remote object exists with same size → skip
    s3.head_object.return_value = {"ContentLength": 100}
    monkeypatch.setattr("livewire_scripts.sync_to_r2._get_s3_client", lambda: s3)
    monkeypatch.setattr("livewire_scripts.sync_to_r2._get_bucket", lambda: "test")

    count = upload(tmp_path / "bronze", dry_run=False)
    s3.upload_file.assert_not_called()
    assert count == 0


def test_upload_pushes_changed_files(tmp_path, monkeypatch):
    """Upload should push files whose size differs from remote."""
    from livewire_scripts.sync_to_r2 import upload
    from unittest.mock import MagicMock
    from botocore.exceptions import ClientError

    bronze = tmp_path / "bronze" / "asset_class=equity" / "symbol=AAPL"
    bronze.mkdir(parents=True)
    parquet = bronze / "1d.parquet"
    parquet.write_bytes(b"x" * 200)

    s3 = MagicMock()
    # Remote object doesn't exist → upload
    s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "HeadObject"
    )
    monkeypatch.setattr("livewire_scripts.sync_to_r2._get_s3_client", lambda: s3)
    monkeypatch.setattr("livewire_scripts.sync_to_r2._get_bucket", lambda: "test")

    count = upload(tmp_path / "bronze", dry_run=False)
    assert count == 1
    s3.upload_file.assert_called_once()
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `python -m pytest tests/test_sync_to_r2.py -v -k "parquet_files_to_sync or skips_unchanged or pushes_changed"`
Expected: FAIL

- [ ] **Step 3: Update `PARQUET_FILES_TO_SYNC` and add incremental logic**

In `livewire_scripts/sync_to_r2.py`:

```python
PARQUET_FILES_TO_SYNC = ("1d.parquet", "1h.parquet", "30m.parquet", "5m.parquet", "1m.parquet")
```

Update `upload()` to check remote size before uploading:

```python
def upload(bronze_dir: Path, prefix: str = "bronze", dry_run: bool = False) -> int:
    if not bronze_dir.exists():
        logger.warning("Bronze dir %s does not exist, nothing to upload", bronze_dir)
        return 0

    from botocore.exceptions import ClientError

    s3 = _get_s3_client()
    bucket = _get_bucket()
    uploaded = 0

    for parquet_filename in PARQUET_FILES_TO_SYNC:
        for parquet_file in bronze_dir.rglob(parquet_filename):
            rel_path = parquet_file.relative_to(bronze_dir.parent)
            s3_key = str(rel_path).replace("\\", "/")
            local_size = parquet_file.stat().st_size

            # Skip if remote size matches
            try:
                head = s3.head_object(Bucket=bucket, Key=s3_key)
                if head["ContentLength"] == local_size:
                    continue
            except ClientError as e:
                if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                    raise

            if dry_run:
                logger.info("[DRY RUN] Would upload %s → s3://%s/%s", parquet_file, bucket, s3_key)
            else:
                logger.info("Uploading %s → s3://%s/%s", parquet_file, bucket, s3_key)
                s3.upload_file(str(parquet_file), bucket, s3_key)

            uploaded += 1

    logger.info("Upload complete: %d files %s", uploaded, "(dry run)" if dry_run else "")
    return uploaded
```

Update `download()` similarly — skip if local file exists with matching size (unless `--force`).

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_sync_to_r2.py -v`
Expected: All pass

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing`
Expected: All pass, 100% coverage

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/sync_to_r2.py tests/test_sync_to_r2.py
git commit -m "feat: incremental R2 sync with size check, add 1m/30m to sync list"
```

---

## Phase 2: Unified CLI

### Task 2.1: Build `scripts/livewire.py` dispatcher

The new single entry point. Routes to 4 commands: `sync`, `backfill`, `check`, `publish`. Each command dispatches to the existing implementation modules.

**Files:**
- Create: `scripts/livewire.py`
- Create: `tests/test_livewire_cli.py`

- [ ] **Step 1: Write failing tests for the dispatcher**

Create `tests/test_livewire_cli.py`:

```python
"""Tests for unified livewire CLI dispatcher."""
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))


def test_help_shows_all_commands():
    from scripts.livewire import main
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0


def test_sync_dispatches_to_sync_runner():
    with patch("scripts.livewire._dispatch_sync") as mock:
        mock.return_value = 0
        from scripts.livewire import main
        result = main(["sync", "--dry-run"])
    mock.assert_called_once()


def test_backfill_dispatches_to_backfill_runner():
    with patch("scripts.livewire._dispatch_backfill") as mock:
        mock.return_value = 0
        from scripts.livewire import main
        result = main(["backfill", "--dry-run"])
    mock.assert_called_once()


def test_check_dispatches_to_quality():
    with patch("scripts.livewire._dispatch_check") as mock:
        mock.return_value = 0
        from scripts.livewire import main
        result = main(["check"])
    mock.assert_called_once()


def test_publish_postgres_dispatches():
    with patch("scripts.livewire._dispatch_publish") as mock:
        mock.return_value = 0
        from scripts.livewire import main
        result = main(["publish", "postgres"])
    mock.assert_called_once()


def test_publish_r2_dispatches():
    with patch("scripts.livewire._dispatch_publish") as mock:
        mock.return_value = 0
        from scripts.livewire import main
        result = main(["publish", "r2"])
    mock.assert_called_once()


def test_sync_scheduled_dispatches_to_job_runner():
    with patch("scripts.livewire._dispatch_sync") as mock:
        mock.return_value = 0
        from scripts.livewire import main
        result = main(["sync", "--scheduled"])
    mock.assert_called_once()


def test_unknown_command_exits_nonzero():
    from scripts.livewire import main
    with pytest.raises(SystemExit) as exc_info:
        main(["nonexistent"])
    assert exc_info.value.code != 0
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `python -m pytest tests/test_livewire_cli.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement `scripts/livewire.py`**

```python
#!/usr/bin/env python3
"""Livewire — unified CLI for the market data warehouse.

Commands:
    sync        Daily catch-up: make all asset classes current
    backfill    Deep historical fill to maximum provider depth
    check       Quality, health, and coverage reporting
    publish     Push bronze data to Postgres or R2
"""
from __future__ import annotations

import argparse
import importlib
import inspect
import os
import sys
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

SYNC_MODULES = {
    "equity": "livewire_scripts.daily_update",
    "volatility": "livewire_scripts.fetch_cboe_volatility",
    "rates": "livewire_scripts.fetch_fred_rates",
}

BACKFILL_MODULES = {
    "daily": "livewire_scripts.fetch_ib_historical",
    "intraday": "livewire_scripts.backfill_intraday",
}

CHECK_MODULES = {
    "health": "livewire_scripts.health_check",
    "coverage": "livewire_scripts.coverage_report",
    "report": "livewire_scripts.data_quality_report",
    "weekly": "livewire_scripts.weekly_quality_summary",
    "universe": "livewire_scripts.universe_screener",
}

PUBLISH_MODULES = {
    "postgres": "livewire_scripts.rebuild_postgres_from_parquet",
    "r2": "livewire_scripts.sync_to_r2",
}


def _has_massive_key() -> bool:
    return bool(os.environ.get("MASSIVE_API_KEY"))


def _dispatch_module(module_name: str, argv: list[str], display: str) -> int:
    module = importlib.import_module(module_name)
    original = sys.argv
    sys.argv = [display, *argv]
    try:
        sig = inspect.signature(module.main)
        try:
            result = module.main(list(argv)) if sig.parameters else module.main()
        except SystemExit as exc:
            if exc.code in (0, None):
                return 0
            raise
    finally:
        sys.argv = original
    return int(result or 0)


def _dispatch_sync(argv: list[str]) -> int:
    """Daily catch-up: equity + volatility + rates, auto source selection."""
    parser = argparse.ArgumentParser(prog="livewire sync")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--scheduled", action="store_true",
                        help="Run as scheduled job with retry + alerting")
    parser.add_argument("--asset-class", choices=["equity", "volatility", "futures", "rates", "all"],
                        default="all")
    args, rest = parser.parse_known_args(argv)

    if args.scheduled:
        return _dispatch_module(
            "livewire_scripts.run_daily_update_job", rest, "livewire sync --scheduled"
        )

    # Auto source: Massive for equity if key present
    results = []
    classes = (["equity", "volatility", "futures", "rates"]
               if args.asset_class == "all" else [args.asset_class])

    for ac in classes:
        cmd_argv = list(rest)
        if args.dry_run:
            cmd_argv.append("--dry-run")
        if args.force:
            cmd_argv.append("--force")

        if ac == "equity":
            if _has_massive_key():
                cmd_argv.extend(["--source", "massive"])
            cmd_argv.extend(["--asset-class", "equity"])
            results.append(_dispatch_module(SYNC_MODULES["equity"], cmd_argv, "livewire sync equity"))

        elif ac == "volatility":
            # Daily vol: CBOE API (no IB needed)
            results.append(_dispatch_module(SYNC_MODULES["volatility"], cmd_argv, "livewire sync volatility"))
            # Also run IB equity daily for vol + futures if not Massive-only
            cmd_argv_ib = list(rest)
            if args.dry_run:
                cmd_argv_ib.append("--dry-run")
            if args.force:
                cmd_argv_ib.append("--force")
            cmd_argv_ib.extend(["--asset-class", "volatility"])
            results.append(_dispatch_module(SYNC_MODULES["equity"], cmd_argv_ib, "livewire sync volatility-ib"))

        elif ac == "futures":
            cmd_argv.extend(["--asset-class", "futures"])
            results.append(_dispatch_module(SYNC_MODULES["equity"], cmd_argv, "livewire sync futures"))

        elif ac == "rates":
            results.append(_dispatch_module(SYNC_MODULES["rates"], cmd_argv, "livewire sync rates"))

    return max(results) if results else 0


def _dispatch_backfill(argv: list[str]) -> int:
    """Deep historical fill with auto source selection."""
    parser = argparse.ArgumentParser(prog="livewire backfill")
    parser.add_argument("--timeframe", nargs="+",
                        choices=["1d", "1h", "30m", "5m", "1m", "all"],
                        default=["all"])
    parser.add_argument("--asset-class", choices=["equity", "volatility", "futures", "all"],
                        default="all")
    parser.add_argument("--years", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--preset", type=str, default=None)
    parser.add_argument("--skip-existing", action="store_true")
    args, rest = parser.parse_known_args(argv)

    timeframes = (["1d", "1h", "30m", "5m", "1m"]
                  if "all" in args.timeframe else args.timeframe)

    results = []
    for tf in timeframes:
        cmd_argv = list(rest)
        if args.dry_run:
            cmd_argv.append("--dry-run")
        if args.preset:
            cmd_argv.extend(["--preset", args.preset])
        if args.skip_existing:
            cmd_argv.append("--skip-existing")

        if tf == "1d":
            # Daily: use historical module
            if _has_massive_key() and args.asset_class in ("equity", "all"):
                cmd_argv.extend(["--source", "massive"])
            if args.years is not None:
                cmd_argv.extend(["--years", str(args.years)])
            results.append(_dispatch_module(
                BACKFILL_MODULES["daily"], cmd_argv, f"livewire backfill {tf}"
            ))
        else:
            # Intraday: use backfill_intraday module
            cmd_argv.extend(["--timeframe", tf])
            if _has_massive_key() and args.asset_class in ("equity", "all"):
                cmd_argv.extend(["--source", "massive"])
            if args.years is not None:
                cmd_argv.extend(["--years", str(args.years)])
            results.append(_dispatch_module(
                BACKFILL_MODULES["intraday"], cmd_argv, f"livewire backfill {tf}"
            ))

    return max(results) if results else 0


def _dispatch_check(argv: list[str]) -> int:
    """Quality, health, and coverage reporting."""
    parser = argparse.ArgumentParser(prog="livewire check")
    parser.add_argument("--mode", choices=list(CHECK_MODULES.keys()),
                        default="coverage")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--weekly", action="store_true")
    parser.add_argument("--universe", action="store_true")
    args, rest = parser.parse_known_args(argv)

    if args.report:
        mode = "report"
    elif args.weekly:
        mode = "weekly"
    elif args.universe:
        mode = "universe"
    else:
        mode = args.mode

    return _dispatch_module(CHECK_MODULES[mode], rest, f"livewire check {mode}")


def _dispatch_publish(argv: list[str]) -> int:
    """Push bronze data to Postgres or R2."""
    parser = argparse.ArgumentParser(prog="livewire publish")
    parser.add_argument("target", choices=["postgres", "r2"], nargs="?", default=None)
    parser.add_argument("--smoke", action="store_true",
                        help="Run smoke test instead of full rebuild (postgres only)")
    parser.add_argument("--migrate", action="store_true",
                        help="Run parquet schema migration")
    args, rest = parser.parse_known_args(argv)

    if args.migrate:
        return _dispatch_module(
            "livewire_scripts.migrate_parquet_filename", rest, "livewire publish --migrate"
        )

    if args.target is None:
        parser.error("target is required (postgres or r2) unless --migrate is set")

    if args.target == "postgres":
        if args.smoke:
            return _dispatch_module(
                "livewire_scripts.smoke_postgres_analytical", rest, "livewire publish postgres --smoke"
            )
        return _dispatch_module(PUBLISH_MODULES["postgres"], rest, "livewire publish postgres")

    return _dispatch_module(PUBLISH_MODULES["r2"], rest, "livewire publish r2")


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)

    parser = argparse.ArgumentParser(
        prog="livewire",
        description="Livewire market data warehouse CLI",
    )
    parser.add_argument(
        "command",
        choices=["sync", "backfill", "check", "publish"],
        help="Command to run",
    )

    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0

    args = parser.parse_args(argv[:1])
    rest = argv[1:]

    dispatch = {
        "sync": _dispatch_sync,
        "backfill": _dispatch_backfill,
        "check": _dispatch_check,
        "publish": _dispatch_publish,
    }

    return dispatch[args.command](rest)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_livewire_cli.py -v`
Expected: All pass

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing`
Expected: All pass, 100% coverage

- [ ] **Step 6: Commit**

```bash
git add scripts/livewire.py tests/test_livewire_cli.py
git commit -m "feat: unified livewire CLI with 4 commands and auto source selection"
```

---

### Task 2.2: IB preflight integration for unified CLI

The unified CLI needs the same IB preflight logic — check IB Gateway only when IB is actually needed.

**Files:**
- Modify: `scripts/livewire.py`
- Modify: `tests/test_livewire_cli.py`

- [ ] **Step 1: Write failing test**

```python
def test_sync_equity_massive_skips_ib_preflight(monkeypatch):
    """When MASSIVE_API_KEY is set, equity sync should not check IB Gateway."""
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    preflight_called = []
    monkeypatch.setattr(
        "clients.ib_gateway_preflight.assert_gateway_up",
        lambda: preflight_called.append(True),
    )
    with patch("scripts.livewire._dispatch_module", return_value=0):
        from scripts.livewire import _dispatch_sync
        _dispatch_sync(["--asset-class", "equity"])
    assert not preflight_called


def test_sync_futures_requires_ib_preflight(monkeypatch):
    """Futures sync always requires IB Gateway."""
    preflight_called = []
    monkeypatch.setattr(
        "clients.ib_gateway_preflight.assert_gateway_up",
        lambda: preflight_called.append(True),
    )
    with patch("scripts.livewire._dispatch_module", return_value=0):
        from scripts.livewire import _dispatch_sync
        _dispatch_sync(["--asset-class", "futures"])
    assert preflight_called
```

- [ ] **Step 2: Add preflight logic to dispatch functions**

Add `_needs_ib(asset_class, source)` helper that returns `True` when IB Gateway must be reachable. Call `assert_gateway_up()` only when needed.

```python
def _needs_ib(asset_class: str, has_massive: bool) -> bool:
    if asset_class in ("futures", "cmdty", "fx"):
        return True
    if asset_class == "volatility":
        return True  # intraday vol is IB-only
    if asset_class == "equity" and not has_massive:
        return True
    return False
```

- [ ] **Step 3: Run tests**

Run: `python -m pytest tests/test_livewire_cli.py -v`
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add scripts/livewire.py tests/test_livewire_cli.py
git commit -m "feat: smart IB preflight in unified CLI — skip when Massive covers equity"
```

---

## Phase 3: Python Orchestrators (replace bash)

### Task 3.1: Build `sync_runner.py` (replaces `run_daily_backfill.sh`)

The 6-phase daily catch-up runner, now in testable Python.

**Files:**
- Create: `livewire_scripts/sync_runner.py`
- Create: `tests/test_sync_runner.py`

- [ ] **Step 1: Write failing test for phase orchestration**

```python
"""Tests for daily sync orchestrator (replaces run_daily_backfill.sh)."""
from unittest.mock import MagicMock, patch, call
from pathlib import Path

import pytest


def test_sync_runner_executes_all_phases(monkeypatch):
    from livewire_scripts.sync_runner import run_sync, SyncConfig
    calls = []
    monkeypatch.setattr(
        "livewire_scripts.sync_runner._run_phase",
        lambda name, fn, *a, **kw: calls.append(name) or 0,
    )
    config = SyncConfig(
        warehouse_dir=Path("/tmp/test-warehouse"),
        massive_available=True,
        ib_available=False,
        postgres_dsn=None,
    )
    result = run_sync(config, dry_run=True)
    assert "equity-daily" in calls
    assert "rates" in calls
    assert "volatility-daily" in calls
    assert result == 0


def test_sync_runner_skips_postgres_when_no_dsn(monkeypatch):
    from livewire_scripts.sync_runner import run_sync, SyncConfig
    calls = []
    monkeypatch.setattr(
        "livewire_scripts.sync_runner._run_phase",
        lambda name, fn, *a, **kw: calls.append(name) or 0,
    )
    config = SyncConfig(
        warehouse_dir=Path("/tmp/test-warehouse"),
        massive_available=True,
        ib_available=False,
        postgres_dsn=None,
    )
    run_sync(config, dry_run=True)
    assert "postgres" not in calls


def test_sync_runner_includes_postgres_when_dsn_set(monkeypatch):
    from livewire_scripts.sync_runner import run_sync, SyncConfig
    calls = []
    monkeypatch.setattr(
        "livewire_scripts.sync_runner._run_phase",
        lambda name, fn, *a, **kw: calls.append(name) or 0,
    )
    config = SyncConfig(
        warehouse_dir=Path("/tmp/test-warehouse"),
        massive_available=True,
        ib_available=False,
        postgres_dsn="postgresql://localhost/test",
    )
    run_sync(config, dry_run=True)
    assert "postgres" in calls
```

- [ ] **Step 2: Run tests, confirm failure**

Run: `python -m pytest tests/test_sync_runner.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement `livewire_scripts/sync_runner.py`**

The sync runner orchestrates these phases in order:

```
Phase 1: Equity daily     (Massive if available, else IB)
Phase 2: FRED rates        (always, no IB needed)
Phase 3: CBOE vol daily    (always, no IB needed)
Phase 4: Equity intraday   (Massive if available; 1m+5m+30m+1h, recent window)
Phase 5: Vol intraday      (IB only; VIX/SPX/NDX/RUT 5m+1h, recent window)
Phase 6: Postgres rebuild   (if MDW_POSTGRES_DSN set)
```

Phases 1-3 run sequentially (fast, ~minutes). Phases 4-5 can run in parallel (ThreadPoolExecutor). Phase 6 runs last.

```python
"""Daily sync orchestrator — Python replacement for run_daily_backfill.sh.

Runs all data catch-up phases in order with proper error handling,
logging, and conditional Postgres rebuild. Phases 4-5 run in parallel.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
# Use the unified CLI — old scripts are removed in Phase 4
LIVEWIRE_SCRIPT = REPO_ROOT / "scripts" / "livewire.py"

log = logging.getLogger("livewire.sync_runner")


@dataclass(frozen=True)
class SyncConfig:
    warehouse_dir: Path
    massive_available: bool
    ib_available: bool
    postgres_dsn: str | None
    intraday_days: int = 7
    intraday_concurrent: int = 20
    python_bin: str = sys.executable


@dataclass
class PhaseResult:
    name: str
    exit_code: int
    duration_seconds: float
    error: str | None = None


def _run_phase(name: str, fn: Callable[[], int], **kwargs) -> PhaseResult:
    start = time.monotonic()
    try:
        code = fn()
        return PhaseResult(name=name, exit_code=code,
                          duration_seconds=time.monotonic() - start)
    except Exception as exc:
        return PhaseResult(name=name, exit_code=1,
                          duration_seconds=time.monotonic() - start,
                          error=str(exc))


def _subprocess(args: list[str], label: str) -> int:
    log.info("[%s] Running: %s", label, " ".join(args))
    result = subprocess.run(args, capture_output=False)
    return result.returncode


def run_sync(config: SyncConfig, dry_run: bool = False) -> int:
    results: list[PhaseResult] = []
    dry = ["--dry-run"] if dry_run else []
    py = config.python_bin

    presets = ["presets/sp500.json", "presets/ndx100.json", "presets/r2k.json"]

    # Phase 1: Equity daily
    def equity_daily():
        source = ["--source", "massive"] if config.massive_available else []
        code = 0
        for preset in presets:
            rc = _subprocess(
                [py, str(LIVEWIRE_SCRIPT), "sync", "--asset-class", "equity", "--preset", preset] + source + dry,
                "equity-daily",
            )
            code = max(code, rc)
        return code

    results.append(_run_phase("equity-daily", equity_daily))

    # Phase 2: FRED rates
    results.append(_run_phase("rates", lambda: _subprocess(
        [py, str(LIVEWIRE_SCRIPT), "sync", "--asset-class", "rates"] + dry, "rates"
    )))

    # Phase 3: CBOE vol daily
    results.append(_run_phase("volatility-daily", lambda: _subprocess(
        [py, str(LIVEWIRE_SCRIPT), "sync", "--asset-class", "volatility"] + dry, "volatility-daily"
    )))

    # Phases 4-5: Parallel intraday lanes
    days_arg = ["--days", str(config.intraday_days)]

    def equity_intraday():
        if not config.massive_available:
            return 0
        code = 0
        for tf in ("1m", "5m", "30m", "1h"):
            for preset in presets:
                rc = _subprocess(
                    [py, str(LIVEWIRE_SCRIPT), "backfill",
                     "--timeframe", tf, "--source", "massive",
                     "--preset", preset,
                     "--max-concurrent", str(config.intraday_concurrent)]
                    + days_arg + dry,
                    f"equity-intraday-{tf}",
                )
                code = max(code, rc)
        return code

    def vol_intraday():
        if not config.ib_available:
            return 0
        code = 0
        for tf in ("5m", "1h"):
            rc = _subprocess(
                [py, str(LIVEWIRE_SCRIPT), "backfill",
                 "--timeframe", tf, "--source", "ib",
                 "--asset-class", "volatility",
                 "--preset", "presets/volatility-intraday.json"]
                + days_arg + dry,
                f"vol-intraday-{tf}",
            )
            code = max(code, rc)
        return code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = {
            pool.submit(lambda: _run_phase("equity-intraday", equity_intraday)): "equity-intraday",
            pool.submit(lambda: _run_phase("vol-intraday", vol_intraday)): "vol-intraday",
        }
        for fut in as_completed(futs):
            results.append(fut.result())

    # Phase 6: Postgres rebuild (conditional)
    if config.postgres_dsn:
        results.append(_run_phase("postgres", lambda: _subprocess(
            [py, str(LIVEWIRE_SCRIPT), "publish", "postgres",
             "--asset-class", "equity", "--timeframe", "all"] + dry,
            "postgres",
        )))

    # Summary
    failures = [r for r in results if r.exit_code != 0]
    for r in results:
        status = "OK" if r.exit_code == 0 else "FAIL"
        log.info("  [%s] %s (%.1fs)", status, r.name, r.duration_seconds)

    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Daily sync orchestrator")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = SyncConfig(
        warehouse_dir=Path(os.getenv("MDW_WAREHOUSE_DIR",
                                     str(Path.home() / "market-warehouse"))),
        massive_available=bool(os.environ.get("MASSIVE_API_KEY")),
        ib_available=_ib_reachable(),
        postgres_dsn=os.environ.get("MDW_POSTGRES_DSN"),
        intraday_days=int(os.getenv("MDW_DAILY_BACKFILL_INTRADAY_DAYS", "7")),
        intraday_concurrent=int(os.getenv("MDW_DAILY_BACKFILL_INTRADAY_CONCURRENT", "20")),
    )

    return run_sync(config, dry_run=args.dry_run)


def _ib_reachable() -> bool:
    import socket
    try:
        with socket.create_connection(("127.0.0.1", 4001), timeout=2):
            return True
    except OSError:
        return False


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_sync_runner.py -v`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/sync_runner.py tests/test_sync_runner.py
git commit -m "feat: Python sync runner replacing run_daily_backfill.sh"
```

---

### Task 3.2: Build `backfill_runner.py` (replaces `run_backfill_all.sh`)

Same pattern as sync_runner but for deep historical backfill with more phases.

**Files:**
- Create: `livewire_scripts/backfill_runner.py`
- Create: `tests/test_backfill_runner.py`

- [ ] **Step 1: Write failing test for backfill orchestration**

```python
"""Tests for deep backfill orchestrator (replaces run_backfill_all.sh)."""
from unittest.mock import patch
from pathlib import Path

import pytest


def test_backfill_runner_executes_daily_then_intraday(monkeypatch):
    from livewire_scripts.backfill_runner import run_backfill, BackfillConfig
    calls = []
    monkeypatch.setattr(
        "livewire_scripts.backfill_runner._run_phase",
        lambda name, fn, *a, **kw: calls.append(name) or 0,
    )
    config = BackfillConfig(
        warehouse_dir=Path("/tmp/test"),
        massive_available=True,
        ib_available=False,
        postgres_dsn=None,
        timeframes=["1d", "1h", "5m"],
    )
    run_backfill(config, dry_run=True)
    # Daily should run before intraday
    assert calls.index("equity-daily") < calls.index("equity-intraday-1h")


def test_backfill_runner_all_timeframes(monkeypatch):
    from livewire_scripts.backfill_runner import run_backfill, BackfillConfig
    calls = []
    monkeypatch.setattr(
        "livewire_scripts.backfill_runner._run_phase",
        lambda name, fn, *a, **kw: calls.append(name) or 0,
    )
    config = BackfillConfig(
        warehouse_dir=Path("/tmp/test"),
        massive_available=True,
        ib_available=True,
        postgres_dsn=None,
        timeframes=["1d", "1h", "30m", "5m", "1m"],
    )
    run_backfill(config, dry_run=True)
    phase_names = [c for c in calls if "intraday" in c]
    assert len(phase_names) >= 4  # 1h + 30m + 5m + 1m
```

- [ ] **Step 2: Implement `livewire_scripts/backfill_runner.py`**

Orchestrates these lanes:

```
Lane A (Massive, parallel): Equity daily (5yr) → seed then backfill, 3 presets
Lane B (Massive, parallel): Equity 1m (5yr), 3 presets
Lane C (Massive, parallel): Equity 5m (5yr), 3 presets
Lane D (Massive, parallel): Equity 30m (5yr), 3 presets
Lane E (Massive, parallel): Equity 1h (5yr), 3 presets
Lane F (IB, sequential):    Equity daily inception (>5yr, IB backfill)
Lane G (FRED):              Rates (DGS3/5/10/30)
Lane H (CBOE):              Volatility daily
Lane I (IB):                Volatility intraday (5m+1h)
Lane J:                     Postgres rebuild (if DSN set)
```

Massive lanes (A-E) run first in parallel. IB lanes (F, I) run after. FRED (G) and CBOE (H) run alongside Massive (no rate limit conflicts). Postgres (J) runs last.

```python
"""Deep backfill orchestrator — Python replacement for run_backfill_all.sh."""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
LIVEWIRE_SCRIPT = REPO_ROOT / "scripts" / "livewire.py"

log = logging.getLogger("livewire.backfill_runner")

PRESETS = ["presets/sp500.json", "presets/ndx100.json", "presets/r2k.json"]


@dataclass(frozen=True)
class BackfillConfig:
    warehouse_dir: Path
    massive_available: bool
    ib_available: bool
    postgres_dsn: str | None
    timeframes: list[str] = field(default_factory=lambda: ["1d", "1h", "30m", "5m", "1m"])
    python_bin: str = sys.executable


@dataclass
class PhaseResult:
    name: str
    exit_code: int
    duration_seconds: float
    error: str | None = None


def _run_phase(name: str, fn: Callable[[], int]) -> PhaseResult:
    start = time.monotonic()
    try:
        code = fn()
        return PhaseResult(name=name, exit_code=code,
                          duration_seconds=time.monotonic() - start)
    except Exception as exc:
        return PhaseResult(name=name, exit_code=1,
                          duration_seconds=time.monotonic() - start,
                          error=str(exc))


def _subprocess(args: list[str], label: str) -> int:
    log.info("[%s] Running: %s", label, " ".join(args))
    result = subprocess.run(args, capture_output=False)
    return result.returncode


def run_backfill(config: BackfillConfig, dry_run: bool = False) -> int:
    results: list[PhaseResult] = []
    dry = ["--dry-run"] if dry_run else []
    py = config.python_bin
    tfs = config.timeframes

    # --- Stage 1: Massive equity lanes (parallel) + non-IB sources ---
    stage1_fns: list[tuple[str, Callable[[], int]]] = []

    if "1d" in tfs and config.massive_available:
        def equity_daily_massive():
            code = 0
            for preset in PRESETS:
                rc = _subprocess(
                    [py, str(LIVEWIRE_SCRIPT), "backfill", "--timeframe", "1d",
                     "--preset", preset, "--skip-existing"] + dry,
                    "equity-daily",
                )
                code = max(code, rc)
            return code
        stage1_fns.append(("equity-daily", equity_daily_massive))

    for tf in ("1m", "5m", "30m", "1h"):
        if tf not in tfs:
            continue
        if not config.massive_available:
            continue
        def make_intraday_fn(timeframe=tf):
            def fn():
                code = 0
                for preset in PRESETS:
                    rc = _subprocess(
                        [py, str(LIVEWIRE_SCRIPT), "backfill", "--timeframe", timeframe,
                         "--preset", preset, "--skip-existing"] + dry,
                        f"equity-intraday-{timeframe}",
                    )
                    code = max(code, rc)
                return code
            return fn
        stage1_fns.append((f"equity-intraday-{tf}", make_intraday_fn()))

    # FRED + CBOE run alongside Massive (no rate limit conflicts)
    stage1_fns.append(("rates", lambda: _subprocess(
        [py, str(LIVEWIRE_SCRIPT), "sync", "--asset-class", "rates"] + dry, "rates"
    )))
    stage1_fns.append(("volatility-daily", lambda: _subprocess(
        [py, str(LIVEWIRE_SCRIPT), "sync", "--asset-class", "volatility"] + dry, "volatility-daily"
    )))

    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {
            pool.submit(lambda n=n, f=f: _run_phase(n, f)): n
            for n, f in stage1_fns
        }
        for fut in as_completed(futs):
            results.append(fut.result())

    # --- Stage 2: IB lanes (sequential, pacing-limited) ---
    if config.ib_available:
        if "1d" in tfs:
            results.append(_run_phase("equity-daily-ib-inception", lambda: _subprocess(
                [py, str(LIVEWIRE_SCRIPT), "backfill", "--timeframe", "1d",
                 "--years", "0", "--skip-existing"] + dry,
                "equity-daily-ib-inception",
            )))

        for tf in ("5m", "1h"):
            if tf not in tfs:
                continue
            results.append(_run_phase(f"vol-intraday-{tf}", lambda tf=tf: _subprocess(
                [py, str(LIVEWIRE_SCRIPT), "backfill", "--timeframe", tf,
                 "--asset-class", "volatility",
                 "--preset", "presets/volatility-intraday.json"] + dry,
                f"vol-intraday-{tf}",
            )))

    # --- Stage 3: Postgres rebuild (if DSN set) ---
    if config.postgres_dsn:
        results.append(_run_phase("postgres", lambda: _subprocess(
            [py, str(LIVEWIRE_SCRIPT), "publish", "postgres",
             "--asset-class", "equity", "--timeframe", "all"] + dry,
            "postgres",
        )))

    # Summary
    failures = [r for r in results if r.exit_code != 0]
    for r in results:
        status = "OK" if r.exit_code == 0 else "FAIL"
        log.info("  [%s] %s (%.1fs)", status, r.name, r.duration_seconds)

    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Deep backfill orchestrator")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeframe", nargs="+", default=["1d", "1h", "30m", "5m", "1m"])
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = BackfillConfig(
        warehouse_dir=Path(os.getenv("MDW_WAREHOUSE_DIR",
                                     str(Path.home() / "market-warehouse"))),
        massive_available=bool(os.environ.get("MASSIVE_API_KEY")),
        ib_available=_ib_reachable(),
        postgres_dsn=os.environ.get("MDW_POSTGRES_DSN"),
        timeframes=args.timeframe,
    )

    return run_backfill(config, dry_run=args.dry_run)


def _ib_reachable() -> bool:
    import socket
    try:
        with socket.create_connection(("127.0.0.1", 4001), timeout=2):
            return True
    except OSError:
        return False


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 3: Run tests**

Run: `python -m pytest tests/test_backfill_runner.py -v`
Expected: All pass

- [ ] **Step 4: Run full suite**

Run: `python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing`
Expected: All pass, 100% coverage

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/backfill_runner.py tests/test_backfill_runner.py
git commit -m "feat: Python backfill runner replacing run_backfill_all.sh"
```

---

### Task 3.3: Wire orchestrators into unified CLI

**Files:**
- Modify: `scripts/livewire.py`
- Modify: `tests/test_livewire_cli.py`

- [ ] **Step 1: Write failing test for orchestrator dispatch**

Add to `tests/test_livewire_cli.py`:

```python
def test_sync_all_uses_sync_runner(monkeypatch):
    """livewire sync (no --asset-class) dispatches to sync_runner."""
    from unittest.mock import MagicMock
    mock_run = MagicMock(return_value=0)
    monkeypatch.setattr("livewire_scripts.sync_runner.run_sync", mock_run)
    from scripts.livewire import _dispatch_sync
    _dispatch_sync(["--dry-run"])
    mock_run.assert_called_once()


def test_backfill_all_uses_backfill_runner(monkeypatch):
    """livewire backfill --timeframe all dispatches to backfill_runner."""
    from unittest.mock import MagicMock
    mock_run = MagicMock(return_value=0)
    monkeypatch.setattr("livewire_scripts.backfill_runner.run_backfill", mock_run)
    from scripts.livewire import _dispatch_backfill
    _dispatch_backfill(["--timeframe", "all", "--dry-run"])
    mock_run.assert_called_once()


def test_sync_single_asset_class_bypasses_runner(monkeypatch):
    """livewire sync --asset-class equity dispatches directly to module."""
    dispatched = []
    monkeypatch.setattr(
        "scripts.livewire._dispatch_module",
        lambda mod, argv, display: dispatched.append(mod) or 0,
    )
    from scripts.livewire import _dispatch_sync
    _dispatch_sync(["--asset-class", "equity"])
    assert "livewire_scripts.daily_update" in dispatched
```

- [ ] **Step 2: Update `_dispatch_sync` and `_dispatch_backfill`**

In `_dispatch_sync`: when `args.asset_class == "all"` and `not args.scheduled`, build a `SyncConfig` and call `sync_runner.run_sync()` directly instead of looping through modules.

In `_dispatch_backfill`: when `"all" in args.timeframe`, build a `BackfillConfig` and call `backfill_runner.run_backfill()` directly.

- [ ] **Step 3: Run tests**

Run: `python -m pytest tests/test_livewire_cli.py -v`
Expected: All pass

- [ ] **Step 4: Run full suite**

Run: `python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing`
Expected: All pass, 100% coverage

- [ ] **Step 5: Commit**

```bash
git add scripts/livewire.py tests/test_livewire_cli.py
git commit -m "feat: wire sync_runner and backfill_runner into unified CLI"
```

---

## Phase 4: Cleanup

### Task 4.1: Remove old entry points and bash scripts

**Files to delete:**
- `scripts/livewire_ingest.py`
- `scripts/livewire_quality.py`
- `scripts/livewire_ops.py`
- `scripts/livewire_store.py`
- `tools/run_backfill_all.sh`
- `tools/run_daily_backfill.sh`
- `livewire_scripts/probe_ib_intraday.py`
- `livewire_scripts/intraday_update.py`
- `tests/test_livewire_entrypoints.py`
- `tests/test_script_consolidation.py`
- `tests/test_intraday_update.py`

- [ ] **Step 1: Verify no remaining imports of deleted modules**

```bash
grep -rn "livewire_ingest\|livewire_quality\|livewire_ops\|livewire_store\|probe_ib_intraday\|intraday_update" livewire_scripts/ clients/ scripts/ tests/ --include="*.py" | grep -v __pycache__ | grep -v "# deleted"
```

Fix any remaining references.

- [ ] **Step 2: Update `run_daily_update_job.py`**

This module shells out to old scripts via `INGEST_SCRIPT`, `OPS_SCRIPT`, and `QUALITY_SCRIPT` constants (lines 20-22). Update all three to use the unified CLI:

```python
# Before:
INGEST_SCRIPT = REPO_ROOT / "scripts" / "livewire_ingest.py"
OPS_SCRIPT = REPO_ROOT / "scripts" / "livewire_ops.py"
QUALITY_SCRIPT = REPO_ROOT / "scripts" / "livewire_quality.py"

# After:
LIVEWIRE_SCRIPT = REPO_ROOT / "scripts" / "livewire.py"
```

Then update every subprocess call:
- `[py, str(INGEST_SCRIPT), "daily", ...]` → `[py, str(LIVEWIRE_SCRIPT), "sync", "--asset-class", "equity", ...]`
- `[py, str(INGEST_SCRIPT), "cboe-vol"]` → `[py, str(LIVEWIRE_SCRIPT), "sync", "--asset-class", "volatility"]`
- `[py, str(QUALITY_SCRIPT), "watchdog", ...]` → `[py, str(LIVEWIRE_SCRIPT), "check", "--mode", "health", ...]`
- `[py, str(OPS_SCRIPT), "send-alert", ...]` → keep the node subprocess call for `send-alert` (it's a Node.js script, not a Python dispatch)

- [ ] **Step 3: Delete files**

```bash
git rm scripts/livewire_ingest.py scripts/livewire_quality.py scripts/livewire_ops.py scripts/livewire_store.py
git rm tools/run_backfill_all.sh tools/run_daily_backfill.sh
git rm livewire_scripts/probe_ib_intraday.py livewire_scripts/intraday_update.py
git rm tests/test_livewire_entrypoints.py tests/test_script_consolidation.py tests/test_intraday_update.py
```

- [ ] **Step 4: Update `pyproject.toml` coverage omit list**

Remove `probe_ib_intraday.py` from the omit list (file no longer exists).

- [ ] **Step 5: Run full test suite**

Run: `python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing`
Expected: All pass, 100% coverage

- [ ] **Step 6: Commit**

```bash
git commit -m "chore: remove old CLI entry points and bash orchestrators"
```

---

### Task 4.2: Update documentation and launchd templates

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `launchd/com.livewire.daily-update.plist.example`

- [ ] **Step 1: Update CLAUDE.md**

Replace all `python scripts/livewire_ingest.py ...` examples with:

```bash
# Daily catch-up (all sources, auto-selected)
python scripts/livewire.py sync

# Deep historical backfill (all timeframes, all presets)
python scripts/livewire.py backfill

# Specific timeframe backfill
python scripts/livewire.py backfill --timeframe 5m 30m

# Quality check
python scripts/livewire.py check

# Coverage report
python scripts/livewire.py check --mode coverage

# Publish to Postgres
python scripts/livewire.py publish postgres

# Publish to R2
python scripts/livewire.py publish r2

# Scheduled daily run (with retry + alerting)
python scripts/livewire.py sync --scheduled
```

Update the "Running the pipeline" section, "Daily updates" section, "Intraday backfill" section, and "Rebuilding Postgres" section.

Add a "CLI Quick Reference" section:

```
livewire sync       Make all asset classes current (daily catch-up)
livewire backfill   Fill deep history to maximum provider depth
livewire check      Quality, health, coverage reporting
livewire publish    Push bronze to Postgres or R2
```

- [ ] **Step 2: Update launchd template**

Change the program arguments from:

```xml
<string>scripts/livewire_ops.py</string>
<string>run-daily-job</string>
```

To:

```xml
<string>scripts/livewire.py</string>
<string>sync</string>
<string>--scheduled</string>
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md README.md launchd/
git commit -m "docs: update CLI examples and launchd templates for unified CLI"
```

---

## Phase 5: Maximum Coverage Backfill

This is not code — this is the operational run plan after shipping Phases 1-4.

### Task 5.1: Pre-backfill R2 snapshot

```bash
# Backup current bronze to R2 before the big backfill
python scripts/livewire.py publish r2
```

### Task 5.2: Run full backfill

```bash
# Full backfill: all timeframes, all presets, maximum depth
# Massive Starter = 5yr for all timeframes
# IB = inception for daily, ~5yr for intraday
python scripts/livewire.py backfill --timeframe all --skip-existing
```

Expected timeline (rough):
- Equity daily (5yr, ~2400 tickers via Massive): ~30 min
- Equity 1m (5yr, ~2400 tickers via Massive): ~4-8 hours
- Equity 5m (5yr, ~2400 tickers via Massive): ~2-4 hours
- Equity 30m (5yr, ~2400 tickers via Massive): ~2-4 hours
- Equity 1h (5yr, ~2400 tickers via Massive): ~1-2 hours
- IB equity daily (inception, older than 5yr): ~2-4 hours
- CBOE vol daily: ~5 min
- IB vol intraday: ~30 min
- FRED rates: ~1 min

### Task 5.3: Post-backfill verification

```bash
# Coverage check
python scripts/livewire.py check --mode coverage

# Health check (intraday gaps)
python scripts/livewire.py check --mode health --intraday --timeframe 5m
python scripts/livewire.py check --mode health --intraday --timeframe 30m
python scripts/livewire.py check --mode health --intraday --timeframe 1h

# Postgres rebuild
python scripts/livewire.py publish postgres

# Final R2 sync
python scripts/livewire.py publish r2
```

---

## Summary: What Changed

| Metric | Before | After |
|--------|--------|-------|
| CLI scripts | 4 | 1 |
| Subcommands | 21 | 4 |
| Bash orchestrators | 2 (20K lines) | 0 (Python) |
| Duplicated code | ~160 lines across 6 functions | 0 (extracted to `clients/ingestion_common.py`) |
| Intraday timeframes | 3 (1m, 1h, 5m) | 4 (1m, 30m, 5m, 1h) |
| 1h depth | 2 years | 5 years |
| 5m depth | 1 year | 5 years |
| R2 sync | Full re-upload every time | Incremental (size-based) |
| Source selection | Manual `--source massive --asset-class equity` | Auto (Massive if key present) |
| Diagnostic scripts | 2 (probe, intraday-status) | 0 (folded or deleted) |
| Test files removed | 3 | — |
| Test files added | 4 | — |
| New modules | 3 (`ingestion_common`, `sync_runner`, `backfill_runner`) | — |
