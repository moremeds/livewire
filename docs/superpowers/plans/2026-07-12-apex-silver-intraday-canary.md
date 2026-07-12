# Apex Silver Intraday and Canary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Apex read adjusted daily Silver and apply Livewire factor intervals to raw intraday bars, then validate the complete path in shadow mode.

**Architecture:** Extend LivewireOhlcProvider with separate bronze/Silver roots and explicit price mode. Daily adjusted reads use materialized Silver; intraday adjusted reads use one DuckDB range join against compact factor intervals. Raw mode remains available for diagnostics during canary.

**Tech Stack:** Python 3.13, DuckDB, PyArrow fixtures, pytest, FastAPI.

## Global Constraints

- Work in `/Users/moremeds/projects/apex` on branch `feat/silver-adjusted-bars` after the watcher and Livewire Silver engine merge.
- Default remains `raw` in this PR; cutover is separate.
- Never silently fall back from adjusted to raw.
- Trading-date factor joins use `America/New_York` for intraday timestamps.
- Existing supported timeframes remain `1m`, `5m`, `30m`, `1h`, and `1d`.

---

### Task 1: Dual-root path contract

**Files:**
- Modify: `src/infrastructure/adapters/livewire/paths.py`
- Test: `tests/unit/infrastructure/livewire/test_paths.py`

- [ ] Write failing tests for bronze intraday, Silver daily, factor paths, encoded symbols, and unsupported timeframe.
- [ ] Add `daily_silver_path(silver_root, symbol)` and `factor_path(silver_root, symbol)` while retaining `parquet_path` for bronze.
- [ ] Run focused tests; expect PASS.
- [ ] Commit with `git commit -m "feat: resolve Livewire Silver paths"`.

### Task 2: Adjusted provider queries

**Files:**
- Modify: `src/infrastructure/adapters/livewire/ohlc_provider.py`
- Test: `tests/unit/infrastructure/livewire/test_ohlc_provider.py`

**Interfaces:**
- Produces constructor `LivewireOhlcProvider(bronze_root: Path, silver_root: Path | None = None, price_mode: Literal["raw", "adjusted"] = "raw")`.

- [ ] Create fixtures spanning a split and dividend for daily and 1m data; write tests proving adjusted OHLC, split-only volume, identity factors after the latest action, and explicit failure on missing factors.
- [ ] Verify tests fail against the existing one-root provider.
- [ ] For daily adjusted mode, query Silver `1d.parquet` directly.
- [ ] For intraday adjusted mode, use DuckDB SQL equivalent to:

```sql
SELECT b.bar_timestamp,
       b.open * f.price_adjustment_factor AS open,
       b.high * f.price_adjustment_factor AS high,
       b.low * f.price_adjustment_factor AS low,
       b.close * f.price_adjustment_factor AS close,
       CAST(ROUND(b.volume * f.split_volume_factor) AS BIGINT) AS volume
FROM read_parquet(?) b
JOIN read_parquet(?) f
  ON CAST(timezone('America/New_York', b.bar_timestamp) AS DATE)
     >= COALESCE(f.effective_start, DATE '0001-01-01')
 AND CAST(timezone('America/New_York', b.bar_timestamp) AS DATE)
     <= COALESCE(f.effective_end, DATE '9999-12-31')
WHERE b.bar_timestamp BETWEEN ? AND ?
ORDER BY b.bar_timestamp
```

- [ ] Assert query row count equals raw row count; a missing interval raises `AdjustedDataUnavailable`.
- [ ] Run provider tests; expect PASS.
- [ ] Commit with `git commit -m "feat: read adjusted Livewire bars"`.

### Task 3: Server configuration and health

**Files:**
- Modify: `src/api/server.py`
- Modify: `src/api/routes/health.py`
- Modify: `README.md`
- Modify: `docs/livewire-apex-integration.md`
- Test: `tests/unit/api/test_server_lifespan.py`
- Test: `tests/unit/api/test_health.py`

- [ ] Test `APEX_LIVEWIRE_SILVER_ROOT` and `APEX_LIVEWIRE_PRICE_MODE=adjusted` wiring, invalid mode startup failure, and health reporting.
- [ ] Construct one provider instance shared by chart and subscription paths.
- [ ] Keep default mode raw and report configured/effective mode plus revision state.
- [ ] Run focused tests; expect PASS.
- [ ] Commit with `git commit -m "feat: configure adjusted Livewire reads"`.

### Task 4: End-to-end shadow canary

**Files:**
- Create: `scripts/check_silver_canary.py`
- Create: `tests/integration/test_silver_revision_e2e.py`
- Modify: `/Users/moremeds/apex-deploy/compose.yml` only after explicit deployment approval.

- [ ] Build an integration fixture that starts FastAPI with an active NVDA subscription, atomically replaces Silver fixtures/current manifest, and proves watcher reseed without process restart.
- [ ] Test NVDA split continuity, AAPL/SPY dividend continuity, control-symbol identity, daily chart reads, intraday joins, and subscription revision health.
- [ ] Implement a read-only canary script comparing raw vs adjusted returns and revision state; it must not modify Livewire artifacts.
- [ ] Run `uv run pytest tests/integration/test_silver_revision_e2e.py -q --no-cov`; expect PASS.
- [ ] Run `uv run pytest tests -q` and `uv run mypy src/ --ignore-missing-imports`; expect PASS.
- [ ] Commit with `git commit -m "test: validate Silver revisions end to end"`.
