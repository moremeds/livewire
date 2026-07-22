# Livewire Silver Adjustment Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Derive adjusted daily bars and compact intraday factor intervals from raw bronze plus canonical corporate actions, then publish an atomic revision manifest.

**Architecture:** A pure Decimal-based factor engine creates deterministic intervals. Focused Silver storage clients materialize daily data and factor Parquet. A locked publisher validates every artifact and advances `current.json` last as the transaction commit record.

**Tech Stack:** Python 3.13, Decimal, PyArrow, JSON, SHA-256, file locks, pytest/Hypothesis.

## Global Constraints

- Work in `/Users/moremeds/projects/livewire` on branch `feat/silver-adjustment-engine` after corporate-action ingestion merges.
- Bronze bars and events are read-only inputs.
- Cash dividends adjust prices only; splits adjust prices and volume.
- Latest fully back-adjusted history is the default Silver representation.
- Missing/invalid reference closes block publication for the affected symbol.
- Manifest `current.json` advances only after every listed artifact is valid.
- Coordinate `MDW_SILVER_DIR` with uplift Plan 6 path resolution and lock conventions with Plan 8.

## Reality check (verified 2026-07-12)

- **`hypothesis` is NOT yet a dev dependency.** `pyproject.toml` dev deps are
  `pytest`, `pytest-cov`, `responses`, `ruff`, `pyright`. Task 1's property tests
  require adding `hypothesis` to `[dependency-groups].dev` (or equivalent) and
  `uv sync --dev` **before** writing them — do this as the first step of Task 1.
- **Plan 6 (`unify-warehouse-path-resolution`) and Plan 8
  (`add-bronze-merge-locking`) have landed on `main`.** Resolve the Silver default
  from `livewire_scripts.paths.data_lake_dir()` while honoring an explicit
  `MDW_SILVER_DIR`. Reuse `clients.parquet_io.symbol_lock` for per-artifact
  mutation. The Silver revision publication lock remains a **new, coarser**
  root-level lock because one revision atomically covers multiple artifacts;
  implement it with the same persistent sidecar + `fcntl.flock` convention.
- `publish_parquet(..., sort_column=...)` (`clients/parquet_io.py:29`) and
  `encode_symbol` (`clients/symbol_paths.py:10`) exist and are reusable. New
  `clients/{adjustment_engine,silver_client,silver_revision}.py` are all inside
  the 95% coverage gate (only `ib_client.py`/`historical_provider.py` are omitted).
- `livewire_scripts/run_daily_update_job.py` exists; its `_spawn_post_success_quality`
  fire-and-log seam (called from `main()`) is the insertion point for the
  post-ingestion Silver reconciliation in Task 5.

---

### Task 1: Pure adjustment factor engine

**Files:**
- Create: `clients/adjustment_engine.py`
- Test: `tests/test_adjustment_engine.py`

**Interfaces:**
- Produces: `build_factor_intervals(bars, actions) -> list[FactorInterval]`.
- Produces: `adjust_daily_rows(rows, intervals, revision) -> list[dict]`.

- [ ] Add `hypothesis` to dev dependencies and run `uv sync --dev` (it is not currently installed).
- [ ] Write example and Hypothesis tests for 4:1 and 10:1 cumulative NVDA-style splits, recurring dividends, same-day split/dividend ordering, no-action identity, volume invariance under dividends, duplicate dates, missing prior close, currency mismatch, and invalid event values.
- [ ] Run `uv run pytest tests/test_adjustment_engine.py -q`; expect import failure.
- [ ] Implement factors with `Decimal(str(value))`, stable action ordering `(ex_date, split-before-dividend, action_id)`, and conversion to float only at artifact boundaries.
- [ ] Require exhaustive, ordered, non-overlapping factor intervals over the bronze date range.
- [ ] Run tests; expect PASS.
- [ ] Commit with `git commit -m "feat: compute corporate action factors"`.

Core formula test:

```python
def test_dividend_adjusts_price_not_volume() -> None:
    intervals = build_factor_intervals(_bars(close=100), [_dividend(ex_date="2026-01-03", cash="1")])
    adjusted = adjust_daily_rows(_bars(close=100), intervals, revision=1)
    assert adjusted[0]["close"] == pytest.approx(99.0)
    assert adjusted[0]["volume"] == 1_000
```

### Task 2: Silver Parquet publishers

**Files:**
- Create: `clients/silver_client.py`
- Test: `tests/test_silver_client.py`

**Interfaces:**
- Produces: `SilverClient.publish_daily(symbol, rows) -> PublishedArtifact`.
- Produces: `SilverClient.publish_factors(symbol, intervals) -> PublishedArtifact`.

- [ ] Write failing schema, factor-identity, sorting, duplicate, atomic-replace, and checksum tests.
- [ ] Implement paths `silver/asset_class=equity/symbol={encoded}/1d.parquet` and `silver/adjustments/asset_class=equity/symbol={encoded}/factors.parquet` using existing symbol encoding and `publish_parquet`.
- [ ] Preserve Apex-required daily columns and append factor/revision columns.
- [ ] Run `uv run pytest tests/test_silver_client.py -q`; expect PASS.
- [ ] Commit with `git commit -m "feat: publish Silver adjustment artifacts"`.

### Task 3: Transactional revision publisher

**Files:**
- Create: `clients/silver_revision.py`
- Test: `tests/test_silver_revision.py`

**Interfaces:**
- Produces: `SilverRevisionPublisher.publish(artifacts, affected, actions_as_of) -> SilverRevision`.

- [ ] Write failing tests for monotonic revisions, concurrent lock exclusion, no-op behavior, immutable manifest, checksum mismatch, partial artifact failure, and atomic `current.json` replacement.
- [ ] Implement a Silver-root lock file, read-current-under-lock, SHA-256 calculation, strict schema-v1 serializer, immutable `revision={n}.json`, then atomic `current.json` last.
- [ ] Ensure failure before the final replace leaves the previous current manifest byte-identical.
- [ ] Run focused tests; expect PASS.
- [ ] Commit with `git commit -m "feat: publish atomic Silver revisions"`.

### Task 4: Rebuild and incremental CLI

**Files:**
- Create: `livewire_scripts/rebuild_silver.py`
- Modify: `scripts/livewire_store.py`
- Test: `tests/test_rebuild_silver.py`
- Modify: `tests/test_livewire_entrypoints.py`

**Interfaces:**
- Produces CLI: `scripts/livewire_store.py rebuild-silver --tickers ... [--full] [--dry-run]`.

- [ ] Write failing tests for targeted rebuild, full discovery, unchanged no-op, one-symbol validation failure blocking the batch manifest, and dry-run output.
- [ ] Implement batch staging: read all inputs, compute all outputs, validate/stage all artifacts, publish artifacts, then publish one manifest listing affected symbols.
- [ ] Emit counters for rebuilt, unchanged, failed, action count, earliest affected date, and revision.
- [ ] Run focused tests; expect PASS.
- [ ] Commit with `git commit -m "feat: rebuild adjusted Silver bars"`.

### Task 5: Scheduling, canary, and documentation

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py`
- Create: `livewire_scripts/validate_silver_canary.py`
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `.codex/project-memory.md`
- Test: `tests/test_run_daily_update_job.py`
- Test: `tests/test_validate_silver_canary.py`

- [ ] Write tests for post-action/post-daily ordering, failed publication preventing revision advance, and canary checks for NVDA, AAPL, SPY, plus a no-action control.
- [ ] Schedule pre-market action sync and post-ingestion Silver reconciliation; add weekly full provider reconciliation without a permanent daemon.
- [ ] Implement canary output comparing ex-date returns, factors, volume scaling, and bronze SHA-256 before/after.
- [ ] Correct older documentation that assigns adjusted data to Gold or claims all bronze volume is already split-adjusted.
- [ ] Run focused tests, then `uv run pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing` and `uv run pytest tests -q -W error::RuntimeWarning`; expect all gates PASS.
- [ ] Commit with `git commit -m "feat: operate and validate Silver adjustments"`.
