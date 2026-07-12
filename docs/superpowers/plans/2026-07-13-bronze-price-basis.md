# Bronze Price-Basis Normalization and Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make equity Bronze provably raw, repair existing mixed-basis history safely, and make Silver enforce row-level price basis.

**Architecture:** Extend only the equity-daily Bronze schema with source and basis metadata. Normalize split-adjusted IB input through a pure corporate-action transform, migrate legacy rows to unknown, repair them from manifest-approved Massive raw data, and make Silver fail closed on unresolved split-affected rows.

**Tech Stack:** Python 3.13, Decimal, PyArrow, IB TWS API, Massive REST, pytest, Ruff, Pyright.

## Global Constraints

- Bronze remains the canonical raw layer.
- Do not mutate production Bronze or advance production Silver without a later explicit go-ahead.
- Preserve atomic Parquet replacement and per-symbol locking.
- Ratio inference is diagnostic only and never authorizes repair.
- IB volume normalization must be calibrated before IB rows can be published as raw.
- Non-equity and intraday schemas remain unchanged.
- Maintain at least 95 percent configured test coverage.

---

### Task 1: Equity Bronze provenance schema

**Files:**
- Modify: `clients/bronze_client.py`
- Modify: `clients/ingestion_common.py`
- Modify: Massive/Nasdaq/Stooq equity row producers under `livewire_scripts/`
- Test: `tests/test_bronze_client.py`, `tests/test_fetch_ib_historical.py`, `tests/test_daily_update.py`

**Interfaces:**
- Produce equity row fields `source: str` and `price_basis: Literal["raw", "split_adjusted", "unknown"]`.
- Produce `bars_to_rows(..., source: str, price_basis: str) -> list[dict]` with required keyword arguments for equities.

- [ ] Add failing Parquet round-trip, merge replacement, provider labelling, and non-equity schema tests.
- [ ] Run focused tests and observe missing-column/interface failures.
- [ ] Add non-null equity schema columns and strict value validation; label legacy test fixtures explicitly.
- [ ] Update every equity daily producer: Massive `raw`, IB input `split_adjusted`, fallbacks with verified or `unknown` basis.
- [ ] Run focused tests, Ruff, and Pyright.
- [ ] Commit with `feat: record Bronze price basis`.

### Task 2: Provider calibration and IB normalization

**Files:**
- Create: `clients/price_basis.py`
- Create: `livewire_scripts/calibrate_daily_basis.py`
- Create: `tests/test_price_basis.py`
- Create: `tests/test_calibrate_daily_basis.py`
- Modify: IB equity publication call sites.

**Interfaces:**
- Produce `normalize_split_adjusted_rows(rows, actions, as_of_date, *, volume_mode) -> list[dict]`.
- Produce calibration JSON containing provider rows, inferred OHLC transform, volume mode, and pass/fail per split window.

- [ ] Add failing Decimal tests for 2:1, 3:2, 4:1, 7:1, 10:1, cumulative, reverse, future/cancelled, calendar-gap, malformed, and idempotence cases.
- [ ] Implement price normalization and a fail-closed `volume_mode="unverified"` path.
- [ ] Add calibration CLI tests using deterministic provider fixtures.
- [ ] Run real calibration against MSFT 2003, AAPL 2020, and NVDA 2024 when credentials/connectivity are available.
- [ ] Enable IB publication only for the calibrated volume mode; otherwise fail before Bronze mutation.
- [ ] Run focused/static gates and commit with `feat: normalize IB daily prices to raw`.

### Task 3: Atomic legacy migration

**Files:**
- Create: `livewire_scripts/migrate_equity_price_basis.py`
- Create: `tests/test_migrate_equity_price_basis.py`
- Modify: `scripts/livewire_store.py`

**Interfaces:**
- Produce `migrate --tickers ...|--full [--dry-run]` with per-file source/target hashes, cursor, and counters.

- [ ] Add failing dry-run, targeted/full, atomic-failure, resume, idempotence, and hash tests.
- [ ] Implement migration to `legacy/unknown` without OHLCV changes.
- [ ] Verify failed publication preserves original bytes.
- [ ] Wire the operator entry point, run focused/static gates, and commit with `feat: migrate Bronze price basis metadata`.

### Task 4: Read-only split audit and manifest repair

**Files:**
- Create: `livewire_scripts/audit_split_basis.py`
- Create: `livewire_scripts/repair_split_basis.py`
- Create: `tests/test_audit_split_basis.py`
- Create: `tests/test_repair_split_basis.py`
- Modify: `scripts/livewire_quality.py`, `scripts/livewire_store.py`

**Interfaces:**
- Produce a versioned JSON manifest with source hash, original rows, authoritative raw replacements, inference diagnostics, and approval state.
- Produce repair and rollback commands that require an explicit manifest and reject stale hashes.

- [ ] Add failing audit no-write, deterministic-manifest, known-boundary, and missing-provider tests.
- [ ] Implement read-only boundary analysis and authoritative Massive `adjusted=false` retrieval.
- [ ] Add failing stale-manifest, validation, atomic apply, exact rollback, and idempotent reapply tests.
- [ ] Implement locked recheck/apply/rollback with free-space validation.
- [ ] Rehearse audit, repair, rollback, and reapply in a disposable lake; verify hashes.
- [ ] Run focused/static gates and commit with `feat: audit and repair mixed Bronze basis`.

### Task 5: Basis-aware Silver and canary

**Files:**
- Modify: `clients/adjustment_engine.py`
- Modify: `livewire_scripts/rebuild_silver.py`
- Modify: `livewire_scripts/validate_silver_canary.py`
- Modify: related tests and operator documentation.

**Interfaces:**
- Consume `source` and `price_basis` on every equity daily row.
- Apply split factors only to raw rows; block unknown split-affected rows; retain dividend adjustment.

- [ ] Add failing raw, split-adjusted, unknown, basis-transition, dividend, and mechanical-jump tests.
- [ ] Implement row-basis-aware factor construction and batch failure semantics.
- [ ] Extend the canary with basis/factor and split-jump invariants.
- [ ] Build disposable Silver from repaired fixtures and verify AAPL 2020, NVDA 2024, and MSFT 2003 continuity.
- [ ] Run local Apex daily/intraday smoke against the disposable revision.
- [ ] Run the complete CI-equivalent suite, coverage, self-review, and commit with `fix: enforce Bronze price basis in Silver`.

### Task 6: Handoff

**Files:**
- Modify: `.codex/project-memory.md`, `README.md`, `CLAUDE.md`, `tasks/todo.md`

- [ ] Document the schema, calibration gate, migration, audit, repair, rollback, and Silver failure modes.
- [ ] Confirm worktree clean and list all commits relative to PR #50 head.
- [ ] Rebase/retarget after PR #50 merges, then push and create a separate draft PR only on explicit request.
- [ ] Do not execute production migration or repair; present the reviewed manifest and request separate approval.
