# Silver Causal Canary Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect and prevent future corporate-action contamination in persisted Silver artifacts across dividends, splits, calendar gaps, and multi-symbol batches.

**Architecture:** Keep factor semantics in the pure adjustment engine, but make the canary recompute a fresh causal expectation from canonical Bronze and compare it with persisted Silver factors. Resolve one New York cutoff per command and expose effective/future counts in rebuild and validation output.

**Tech Stack:** Python 3.13, `zoneinfo`, PyArrow, pytest, Ruff, Pyright.

## Global Constraints

- Preserve future action announcements in Bronze.
- Do not change Silver Parquet or revision manifest schemas.
- Use one `America/New_York` as-of date for each rebuild or validation batch.
- Future actions are observable normal state, not validation failures.
- Existing contaminated artifacts must fail causal validation.
- Follow test-driven development.

---

### Task 1: Complete causal engine and batch-boundary coverage

**Files:**
- Modify: `tests/test_adjustment_engine.py`
- Modify: `tests/test_rebuild_silver.py`

**Interfaces:**
- Consumes: `build_factor_intervals(bars, actions, as_of_date)`.
- Consumes: `rebuild_silver.run(..., as_of_date=date | None)`.
- Produces: regression coverage for future splits, exact ex-date activation, calendar gaps, and injected batch cutoffs.

- [x] **Step 1: Add failing edge-case tests**

Add tests equivalent to:

```python
future = build_factor_intervals(bars, [future_split], date(2026, 1, 2))
effective = build_factor_intervals(bars, [future_split], date(2026, 1, 3))
assert future[0].price_adjustment_factor == Decimal("1")
assert effective[0].price_adjustment_factor == Decimal("0.5")
```

Also prove an action on a date absent from Bronze adjusts only earlier bars once effective, and a rebuild with an injected cutoff applies that same cutoff to two symbols.

- [x] **Step 2: Run the focused tests**

Run: `uv run pytest tests/test_adjustment_engine.py tests/test_rebuild_silver.py -q`

Expected: all edge cases pass under the existing explicit cutoff implementation; any failure identifies a production gap before canary changes.

- [x] **Step 3: Commit the edge-case coverage**

```bash
git add tests/test_adjustment_engine.py tests/test_rebuild_silver.py
git commit -m "test: cover Silver cutoff edge cases"
```

### Task 2: Enforce causal artifact validation and expose counters

**Files:**
- Modify: `livewire_scripts/rebuild_silver.py`
- Modify: `livewire_scripts/validate_silver_canary.py`
- Modify: `tests/test_rebuild_silver.py`
- Modify: `tests/test_validate_silver_canary.py`
- Modify: `.codex/project-memory.md`
- Modify: `tasks/todo.md`

**Interfaces:**
- Produces: rebuild summary fields `as_of_date`, `effective_action_count`, and `future_action_count`.
- Produces: `validate_silver_canary.run(..., as_of_date: date | None = None) -> int`.
- Produces: top-level and per-symbol canary action counters plus `factor intervals do not match causal expectation` errors.

- [x] **Step 1: Add failing rebuild-counter tests**

Assert a rebuild containing one effective and one future action returns:

```python
assert summary["as_of_date"] == "2026-01-03"
assert summary["action_count"] == 2
assert summary["effective_action_count"] == 1
assert summary["future_action_count"] == 1
```

- [x] **Step 2: Add failing contaminated-artifact canary test**

Create a future dividend, publish a deliberately contaminated factor and daily pair that agree internally, invoke the canary with an earlier `as_of_date`, and assert return code 1 with `factor intervals do not match causal expectation`.

- [x] **Step 3: Verify the new tests fail for missing behavior**

Run: `uv run pytest tests/test_rebuild_silver.py tests/test_validate_silver_canary.py -q`

Expected: failures for missing counters, missing injected canary cutoff, and missing causal comparison.

- [x] **Step 4: Implement minimal counters and causal comparison**

Resolve one cutoff per invocation. Count actions with:

```python
effective = [action for action in actions if action.ex_date <= effective_as_of]
future = [action for action in actions if action.ex_date > effective_as_of]
```

In the canary, compute `build_factor_intervals(bronze_rows, actions, effective_as_of)`, normalize expected and persisted intervals to `(effective_start, effective_end, price_factor, volume_factor)`, and fail the symbol when those semantic lists differ.

- [x] **Step 5: Run focused tests and static gates**

```bash
uv run pytest tests/test_adjustment_engine.py tests/test_rebuild_silver.py tests/test_validate_silver_canary.py -q
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

Expected: focused tests and Ruff pass; Pyright reports zero errors.

- [x] **Step 6: Run CI-equivalent verification**

Run: `uv run pytest tests/ -m "not integration and not postgres_live" --cov --cov-fail-under=95 -W error::RuntimeWarning -q`

Expected: all selected tests pass with at least 95% coverage.

- [x] **Step 7: Run the five-symbol local causal sweep**

Build disposable Silver artifacts for every locally reconciled symbol and validate them with the new canary. Confirm MSFT reports one ignored future action, all causal factor comparisons pass, and production Bronze/Silver hashes remain unchanged.

- [x] **Step 8: Review, commit, and update PR #50**

```bash
git diff --check
git add livewire_scripts/rebuild_silver.py livewire_scripts/validate_silver_canary.py tests/test_rebuild_silver.py tests/test_validate_silver_canary.py .codex/project-memory.md tasks/todo.md docs/superpowers/plans/2026-07-13-silver-causal-canary.md
git commit -m "fix: enforce causal Silver canary"
git push origin feat/silver-adjustment-engine
```

Then verify PR #50 contains the new commits and inspect its CI checks without merging.
