# Silver Future Corporate-Action Cutoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent announced corporate actions from affecting Silver prices before their New York ex-date.

**Architecture:** The pure adjustment engine receives an explicit batch `as_of_date` and filters active actions at its causal boundary. The rebuild command resolves one New York date per invocation and passes it to every symbol, with an injectable date for deterministic tests and replays.

**Tech Stack:** Python 3.13, `datetime`, `zoneinfo`, PyArrow, pytest, Hypothesis.

## Global Constraints

- Preserve all future announcements in canonical Bronze.
- Filter only the Silver derivation input; do not change artifact or manifest schemas.
- Apply effective actions to bars strictly before their ex-date.
- Use one `America/New_York` cutoff for an entire rebuild batch.
- Follow test-driven development and keep the implementation minimal.

---

### Task 1: Enforce the causal cutoff and verify publication

**Files:**
- Modify: `clients/adjustment_engine.py:38-55`
- Modify: `livewire_scripts/rebuild_silver.py:12-150`
- Modify: `tests/test_adjustment_engine.py`
- Modify: `tests/test_rebuild_silver.py`
- Modify: `tasks/todo.md`

**Interfaces:**
- Consumes: `CorporateAction.ex_date: date`, `CorporateAction.status: str`.
- Produces: `build_factor_intervals(bars: list[dict], actions: list[CorporateAction], as_of_date: date) -> list[FactorInterval]`.
- Produces: `run(..., as_of_date: date | None = None) -> int`, defaulting once per invocation to `datetime.now(ZoneInfo("America/New_York")).date()`.

- [x] **Step 1: Add failing pure-engine regression coverage**

Add a test with a dividend ex-date after the supplied cutoff and assert all interval price factors are `Decimal("1")`. In the same test, advance the cutoff to the ex-date and assert earlier sessions receive `Decimal("0.99")` while the ex-date session remains `Decimal("1")`.

- [x] **Step 2: Verify the pure-engine test fails for the missing interface**

Run: `uv run pytest tests/test_adjustment_engine.py -q`

Expected: failure because `build_factor_intervals` does not accept `as_of_date` and currently applies the future dividend.

- [x] **Step 3: Add failing rebuild-boundary coverage**

Create a future `MassiveDividend` in `tests/test_rebuild_silver.py`, invoke `rebuild_silver.run(..., as_of_date=date(2026, 1, 3))`, and assert published closes and factors remain at identity. This proves the cutoff propagates through the artifact publication boundary.

- [x] **Step 4: Implement the minimal engine and rebuild changes**

Require `as_of_date` in `build_factor_intervals` and select actions with:

```python
action.status == "active" and action.ex_date <= as_of_date
```

Resolve one batch cutoff in `run`:

```python
effective_as_of = as_of_date or datetime.now(ZoneInfo("America/New_York")).date()
```

Pass `effective_as_of` to every factor build and update existing direct test calls with explicit deterministic dates.

- [x] **Step 5: Run focused tests and verify green**

Run: `uv run pytest tests/test_adjustment_engine.py tests/test_rebuild_silver.py -q`

Expected: all tests pass.

- [x] **Step 6: Run formatting, typing, and CI-equivalent verification**

Run:

```bash
uv lock --check
uv sync --dev
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/ -m "not integration and not postgres_live" --cov --cov-fail-under=95 -W error::RuntimeWarning
```

Expected: lockfile, formatting, lint, typing, tests, RuntimeWarning enforcement, and at least 95% configured coverage all pass.

- [x] **Step 7: Repeat the disposable real-MSFT smoke test**

Rebuild MSFT into a new temporary Silver root using production Bronze and the reconciled disposable corporate-action artifact. Confirm the 2026-08-20 dividend does not affect a revision built before that date, Apex reads the corrected values, and the factor after the latest completed ex-date is `1.0`.

- [x] **Step 8: Review and commit**

Inspect `git diff`, run `git diff --check`, confirm only planned files changed, then commit with:

```bash
git add clients/adjustment_engine.py livewire_scripts/rebuild_silver.py tests/test_adjustment_engine.py tests/test_rebuild_silver.py tasks/todo.md docs/superpowers/plans/2026-07-13-silver-future-action-cutoff.md
git commit -m "fix: exclude future Silver actions"
```
