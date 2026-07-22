# Fix: gap_aware_completed counts unbounded tickers as complete

**Item:** M2 · Severity: medium · Status: proposed

## Problem

`gap_aware_completed` (`livewire_scripts/backfill_runner.py:100-216`) verifies
backfill completeness against `TagRegistry` (`<warehouse>/registry.json`,
`clients/tag_registry.py`). In the registry-present branch, a ticker whose entry
lacks `earliest_available` is skipped with only a warning (`n_no_bounds`), and the
completion count `total - n_with_gaps` treats it as done. Locked in by test
`test_tickers_without_earliest_are_skipped` (`tests/test_backfill_runner.py`).

Consequence: new preset tickers not yet registered report "backfill complete" from
`run_until_done` without any verification — their history may be full of holes.

## Fix

Decide the semantics explicitly, then implement:

**Chosen default: unbounded ≠ complete when the ticker came from the preset scope.**
Count `n_no_bounds` tickers as incomplete (`total - n_with_gaps - n_no_bounds`),
keeping the warning log but making it consequential. Rationale: `run_until_done`'s
whole purpose is "don't stop until verified"; an unverifiable ticker must not
terminate the loop.

**Implementation trap:** the return at `backfill_runner.py:216` is guarded by
`if n_with_gaps > 0:` (`:210`). Changing only the formula leaves the
`n_with_gaps == 0 and n_no_bounds > 0` case falling through to the cursor-count —
the bug survives. The guard must become
`if n_with_gaps > 0 or n_no_bounds > 0:` (subject to the all-unbounded escape hatch
below taking precedence).

### Precondition (drift guard)

Before editing, confirm current source at `backfill_runner.py`:
`:210` reads exactly `        if n_with_gaps > 0:` and `:216` reads exactly
`            return total - n_with_gaps`.
Verify: `sed -n '210p;216p' livewire_scripts/backfill_runner.py`.
If either differs, STOP and report — the counting logic has changed since this plan
was written.

Guard against the pathological case: if **all** tickers lack bounds (fresh
deployment, registry never populated), returning 0 forever would wedge
`run_until_done` until `max_stale`. Add an escape hatch: when `n_no_bounds == total`,
fall back to the cursor count (current pre-registry behavior) and log one loud
warning naming the registry path — the registry being empty is a setup problem, not
a per-ticker gap. Threshold behavior between the extremes stays strict.

Follow-up (note-only unless trivial): the registry writer that should populate
`earliest_available`. Locate it with `grep -rn "earliest_available" clients/
livewire_scripts/` and `grep -rn "TagRegistry(" clients/ livewire_scripts/`. If a
single obvious writer sets/omits `earliest_available` and adding it is a <10-line
change, include it. Otherwise it is OUT of scope for this PR — record the writer's
file:line here and open no second PR. Do NOT refactor `TagRegistry`.

## Files to change

- `livewire_scripts/backfill_runner.py` — counting logic + escape hatch

## Tests

`tests/test_backfill_runner.py::TestGapAwareCompleted`:

- Rewrite `test_tickers_without_earliest_are_skipped` →
  `test_tickers_without_earliest_count_as_incomplete` (mixed registry: 2 bounded
  complete, 1 unbounded → returns total-1).
- New: `test_all_unbounded_falls_back_to_cursor_count` (escape hatch).
- The class has 19 tests total; ~5 exercise the registry-present branch and pass
  today only because their fixtures are fully bounded — confirm none injects an
  unbounded ticker before assuming they pass unchanged
  (`grep -n "earliest_available\|n_no_bounds\|registry" tests/test_backfill_runner.py`).
  The no-registry-branch tests are untouched by this change.

### Per-step verification

After rewriting the counting logic + escape hatch:
`uv run pytest tests/test_backfill_runner.py -k TestGapAwareCompleted -q`
→ PASS; expected count: 19 existing + 1 net new (`all_unbounded_falls_back`) = 20; the
renamed test asserts `total - 1`.

## Risks / notes

- Behavior change: existing deployments with sparse registries will see
  `run_until_done` keep retrying tickers it previously declared done. That is
  correct but may lengthen `backfill-all` runs until the registry is populated.
- Do not change the no-registry fallback branches (`backfill_depth` /
  `MDW_BACKFILL_MIN_DEPTH_DATE` logic) — separate concern, currently sound.

## Acceptance criteria

- Preset with one unregistered ticker → completion count excludes it; runner
  continues instead of declaring COMPLETE.
- Empty registry → cursor-count fallback with a single loud warning; no infinite
  stall introduced.
- Global gates green:
  1. `uv run pytest tests/ -v -m "not integration"` → all pass (`-m "not integration"`
     mandatory — 2 time-bomb integration tests hang the suite).
  2. `uv run pytest tests/ -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
     → exit 0 (coverage ≥ 95%).

STOP condition: if any gate fails for a reason other than the test this plan renames or
adds, revert and report — do not lower thresholds or deselect additional tests.
