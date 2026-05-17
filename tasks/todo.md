# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Update README command documentation for each supported ingest/update/test workflow.

## Success Criteria
- README has commands for historical fetches by asset class.
- README has commands for daily updates by asset class.
- README has commands for backfills, DuckDB rebuilds, IB connectivity checks, and tests.
- Existing unrelated dirty worktree changes remain untouched.

## Dependency Graph
- T1 -> T2
- T2 -> T3

## Tasks
- [x] T1 Inspect current README command sections
  depends_on: []
- [x] T2 Update README with complete command reference
  depends_on: [T1]
- [x] T3 Verify README diff and summarize
  depends_on: [T2]

## Review
- Outcome:
  - README now documents historical fetch, backfill, daily update, scheduled runner, DuckDB rebuild, IB connectivity check, and test commands.
  - Commands cover equities, futures, CBOE volatility, `cmdty`, and `fx` where the repo currently supports them.
  - README notes that `cmdty` and `fx` are canonical in bronze Parquet but do not yet have DuckDB rebuild targets.
  - README notes that the scheduled runner currently covers equities, futures, and CBOE volatility, so `cmdty` and `fx` daily updates need explicit commands.
- Verification:
  - `git diff --check`
  - README diff reviewed.
