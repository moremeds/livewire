# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Retire DuckDB as an active Livewire analytical layer and dependency, leaving bronze Parquet as canonical storage and Postgres as the replayable analytical publish target.

## Success Criteria
- No active runtime code imports or shells out to DuckDB.
- `DBClient`, the DuckDB rebuild implementation, and `scripts/livewire_store.py rebuild-duckdb` are removed.
- Parquet aggregate/helper reads formerly using DuckDB are implemented with PyArrow/Parquet readers.
- Bootstrap, active operator docs, and tests no longer require DuckDB.
- Verification evidence includes targeted red/green tests, full coverage, a DuckDB-reference inventory, and manual self/adversarial review.

## Dependency Graph
- T0 -> T1 -> T2 -> T3 -> T4 -> T5 -> T6

## Tasks
- [ ] T0 Record baseline state and current DuckDB inventory
  depends_on: []
- [ ] T1 Add failing tests for DuckDB retirement behavior
  depends_on: [T0]
- [ ] T2 Replace DuckDB-backed Parquet helper reads with PyArrow readers
  depends_on: [T1]
- [ ] T3 Remove DuckDB client, rebuild command, fixtures, and tests
  depends_on: [T2]
- [ ] T4 Remove DuckDB from bootstrap and active operator docs
  depends_on: [T3]
- [ ] T5 Run targeted and full verification, then commit implementation milestone
  depends_on: [T4]
- [ ] T6 Perform rigid self-review and adversarial review, patch any findings, and commit review/docs milestone
  depends_on: [T5]

## Review
- Pending.
