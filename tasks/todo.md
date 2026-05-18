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
- [x] T0 Record baseline state and current DuckDB inventory
  depends_on: []
- [x] T1 Add failing tests for DuckDB retirement behavior
  depends_on: [T0]
- [x] T2 Replace DuckDB-backed Parquet helper reads with PyArrow readers
  depends_on: [T1]
- [x] T3 Remove DuckDB client, rebuild command, fixtures, and tests
  depends_on: [T2]
- [x] T4 Remove DuckDB from bootstrap and active operator docs
  depends_on: [T3]
- [x] T5 Run targeted and full verification, then commit implementation milestone
  depends_on: [T4]
- [ ] T6 Perform rigid self-review and adversarial review, patch any findings, and commit review/docs milestone
  depends_on: [T5]

## Review
- Baseline before implementation:
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing -W error::RuntimeWarning` -> 888 passed, 1 skipped, 100% coverage.
- Red tests:
  - `python -m pytest tests/test_duckdb_retirement.py -q` -> failed on active DuckDB imports, `rebuild-duckdb`, and setup bootstrap/install references.
- Targeted verification:
  - `python -m pytest tests/test_duckdb_retirement.py tests/test_bronze_client.py tests/test_coverage_report.py tests/test_health_check.py tests/test_run_ib_fetch_robust.py tests/test_script_consolidation.py tests/test_storage_client_compat.py -q` -> 145 passed.
  - `python -m pytest tests/test_bronze_client.py tests/test_coverage_report.py -q` -> 44 passed.
- Full verification:
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing -W error::RuntimeWarning` -> 839 passed, 1 skipped, 100% coverage.
