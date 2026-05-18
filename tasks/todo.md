# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Prepare PR #3 for merge by updating `feat/postgres-analytical-layer` onto current `main`, preserving the five-script operator surface, and verifying the resolved branch.

## Success Criteria
- PR #3 reports a clean mergeable state against `main`.
- Postgres analytical rebuild and smoke commands remain available through `scripts/livewire_store.py`.
- `scripts/` still contains only the five operator-facing files.
- Verification evidence is collected after conflict resolution.

## Dependency Graph
- T1 -> T2 -> T3

## Tasks
- [x] T1 Inspect PR #3 merge state and identify conflicts
  depends_on: []
- [x] T2 Resolve conflicts against current `main`
  depends_on: [T1]
- [x] T3 Run verification on the resolved PR branch
  depends_on: [T2]

## Review
- Conflict resolution kept Postgres as a replayable analytical publish target and adapted its two CLI flows into `scripts/livewire_store.py`.
- `scripts/` contains exactly the five operator-facing files after resolution.
- PR #3 was pushed at `733d359` and reports `mergeable_state=clean` against `main`.
- Verification:
  - `python -m pytest tests/test_script_consolidation.py tests/test_livewire_entrypoints.py tests/test_rebuild_postgres_from_parquet.py tests/test_smoke_postgres_analytical.py -q` -> 23 passed.
  - `git diff --check` -> passed.
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing -W error::RuntimeWarning` -> 937 passed, 1 skipped, 100% coverage.
  - `python scripts/livewire_store.py smoke-postgres --ensure-schema` against local Postgres -> `SELECT 1 ok` and all seven analytical tables counted.
  - `python -m pytest tests/test_postgres_client_live.py -q` with local Postgres DSN -> 1 passed.
