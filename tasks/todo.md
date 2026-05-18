# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Execute the Sub-B Postgres analytical layer implementation plan on `feat/postgres-analytical-layer`.

## Success Criteria
- Postgres is added as an analytical publish target rebuilt from canonical bronze Parquet and reliability JSONL artifacts.
- Daily ingestion remains parquet-first and does not write to Postgres.
- DuckDB remains supported during Sub-B; full DuckDB retirement stays deferred to Sub-B2.
- Milestone commits are created after coherent checkpoints.
- Verification evidence is recorded before completion, including the full coverage gate.

## Dependency Graph
- T0 -> T1 -> T2 -> T3
- T3 -> T4
- T3 -> T5
- T3 -> T6
- T3 -> T7
- T3 -> T9
- T4, T5, T6, T7 -> T8
- T8, T9 -> T10
- T10 -> T11 -> T12
- T12 -> T13

## Tasks
- [x] T0 Preflight, branch/worktree setup, and baseline gate
  depends_on: []
- [x] T1 Add Postgres dependency and configuration surface
  depends_on: [T0]
- [ ] T2 Define Postgres schema SQL
  depends_on: [T1]
- [ ] T3 Add Postgres client lifecycle and schema creation
  depends_on: [T2]
- [ ] T4 Implement daily equity and volatility replace from Parquet
  depends_on: [T3]
- [ ] T5 Implement futures replace from Parquet
  depends_on: [T3]
- [ ] T6 Implement intraday replace from Parquet
  depends_on: [T3]
- [ ] T7 Import reliability JSONL into Postgres
  depends_on: [T3]
- [ ] T8 Add Postgres rebuild CLI
  depends_on: [T4, T5, T6, T7]
- [ ] T9 Add optional live Postgres smoke script
  depends_on: [T3]
- [ ] T10 Documentation sweep
  depends_on: [T8, T9]
- [ ] T11 Full test and coverage gate
  depends_on: [T10]
- [ ] T12 Manual verification checklist
  depends_on: [T11]
- [ ] T13 Ship branch
  depends_on: [T12]

## Review
- Outcome:
  - T0 baseline gate passed before implementation edits.
  - Feature branch/worktree: `feat/postgres-analytical-layer` at `/Users/chenxi/.config/superpowers/worktrees/livewire/feat-postgres-analytical-layer`.
  - T1 added the `postgres_live` pytest marker, non-secret Postgres env placeholders, and the `psycopg[binary]` bootstrap package.
- Verification:
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 883 passed, 100% coverage.
  - `python -m pip install 'psycopg[binary]'` -> installed `psycopg-3.3.4` and `psycopg-binary-3.3.4`.
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing` -> 883 passed, 100% coverage.
