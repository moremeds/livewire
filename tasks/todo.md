# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Draft the Sub-B Postgres analytical layer implementation plan after the Sub-A reliability foundation merge.

## Success Criteria
- Plan defines Sub-B scope without changing the parquet-first ingestion contract.
- Plan includes dependency graph and `depends_on: []` task annotations.
- Plan identifies files to create/modify, testing strategy, and verification gates.
- No runtime code is changed during planning.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4

## Tasks
- [x] T1 Inspect current Sub-A docs and DuckDB analytical rebuild path
  depends_on: []
- [x] T2 Define Sub-B architecture, scope boundaries, and migration approach
  depends_on: [T1]
- [x] T3 Write implementation plan artifact
  depends_on: [T2]
- [x] T4 Review plan for repo constraints and summarize next step
  depends_on: [T3]

## Review
- Outcome:
  - Drafted Sub-B as a Postgres analytical publish layer rebuilt from bronze parquet and reliability JSONL artifacts.
  - Preserved bronze parquet as system of record; Postgres is a query/publish target, not an ingestion write path.
  - Scoped Sub-B away from Sub-C provider work, Sub-D new timeframes, Sub-E options, and Sub-F gold tables.
  - Explicitly deferred full DuckDB removal to follow-up phase Sub-B2: Retire DuckDB Analytical Layer.
- Verification:
  - Plan checked against current `clients/db_client.py`, `scripts/rebuild_duckdb_from_parquet.py`, tests, and Sub-A spec.
