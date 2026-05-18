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
- [x] T2 Define Postgres schema SQL
  depends_on: [T1]
- [x] T3 Add Postgres client lifecycle and schema creation
  depends_on: [T2]
- [x] T4 Implement daily equity and volatility replace from Parquet
  depends_on: [T3]
- [x] T5 Implement futures replace from Parquet
  depends_on: [T3]
- [x] T6 Implement intraday replace from Parquet
  depends_on: [T3]
- [x] T7 Import reliability JSONL into Postgres
  depends_on: [T3]
- [x] T8 Add Postgres rebuild CLI
  depends_on: [T4, T5, T6, T7]
- [x] T9 Add optional live Postgres smoke script
  depends_on: [T3]
- [x] T10 Documentation sweep
  depends_on: [T8, T9]
- [x] T11 Full test and coverage gate
  depends_on: [T10]
- [x] T12 Manual verification checklist
  depends_on: [T11]
- [x] T13 Ship branch
  depends_on: [T12]

## Review
- Outcome:
  - T0 baseline gate passed before implementation edits.
  - Feature branch/worktree: `feat/postgres-analytical-layer` at `/Users/chenxi/.config/superpowers/worktrees/livewire/feat-postgres-analytical-layer`.
  - T1 added the `postgres_live` pytest marker, non-secret Postgres env placeholders, and the `psycopg[binary]` bootstrap package.
  - T2 added validated Postgres analytical DDL for symbols, daily/intraday market data, telemetry events, and quality flags.
  - T3 added `PostgresClient` lifecycle, environment configuration, schema creation, and package export.
  - T3 review gate: schema/client shape is sufficient to proceed to market-data loaders.
  - T4-T7 added PyArrow batch loaders for daily equity/volatility, futures, intraday `1h`/`5m`, and reliability JSONL imports.
  - T8-T9 added the Postgres rebuild CLI and optional analytical smoke script.
  - T10 updated operator and agent docs with Postgres role, env vars, rebuild examples, and rollback guidance.
  - Self-review tightened parquet loaders so market-data rows stream into COPY instead of being accumulated as full Python lists.
  - T12 started a disposable local Postgres 15 database, ran live schema/smoke/rebuild/import checks, and verified source parquet/JSONL counts match Postgres counts.
  - T12 detected and loaded equity daily, volatility daily, equity `1h`, and equity `5m` bronze inputs; futures bronze input is absent.
  - T13 pushed `feat/postgres-analytical-layer` and opened PR #3.
- Verification:
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 883 passed, 100% coverage.
  - `python -m pip install 'psycopg[binary]'` -> installed `psycopg-3.3.4` and `psycopg-binary-3.3.4`.
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing` -> 883 passed, 100% coverage.
  - Red: `python -m pytest tests/test_postgres_schema.py -q` -> failed with `ModuleNotFoundError: No module named 'clients.postgres_schema'`.
  - Green: `python -m pytest tests/test_postgres_schema.py -q` -> 9 passed.
  - Repo coverage gate: `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 892 passed, 100% coverage.
  - Red: `python -m pytest tests/test_postgres_client.py -q` -> failed with `ModuleNotFoundError: No module named 'clients.postgres_client'`.
  - Green: `python -m pytest tests/test_postgres_client.py -q` -> 7 passed.
  - Live gate skip: `python -m pytest tests/test_postgres_client_live.py -q` -> 1 skipped because `MDW_TEST_POSTGRES_DSN` is unset.
  - Repo coverage gate: `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 899 passed, 1 skipped, 100% coverage.
  - Red: `python -m pytest tests/test_postgres_client.py -q` -> 10 failed on missing loader/import methods.
  - Green: `python -m pytest tests/test_postgres_client.py -q` -> 21 passed.
  - Repo coverage gate: `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 913 passed, 1 skipped, 100% coverage.
  - Red: `python -m pytest tests/test_rebuild_postgres_from_parquet.py tests/test_smoke_postgres_analytical.py -q` -> failed on missing script modules.
  - Green: `python -m pytest tests/test_rebuild_postgres_from_parquet.py tests/test_smoke_postgres_analytical.py -q` -> 11 passed.
  - Repo coverage gate: `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 924 passed, 1 skipped, 100% coverage.
  - Docs sweep: `git diff --check` -> passed.
  - Repo coverage gate: `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 924 passed, 1 skipped, 100% coverage.
  - Final coverage gate: `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 924 passed, 1 skipped, 100% coverage.
  - Live-gated Postgres test before DSN setup: `python -m pytest tests/test_postgres_client_live.py -q` -> 1 skipped because `MDW_TEST_POSTGRES_DSN` was unset.
  - Smoke DSN guard before DSN setup: `python scripts/smoke_postgres_analytical.py` -> exited 2 with `MDW_POSTGRES_DSN is required unless --dsn is supplied`.
  - CLI help: `python scripts/rebuild_postgres_from_parquet.py --help` and `python scripts/smoke_postgres_analytical.py --help` -> exited 0.
  - Self-review fix gate: `python -m pytest tests/test_postgres_client.py -q` -> 22 passed.
  - Final coverage gate after self-review fix: `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning` -> 925 passed, 1 skipped, 100% coverage.
  - Disposable Postgres smoke: `python scripts/smoke_postgres_analytical.py --ensure-schema` -> table counts all zero after schema creation.
  - Disposable live test: `python -m pytest tests/test_postgres_client_live.py -q` -> 1 passed.
  - Disposable rebuilds:
    - `python scripts/rebuild_postgres_from_parquet.py --asset-class equity --timeframe 1d` -> 439 symbols, 937,229 rows.
    - `python scripts/rebuild_postgres_from_parquet.py --asset-class volatility` -> 14 symbols, 65,872 rows.
    - `python scripts/rebuild_postgres_from_parquet.py --asset-class equity --timeframe 1h` -> 10,731 rows.
    - `python scripts/rebuild_postgres_from_parquet.py --asset-class equity --timeframe 5m` -> 58,989 rows.
    - `python scripts/rebuild_postgres_from_parquet.py --include-reliability` -> telemetry=69, quality=3, skipped=0.
  - Source-to-Postgres count comparison -> `source_to_postgres_counts_match=yes` for equity daily, volatility daily, intraday `1h`/`5m`, telemetry, and quality flags.
  - `git push -u origin feat/postgres-analytical-layer` -> pushed branch and set upstream.
  - `gh pr create --title "Sub-B: Postgres Analytical Layer" --body-file /tmp/livewire-postgres-sub-b-pr.md` -> https://github.com/moremeds/livewire/pull/3.
