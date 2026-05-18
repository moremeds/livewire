# Script Consolidation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reduce the operator-facing `scripts/` directory to five files while preserving current ingestion, quality, operations, storage, and setup behavior.

**Architecture:** Move existing script implementation modules into importable packages and leave only four Python command aggregators plus the bootstrap shell script in `scripts/`. Each aggregator dispatches to the moved module `main()` functions through explicit subcommands, while subprocess call sites are updated to call the new aggregators. Tests move their imports and patches from `scripts.<old_module>` to the package modules.

**Tech Stack:** Python 3.13, argparse, pytest, coverage.py, Node test runner for the alert mailer modules.

---

## Dependency Graph

- T1 -> T2
- T2 -> T3
- T3 -> T4
- T4 -> T5
- T5 -> T6
- T6 -> T7

## Tasks

- [ ] T1 Baseline and red tests
  depends_on: []
  - Run representative current tests for script modules to confirm baseline.
  - Add tests for the new five-script surface: expected files under `scripts/`, aggregator help, and dispatch wiring.
  - Run the new tests and confirm they fail because the aggregators do not exist yet.

- [ ] T2 Move implementation modules out of `scripts/`
  depends_on: [T1]
  - Create `livewire_scripts/` for Python command implementations.
  - Move current Python script implementation files into `livewire_scripts/`.
  - Create `livewire_node/` for Node alert helpers and move `.mjs` implementation/tests out of `scripts/`.
  - Move launchd plist templates to `launchd/`.
  - Move the pre-commit hook to `tools/`.

- [ ] T3 Add five operator entrypoints
  depends_on: [T2]
  - Create `scripts/livewire_ingest.py` with subcommands: `daily`, `historical`, `robust`, `cboe-vol`, `intraday-backfill`, `intraday-status`, `universe`.
  - Create `scripts/livewire_quality.py` with subcommands: `health`, `coverage`, `report`, `weekly`, `watchdog`.
  - Create `scripts/livewire_ops.py` with subcommands: `run-daily-job`, `send-alert`, `ibc-install`, `ibc-start`.
  - Create `scripts/livewire_store.py` with subcommands: `rebuild-duckdb`, `sync-r2`, `migrate-parquet`.
  - Keep `scripts/setup_market_warehouse.sh` as the fifth script.

- [ ] T4 Update internal subprocess references
  depends_on: [T3]
  - Update scheduled job, coverage recovery, health repair, universe backfill, quality email, and watchdog paths to invoke the new aggregators.
  - Update launchd templates to call `livewire_ops.py run-daily-job` and `livewire_quality.py watchdog`.

- [ ] T5 Update tests and docs
  depends_on: [T4]
  - Rewrite Python tests to import and patch `livewire_scripts.<module>`.
  - Move/update Node tests to import from `livewire_node/`.
  - Update `README.md`, `CLAUDE.md`, `.codex/project-memory.md`, and hook install docs to the new commands.
  - Update `pyproject.toml` coverage source/omits.

- [ ] T6 Remove redundant scripts and verify count
  depends_on: [T5]
  - Ensure `find scripts -maxdepth 1 -type f` returns exactly five files.
  - Ensure old script names are not referenced except in historical migration notes where unavoidable.

- [ ] T7 Full verification and milestone commit
  depends_on: [T6]
  - Run Python unit/coverage gate.
  - Run runtime-warning gate.
  - Run Node tests for alert helpers.
  - Run aggregator `--help` smoke checks.
  - Commit the completed migration.
