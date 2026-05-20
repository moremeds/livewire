# Massive Equity Incremental Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route recent daily equity recovery through Massive while keeping deep older-history backfill safe.

**Architecture:** Extend the existing daily historical script with a provider selector. Keep the storage path unchanged: provider bars are normalized, validated, and merged through `BronzeClient`.

**Tech Stack:** Python 3.13, argparse, `MassiveClient`, `BronzeClient`, pytest.

---

### Task 1: Historical Daily Backfill Source Selection

**Files:**
- Modify: `livewire_scripts/fetch_ib_historical.py`
- Test: `tests/test_fetch_ib_historical.py`

- [ ] Add `--source {auto,ib,massive}` to the historical parser.
- [ ] Add Massive daily backfill helpers that convert `MassiveDailyBar` objects into standard daily bronze rows.
- [ ] Resolve `auto` to IB for deep older-history `historical --backfill`.
- [ ] Preserve IB behavior for normal seed and all non-equity asset classes.
- [ ] Add tests for forced Massive backfill without `IBClient`, partial Massive range cursor behavior, forced IB backfill, forced Massive auth failure return path, and source validation.

### Task 2: Command Wrappers and Orchestration

**Files:**
- Modify: `scripts/livewire_ingest.py`
- Modify: `livewire_scripts/run_ib_fetch_robust.py`
- Modify: `livewire_scripts/coverage_report.py`
- Modify: `tools/run_backfill_all.sh`
- Test: `tests/test_livewire_entrypoints.py`
- Test: `tests/test_run_ib_fetch_robust.py`
- Test: `tests/test_coverage_report.py`
- Test: `tests/test_script_consolidation.py`

- [ ] Skip IB preflight for historical equity backfill when source resolves to Massive.
- [ ] Add `--source` to robust orchestration and pass it to historical workers.
- [ ] Add `daily --source massive --target-date ... --tickers ...` to daily coverage recovery.
- [ ] Add `--source auto` to `backfill-all` Phase 2.
- [ ] Update tests that assert generated commands.

### Task 3: Documentation and Verification

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `.codex/project-memory.md`
- Modify: `tasks/todo.md`

- [ ] Document Massive-preferred daily equity incremental backfill.
- [ ] Run targeted tests for changed files.
- [ ] Run full coverage gate before completion.
