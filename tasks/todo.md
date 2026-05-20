# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Incorporate FRED Treasury yield ingestion into the consolidated `backfill-all` runner.

## Success Criteria
- `python scripts/livewire_ingest.py backfill-all` invokes FRED rates ingestion after the existing equity normal/backfill phases.
- The FRED step writes to the existing backfill log directory and uses the repo-local `.env` when present so `FRED_API_KEY` is available.
- A FRED failure causes the backfill runner to fail rather than silently reporting all done.
- Tests cover the script wiring and shell syntax.

## Dependency Graph
- T0 -> T1 -> T2 -> T3

## Tasks
- [x] T0 Inspect current backfill-all shell runner and command dispatch tests
  depends_on: []
- [x] T1 Add failing tests for FRED rates inclusion in backfill-all
  depends_on: [T0]
- [x] T2 Patch `tools/run_backfill_all.sh` and docs
  depends_on: [T1]
- [x] T3 Run targeted and full verification
  depends_on: [T2]

## Review
- Design: add a final non-IB FRED phase after equity Phase 2, log to `backfill_fred_rates.log`, source `.env` if present, and let nonzero `fred-rates` exit codes fail the runner.
- Red test proof:
  - `python -m pytest tests/test_livewire_entrypoints.py::test_backfill_all_runner_includes_fred_rates_phase -q` -> failed because `PHASE 3: FRED Treasury rates` was absent from `tools/run_backfill_all.sh`.
- Targeted verification:
  - `python -m pytest tests/test_livewire_entrypoints.py tests/test_script_consolidation.py -q` -> 20 passed.
- Full verification:
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing -W error::RuntimeWarning` -> 905 passed, 1 skipped, 100% coverage.
