# VIX/SPX Intraday Volatility Support

## Goal

Extend the existing intraday backfill support so `VIX` and `SPX` have an explicit IB-backed volatility/index intraday path, with correct volatility metadata, a scoped preset, operator docs, and regression coverage.

## Dependency Graph

- T0 -> T1
- T1 -> T2
- T2 -> T3
- T3 -> T4

## Tasks

- [x] T0 Add failing tests for VIX/SPX volatility intraday routing and metadata.
  depends_on: []
  - Red proof: focused tests failed on missing `asset_class` quality metadata, missing `presets/volatility-intraday.json`, and backfill-all still using `presets/volatility.json` for intraday.

- [x] T1 Implement the runtime fix and scoped volatility intraday preset.
  depends_on: [T0]
  - Added `asset_class` propagation into intraday quality metadata and a dedicated `presets/volatility-intraday.json` containing `VIX` and `SPX`.

- [x] T2 Update backfill-all and docs to use the VIX/SPX intraday preset.
  depends_on: [T1]
  - Backfill-all now runs volatility intraday against `presets/volatility-intraday.json`; docs and project memory distinguish daily CBOE volatility from IB VIX/SPX intraday.

- [x] T3 Run focused and full verification.
  depends_on: [T2]
  - Focused verification: `77 passed`, 100% coverage for `backfill_intraday` and `fetch_cboe_volatility`.
  - Shell/diff checks: `bash -n tools/run_backfill_all.sh` and `git diff --check` passed.
  - Full gate: `941 passed, 1 skipped`, 100% coverage.
  - Operator dry-run: VIX/SPX volatility preset reports 53 `5m` IB chunks each.

- [x] T4 Commit, push, and open a PR if verification passes.
  depends_on: [T3]
  - Commit: `a951f4c feat: add vix spx volatility intraday support`.
  - PR: https://github.com/moremeds/livewire/pull/12

## Review
- Design: add a final non-IB FRED phase after equity Phase 2, log to `backfill_fred_rates.log`, source `.env` if present, and let nonzero `fred-rates` exit codes fail the runner.
- Red test proof:
  - `python -m pytest tests/test_livewire_entrypoints.py::test_backfill_all_runner_includes_fred_rates_phase -q` -> failed because `PHASE 3: FRED Treasury rates` was absent from `tools/run_backfill_all.sh`.
- Targeted verification:
  - `python -m pytest tests/test_livewire_entrypoints.py tests/test_script_consolidation.py -q` -> 20 passed.
- Full verification:
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing -W error::RuntimeWarning` -> 905 passed, 1 skipped, 100% coverage.

# Massive Equity Incremental Backfill

Dependency graph:
- `task-1-historical-source-selector` depends_on: []
- `task-2-orchestration-callers` depends_on: ["task-1-historical-source-selector"]
- `task-3-docs-verification` depends_on: ["task-1-historical-source-selector", "task-2-orchestration-callers"]

Tasks:
- [x] `task-1-historical-source-selector` depends_on: [] Add `--source {auto,ib,massive}` to daily historical fetch/backfill, keep `auto` on IB for deep older-history backfill, and prevent forced Massive partial ranges from completing cursors.
- [x] `task-2-orchestration-callers` depends_on: ["task-1-historical-source-selector"] Route robust backfill, coverage recovery, `livewire_ingest.py` preflight, and `backfill-all` Phase 2 through the source selector and Massive daily repair path.
- [x] `task-3-docs-verification` depends_on: ["task-1-historical-source-selector", "task-2-orchestration-callers"] Update docs and run targeted plus full coverage verification.
