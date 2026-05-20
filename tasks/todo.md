# Daily Backfill Runner

## Goal

Add a lightweight daily backfill runner separate from `backfill-all`. It should use Massive for recent equity daily repair and equity intraday catch-up, while keeping non-equity lanes on their current sources.

## Dependency Graph

- T0 -> T1
- T1 -> T2
- T2 -> T3
- T3 -> T4

## Tasks

- [x] T0 Add failing coverage for the daily runner and recent intraday window support.
  depends_on: []
  - Red proof: focused tests failed on missing `daily-backfill` dispatcher, missing runner file, and missing `--days`/recent-window intraday support.

- [x] T1 Add recent-window intraday support.
  depends_on: [T0]
  - Add `--days` to `intraday-backfill`, preserving existing `--years` defaults for full builds.

- [x] T2 Add the daily backfill runner.
  depends_on: [T1]
  - Add `tools/run_daily_backfill.sh` and `scripts/livewire_ingest.py daily-backfill`.

- [x] T3 Update operator docs.
  depends_on: [T2]
  - Document when to use daily backfill versus full `backfill-all`.

- [x] T4 Verify.
  depends_on: [T3]
  - Run focused tests, shell syntax checks, and the repo coverage gate if practical.
  - Verification: `bash -n tools/run_daily_backfill.sh && bash -n tools/run_backfill_all.sh`, `git diff --check`, focused suite `80 passed`, intraday `--days` dry-run, full coverage gate `972 passed, 1 skipped`, 100% coverage.

# Preset Universe Cleanup

## Goal

Remove target-date unavailable symbols from managed preset universes only when provider evidence shows they are inactive or absent from reference metadata. Keep active symbols in the universe even if a single daily repair run could not fill them.

## Dependency Graph

- U0 -> U1
- U1 -> U2

## Tasks

- [x] U0 Verify unavailable daily symbols against parquet and providers.
  depends_on: []
  - Evidence: parquet still missed 47 symbols for `2026-05-19` after explicit IB retry; Massive metadata showed `KFS`, `MCW`, and `SLNO` are still active.

- [x] U1 Remove only inactive or metadata-missing symbols from managed presets.
  depends_on: [U0]
  - Remove inactive/missing reference symbols from affected S&P 500 and Russell 2000 preset files.
  - Kept active unresolved symbols: `KFS`, `MCW`, `SLNO`.

- [x] U2 Verify preset cleanup and daily coverage view.
  depends_on: [U1]
  - Recheck preset counts, cursor consistency, and remaining daily gaps.
  - Verification: all 162 preset JSON files parse; cleaned preset union is 2,401 symbols; only active unresolved daily gaps are `KFS`, `MCW`, and `SLNO`; `git diff --check`; focused tests `31 passed`.
