# Daily Backfill Runner

# Warehouse Health HTML Report

# Warehouse Warn/Error Cleanup

# Warehouse Report Grouped Views

## Goal

Make the warehouse HTML report easier to read by grouping results first by asset class and then by ticker, while keeping the detailed per-file table available as a drilldown.

## Dependency Graph

- G0 -> G1
- G1 -> G2
- G2 -> G3

## Tasks

- [x] G0 Add coverage for grouped report sections.
  depends_on: []
  - Assert the HTML renders asset and ticker summary sections before the per-file details.
  - Added regression coverage for asset summary, ticker summary, grouped asset sections, collapsed details, density, and reason columns.

- [x] G1 Implement grouped asset/ticker rendering.
  depends_on: [G0]
  - Add an asset summary table and a ticker summary table with worst status, timeframe list, row totals, density, latest bar, and reason summary.
  - Ticker rows are now grouped into expandable asset sections, with one ticker row summarizing its available timeframes.

- [x] G2 Collapse the long detail table by default.
  depends_on: [G1]
  - Keep per-file rows searchable/sortable but put them inside an expandable details block.
  - Per-file snapshots remain searchable/sortable under the collapsed `Per-File Details` section.

- [x] G3 Verify and regenerate the report.
  depends_on: [G2]
  - Run focused tests, regenerate `/Users/moremeds/market-warehouse/reports/warehouse_health.html`, and run the coverage gate if code changed.
  - Verification: `git diff --check`; focused tests `34 passed`; regenerated `/Users/moremeds/market-warehouse/reports/warehouse_health.html` with 9,687 snapshots, 2,454 symbols, 21,328,763 rows, density 86.36%; model check `0 warn`, `0 error`, `0 repair_actions`; coverage gate `992 passed, 1 skipped`, 100%; warning-sensitive suite `992 passed, 1 skipped`.

## Goal

Classify every warning/error from the generated warehouse health report, identify which rows are true data gaps versus report-model false positives, and repair or narrow the report logic until the remaining warn/error set is actionable.

## Dependency Graph

- W0 -> W1
- W1 -> W2
- W2 -> W3
- W3 -> W4

## Tasks

- [x] W0 Extract and group all warn/error rows from the scanner model.
  depends_on: []
  - Group by status, asset class, timeframe, root cause, and likely repair path.
  - Initial report had 5,556 errors and 1,042 warnings; after fixing false-positive rules, actionable state was 0 errors and 58 warnings.

- [x] W1 Fix report-model false positives.
  depends_on: [W0]
  - If the report marks healthy files incorrectly, add regression coverage and fix the scanner.
  - Intraday sparse trade bars no longer count as errors when fresh.
  - Default target date now uses the previous complete U.S. trading day.
  - Intraday-only orphan snapshots no longer create actionable warnings.
  - Daily historical density remains visible but no longer blocks health when current.

- [x] W2 Repair true parquet data gaps where an existing command can safely do so.
  depends_on: [W1]
  - Prefer `daily-backfill`/Massive for recent equity gaps and existing CBOE/FRED/IB lanes for non-equity.
  - Added `warehouse --repair` and `warehouse --repair --dry-run`.
  - Repair runner loads `.env` for child repair commands without printing secret values.
  - Repaired stale equity daily rows with Massive and stale `XAUUSD`/`USDEUR` daily rows with IB.

- [x] W3 Regenerate the warehouse report and compare warn/error deltas.
  depends_on: [W2]
  - Write the refreshed HTML report and summarize the remaining unresolved rows.
  - Final report `/Users/moremeds/market-warehouse/reports/warehouse_health.html`: 9,687 snapshots, 2,454 symbols, 21,328,763 rows, 0 warnings, 0 errors.

- [x] W4 Verify code changes and document evidence.
  depends_on: [W3]
  - Run focused tests and the coverage gate when code changes are made.
  - Verification: `git diff --check`; focused tests `34 passed`; coverage gate `992 passed, 1 skipped`, 100%; warning-sensitive suite `992 passed, 1 skipped`.

## Goal

Add a parquet-first warehouse scanner that produces a static HTML health report showing every discovered asset class, ticker, timeframe, row count, date/timestamp range, approximate coverage, staleness, and report-wide summary cards.

## Dependency Graph

- H0 -> H1
- H1 -> H2
- H2 -> H3
- H3 -> H4
- H4 -> H5

## Tasks

- [x] H0 Add failing coverage for a warehouse report command.
  depends_on: []
  - Red proof: `tests/test_warehouse_health_report.py` initially failed with missing `livewire_scripts.warehouse_health_report`.
  - Covered parquet discovery, coverage metric calculation, HTML rendering, and `livewire_quality.py warehouse` dispatch.

- [x] H1 Implement the parquet scanner.
  depends_on: [H0]
  - Scan actual `data-lake/bronze/asset_class=*/symbol=*/*.parquet` files.
  - Read only metadata and key date/timestamp columns needed for health metrics.
  - Added calendar caching so full-warehouse scans avoid repeated trading-calendar work.

- [x] H2 Implement static HTML rendering.
  depends_on: [H1]
  - Render self-contained HTML with summary cards, asset/timeframe sections, and a searchable sortable ticker table.

- [x] H3 Wire the CLI and operator docs.
  depends_on: [H2]
  - Add `python scripts/livewire_quality.py warehouse --output ...`.
  - Document the command in the README quality section.

- [x] H4 Verify.
  depends_on: [H3]
  - Run focused tests, warning-sensitive tests if relevant, and the repo coverage gate if practical.
  - Verification: `git diff --check`; focused tests `29 passed`; coverage gate `987 passed, 1 skipped`, 100%; warning-sensitive suite `987 passed, 1 skipped`.

- [x] H5 Generate the live report.
  depends_on: [H4]
  - Run against the real `~/market-warehouse/data-lake/bronze` parquet tree and report the output path plus summary counts.
  - Generated `/Users/moremeds/market-warehouse/reports/warehouse_health.html`: 9,687 snapshots, 2,454 symbols, 21,326,122 rows, aggregate coverage 86.37%, runtime 25.6s.

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
