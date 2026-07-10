# Archive — Completed Tasks

This file preserves completed task lists that were previously in [todo.md](todo.md). Newest sections are at the top; older completed sections at the bottom.

---

# Plan 6: Unified Warehouse Path Resolution

## Dependency Graph

- T1 -> T2 -> T3 -> T4 -> T5

## Tasks

- [x] T1 Add resolver-contract tests for default, cascading override, specific
  override, and call-time environment resolution.
  depends_on: []
- [x] T2 Implement `livewire_scripts.paths` and fix the three divergent sites:
  historical cursor output, robust row counting, and Postgres rebuild defaults.
  depends_on: [T1]
- [x] T3 Migrate the approved `livewire_scripts` sweep while preserving explicit
  CLI/function argument precedence and dedicated scheduler log variables.
  depends_on: [T2]
- [x] T4 Migrate telemetry and quality artifact fallback paths and verify the
  hardcoded-path acceptance scan.
  depends_on: [T3]
- [x] T5 Run static analysis, tests, coverage, and custom-warehouse smoke checks.
  depends_on: [T4]
  - Ruff and formatting passed; Pyright reported zero errors.
  - Full non-integration suite: 1,328 passed, 109 deselected.
  - Warning-sensitive historical fetch suite: 135 passed.
  - Coverage: 98.96% against the required 95%.
  - `MDW_WAREHOUSE_DIR=/tmp/livewire-plan6-smoke` entrypoint smoke passed.

# Plan 7: Massive Trade-Date Conversion

## Dependency Graph

- T1 -> T2 -> T3 -> T4 -> T5

## Tasks

- [x] T1 Establish matching REST and raw S3 timestamp semantics from live Massive data.
  depends_on: []
  - Six AAPL sessions covered standard time, daylight time, and both 2024 DST transitions.
  - REST timestamps represent market close; S3 timestamps represent midnight Eastern.
- [x] T2 Add a shared Eastern-calendar converter and cross-lane regression coverage.
  depends_on: [T1]
- [x] T3 Audit existing bronze for adjacent-date duplicate OHLCV rows.
  depends_on: [T2]
  - Scanned 18,945,253 bronze rows and 13,996,124 staged raw rows with no read errors.
  - Found zero staged trade-date versus provider-partition mismatches; no parquet repair is required.
- [x] T4 Run focused tests, static analysis, full tests, coverage, and smoke verification.
  depends_on: [T3]
  - Focused suite: 52 passed.
  - Full non-integration suite: 1,317 passed, 1 skipped.
  - Coverage: 98.96% against the required 95%.
  - Ruff and formatting passed; Pyright reported zero errors.
- [x] T5 Commit, push, and open pull request #43.
  depends_on: [T4]

# Plan 5: UTC Date-Key Consistency

## Dependency Graph

- T1 -> T2 -> T3 -> T4

## Tasks

- [x] T1 Add regression tests that distinguish local dates from UTC dates.
  depends_on: []
- [x] T2 Use UTC defaults for watchdog, daily-log, digest, and coverage date keys.
  depends_on: [T1]
- [x] T3 Run focused tests, static analysis, full tests, coverage, and smoke verification.
  depends_on: [T2]
  - Focused affected suite: 93 passed.
  - Full non-integration suite: 1,302 passed, 1 skipped.
  - Coverage: 98.96% against the required 95%.
  - Ruff, formatting, and Pyright passed.
  - Los Angeles timezone-boundary smoke confirmed UTC date selection across all four paths.
- [x] T4 Commit, push, and open pull request #42.
  depends_on: [T3]

# Intraday Catch-Up Failure Recovery

## Goal

Recover the 2026-06-25 `intraday_catchup` failure and prevent corrupt derived
intraday parquet snapshots from aborting future Massive flat-file catch-up runs.

## Dependency Graph

- R0 -> R1
- R1 -> R2
- R2 -> R3
- R0 -> R4

## Tasks

- [x] R0 Diagnose the alert from source logs.
  depends_on: []
  - Root cause was a corrupt derived snapshot:
    `symbol=SONM/1h.parquet` raised `Parquet magic bytes not found in footer`
    while publishing bucket 173.

- [x] R1 Repair the live data file.
  depends_on: [R0]
  - Preserved the corrupt file as
    `1h.parquet.corrupt-20260625-150121`.
  - Rebuilt SONM `1h.parquet` from the readable SONM `1m.parquet`; rebuilt
    file validated with 10,475 rows.

- [x] R2 Rerun the resumable flat-file catch-up.
  depends_on: [R1]
  - `flatfile-ingest catch-up --days 7 --workers 4` resumed from the incomplete
    scope and exited 0.
  - Final state: 256/256 buckets complete, 12,591 tickers complete.

- [x] R3 Verify the operator-facing outcome.
  depends_on: [R2]
  - Retry output: `Downloaded=0 skipped=5 published_tickers=3898`.
  - The original scheduled alert remains in
    `intraday_catchup_2026-06-25.log`, but the failed equity-intraday phase has
    been repaired by the successful retry.

- [x] R4 Add prevention coverage and code.
  depends_on: [R0]
  - Added a regression test for corrupt derived parquet recovery.
  - Flat-file publishing now rebuilds corrupt derived `5m`, `30m`, or `1h`
    snapshots from the already-merged `1m` source snapshot instead of aborting.
  - Verification: focused tests `29 passed`; full coverage command was
    interrupted after an unrelated idle SSL/proxy hang with `1070 passed, 1
    skipped`.

# Massive Flat-File Post-Ship Documentation

## Dependency Graph

```text
D1 Audit active docs against merged behavior
 └─> D2 Update operator and contributor docs
      └─> D3 Mark superseded design guidance
           └─> D4 Verify documentation consistency
```

- [x] D1 Audit active docs against merged behavior.
  depends_on: []
- [x] D2 Update README, CLAUDE, AGENTS, environment examples, changelog, and project memory.
  depends_on: [D1]
- [x] D3 Mark obsolete ticker-scoped Massive intraday design guidance as superseded.
  depends_on: [D2]
- [x] D4 Run stale-reference, command-help, formatting, and repository verification gates.
  depends_on: [D3]
  - Stale active-doc reference scan returned no matches.
  - `flatfile-ingest --help` confirmed only `discover`, `backfill`, `catch-up`, and `repair`.
  - Unified `backfill --help` confirmed `--source {auto,ib}` and the mandatory flat-file equity-intraday route.
  - Ruff check/format passed.
  - Repository gate: 1,259 passed, 1 skipped, 100.00% coverage.

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

# Backfill Terminal No-Data Spec

## Goal

Write a design spec for preventing full historical backfills from repeatedly retrying tickers whose older-history provider result is terminal no-data rather than transient failure.

## Dependency Graph

- B0 -> B1

## Tasks

- [x] B0 Identify the inefficient retry behavior and cursor boundary.
  depends_on: []
  - Backfill cursors currently mark a ticker complete only when older rows are inserted; clean zero-row no-data cases stay retryable.

- [x] B1 Write the terminal no-data cursor semantics spec.
  depends_on: [B0]
  - Define statuses, migration expectations, retry policy, observability, and verification requirements.

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

# Massive Flat-File Full-Market Planning

## Goal

Design and plan a resumable pipeline that ingests every U.S. equity symbol in
Massive minute flat files for the maximum entitled history.

## Dependency Graph

- F0 -> F1
- F1 -> F2

## Tasks

- [x] F0 Investigate the current flat-file, REST, scheduler, and bronze write paths.
  depends_on: []
  - Confirmed scheduled orchestrators use REST despite documentation claiming S3 preference.
  - Confirmed the existing flat-file writer rewrites complete ticker snapshots once per day.
  - Live provider probe: 5,721 objects, 78.04 GiB compressed, accessible from 2003-09-10 through 2026-06-05.

- [x] F1 Write and approve the full-market flat-file design.
  depends_on: [F0]
  - Approved scope: every symbol, maximum available history, bucketed raw staging, symbol-oriented bounded publication, and all derived intraday timeframes.
  - Revised boundary: replacement only; remove legacy ticker-filtered flat-file and Massive REST equity-intraday paths with no backwards compatibility.

- [x] F2 Write and review the task-by-task implementation plan.
  depends_on: [F1]
  - Plan: `docs/plans/2026-06-06-massive-flatfile-full-market.md`
  - Includes explicit deletion of legacy ticker-filtered flat-file and Massive REST equity-intraday paths.

# Massive Flat-File Full-Market Implementation

## Dependency Graph

- M0 -> M1 -> M2 -> M3 -> M4

## Tasks

- [x] M0 Create isolated implementation worktree and verify baseline.
  depends_on: []
- [x] M1 Add S3 classification, bucketed raw staging, and durable resume state.
  depends_on: [M0]
- [x] M2 Add capacity planning, resumable downloads, and bounded ticker-oriented publication.
  depends_on: [M1]
- [x] M3 Remove legacy ticker-filtered flat-file and Massive REST equity-intraday runtime paths.
  depends_on: [M2]
- [x] M4 Complete isolated live validation and final repository gates.
  depends_on: [M3]
  - Live discovery: 5,721 objects, 2003-09-10 through 2026-06-05, 78.04 GiB compressed, 624.28 GiB projected.
  - Capacity gate: full build blocked before download with 576.50 GiB free and 25 GiB minimum reserve.
  - Isolated repair for 2023-10-27: 1,462,240 raw rows and 10,145 exact provider symbols.
  - Every isolated `1m`, `5m`, `30m`, and `1h` bronze set exactly matched all 10,145 raw symbols.
  - Case-distinct provider symbols (`BCPC`/`BCpC`, `CPK`/`CpK`, `TPC`/`TpC`) verified after filesystem-safe encoding fix.
  - AAPL verification: 712 unique `1m` timestamps, stable ID match, and exact derived OHLCV for all three derived timeframes.
  - Resume verification: completed raw date skipped with zero provider inspections; completed publish scope wrote zero tickers.
  - Repository coverage gate: 1,259 passed, 1 skipped, 100.00% coverage.
