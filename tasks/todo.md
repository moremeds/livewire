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
