# Sub-D 1m Intraday Backfill Default

## Adversarial Review Fixes

## Goal

Close the merge-blocking issues found during the adversarial review of the current Sub-D/backfill-all branch: failed Massive windows must not advance cursors, `1m` must be either covered by downstream publish/quality surfaces or explicitly supported, expected Massive extended-hours rows must not become quality noise, and the backfill-all tests must exercise behavior instead of only static string presence.

## Dependency Graph

- V0 -> V1
- V1 -> V2
- V2 -> V3

## Tasks

- [x] V0 Add failing regression tests for Massive all-window failure cursor behavior, Postgres `1m` rebuild, coverage `1m`, Massive RTH filtering, and backfill-all behavior checks.
  depends_on: []
  - Red tests confirmed the gaps: provider errors still completed cursors, Postgres rejected/skipped `1m`, coverage omitted `1m`, and `backfill-all` lacked an executable phase smoke.

- [x] V1 Implement fixes for cursor failure semantics, `1m` Postgres/coverage surfaces, and Massive RTH filtering.
  depends_on: [V0]
  - Provider errors now leave preset cursors untouched and exit non-zero, Massive extended-hours rows are filtered before validation, Postgres has `equities_1m`, rebuild supports `--timeframe 1m`, and coverage/weekly summaries include `1m`.

- [x] V2 Update docs and durable project memory to reflect `1m` coverage/Postgres behavior.
  depends_on: [V1]
  - Updated README, CLAUDE, AGENTS, and `.codex/project-memory.md`.

- [x] V3 Run focused tests, shell validation, diff check, and the full configured pytest gate.
  depends_on: [V2]
  - Focused regression suite: 137 passed.
  - Shell/diff validation: `bash -n tools/run_backfill_all.sh` passed; `git diff --check` passed.
  - Full gate: `918 passed, 1 skipped`, 100% coverage.

## Goal

Make the warehouse backfill job build five years of `1m` equity intraday bronze parquet by default, using Massive as the preferred source for the default equity job path. Intraday for non-equity asset classes remains IB-backed.

## Dependency Graph

- T0 -> T1
- T0 -> T2
- T1, T2 -> T3
- T3 -> T4
- T4 -> T5
- T5 -> T6

## Tasks

- [x] T0 Add failing tests for `1m` intraday support and Massive source wiring.
  depends_on: []
  - Added tests for `1m` parquet constants, Massive intraday aggregates, Massive equity preflight bypass, equity-only Massive enforcement, and the Massive `1m` dry-run default.

- [x] T1 Add `1m` to intraday storage/timeframe constants and validation.
  depends_on: [T0]
  - Added `1m.parquet`, `1 min` IB bar size, 5-year depth, 1-day IB chunks, and 1-minute grid validation.

- [x] T2 Add Massive intraday aggregate normalization/fetch support.
  depends_on: [T0]
  - Added `MassiveIntradayBar`, `get_intraday_bars()`, and normalization from Massive aggregate timestamps to UTC bars.

- [x] T3 Teach `intraday-backfill` to support `--source massive`, default `1m` depth to 5 years, bypass IB preflight only for Massive equity runs, and reject Massive for non-equity intraday.
  depends_on: [T1, T2]
  - Added `--source`, `--asset-class`, Massive window fetch/merge path, equity-only guard, and preflight bypass only for Massive equity intraday.

- [x] T4 Add `1m`/Massive as the default intraday phase in the bulk backfill runner.
  depends_on: [T3]
  - Added Phase 3 to `tools/run_backfill_all.sh`: `intraday-backfill --timeframe 1m --source massive --asset-class equity --years 5 --skip-existing`.

- [x] T5 Update operator docs and durable project memory.
  depends_on: [T4]
  - Updated `README.md`, `CLAUDE.md`, `AGENTS.md`, and `.codex/project-memory.md`.

- [x] T6 Run focused tests and the full configured gate.
  depends_on: [T5]
  - Focused tests: 144 passed across intraday storage/backfill, Massive client, entrypoint preflight, intraday status, health timestamp generation, and intraday validation.
  - Shell syntax: `bash -n tools/run_backfill_all.sh` passed.
  - Operator dry-run: Massive `1m` equity AAPL dry-run reported `years=5` and `61 Massive date windows`.
  - Full gate: `912 passed, 1 skipped`, 100% coverage.

# Expand Backfill-All Default Scope

## Goal

Expand `livewire-backfill` / `scripts/livewire_ingest.py backfill-all` so the default warehouse build also includes equity `5m` and `1h` intraday builds, CBOE volatility indices, and Postgres analytical rebuilds after parquet backfill.

## Dependency Graph

- U0 -> U1
- U1 -> U2
- U2 -> U3
- U3 -> U4

## Tasks

- [x] U0 Add failing coverage for the expected `backfill-all` phases.
  depends_on: []
  - Added `tests/test_script_consolidation.py::test_backfill_all_includes_default_full_warehouse_phases`; watched it fail before implementation.

- [x] U1 Update `tools/run_backfill_all.sh` with equity `5m`/`1h`, volatility, and Postgres rebuild phases.
  depends_on: [U0]
  - Added Massive equity `5m` and `1h` phases, CBOE volatility daily, IB volatility/index `5m` and `1h`, and Postgres rebuild when `MDW_POSTGRES_DSN` is set.
  - Massive equity intraday and IB/CBOE volatility lanes run in parallel; multiple IB intraday lanes are not parallelized against each other.

- [x] U2 Update docs/memory to define what `backfill-all` now means.
  depends_on: [U1]
  - Updated `README.md`, `CLAUDE.md`, `AGENTS.md`, and `.codex/project-memory.md`.

- [x] U3 Run focused shell/doc tests and syntax validation.
  depends_on: [U2]
  - Focused tests: 19 passed.
  - `bash -n tools/run_backfill_all.sh` passed.
  - `git diff --check` passed.

- [x] U4 Run full configured gate.
  depends_on: [U3]
  - Full gate: `913 passed, 1 skipped`, 100% coverage.

# E2E Validation Run

## Goal

Run real command-line E2E checks for Sub-A, Sub-B, Sub-C, and DuckDB retirement. Unit/integration pytest gates are supporting evidence only; the primary evidence must come from operator entrypoints writing and reading actual artifacts.

## Dependency Graph

- T0 -> T1
- T1 -> T2
- T1 -> T3
- T2, T3 -> T4
- T1 -> T5
- T2, T3, T4, T5 -> T6
- T6 -> T7

## Tasks

- [x] T0 Write the E2E validation spec.
  depends_on: []
  - Added `docs/superpowers/specs/2026-05-19-livewire-e2e-validation-spec.md`.

- [x] T1 Build an isolated E2E fixture warehouse.
  depends_on: [T0]
  - Temp root: `/private/tmp/livewire-e2e.xMn5eM`.
  - Created 2 daily parquet snapshots, 1 `1h.parquet`, 1 `5m.parquet`, 2 telemetry JSONL events, and 1 quality-audit JSONL flag.

- [x] T2 Run Sub-A reliability E2E through quality CLI/reporting artifacts.
  depends_on: [T1]
  - `livewire_quality.py report --view summary` read temp telemetry/audit artifacts and reported `ib events=1`, `massive events=1`, `row_count_anomaly: 1`, `AAPL: 1 flag(s)`.
  - `livewire_quality.py report --view quality --severity warning` reported `massive/AAPL/1d row_count_anomaly`.
  - `MDW_WAREHOUSE_DIR=/private/tmp/livewire-e2e.xMn5eM livewire_quality.py coverage --target-date 2024-01-03 --no-recover` wrote a temp coverage log with `1d=1/2`, `1h=1/2`, `5m=1/2`.
  - Seeded completion and quality summary markers, then `livewire_quality.py watchdog --run-date 2024-01-03` exited 0.

- [x] T3 Run Sub-B Postgres E2E through rebuild and smoke commands.
  depends_on: [T1]
  - First sandboxed attempt was blocked by local TCP permission; reran with approved local Postgres access.
  - `livewire_store.py rebuild-postgres --dsn postgresql://127.0.0.1:5432/trading --schema md_e2e_20260519_xmn5em --bronze-dir /private/tmp/livewire-e2e.xMn5eM/data-lake/bronze/asset_class=equity --timeframe all --include-reliability ...` -> daily 2 symbols / 3 rows, 1h 1 row, 5m 2 rows, telemetry 2 rows, quality 1 row.
  - `livewire_store.py smoke-postgres --dsn postgresql://127.0.0.1:5432/trading --schema md_e2e_20260519_xmn5em --ensure-schema` -> `SELECT 1 ok`, counts matched fixture.

- [x] T4 Run Sub-C Massive E2E through the daily source CLI when live credentials are available.
  depends_on: [T2, T3]
  - `MASSIVE_API_KEY` was not exported from `.env`, `~/.secrets`, or `~/market-warehouse/.env`.
  - Ran the real `livewire_ingest.py daily --source massive` command against the temp fixture; it reached gap detection for AAPL and then failed at `MassiveClient()` with missing `MASSIVE_API_KEY`.
  - Added the Massive API key to local ignored `.env` without printing the value.
  - Reran with approved network access after sandbox DNS failure.
  - Result: live Sub-C Massive E2E passed. The command bypassed IB preflight, fetched AAPL from Massive, published 1 bar, and exited 0 with `Tickers updated: 1`, `Tickers failed: 0`, `Source massive: 1`, `Bars inserted: 1`, `Bars validated: 1`, `Validation issues: 0`.
  - Parquet verification: AAPL row count became 2 and latest row is `2024-01-03` with OHLCV `184.22/185.88/183.43/184.25/58414460`.

- [x] T5 Run DuckDB-removal E2E through active command surfaces and reference inventory.
  depends_on: [T1]
  - `python scripts/livewire_store.py --help` lists only `rebuild-postgres`, `smoke-postgres`, `sync-r2`, and `migrate-parquet`.
  - Active DuckDB reference inventory returned no matches. `rg` exit code 1 is expected for no matches.
  - `bash -n scripts/setup_market_warehouse.sh` passed.

- [x] T6 Record E2E outcomes, blockers, and cleanup.
  depends_on: [T2, T3, T4, T5]
  - Dropped temporary Postgres schema `md_e2e_20260519_xmn5em`.
  - Removed temporary fixture root `/private/tmp/livewire-e2e.xMn5eM`.
  - Removed generated Python caches, pytest cache, and coverage artifact.
  - E2E outcomes: Sub-A passed, Sub-B passed, Sub-C live Massive passed, DuckDB removal passed.

- [x] T7 Final status check.
  depends_on: [T6]
  - `git status --short --ignored` -> modified `tasks/todo.md`, new E2E spec doc, ignored local `.env` and `.venv/`.
  - Temp fixture removed.
  - No generated `__pycache__/` directories remain.
