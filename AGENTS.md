# Codex Agent Guide — Livewire

This file is the repo-root startup guide for Codex. Keep it concise, durable, and aligned with the live codebase.

## Session Start

At the start of every new Codex session in this repo:

1. Read [CLAUDE.md](./CLAUDE.md) for implementation details, repo layout, and testing rules.
2. Read [README.md](./README.md) for the current architecture, runtime behavior, and operator-facing commands.
3. Read [.codex/project-memory.md](./.codex/project-memory.md) for durable project-specific memory that should persist across sessions.
4. For native macOS client work, see the standalone Sift repo at `~/dev/apps/util/sift/`.
5. Read [tasks/lessons.md](./tasks/lessons.md) when the task touches workflow, operational recovery, or a recently corrected mistake.
6. Run `git status --short` before making assumptions about the worktree.

## Project Purpose

This repo is **Livewire**, a local-first market data warehouse optimized for single-machine operation. Rebranded 2026-05-17 from "market-data-warehouse"; the name is the project, "market data warehouse" describes the role.

Current live shape:
- Canonical storage is per-ticker bronze Parquet under `~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=<ticker>/1d.parquet`
- Delisted symbols that should no longer participate in future syncs or backfills are archived under `~/market-warehouse/data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/1d.parquet`
- DuckDB is a local analytical and rebuild target, not the live write path
- Interactive Brokers is the primary source for ingestion
- Daily syncs can recover unresolved target-day gaps for the current U.S. equity universe with a narrow external fallback chain
- The native macOS client has been extracted to the standalone **Sift** app at `~/dev/apps/util/sift/`
- The long-term direction is broader multi-asset support and future ClickHouse publishing

## Working Rules

- For non-trivial work, write a plan to [tasks/todo.md](./tasks/todo.md) first.
- Every plan must include a dependency graph and `depends_on: []` task annotations.
- Use `rg` for search and `rg --files` for file discovery.
- Use `apply_patch` for manual file edits.
- Do not revert unrelated user changes.
- Treat bronze Parquet as the system of record unless the task explicitly says otherwise.
- Keep changes minimal and direct. Prefer the smallest coherent fix over speculative refactors.

## Coding Expectations

- Prefer Python 3.13-compatible code.
- Preserve the current parquet-first write path.
- Keep data integrity explicit: validate before publish, keep atomic file replacement semantics intact.
- Keep runtime behavior observable. If you add a recovery path or new branch, expose enough counters or logs to make it diagnosable.
- Do not introduce a second canonical write path for the same data.

## Testing Expectations

- All code in `clients/` and `scripts/` needs tests.
- The repo enforces `100%` coverage for the configured source set.
- Before finishing meaningful changes, run:
  - `source ~/market-warehouse/.venv/bin/activate`
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing`
- The native macOS client tests are now in the standalone Sift repo at `~/dev/apps/util/sift/`
- When script tests mock async runners such as `ib.ib.run(...)`, also run:
  - `python -m pytest tests -q -W error::RuntimeWarning`
- When fixing a bug, add or update a regression test if it fits.

## Bug Fixing

- Start from the actual failing behavior: logs, tests, or reproducible commands.
- Fix the root cause, not just the symptom.
- If the issue is in a test seam, prefer fixing the seam instead of adding runtime-only workaround logic.
- If the user corrects a prior assumption or answer, update [tasks/lessons.md](./tasks/lessons.md).

## Operational Facts

- IB Gateway is expected on `127.0.0.1:4001` by default, configurable via `MDW_IB_HOST`/`MDW_IB_PORT` env vars or `--host`/`--port` CLI flags.
- Gateway can run via **Docker** (`docker/ib-gateway/`, recommended) or the native **macOS IBC service** (`~/ibc`, `~/ibc-install`, `~/Library/LaunchAgents/local.ibc-gateway.plist`).
- The Docker setup uses `gnzsnz/ib-gateway-docker` with file-based secrets and SOCAT port relay (host 4001 → container 4003 → Gateway 4001).
- The native IBC service is not scoped to this repo and should be treated as shared machine-local infrastructure.
- `IBClient.connect()` already retries successive `clientId` values after IB error `326`.
- `scripts/daily_update.py` is the scheduled parquet-first daily sync and supports `--target-date YYYY-MM-DD` for fixed-date catch-up runs without publishing later bars.
- `scripts/fetch_cboe_volatility.py` fetches all CBOE volatility indices directly from CBOE's public API. This is the authoritative daily sync source for VIX, VVIX, VXHYG, VXSMH, and all other volatility indices in `presets/volatility.json`.
- `scripts/run_daily_update_job.py` syncs equities and futures via IB, then all volatility indices via CBOE in a single daemon run.
- `scripts/run_ib_fetch_robust.py` is the canonical multi-ticker IB execution model. Use it instead of bare `fetch_ib_historical.py` for any bulk run over five tickers; outcomes are reported as `ok`, `ok-noop`, `skip`, `fail`, or `timeout`.
- `scripts/data_quality_report.py --view summary --since 24h --email` is the daily quality rollup. The end-of-day path in `scripts/run_daily_update_job.py` invokes it after successful market-data syncs.
- Reliability telemetry and quality audit events are source-tagged JSONL. Valid source values are the closed set `ib`, `uw`, and `massive`.
- Quality flags are emitted independently to the parquet sidecar, central audit JSONL, and Nodemailer alert path; one failed emit path should not block the others.
- `scripts/rebuild_duckdb_from_parquet.py` rebuilds DuckDB from bronze when a local DB file is needed and recreates the analytical tables from scratch on each run.
- `scripts/backfill_intraday.py` is the canonical entry point for full historical 1h/5m backfills. It is the **only** script that actually fetches intraday bars from IB — `fetch_ib_historical.py` is daily-only and `intraday_update.py` only classifies session state. Reuses `compute_intraday_chunks` (1 W chunks for 5m, 1 M for 1h) and `validate_intraday_bar`. Per-timeframe cursor at `~/market-warehouse/cursors/cursor_intraday_{tf}_{name}.json`.
- `scripts/coverage_report.py` runs after the upload step in the entrypoint job cycle. Writes a one-line coverage summary per day, and triggers a targeted backfill when any timeframe drops below `MDW_COVERAGE_ALERT_THRESHOLD` (default `0.95`). 1d branch shells out to `fetch_ib_historical.py`; 1h/5m branch shells out to `backfill_intraday.py`. Safety cap of 100 missing symbols aborts the auto-recovery and emails immediately. Email goes out only when post-recovery gaps remain.
- `scripts/weekly_quality_summary.py` aggregates seven daily coverage logs into `~/market-warehouse/logs/quality_weekly_YYYY-WW.md`. Self-skips on non-Sunday so it can be called daily without a date branch.
- `scripts/health_check.py --intraday` is report-only by default. Repair fires implicitly only when `--symbol`, `--since`, and `--timeframe` are all set (targeted, narrow, explicit) and shells out to `backfill_intraday.py`.
- The native macOS app (build scripts, Metal shaders, UI smoke tests) has been extracted to the standalone Sift repo at `~/dev/apps/util/sift/`.
- Daily fallback provider order for equities:
  - Nasdaq historical quote API with `assetclass=stocks`
  - Nasdaq historical quote API with `assetclass=etf`
  - Stooq U.S. daily CSV

## Known Environment Gotchas

Common traps — check these before investigating further:

- **IB Gateway availability**: Check `docker compose ps` (Docker) or `~/ibc/logs/ibc-gateway-service.log` (native) and port 4001 before assuming IB is reachable.
- **Docker vs native Gateway port conflict**: Both bind to `127.0.0.1:4001` by default. Do not run both simultaneously.
- **DuckDB file locks**: Never open `market.duckdb` from the live service path. The daily update intentionally avoids DuckDB writes — this is by design, not a bug.
- **Empty IB head timestamps**: IB returns empty head timestamps for some symbols. The fallback to `IB_EARLIEST_DATE` is intentional — do not treat it as an error.
- **IB error 326 (client ID in use)**: Handled by auto-retry in `IBClient.connect()`. Do not manually reassign client IDs.
- **Weekend/holiday runs**: IB returns no data on non-trading days. These are harmless no-ops — do not debug "no data returned" on weekends or holidays.
- **CBOE volatility fetch**: Volatility indices use CBOE's public API, not IB. If VIX data looks stale, check `fetch_cboe_volatility.py`, not IB connectivity.

## Memory Files

- Use [.codex/project-memory.md](./.codex/project-memory.md) for durable, cross-session project memory.
- Do not put ephemeral task state there. Use [tasks/todo.md](./tasks/todo.md) for active work and [tasks/lessons.md](./tasks/lessons.md) for correction-driven lessons.
- If a project rule, architecture decision, or stable operational fact changes, update `.codex/project-memory.md` in the same task.
