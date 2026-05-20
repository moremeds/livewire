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
- Postgres is the replayable analytical publish target when SQL access is needed; it is not the live write path
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
- Gateway is owned by the separate **trading-stack** project at `~/trading-stack/`. IBC installed at `/opt/ibc/`, watchdog LaunchAgent `local.ibc-watchdog` runs `~/trading-stack/scripts/ibc_watchdog_launchd.sh` every 5 min. Status via `~/trading-stack/scripts/ibc_gateway_status.sh`; combined bounce via `~/trading-stack/scripts/bounce_ibc_xenon.sh`. Authoritative runbook: `~/runbooks/trading-stack/ib-gateway-ibc.md`.
- IB Gateway is pinned to **10.45** (10.46 is incompatible). 2FA is approved manually in IBKR Mobile on every fresh login. Do not read `/opt/ibc/config.ini`, write order workflows, or auto-restart Gateway on failure.
- `IBClient.connect()` already retries successive `clientId` values after IB error `326`.
- `scripts/livewire_ingest.py daily` is the scheduled parquet-first daily sync and supports `--target-date YYYY-MM-DD` for fixed-date catch-up runs without publishing later bars.
- `scripts/livewire_ingest.py cboe-vol` fetches all CBOE volatility indices directly from CBOE's public API. This is the authoritative daily sync source for VIX, VVIX, VXHYG, VXSMH, and all other volatility indices in `presets/volatility.json`; for `VIX` and `SPX`, it appends newer official daily-price CSV backup rows when the chart JSON lags.
- `scripts/livewire_ops.py run-daily-job` syncs equities and futures via IB, then all volatility indices via CBOE in a single daemon run.
- `scripts/livewire_ingest.py robust` is the canonical multi-ticker IB execution model. Use it instead of bare `fetch_ib_historical.py` for any bulk run over five tickers; outcomes are reported as `ok`, `ok-noop`, `skip`, `fail`, or `timeout`.
- `scripts/livewire_ingest.py backfill-all` is the default warehouse build: equity daily seed/backfill for `sp500`, `ndx100`, and `r2k`; FRED Treasury yield rates; Massive equity intraday (`1m`, `5m`, `1h`, 5 years); CBOE volatility daily; IB `VIX`/`SPX` volatility/index intraday (`5m`, `1h`); and Postgres rebuild when `MDW_POSTGRES_DSN` is set. Massive equity and IB/CBOE volatility lanes run in parallel after daily equity/FRED backfill.
- `scripts/livewire_quality.py report --view summary --since 24h --email` is the daily quality rollup. The end-of-day path in `scripts/livewire_ops.py run-daily-job` invokes it after successful market-data syncs.
- Reliability telemetry and quality audit events are source-tagged JSONL. Valid source values are the closed set `ib`, `uw`, and `massive`.
- Quality flags are emitted independently to the parquet sidecar, central audit JSONL, and Nodemailer alert path; one failed emit path should not block the others.
- `scripts/livewire_store.py rebuild-postgres` rebuilds Postgres analytical tables from bronze parquet and reliability JSONL artifacts.
- `scripts/livewire_ingest.py intraday-backfill` is the canonical entry point for full historical intraday backfills. The default equity intraday build is Massive `1m` for 5 years; non-equity intraday stays IB-backed. Volatility intraday is scoped to `VIX` and `SPX` through `presets/volatility-intraday.json`. `fetch_ib_historical.py` is daily-only and `intraday_update.py` only classifies session state. IB chunks use `compute_intraday_chunks` (1 D for 1m, 1 W for 5m, 1 M for 1h) and all paths validate with `validate_intraday_bar`. Per-timeframe cursor at `~/market-warehouse/cursors/cursor_intraday_{tf}_{name}.json`.
- `scripts/livewire_quality.py coverage` runs after the upload step in the entrypoint job cycle. Writes a one-line coverage summary per day for `1d`, `1m`, `1h`, and `5m`, and triggers a targeted backfill when any timeframe drops below `MDW_COVERAGE_ALERT_THRESHOLD` (default `0.95`). 1d branch shells out to historical backfill; intraday branches shell out to `intraday-backfill --source massive --asset-class equity`. Safety cap of 100 missing symbols aborts the auto-recovery and emails immediately. Email goes out only when post-recovery gaps remain.
- `scripts/livewire_quality.py weekly` aggregates seven daily coverage logs into `~/market-warehouse/logs/quality_weekly_YYYY-WW.md`. Self-skips on non-Sunday so it can be called daily without a date branch.
- `scripts/livewire_quality.py health --intraday` is report-only by default. Repair fires implicitly only when `--symbol`, `--since`, and `--timeframe` are all set (targeted, narrow, explicit) and shells out to `backfill_intraday.py`.
- The native macOS app (build scripts, Metal shaders, UI smoke tests) has been extracted to the standalone Sift repo at `~/dev/apps/util/sift/`.
- Daily fallback provider order for equities:
  - Nasdaq historical quote API with `assetclass=stocks`
  - Nasdaq historical quote API with `assetclass=etf`
  - Stooq U.S. daily CSV

## Known Environment Gotchas

Common traps — check these before investigating further:

- **IB Gateway availability**: Check `~/ibc/logs/ibc-gateway-service.log` and port 4001 before assuming IB is reachable.
- **Analytical publish targets**: Live ingestion writes bronze parquet only. Rebuild Postgres explicitly when SQL access needs refreshed analytical tables.
- **Empty IB head timestamps**: IB returns empty head timestamps for some symbols. The fallback to `IB_EARLIEST_DATE` is intentional — do not treat it as an error.
- **IB error 326 (client ID in use)**: Handled by auto-retry in `IBClient.connect()`. Do not manually reassign client IDs.
- **Weekend/holiday runs**: IB returns no data on non-trading days. These are harmless no-ops — do not debug "no data returned" on weekends or holidays.
- **CBOE volatility fetch**: Volatility indices use CBOE's public API, not IB. If VIX or SPX data looks stale, check `fetch_cboe_volatility.py` and the official daily-price CSV backup behavior, not IB connectivity.

## Memory Files

- Use [.codex/project-memory.md](./.codex/project-memory.md) for durable, cross-session project memory.
- Do not put ephemeral task state there. Use [tasks/todo.md](./tasks/todo.md) for active work and [tasks/lessons.md](./tasks/lessons.md) for correction-driven lessons.
- If a project rule, architecture decision, or stable operational fact changes, update `.codex/project-memory.md` in the same task.
