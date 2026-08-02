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
- DuckDB is the analytical query layer: views over the parquet lake plus a small coverage table of per-symbol file statistics. It copies no bar data and is never a second system of record
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
- The repo enforces `95%` coverage for the configured source set (`fail_under = 95`, CI `--cov-fail-under=95`).
- Before finishing meaningful changes, run (matches CI):
  - `uv run pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing`
- The native macOS client tests are now in the standalone Sift repo at `~/dev/apps/util/sift/`
- When script tests mock async runners such as `ib.ib.run(...)`, also run:
  - `uv run pytest tests -q -W error::RuntimeWarning`
- When fixing a bug, add or update a regression test if it fits.

## Bug Fixing

- Start from the actual failing behavior: logs, tests, or reproducible commands.
- Fix the root cause, not just the symptom.
- If the issue is in a test seam, prefer fixing the seam instead of adding runtime-only workaround logic.
- If the user corrects a prior assumption or answer, update [tasks/lessons.md](./tasks/lessons.md).

## Operational Facts

- **IB Gateway + IBC run on the Mac mini — which is the host these sessions run ON.** livewire consumes that infrastructure and never installs/restarts the Gateway. ⚠️ **Connect to `127.0.0.1:4001`, never the LAN IP.** The mini's LAN address is TCP-open, so `nc -z` against it succeeds — but `TrustedTwsApiClientIPs` is empty, so an API connection there silently times out after ~4 minutes with no error. A "hanging" IB run is almost always this. The code default `127.0.0.1:4001` is already correct; do not override it. `MDW_IB_HOST`/`MDW_IB_PORT` and `--host`/`--port` exist but need no change locally. Gateway pinned to **10.45**; 2FA approved manually in IBKR Mobile. Do not write order workflows or auto-restart the Gateway on failure.
- `IBClient.connect()` already retries successive `clientId` values after IB error `326`.
- `scripts/livewire_ingest.py daily` is the scheduled parquet-first daily sync and supports `--target-date YYYY-MM-DD` for fixed-date catch-up runs without publishing later bars.
- `scripts/livewire_ingest.py cboe-vol` fetches all CBOE volatility indices directly from CBOE's public API. This is the authoritative daily sync source for VIX, VVIX, VXHYG, VXSMH, and all other volatility indices in `presets/volatility.json`; for `VIX` and `SPX`, it appends newer official daily-price CSV backup rows when the chart JSON lags.
- `scripts/livewire_ops.py run-daily-job` syncs equities and futures via IB, then all volatility indices via CBOE in a single daemon run.
- `scripts/livewire_ingest.py robust` is the canonical multi-ticker IB execution model. Use it instead of bare `fetch_ib_historical.py` for any bulk run over five tickers; outcomes are reported as `ok`, `ok-noop`, `skip`, `fail`, or `timeout`.
- `scripts/livewire_ingest.py backfill-all` runs the maximum-entitled-history full-market Massive flat-file equity-intraday build once, in parallel with the CBOE/IB volatility lane, after equity daily and FRED backfill.
- `scripts/livewire_quality.py report --view summary --since 24h --email` is the daily quality rollup. The end-of-day path in `scripts/livewire_ops.py run-daily-job` invokes it after successful market-data syncs.
- Reliability telemetry and quality audit events are source-tagged JSONL. Valid source values are the closed set `ib`, `uw`, and `massive`.
- Quality flags are emitted independently to the parquet sidecar, central audit JSONL, and Nodemailer alert path; one failed emit path should not block the others.
- `scripts/livewire_store.py duckdb` is the analytical surface: `build` (rebuild + publish the coverage table), `freshness`, `lag`, `stale`, `bars`, `sql`, `views`. The nightly orchestrators run `duckdb build` last, after every writer.
- **Name your symbols when you can.** `duckdb bars` / `read_symbols()` construct `symbol=<TICKER>/<tf>.parquet` paths directly and return in well under a second; the same query through a glob view must enumerate every file behind it first (221s to bind the equity `1h` glob, measured 2026-08-02). Views are registered on demand for the same reason — `connect()` registers none by default.
- `scripts/livewire_ingest.py flatfile-ingest` is the only equity-intraday path. Modes are `discover`, `backfill`, `catch-up`, and `repair`; every mode operates on every symbol present in the selected whole-market files. `intraday-backfill` remains IB-only for non-equity.
- Equity-intraday orchestrators require `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY` and fail before other phases when they are absent. There is no equity-intraday REST or IB fallback.
- `flatfile-ingest backfill` discovers the provider-entitled range and enforces projected-storage plus free-space-reserve checks before downloading.
- `scripts/livewire_quality.py coverage` uses the target day's raw `_symbols.parquet` set when available and repairs intraday gaps with `flatfile-ingest repair --dates <date>`.
- `scripts/livewire_quality.py weekly` aggregates seven daily coverage logs into `~/market-warehouse/logs/quality_weekly_YYYY-WW.md`. Self-skips on non-Sunday so it can be called daily without a date branch.
- `scripts/livewire_quality.py health --intraday` is report-only by default. Repair fires implicitly only when `--symbol`, `--since`, and `--timeframe` are all set (targeted, narrow, explicit) and shells out to `backfill_intraday.py`.
- The native macOS app (build scripts, Metal shaders, UI smoke tests) has been extracted to the standalone Sift repo at `~/dev/apps/util/sift/`.
- Daily fallback provider order for equities:
  - Nasdaq historical quote API with `assetclass=stocks`
  - Nasdaq historical quote API with `assetclass=etf`
  - Stooq U.S. daily CSV

## Known Environment Gotchas

Common traps — check these before investigating further:

- **IB Gateway availability**: the Gateway runs on the mini, which is this host — check `nc -z 127.0.0.1 "${MDW_IB_PORT:-4001}"` before assuming IB is up. **A `nc -z` against the LAN IP also succeeds and is a trap**: the port is open but the API connection silently times out. Do not attempt restarts — failures usually mean 2FA, IBKR maintenance, or session conflict, not something livewire should recover.
- **Cold lake reads are minutes, not seconds**: the nightly job writes 23.57 GB of intraday, which evicts the filesystem metadata cache. The same whole-universe query measured 0.86s warm and 283.84s cold, so cold is the normal morning state. Ask freshness/coverage questions through the `duckdb` coverage table (milliseconds, touches no parquet) rather than re-deriving them from 13,270 footers.
- **Empty IB head timestamps**: IB returns empty head timestamps for some symbols. The fallback to `IB_EARLIEST_DATE` is intentional — do not treat it as an error.
- **IB error 326 (client ID in use)**: Handled by auto-retry in `IBClient.connect()`. Do not manually reassign client IDs.
- **Weekend/holiday runs**: IB returns no data on non-trading days. These are harmless no-ops — do not debug "no data returned" on weekends or holidays.
- **CBOE volatility fetch**: Volatility indices use CBOE's public API, not IB. If VIX or SPX data looks stale, check `fetch_cboe_volatility.py` and the official daily-price CSV backup behavior, not IB connectivity.

## Memory Files

- Use [.codex/project-memory.md](./.codex/project-memory.md) for durable, cross-session project memory.
- Do not put ephemeral task state there. Use [tasks/todo.md](./tasks/todo.md) for active work and [tasks/lessons.md](./tasks/lessons.md) for correction-driven lessons.
- If a project rule, architecture decision, or stable operational fact changes, update `.codex/project-memory.md` in the same task.
