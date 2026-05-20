# Project Memory

Use this file for durable, cross-session project memory only.

Do not store:
- ephemeral task status
- one-off debugging notes
- temporary counts, dates, or command output

Use this file for:
- stable architecture decisions
- durable workflow rules
- operational facts that future Codex sessions should not have to rediscover

## Durable Facts

- This project is **Livewire** (rebranded 2026-05-17 from "market-data-warehouse"). The git repo directory is `~/projects/livewire/`. The on-disk data tree intentionally stays at `~/market-warehouse/` — that path is descriptive of the role, not the project name, so it was not renamed. Functional identifiers (`MDW_*` env vars, `mdw.*` logger names, `md.*` analytical schema) are unchanged.
- Canonical storage is bronze Parquet.
- Postgres is an optional replayable analytical publish target rebuilt from bronze parquet and reliability JSONL; it is not canonical storage and live ingestion scripts do not write to it.
- Live equity data is stored per ticker at `~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=<ticker>/1d.parquet`.
- Delisted symbols that should no longer participate in future syncs or backfills are archived outside the canonical sync path under `~/market-warehouse/data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/1d.parquet`.
- `scripts/livewire_ingest.py daily` is parquet-first and does not write to analytical databases.
- `scripts/livewire_ingest.py daily` supports `--target-date YYYY-MM-DD` for fixed-date catch-up runs and only publishes bars with `latest < trade_date <= target`.
- `scripts/livewire_store.py rebuild-postgres` rebuilds Postgres analytical tables under `MDW_POSTGRES_SCHEMA` (default `md`) from bronze parquet and can import telemetry / quality JSONL artifacts.
- `scripts/livewire_store.py smoke-postgres --ensure-schema` verifies Postgres connectivity, creates the schema when requested, and prints table counts.
- Scheduled daily syncs now run through `scripts/livewire_ops.py run-daily-job`, which retries failures before sending Nodemailer-based terminal alerts.
- A separate `scripts/livewire_quality.py watchdog` watchdog is available to alert when the scheduled daily sync never starts or never writes a completion marker.
- Failure alerts can now generate a human-readable Markdown incident report and include a Cerebras-generated summary plus proposed remediation in the email body when the AI config is available.
- Daily syncs use IB as the primary source for equities and futures; CBOE's public API is the authoritative source for all volatility indices.
- `scripts/livewire_ingest.py cboe-vol` fetches all volatility indices from `presets/volatility.json` directly from CBOE's API (`cdn.cboe.com/api/global/delayed_quotes/charts/historical/`).
- `scripts/livewire_ingest.py fred-rates` fetches U.S. Treasury constant-maturity yield series from FRED using `FRED_API_KEY`. Defaults are `DGS3`, `DGS5`, `DGS10`, and `DGS30`, persisted under `~/market-warehouse/data-lake/bronze/asset_class=rates/symbol=<series>/1d.parquet` with a rates-specific schema.
- `scripts/livewire_ops.py run-daily-job` syncs equities and futures via IB, then all volatility indices via CBOE in a single daemon run.
- The canonical multi-ticker IB execution model is `scripts/livewire_ingest.py robust`. Use it instead of direct historical command loops for any bulk run >5 tickers.
- Telemetry events (IB farm states, connection lifecycle) land in `~/market-warehouse/logs/telemetry.jsonl`. Schema is source-tagged JSONL with `{ts, source, event, ...}`.
- Quality flags (range_shortfall, interior_gaps, fetch_tainted, row_count_anomaly) are emitted to three independent paths: sidecar `<parquet>.meta.json`, central `quality_audit.jsonl`, and Nodemailer email via `--mode flag-alert`.
- `scripts/livewire_quality.py report --view summary --since 24h --email` is the daily rollup; it runs end-of-day from `scripts/livewire_ops.py run-daily-job` and writes a `quality_summary_YYYY-MM-DD.marker`.
- Source enum is closed-set `{"ib", "uw", "massive"}` validated at every JSONL emit boundary.
- Equities fallback scope is the repo's U.S. equity and ETF universe on the NYSE trading calendar.
- Equities fallback provider order is:
  - Nasdaq historical quote API with `assetclass=stocks`
  - Nasdaq historical quote API with `assetclass=etf`
  - Stooq U.S. daily CSV
- `IBClient.connect()` already retries successive `clientId` values after IB error `326`.
- `PostgresClient.replace_equities_from_parquet()` recreates the selected analytical tables from scratch on rebuild so repeat Postgres rebuilds are replayable from bronze.
- Preferred IBC startup on macOS is the machine-local secure service installed by `scripts/livewire_ops.py ibc-install`, which writes wrappers under `~/ibc/bin`, a LaunchAgent under `~/Library/LaunchAgents/local.ibc-gateway.plist`, and renders a temporary runtime config from `~/ibc/config.secure.ini` plus Keychain secrets instead of storing IB credentials in plaintext config.
- For this repo, the secure IBC service is a required machine-local dependency for IB-backed workflows, but the service itself is global to the user's Mac rather than scoped to this repo.
- `symbol_id` for new symbols is a stable 53-bit `blake2b(symbol)`-derived value.
- The native macOS client has been extracted to the standalone **Sift** app at `~/dev/apps/util/sift/`.
- The repo-local quant backtesting skill lives at `.codex/skills/quant-backtest/` and should be used for future backtesting or systematic strategy tasks in this repo.

## Durable Workflow Rules

- For non-trivial work, write a fresh plan to `tasks/todo.md` before editing.
- Every plan must include a dependency graph and `depends_on: []` task annotations.
- If the user corrects an assumption or prior answer, update `tasks/lessons.md`.
- Use `apply_patch` for manual file edits.
- Run coverage for changes in `clients/` or `scripts/`.
- When script tests mock async runners like `ib.ib.run(...)`, also run `-W error::RuntimeWarning` so leaked coroutine warnings fail fast.

## Update Policy

- Update this file only when a rule or fact should survive across future sessions.
- If a detail belongs to operators or contributors generally, also update `README.md` or `CLAUDE.md`.
- If a detail is just about the current task, put it in `tasks/todo.md` instead.
