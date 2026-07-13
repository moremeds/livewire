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
- Raw market/vendor data should land as Parquet first; databases are derived/replayable publish or query targets unless a future project explicitly says otherwise.
- Postgres is an optional replayable analytical publish target rebuilt from bronze parquet and reliability JSONL; it is not canonical storage and live ingestion scripts do not write to it.
- Live equity data is stored per ticker at `~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=<ticker>/1d.parquet`.
- Equity daily Bronze has row-level `source` and `price_basis` provenance. Canonical basis is raw; legacy schema migration writes `legacy/unknown`. IB `TRADES` history is classified per applicable split boundary and only incoming IB rows are normalized; ambiguous applicable events block publication before mutation.
- Split-basis audit manifests are cryptographically tied to both source files and a resolved data-lake root. Apply/rollback reject stale hashes or a different active root. Full legacy migration persists an atomic resumable cursor.
- Daily and intraday bronze replace/merge operations use blocking `fcntl.flock`
  locks keyed by the exact target parquet path. Persistent `*.parquet.lock`
  sidecars are expected coordination artifacts, ignored by discovery/sync paths,
  and were verified to serialize both threads and processes on the local exFAT
  data lake.
- Delisted symbols that should no longer participate in future syncs or backfills are archived outside the canonical sync path under `~/market-warehouse/data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/1d.parquet`.
- `scripts/livewire_ingest.py daily` is parquet-first and does not write to analytical databases.
- `scripts/livewire_ingest.py daily` supports `--target-date YYYY-MM-DD` for fixed-date catch-up runs and only publishes bars with `latest < trade_date <= target`.
- `scripts/livewire_store.py rebuild-postgres` rebuilds Postgres analytical tables under `MDW_POSTGRES_SCHEMA` (default `md`) from bronze parquet and can import telemetry / quality JSONL artifacts.
- `scripts/livewire_store.py smoke-postgres --ensure-schema` verifies Postgres connectivity, creates the schema when requested, and prints table counts.
- Scheduled daily syncs now run through `scripts/livewire_ops.py run-daily-job`, which retries failures before sending Nodemailer-based terminal alerts.
- A separate `scripts/livewire_quality.py watchdog` watchdog is available to alert when the scheduled daily sync never starts or never writes a completion marker.
- Failure alerts can now generate a human-readable Markdown incident report and include a Cerebras-generated summary plus proposed remediation in the email body when the AI config is available.
- Daily syncs use IB as the primary source for equities and futures; CBOE's public API is the authoritative source for all volatility indices.
- `scripts/livewire_ingest.py cboe-vol` fetches all volatility indices from `presets/volatility.json` directly from CBOE's chart API (`cdn.cboe.com/api/global/delayed_quotes/charts/historical/`), and uses CBOE official daily-price CSV backup rows for `VIX` and `SPX` when the chart JSON lags.
- `scripts/livewire_ingest.py fred-rates` fetches U.S. Treasury constant-maturity yield series from FRED using `FRED_API_KEY`. Defaults are `DGS3`, `DGS5`, `DGS10`, and `DGS30`, persisted under `~/market-warehouse/data-lake/bronze/asset_class=rates/symbol=<series>/1d.parquet` with a rates-specific schema.
- `scripts/livewire_ingest.py corporate-actions` reconciles Massive splits and cash dividends into `data-lake/bronze/asset_class=corporate_action/symbol=<encoded_symbol>/events.parquet`. Provider fetches default to four independent worker sessions, while canonical store and scope-specific atomic cursor writes remain serialized. `--resume` continues only an incomplete compatible cursor; successful reconciliation checkpoints the symbol, and cancellation inference still requires `--full-reconcile`.
- `scripts/livewire_store.py rebuild-silver` derives fully back-adjusted daily bars plus compact factor intervals under `data-lake/silver/` without mutating bronze. Splits adjust price and volume; cash dividends adjust price only. One locked revision spans every changed artifact, and `revisions/current.json` is replaced last as the commit record.
- Silver retains announced future corporate actions in canonical Bronze but excludes them from adjustment factors until their ex-date is effective. Each rebuild resolves one `America/New_York` as-of date for the entire batch; effective actions adjust only bars strictly before their ex-date.
- Silver rebuild summaries expose the batch as-of date plus effective and future action counts. The Silver canary independently recomputes causal factor intervals from Bronze and active actions, so it rejects stale or internally consistent artifacts that include not-yet-effective actions.
- Silver consumes equity row basis explicitly: split factors apply only to raw rows, split-affected unknown rows fail closed, and dividend factors remain independent of split basis. The canary rejects mechanical split-ratio jumps after adjustment.
- `scripts/livewire_quality.py validate-adjusted-history` is the strict read-only full-equity-history gate. It uses Massive adjusted bars first and fills unavailable dates with freshly normalized IB history. Every pointwise OHLC difference is reported, but only independent Massive close differences are pointwise failures; open/high/low and IB replay differences are diagnostic because provider filters, aggregate revisions, and IB request shape can vary. Every eligible 20/50/200-session close average, exact Silver total-return reconstruction, coverage, action, and mechanical-jump check remains a hard gate. External caches and the cursor are bound to canonical input hashes and validator schema version.
- `scripts/livewire_ops.py run-daily-job` reconciles corporate actions before the market-data lanes, requests full provider reconciliation on Sunday, and runs a full Silver rebuild only after every prerequisite lane succeeds. No permanent Silver daemon is used.
- `scripts/livewire_ops.py run-daily-job` syncs equities and futures via IB, then all volatility indices via CBOE in a single daemon run.
- The canonical multi-ticker IB execution model is `scripts/livewire_ingest.py robust`. Use it instead of direct historical command loops for any bulk run >5 tickers.
- `scripts/livewire_ingest.py backfill-all` runs one maximum-entitled-history, full-market Massive flat-file equity-intraday build after equity daily/FRED backfill and in parallel with the volatility lane.
- `scripts/livewire_ingest.py daily-backfill` uses one whole-market Massive flat-file catch-up for recent equity intraday and honors `MDW_DAILY_BACKFILL_INTRADAY_DAYS` (default 7).
- Daily 1d coverage auto-recovery for equities uses `scripts/livewire_ingest.py daily --source massive --target-date <date> --tickers ...` so recent target-date repair does not consume IB historical pacing. Explicit missing bronze tickers get the target-date row only; full historical seeding still belongs to `historical`.
- `scripts/livewire_ingest.py flatfile-ingest` is the only equity-intraday path. It discovers the actual entitled range, stages bucketed raw daily Parquet, publishes every provider ticker to `1m`, and derives `5m`, `30m`, and `1h`. `intraday-backfill` is IB-only for non-equity.
- Equity-intraday orchestrators require Massive S3 credentials and fail before any phases when they are missing; there is no REST or IB equity-intraday fallback.
- A full flat-file backfill is capacity-gated before download using the discovered compressed object size, `MDW_FLATFILE_STORAGE_MULTIPLIER`, and `MDW_FLATFILE_MIN_FREE_GB`.
- Equity `1m` is included in Postgres analytical rebuilds (`equities_1m`) and daily/weekly coverage surfaces alongside `1d`, `1h`, and `5m`.
- Intraday for non-equity asset classes remains IB-backed; `--source massive` is equity-only.
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
- Roadmap naming decision: **Sub-F is Silver** and owns the reproducible cleaned/adjusted layer derived from canonical bronze. **Sub-G is Gold** and owns factors, analytics, and strategy-ready derived tables. Sub-B Postgres remains the replayable SQL publish target and should not be described as silver by itself.
- **IB Gateway + IBC run on the Mac mini, not this MacBook** (as of 2026-07-04). Earlier machine-local IBC stories (`/opt/ibc/` trading-stack install, `~/ibc/` secure service via `livewire_ops.py ibc-install`) no longer apply to this machine — livewire connects remotely via `MDW_IB_HOST`/`MDW_IB_PORT` and never manages the Gateway.
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
