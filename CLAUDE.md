# Livewire

Livewire is a local-first market data warehouse for quantitative research. Parquet data lake as system of record, optional Postgres for replayable analytical publishing, and ClickHouse for production benchmarking. Rebranded 2026-05-17 from "market-data-warehouse"; the repo dir is now `livewire/`, the on-disk data tree remains at `~/market-warehouse/` (descriptive, not project-named).

## Project Layout

Two directory trees: this **git repo** and the **data warehouse** at `~/market-warehouse/`.

```
livewire/                           # Git repo
├── clients/
│   ├── __init__.py                 # Exports BronzeClient, DailyBarFallbackClient, IBClient, PostgresClient
│   ├── bronze_client.py            # Canonical per-ticker bronze parquet client
│   ├── daily_bar_fallback.py       # Public daily-bar fallback chain for U.S. equities/ETFs
│   ├── ib_client.py                # Interactive Brokers API client (ib_async)
│   ├── historical_provider.py       # HistoricalProvider abstraction (IBProvider, contract spec helpers)
│   ├── uw_client.py                # Unusual Whales REST API client (kept, not used for historical)
│   ├── postgres_client.py          # Postgres analytical publish client
│   └── postgres_schema.py          # Postgres md.* schema definitions
├── presets/
│   ├── volatility.json             # CBOE Volatility Indices (VIX, VVIX, etc.)
│   ├── futures-index.json          # CME/CBOT Index Futures (ES, NQ, RTY, YM)
│   ├── futures-energy.json         # NYMEX Energy Futures (CL, NG)
│   ├── futures-metals.json         # COMEX Metals Futures (GC, SI)
│   ├── futures-treasuries.json     # CBOT Treasury Futures (ZB, ZN, ZF)
│   └── ...                         # S&P 500, NDX-100, Russell 2000 sector presets
├── scripts/
│   ├── setup_market_warehouse.sh   # One-time system bootstrap
│   ├── livewire_ingest.py          # Ingest subcommands: daily, historical, robust, CBOE, intraday, universe
│   ├── livewire_quality.py         # Quality subcommands: health, coverage, report, weekly, watchdog
│   ├── livewire_ops.py             # Ops subcommands: scheduled job, alerts
│   └── livewire_store.py           # Storage subcommands: Postgres rebuild, smoke checks, R2 sync, parquet migration
├── livewire_scripts/               # Importable implementations behind the script entrypoints
├── livewire_node/                  # Nodemailer + Cerebras alert helpers
├── launchd/                        # macOS launchd templates
├── tools/                          # Developer hooks and helper shell tools
├── tests/
│   ├── conftest.py                 # Shared fixtures
│   ├── test_daily_bar_fallback.py  # Unit tests for fallback providers
│   ├── test_uw_client.py           # Unit tests — HTTP mocked via `responses`
│   ├── test_fetch_ib_historical.py # Tests for IB fetch script
│   ├── test_daily_update.py        # Tests for daily update script
│   ├── test_ib_client.py           # Focused tests for IB client connect fallback
│   └── test_historical_provider.py # Tests for HistoricalProvider, contract spec helpers
├── pyproject.toml                  # pytest config, coverage enforcement
├── .env.example
└── README.md

~/market-warehouse/                 # Data warehouse (created by setup script)
├── .venv/                          # Python 3.13 venv
├── data-lake/
│   ├── bronze/asset_class=equity/  # Per-ticker Hive-partitioned Parquet (symbol=AAPL/1d.parquet)
│   ├── bronze/asset_class=futures/ # Per-contract Hive-partitioned Parquet (symbol=ES_202506/1d.parquet)
│   ├── bronze/asset_class=rates/   # FRED Treasury yields (symbol=DGS10/1d.parquet)
│   ├── bronze-delisted/asset_class=equity/  # Archived delisted symbols excluded from future sync/backfill runs
│   ├── silver/                     # Cleaned / adjusted
│   └── gold/                       # Derived analytics / factor tables
├── logs/telemetry.jsonl            # Reliability telemetry artifact
├── logs/quality_audit.jsonl        # Central quality-flag artifact
├── clickhouse/                     # Optional ClickHouse data
├── scripts/                        # Bootstrap SQL, helper scripts
└── logs/
```

## Architecture

- **Parquet** is the system of record
- **Data lake tiers**: bronze (normalized Parquet) -> silver (cleaned) -> gold (derived)
- **Postgres** is an optional analytical publish target rebuilt from bronze parquet and reliability JSONL; ingestion does not write Postgres
- **ClickHouse** is optional, for production-style benchmarking and concurrency testing
- **Python env** lives at `~/market-warehouse/.venv/` — activate with `source ~/market-warehouse/.venv/bin/activate`

## Native macOS Client (Extracted)

The native macOS client has been extracted to the standalone **Sift** app at `~/dev/apps/util/sift/`.

See the [Sift CLAUDE.md](~/dev/apps/util/sift/CLAUDE.md) for module layout, build instructions, and testing.

## Analytical Targets

Postgres publishes replayable `md.*` analytical tables from canonical bronze parquet. ClickHouse mirrors the same daily schema with MergeTree engines partitioned by `toYYYYMM(trade_date)` when production-style benchmarking is needed.

## IB Gateway / IBC

IB Gateway runs only on this machine. It is managed by **IBC** (IB Controller) and operated by the separate **trading-stack** project at `~/trading-stack/` — livewire is a consumer of that infrastructure, not its owner.

Authoritative runbook: `~/runbooks/trading-stack/ib-gateway-ibc.md`. Read it before changing anything IBC-related.

- **IBC install**: `/opt/ibc/` (system-wide; not in `~`)
- **IBC config**: `/opt/ibc/config.ini` (contains stored credentials; do not read or modify from livewire)
- **IBC logs**: `/opt/ibc/logs/` (rotating daily files; watchdog log at `/opt/ibc/logs/ibc-watchdog.log`)
- **Gateway app**: `~/Applications/IB Gateway 10.45/` — pinned to **10.45** (10.46 is incompatible; 10.46 installs are renamed `*.disabled`)
- **Watchdog LaunchAgent**: `~/Library/LaunchAgents/local.ibc-watchdog.plist` → runs `~/trading-stack/scripts/ibc_watchdog_launchd.sh` every 5 min
- **Gateway API port**: `127.0.0.1:4001` (live)
- **Trading mode**: live
- **2FA**: user manually approves in **IBKR Mobile** on every fresh login; livewire cannot bypass this
- **Status check**: `~/trading-stack/scripts/ibc_gateway_status.sh` (key=value diagnostics, also called by livewire's preflight)
- **Combined bounce (Gateway + Xenon)**: `~/trading-stack/scripts/bounce_ibc_xenon.sh` — stops watchdog, kills IBC/Gateway, restarts via Terminal launcher, waits for port 4001, then restarts Xenon containers
- **Do NOT**: read `/opt/ibc/config.ini`, write order-management workflows, or repeatedly restart Gateway on failure (failures usually mean 2FA, IBKR maintenance, session conflict, or market-data permission — not something livewire should auto-recover)

## Data Ingestion

Primary data source: **Interactive Brokers** via `ib_async`. Requires IB Gateway at `127.0.0.1:4001` — managed by trading-stack (see "IB Gateway / IBC" section above). IB-backed ingest commands run a preflight check before connecting; if the Gateway is down they print the trading-stack status and exit cleanly rather than burning a 4-min IB timeout. `daily --source massive` is the explicit non-IB daily equity path and bypasses IB preflight.

- `IBClient` wraps `ib_async.IB` with connection management, historical data, and contract qualification
- `IBClient.connect()` defaults to `clientId=0` and automatically retries successive `clientId` values if IB reports error `326` (`client id already in use`)
- `IBClient.get_historical_data()` fetches daily bars via `reqHistoricalData`
- `BronzeClient` is the live service storage client: it discovers symbols from parquet, merges or replaces per-ticker snapshots, and publishes with `temp -> validate -> os.replace()`
- `DailyBarFallbackClient` is a narrow recovery client for unresolved target-day gaps in the current U.S. equity universe. Provider order: Nasdaq `assetclass=stocks`, Nasdaq `assetclass=etf`, then Stooq U.S. daily CSV.
- `MassiveClient` is the near-term daily U.S. equity accelerator and validation reference. It uses `MASSIVE_API_KEY`, stores `adjusted=false` bars with `adj_close = close`, and is not used for long historical backfills or broker-specific asset classes.
- `adj_close` is set to `close` (IB TRADES data doesn't provide adjusted prices)
- **CBOE volatility indices** are fetched directly from CBOE's public API (`cdn.cboe.com/api/global/delayed_quotes/charts/historical/`) via `scripts/livewire_ingest.py cboe-vol`, not IB. This is the authoritative source for VIX, VVIX, VXHYG, VXSMH, and all other CBOE volatility indices. The writer normalizes stale parquet schemas on merge (drops extra columns from older schema versions) and rewrites files to fix schema drift even when no new data is available.
- **Treasury yield rates** are fetched from FRED via `scripts/livewire_ingest.py fred-rates` using `FRED_API_KEY`. Default series are `DGS3`, `DGS5`, `DGS10`, and `DGS30`; they write to `data-lake/bronze/asset_class=rates/symbol=<series>/1d.parquet` with `trade_date`, `symbol_id`, `tenor_years`, `yield_pct`, and `source`.

### IB BarData → Bronze mapping

| IB BarData field | Bronze column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from ticker) | `symbol_id` | Read existing parquet ID or derive stable ID |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `adj_close` | Same value |
| `bar.volume` | `volume` | `int(bar.volume)` |

### IB BarData → Futures Bronze mapping

| IB BarData field | Bronze column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from composite ticker) | `contract_id` | Stable hash of composite ticker (e.g. `ES_202506`) |
| (from composite ticker) | `root_symbol` | Parsed from `ticker.rsplit("_", 1)[0]` |
| (from composite ticker) | `expiry_date` | `YYYY-MM-01` derived from expiry code |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `settlement` | Same value (IB doesn't provide settlement) |
| `bar.volume` | `volume` | `int(bar.volume)` |
| (default) | `open_interest` | `0` (IB BarData doesn't include OI) |

### Running the pipeline

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py historical                                  # Mag 7 default
python scripts/livewire_ingest.py historical --tickers AAPL NVDA              # Custom tickers
python scripts/livewire_ingest.py historical --preset presets/sp500.json      # From preset with cursor resume
python scripts/livewire_ingest.py historical --years 0 --skip-existing        # Inception, skip existing
python scripts/livewire_ingest.py historical --preset presets/sp500.json --backfill  # Backfill older data
python scripts/livewire_ingest.py historical --preset presets/volatility.json --asset-class volatility  # CBOE vol indices (IB backfill)
python scripts/livewire_ingest.py cboe-vol                                                        # CBOE vol indices (daily sync, preferred)
python scripts/livewire_ingest.py fred-rates                                                      # FRED Treasury yields (DGS3/DGS5/DGS10/DGS30)
python scripts/livewire_ingest.py historical --preset presets/futures-index.json --asset-class futures  # CME/CBOT index futures
python scripts/livewire_ingest.py historical --preset presets/futures-energy.json --asset-class futures  # NYMEX energy futures
python scripts/livewire_ingest.py historical --host 192.168.1.50 --port 4001 --tickers AAPL            # Remote IB Gateway
```

IB connection defaults to `127.0.0.1:4001`, configurable via `--host`/`--port` flags or `MDW_IB_HOST`/`MDW_IB_PORT` environment variables.

Reliability foundation environment variables:
- `MDW_TELEMETRY_PATH` (default `~/market-warehouse/logs/telemetry.jsonl`): telemetry JSONL append path; set to `none` to disable telemetry.
- `MDW_QUALITY_AUDIT_PATH` (default `~/market-warehouse/logs/quality_audit.jsonl`): central quality-flag audit JSONL append path.
- `MDW_ALERT_SEVERITY_THRESHOLD` (default `warning`): minimum quality-flag severity that triggers per-flag email.
- `MDW_ALERT_RATE_LIMIT_SECONDS` (default `300`): de-dup window for identical `(source, ticker, category)` alert emails.
- `MDW_ORCHESTRATOR_TIMEOUT_SECONDS` (default `300`): per-ticker hard timeout for `scripts/livewire_ingest.py robust`.
- `MDW_ORCHESTRATOR_MAX_ATTEMPTS` (default `3`): per-ticker retry budget for `scripts/livewire_ingest.py robust`.
- `MDW_ORCHESTRATOR_COOLDOWN_SECONDS` (default `60`): sleep between orchestrator retry attempts.
- `MDW_LOG_LEVEL` (default `INFO`): logger root level for reliability tooling.
- `MDW_UNDELIVERED_DIR` (default `~/market-warehouse/logs/quality_alerts_undelivered/`): where failed per-flag alert HTML bodies are preserved.
- `MDW_LOG_DIR` (default `~/market-warehouse/logs/`): where `scripts/livewire_quality.py report --email` writes `quality_summary_YYYY-MM-DD.marker`.

Postgres analytical publish environment variables:
- `MDW_POSTGRES_DSN`: Postgres DSN for `scripts/livewire_store.py rebuild-postgres` and `scripts/livewire_store.py smoke-postgres`.
- `MDW_POSTGRES_SCHEMA` (default `md`): target analytical schema.
- `MDW_TEST_POSTGRES_DSN`: disposable database DSN for live-gated Postgres integration tests. Tests skip cleanly when unset.

Current fetch behavior:
- Normal mode atomically replaces the per-ticker bronze snapshot
- Backfill mode merges older bars into the same per-ticker bronze snapshot
- The live service path writes bronze parquet only
- If IB returns an empty head timestamp, the fetcher falls back to `IB_EARLIEST_DATE` instead of skipping the symbol
- `--asset-class volatility` uses `Index('SYMBOL', 'CBOE')` contracts instead of `Stock('SYMBOL', 'SMART')` and writes to `data-lake/bronze/asset_class=volatility/`
- `--asset-class futures` uses `Future(root, expiry, exchange)` contracts with composite tickers (`ES_202506`), writes to `data-lake/bronze/asset_class=futures/`, and uses the futures parquet schema (contract_id, root_symbol, expiry_date, settlement, open_interest)

### Backfill mode

`--backfill` fetches only missing older data for tickers already in bronze parquet:
- Queries each ticker's oldest existing `trade_date` from parquet
- Fetches IB inception → oldest_date gap
- Merges older rows into the canonical parquet snapshot
- Uses separate cursor JSON: `cursor_backfill_{name}.json`
- Skips tickers not in bronze parquet (use normal fetch first)

### Auto-restarting runner

```bash
python scripts/livewire_ingest.py backfill-all   # Runs equity presets with stall detection, then FRED rates
```

Output: per-ticker bronze Parquet at `data-lake/bronze/asset_class=equity/symbol=<ticker>/1d.parquet` (or `asset_class=futures/symbol=ES_202506/1d.parquet` for futures). Postgres is rebuilt separately when SQL access is needed.

### Futures preset format

Futures presets use a `contracts` array instead of `tickers`:
```json
{
  "name": "futures-index",
  "asset_class": "futures",
  "contracts": [
    {"root": "ES", "exchange": "CME", "expiry": "202506"},
    {"root": "NQ", "exchange": "CME", "expiry": "202506"}
  ]
}
```
`load_preset()` flattens these into composite tickers (`ES_202506`) and returns an exchange map for contract construction.

Delisted symbols that should no longer participate in future syncs or backfills should be archived outside the canonical sync path under `data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/1d.parquet`.

### Daily updates

`scripts/livewire_ingest.py daily` is the lightweight command for daily scheduled runs (~2,500 tickers). It discovers tickers from bronze parquet, detects gaps vs the latest trading day, fetches only missing bars, validates OHLCV integrity, and atomically rewrites only the affected per-ticker snapshots. If IB leaves unresolved target trading days after validation, the command can recover those dates from the fallback chain before publishing parquet.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py daily                                  # Normal daily run
python scripts/livewire_ingest.py daily --dry-run                        # Report gaps without fetching
python scripts/livewire_ingest.py daily --force                          # Run on non-trading day
python scripts/livewire_ingest.py daily --target-date 2026-03-11        # Recover through a fixed trading date
python scripts/livewire_ingest.py daily --preset presets/sp500.json      # Limit to preset tickers
python scripts/livewire_ingest.py daily --asset-class equity --source massive  # Explicit Massive equity daily path
python scripts/livewire_ingest.py daily --host 127.0.0.1 --port 7497 --max-concurrent 4   # Custom IB config
python scripts/livewire_ingest.py daily --batch-size 25                  # Custom batch size
python scripts/livewire_ingest.py daily --asset-class volatility          # Daily update for volatility indices
python scripts/livewire_ingest.py daily --asset-class futures             # Daily update for futures contracts
```

**Scheduling with launchd** (macOS):
```bash
# Copy examples, replace /path/to/repo with your actual repo path
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
```
`scripts/livewire_ops.py run-daily-job` loads `~/.secrets`, repo `.env`, and `~/market-warehouse/.env` before invoking the retrying scheduled runner. This preserves the old launchd wrapper behavior for API keys like `CEREBRAS_API_KEY`. The runner automatically syncs equities and futures via IB, then all volatility indices via CBOE's public API in a single invocation; pass `--asset-class <name>` to run only one IB asset class (skips CBOE volatility sync).

The main sync runs at 13:05 Pacific local time daily (4:05 PM Eastern year-round). The watchdog runs at 18:30 Pacific by default and alerts if the scheduled sync never started or never logged a completion marker. Non-trading days are harmless no-ops.

**Key design:**
- Discovers tickers from parquet via `BronzeClient.get_latest_dates()` — no hardcoded lists
- `--target-date YYYY-MM-DD` lets operators run a fixed-date catch-up and prevents bars later than the requested target from being published
- Live service writes avoid analytical database file-lock contention
- Bar validation: checks OHLCV relationships, positive prices, valid trading days, duplicate dates
- Atomically rewrites a per-ticker bronze snapshot after each successful merge
- The active sync universe is the canonical bronze tree only; archive delisted symbols outside that tree if they should stop participating in future syncs/backfills
- Recovery path for unresolved target-day gaps (equity only): IB first, Massive second when `MASSIVE_API_KEY` is configured, then Nasdaq historical quote API (`stocks`, then `etf`) and Stooq `symbol.us`; fallback is skipped for non-equity asset classes (volatility, futures, CMDTY, FX)
- Fallback bars use the same validation and bronze merge path as IB bars
- Run summary exposes `Fallback attempts`, `Fallback successes`, `Fallback symbols`, and per-source counts
- Pure-Python NYSE trading calendar — no new dependencies
- Logs to `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`
- Terminal scheduled failures use the Nodemailer CLI at `scripts/livewire_ops.py send-alert`
- Failure alerts can write a sibling `.human.md` incident report and optionally enrich the email body with a Cerebras-generated summary plus proposed remediation
- Failure emails can include Cerebras-generated human-readable incident summaries and write a sibling `*.human.md` incident report beside the raw log

### Reliability tooling

`scripts/livewire_ingest.py robust` is the productized per-ticker IB orchestrator for bulk daily-bar seed/backfill runs. Use it instead of direct historical command loops for any IB bulk run larger than roughly five tickers:

```bash
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode seed
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode backfill
python scripts/livewire_ingest.py robust --tickers AAPL MSFT --mode seed --timeout 300 --max-attempts 3 --cooldown 60
```

Outcome categories:

| Category | Meaning |
| --- | --- |
| `ok` | Child exited cleanly and bronze exists; row delta is included when known. |
| `ok-noop` | Backfill exited cleanly with no row delta, treated as "no older history". |
| `skip` | Seed skipped because bronze already exists, or backfill skipped because no seed parquet exists. |
| `fail` | Child exited non-zero, exhausted retries, or exited zero without producing seed bronze. |
| `timeout` | All attempts hit the hard timeout. |

`scripts/livewire_quality.py report` reads telemetry and quality-audit JSONL:

```bash
python scripts/livewire_quality.py report --view summary --since 24h
python scripts/livewire_quality.py report --view flap --since 24h --source ib
python scripts/livewire_quality.py report --view quality --since 24h --severity critical
python scripts/livewire_quality.py report --view summary --since 24h --email
```

Views are `summary`, `flap`, and `quality`; `--source` accepts `all`, `ib`, `uw`, or `massive`. `--email` sends the daily-summary Nodemailer mode and writes `quality_summary_YYYY-MM-DD.marker` for the watchdog. Quality flags are emitted beside parquet as `<parquet>.meta.json`; the sidecar schema and central audit JSONL schema are specified in `docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md`.

### Intraday backfill (1h / 5m)

`scripts/livewire_ingest.py intraday-backfill` is the canonical entry point for full historical intraday backfills. It is the **only** operator command that actually pulls 1h/5m bars from IB; `scripts/livewire_ingest.py historical` is daily-only and `scripts/livewire_ingest.py intraday-status` is a session-state classifier. Reuses `compute_intraday_chunks` (1 W chunks for 5m, 1 M for 1h) and `validate_intraday_bar` from the Phase 1 plumbing; rejected bars are logged but never written to bronze.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py intraday-backfill --timeframe 5m --tickers AAPL MSFT          # Explicit list
python scripts/livewire_ingest.py intraday-backfill --timeframe 1h --preset presets/sp500.json  # Preset
python scripts/livewire_ingest.py intraday-backfill --timeframe 5m --tickers AAPL --dry-run     # Plan only
python scripts/livewire_ingest.py intraday-backfill --timeframe 5m --preset presets/screened-universe.json --skip-existing
python scripts/livewire_ingest.py intraday-backfill --timeframe 5m --preset presets/sp500.json --max-tickers 50
```

- Per-timeframe cursor: `~/market-warehouse/cursors/cursor_intraday_{1h,5m}_{preset}.json`. Resumes after interrupt.
- IB error 162/200 ("HMDS no data" / ambiguous contract) marks the ticker complete and moves on — no infinite retry loop.
- Default depth: 2 years for 1h, 1 year for 5m (matches `INTRADAY_MAX_DEPTH`).
- `--skip-existing` consults `min(bar_timestamp)` in the existing per-ticker parquet and skips if it already covers the requested depth.
- IB BarData with `formatDate=1` returns naive ET datetimes; the script attaches `America/New_York` and converts to UTC before validation/merge.
- Logs to `~/market-warehouse/logs/backfill_intraday_{1h,5m}_YYYY-MM-DD.log`.

### Coverage tracking + auto-recovery

`scripts/livewire_quality.py coverage` runs every day after the upload step in the container entrypoint. For each of the three timeframes (1d, 1h, 5m) it counts how many symbols have bars current as-of the target trading day, writes a one-line summary to `~/market-warehouse/logs/coverage_YYYY-MM-DD.log`, and — when coverage drops below `MDW_COVERAGE_ALERT_THRESHOLD` (default `0.95`) — triggers a targeted backfill subprocess and re-checks.

```bash
python scripts/livewire_quality.py coverage                                # Today's coverage + auto-recovery
python scripts/livewire_quality.py coverage --no-recover                   # Report only
python scripts/livewire_quality.py coverage --target-date 2026-04-06       # Specific trading day
python scripts/livewire_quality.py coverage --threshold 0.99               # Stricter threshold
python scripts/livewire_quality.py coverage --force                        # Run on a non-trading day
```

- 1d recovery shells out to `scripts/livewire_ingest.py historical`; 1h/5m recovery shells out to `scripts/livewire_ingest.py intraday-backfill`.
- **Safety cap (default 100):** if more than N symbols are missing for any single timeframe, the script aborts the auto-recovery and emails immediately. This prevents a runaway IB rate-limit hit when an entire daily run failed for some other reason.
- Email goes out only when post-recovery gaps remain. A fully successful recovery downgrades to an INFO log — no false-positive email storms.
- Reuses the existing Nodemailer alert path at `scripts/livewire_ops.py send-alert`.

### Weekly quality summary

`scripts/livewire_quality.py weekly` is a pure parser over the seven daily coverage logs from the previous ISO week. Self-skips on non-Sunday so the entrypoint can call it unconditionally every day.

```bash
python scripts/livewire_quality.py weekly            # Sunday: write the report; other days: noop
python scripts/livewire_quality.py weekly --force    # Render anyway
python scripts/livewire_quality.py weekly --week 2026-14
```

Output: `~/market-warehouse/logs/quality_weekly_YYYY-WW.md` with a coverage trend table, symbol churn (added/removed), and persistent gaps (≥3 consecutive missing days at any timeframe).

### Health check (intraday)

`scripts/livewire_quality.py health --intraday --timeframe {1h,5m}` performs interior gap detection for the intraday parquet, with optional suspected-halt annotation (contiguous gap < 30 min surrounded by normal bars). Default behaviour is **report-only**. When `--symbol`, `--since`, and `--timeframe` are all set, the command implicitly repairs that narrow window by shelling out to `scripts/livewire_ingest.py intraday-backfill` (no separate `--repair` flag — full scope means repair).

```bash
python scripts/livewire_quality.py health --intraday --timeframe 5m                       # Scan all symbols
python scripts/livewire_quality.py health --intraday --timeframe 5m --symbol AAPL         # Scan one symbol
python scripts/livewire_quality.py health --intraday --timeframe 5m --symbol AAPL --since 2026-04-01  # Repair window
```

### Rebuilding Postgres

```bash
source ~/market-warehouse/.venv/bin/activate
export MDW_POSTGRES_DSN="postgresql://user:password@localhost:5432/livewire"
export MDW_POSTGRES_SCHEMA="md"

python scripts/livewire_store.py smoke-postgres --ensure-schema
python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe 1d
python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe all
python scripts/livewire_store.py rebuild-postgres --asset-class volatility
python scripts/livewire_store.py rebuild-postgres --asset-class futures
python scripts/livewire_store.py rebuild-postgres --include-reliability
```

Postgres is a replayable publish target, not canonical storage. Rollback means dropping or truncating the target schema and rerunning rebuilds from bronze parquet plus `telemetry.jsonl` / `quality_audit.jsonl`. Futures and intraday rebuilds are conditional on corresponding bronze parquet existing.

## Testing

**All new code in `clients/` and `scripts/` must have tests. Coverage is enforced at 100% for the source currently included by `pyproject.toml`; `clients/ib_client.py` is still omitted from the fail-under gate and covered by focused tests separately.**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v                                                        # Run all
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing  # With coverage
python -m pytest tests/ -v -m "not integration"                                   # Unit tests only
python -m pytest tests/ -v -W error::RuntimeWarning                               # Catch leaked coroutine warnings
# Native macOS tests are now in the standalone Sift repo at ~/dev/apps/util/sift
```

### Rules for new code

1. Add tests in `tests/test_<module>.py`
2. Mock all external I/O (IB connections via `MagicMock`, file paths via `patch`)
3. Use temp parquet roots or disposable Postgres DSNs for storage tests
4. Mark DB tests with `@pytest.mark.integration`
5. Run coverage and confirm 100% before committing
6. Run `-W error::RuntimeWarning` at least once before committing when script tests mock async runners such as `ib.ib.run(...)`
7. `pyproject.toml` enforces `fail_under = 100`; `if __name__ == "__main__"` blocks are excluded
8. `clients/ib_client.py` is excluded from the coverage fail-under gate, but focused behavior tests now live in `tests/test_ib_client.py`

### Test deps

`pytest`, `pytest-cov`, `responses` (installed in `~/market-warehouse/.venv/`)

## Pre-commit Hook

A secrets scanner runs on every commit, checking staged files for API keys, passwords, private keys, tokens, and credentials. Install with:

```bash
ln -sf ../../tools/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Catches: AWS keys, API key/secret/password assignments, private key headers, GitHub/Slack tokens, Google API keys, connection strings with credentials, hardcoded IB credentials, staged `.env` files. Allowlists test files, placeholders, comments, `os.environ` reads, and error messages to avoid false positives. Bypass with `git commit --no-verify` if needed.

## Key Implementation Details

- IB BarData provides native float/int types — no string parsing needed
- `symbol_id` is now a stable 53-bit hash from `blake2b(symbol)` for new symbols
- Live ingestion writes bronze parquet directly; Postgres is rebuilt from bronze when SQL access is needed
- Empty IB head timestamps now fall back to the earliest supported IB historical date instead of skipping the symbol
- Bronze Parquet uses per-ticker Hive-partitioned layout: `data-lake/bronze/asset_class=equity/symbol=AAPL/1d.parquet` (futures: `asset_class=futures/symbol=ES_202506/1d.parquet`)
- Bronze publication is atomic at the file level: write temp parquet, validate it, then `os.replace()` into place
- `BronzeClient` accepts `asset_class` constructor param (`"equity"`, `"volatility"`, or `"futures"`) to select the appropriate parquet schema. Default `"equity"` preserves all existing behavior.
- `IBClient.connect()` auto-retries successive `clientId` values after IB error `326`, then records the actual connected ID

## Known Environment Gotchas

Common traps that derail debugging sessions — check these before investigating further:

- **IB Gateway availability**: Run `~/trading-stack/scripts/ibc_gateway_status.sh` and `nc -z 127.0.0.1 4001` before assuming IB is reachable. If down: `tail -30 /opt/ibc/logs/ibc-watchdog.log`; if still stuck, full bounce via `~/trading-stack/scripts/bounce_ibc_xenon.sh` (touches Xenon too — coordinate). Do NOT auto-retry restarts: failures usually mean 2FA, IBKR maintenance, or session conflict, not transient.
- **Empty IB head timestamps**: IB returns empty head timestamps for some symbols. The fallback to `IB_EARLIEST_DATE` is intentional — don't treat it as an error.
- **IB error 326 (client ID in use)**: Handled by auto-retry in `IBClient.connect()`. Don't manually reassign client IDs.
- **Weekend/holiday runs**: IB returns no data on non-trading days. These are harmless no-ops — don't debug "no data returned" on weekends or holidays.
- **CBOE volatility fetch**: Volatility indices use CBOE's public API, not IB. If VIX data looks stale, check `scripts/livewire_ingest.py cboe-vol`, not IB connectivity.

## Provider Interface

`clients/historical_provider.py` defines:
- `HistoricalProvider` — abstract interface for fetching IB historical data
- `IBProvider` — direct IB Gateway connection via ib_async
- `BarRecord` — OHLCV bar dataclass
- `ib_contract_to_spec()` / `spec_to_ib_contract()` — contract serialization helpers

### Date Formats

All dates are ISO format:
- Bar dates: `YYYY-MM-DD` (e.g., `2025-01-02`)
- Head timestamps: ISO 8601 datetime (e.g., `2010-01-04T09:30:00`)
