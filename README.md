# Livewire

A **local-first financial data warehouse** for universe-scale market data.

---

## Overview

Livewire is a market data warehouse designed for storing and analyzing historical **OHLCV data across equities, futures, volatility indices, spot commodities, FX pairs, and rates**, with a clear path from **daily bars → intraday data → production analytics**.

### Core Stack

* **Parquet data lake** → canonical storage
* **Postgres (optional)** → replayable analytical publish target for SQL queries
* **ClickHouse (optional)** → large-scale aggregation & concurrency

### Current Capabilities

* Daily ingestion for:

  * **Equities (IB)**
  * **Futures (IB)**
  * **Volatility indices (CBOE API)**
  * **Spot commodities (IB CMDTY MIDPOINT)**
  * **FX pairs (IB Forex MIDPOINT, with reverse-pair inversion when needed)**
  * **Treasury yields (FRED API)**
* Per-ticker **bronze Parquet snapshots**
* **Atomic writes + validation**
* **Fallback recovery pipeline** for missing data
* Optional **Postgres analytical rebuilds** from Parquet and reliability JSONL

> **In one sentence:**
> Livewire — a local-first, production-ready market data warehouse for serious quantitative workflows.

---

## Goals

* ⚡ High-performance **local quant research environment**
* 🧱 Scalable **multi-asset data model**
* 🔁 Clean **local → production transition**
* 🌐 **Polyglot workflows** (Python, Rust, Node.js)

---

## Architecture

### Data Flow

```text
Raw → Bronze → Silver → Gold
```

* **Raw** → vendor data
* **Bronze** → canonical Parquet (primary ingestion layer)
* **Silver** → cleaned / adjusted datasets
* **Gold** → analytics, factors, derived tables

### Storage Strategy

* **System of record**: Parquet (`data-lake/`)
* **Analytical publish target (optional)**: Postgres
* **Warehouse (optional)**: ClickHouse

Live ingestion writes bronze Parquet only. Postgres is the replayable analytical target and can be dropped or rebuilt from bronze Parquet plus reliability JSONL artifacts.

---

## Directory Structure

```text
~/market-warehouse/
├── data-lake/
│   ├── raw/
│   ├── bronze/
│   │   ├── asset_class=equity/symbol=AAPL/1d.parquet
│   │   ├── asset_class=volatility/symbol=VIX/1d.parquet
│   │   ├── asset_class=futures/symbol=ES_202506/1d.parquet
│   │   ├── asset_class=cmdty/symbol=XAUUSD/1d.parquet
│   │   ├── asset_class=fx/symbol=USDEUR/1d.parquet
│   │   └── asset_class=rates/symbol=DGS10/1d.parquet
│   ├── silver/
│   └── gold/
├── logs/
│   ├── telemetry.jsonl
│   └── quality_audit.jsonl
├── scripts/
└── .venv/
```

---

## Installation

### Requirements

* macOS (Apple Silicon recommended)
* Homebrew
* Python 3.13+
* Node.js 22+
* [Interactive Brokers](https://ibkr.com/referral/joseph5632) account
* ClickHouse (optional)

---

### Quick Start

```bash
chmod +x scripts/setup_market_warehouse.sh
scripts/setup_market_warehouse.sh
```

### Full Bootstrap

```bash
scripts/setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
```

---

## Operator Scripts

Livewire keeps the operator-facing command surface to five files:

| Script | Function | Typical usage |
| --- | --- | --- |
| `scripts/setup_market_warehouse.sh` | One-time local warehouse bootstrap | Create `~/market-warehouse/`, venv, directories, optional ClickHouse helpers, optional sample data |
| `scripts/livewire_ingest.py` | Data ingestion | Historical seeds, daily updates, robust IB runs, CBOE volatility, intraday backfill, universe checks |
| `scripts/livewire_quality.py` | Quality and health reporting | Bronze health checks, coverage reports, daily rollup, weekly summary, watchdog alerts |
| `scripts/livewire_ops.py` | Operations | Scheduled daily job, alert sending |
| `scripts/livewire_store.py` | Storage maintenance | Postgres rebuilds, Postgres smoke checks, R2 sync, parquet filename migration |

Use `--help` at the top level or after a subcommand to inspect exact flags:

```bash
python scripts/livewire_ingest.py --help
python scripts/livewire_ingest.py historical --help
python scripts/livewire_quality.py --help
python scripts/livewire_ops.py --help
python scripts/livewire_store.py --help
```

Top-level subcommands:

```text
livewire_ingest.py   daily | historical | robust | cboe-vol | fred-rates | intraday-backfill | intraday-status | probe-intraday | universe | backfill-all
livewire_quality.py  health | coverage | report | weekly | watchdog
livewire_ops.py      run-daily-job | ibc-install | ibc-start | send-alert
livewire_store.py    rebuild-postgres | smoke-postgres | sync-r2 | migrate-parquet
```

---

## [Interactive Brokers](https://ibkr.com/referral/joseph5632) Gateway

You need a running IB Gateway for ingestion.

---

### Native macOS (IBC)

IB Gateway and IBC are owned by the separate **trading-stack** project at `~/trading-stack/` — livewire only consumes the API on `127.0.0.1:4001`. The full setup runbook lives at `~/runbooks/trading-stack/ib-gateway-ibc.md`.

#### Day-to-day commands

```bash
~/trading-stack/scripts/ibc_gateway_status.sh   # health + watchdog state
~/trading-stack/scripts/bounce_ibc_xenon.sh     # full restart cycle
tail -30 /opt/ibc/logs/ibc-watchdog.log
nc -z 127.0.0.1 4001
```

> Gateway pinned to **10.45** (10.46 incompatible). 2FA approval via IBKR Mobile is manual on every fresh login.

---

## Data Ingestion

### Prerequisites

* IB Gateway running (`127.0.0.1:4001` by default)
* Configurable via:

  * CLI flags (`--host`, `--port`)
  * Env vars (`MDW_IB_HOST`, `MDW_IB_PORT`)

Activate the project environment before running Python commands:

```bash
source ~/market-warehouse/.venv/bin/activate
```

Check the IB API endpoint without fetching account/order startup data:

```bash
python - <<'PY'
from ib_async import IB
from ib_async.ib import StartupFetch

ib = IB()
try:
    ib.connect("127.0.0.1", 4001, clientId=38, timeout=8, readonly=True, fetchFields=StartupFetch(0))
    print(f"connected={ib.isConnected()} serverVersion={ib.client.serverVersion()}")
finally:
    if ib.isConnected():
        ib.disconnect()
PY
```

---

### Fetch Historical Data

```bash
# Default (Mag 7)
python scripts/livewire_ingest.py historical

# Specific tickers
python scripts/livewire_ingest.py historical --tickers AAPL NVDA

# Preset universe
python scripts/livewire_ingest.py historical --preset presets/sp500.json

# Futures by preset
python scripts/livewire_ingest.py historical --preset presets/futures-index.json --asset-class futures
python scripts/livewire_ingest.py historical --preset presets/futures-energy.json --asset-class futures
python scripts/livewire_ingest.py historical --preset presets/futures-metals.json --asset-class futures
python scripts/livewire_ingest.py historical --preset presets/futures-treasuries.json --asset-class futures

# Spot commodities via IB CMDTY MIDPOINT
python scripts/livewire_ingest.py historical --preset presets/cmdty-metals.json --asset-class cmdty

# FX via IB Forex MIDPOINT
python scripts/livewire_ingest.py historical --preset presets/fx-pairs.json --asset-class fx

# Volatility (CBOE direct)
python scripts/livewire_ingest.py cboe-vol

# Treasury yields from FRED (DGS3, DGS5, DGS10, DGS30)
FRED_API_KEY=... python scripts/livewire_ingest.py fred-rates

# Volatility historical backfill through IB Index contracts, when needed
python scripts/livewire_ingest.py historical --preset presets/volatility.json --asset-class volatility
```

For bulk IB runs over more than a few tickers, prefer the robust orchestrator. It retries per ticker, records outcomes, and reports `ok`, `ok-noop`, `skip`, `fail`, or `timeout`:

```bash
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode seed
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode backfill
python scripts/livewire_ingest.py robust --preset presets/futures-index.json --asset-class futures --mode seed
```

Run all documented daily seed groups manually:

```bash
python scripts/livewire_ingest.py historical --preset presets/sp500.json
python scripts/livewire_ingest.py historical --preset presets/futures-index.json --asset-class futures
python scripts/livewire_ingest.py historical --preset presets/futures-energy.json --asset-class futures
python scripts/livewire_ingest.py historical --preset presets/futures-metals.json --asset-class futures
python scripts/livewire_ingest.py historical --preset presets/futures-treasuries.json --asset-class futures
python scripts/livewire_ingest.py historical --preset presets/cmdty-metals.json --asset-class cmdty
python scripts/livewire_ingest.py historical --preset presets/fx-pairs.json --asset-class fx
python scripts/livewire_ingest.py cboe-vol
FRED_API_KEY=... python scripts/livewire_ingest.py fred-rates
```

Notes:

* `cmdty` and `fx` use IB `MIDPOINT` daily bars and store volume as `0`.
* FX pairs are six-letter local symbols. If IB supports only the reverse cross, the fetcher requests the supported pair and stores inverted OHLC rows. Example: `USDEUR` fetches `EURUSD`, then stores inverted `USDEUR`.
* CBOE direct sync is the authoritative daily source for volatility indices.
* FRED rates sync writes Treasury constant-maturity yields as percent values to `asset_class=rates` with columns `trade_date`, `symbol_id`, `tenor_years`, `yield_pct`, and `source`. Native FRED frequency is daily; `--frequency` can request lower-frequency aggregation such as `w`, `m`, `q`, or `a`.

---

### Equity Preset Backfill

The consolidated backfill entrypoint runs `sp500`, `ndx100`, and `r2k` daily-bar normal fetches, then older-history backfills with stall detection and cursor resume. It finishes by syncing FRED Treasury yield rates.

```bash
python scripts/livewire_ingest.py backfill-all
```

For a long local run, use `tmux` and keep logs under `~/market-warehouse/logs/`:

```bash
tmux new-session -s livewire_equity_backfill 'cd /Users/chenxi/projects/livewire && source ~/market-warehouse/.venv/bin/activate && python scripts/livewire_ingest.py backfill-all'
```

---

### Backfill Missing Data

```bash
# Equity backfill
python scripts/livewire_ingest.py historical --preset presets/sp500.json --backfill

# Futures backfill
python scripts/livewire_ingest.py historical --preset presets/futures-index.json --asset-class futures --backfill

# Spot commodity backfill
python scripts/livewire_ingest.py historical --preset presets/cmdty-metals.json --asset-class cmdty --backfill

# FX backfill
python scripts/livewire_ingest.py historical --preset presets/fx-pairs.json --asset-class fx --backfill

# Volatility IB historical backfill, if CBOE direct history is not enough
python scripts/livewire_ingest.py historical --preset presets/volatility.json --asset-class volatility --backfill
```

* Fills only missing history
* Preserves existing data
* Independent cursor tracking

---

### Intraday Data

Intraday bars are fetched through the ingest entrypoint. `historical` is daily-only; use `intraday-backfill` for 1h and 5m bars:

```bash
# Probe IB intraday timestamp behavior for the built-in AAPL fixture
python scripts/livewire_ingest.py probe-intraday

# Report intraday session state
python scripts/livewire_ingest.py intraday-status --timeframe 5m

# Backfill one symbol
python scripts/livewire_ingest.py intraday-backfill --tickers AAPL --timeframe 5m --years 1

# Backfill a preset and skip files already present
python scripts/livewire_ingest.py intraday-backfill --preset presets/sp500.json --timeframe 1h --skip-existing
```

---

### Daily Updates

```bash
# Equity daily update
python scripts/livewire_ingest.py daily

# Equity daily update through Massive instead of IB
python scripts/livewire_ingest.py daily --asset-class equity --source massive

# Futures daily update
python scripts/livewire_ingest.py daily --asset-class futures

# Spot commodity daily update
python scripts/livewire_ingest.py daily --asset-class cmdty --preset presets/cmdty-metals.json

# FX daily update
python scripts/livewire_ingest.py daily --asset-class fx --preset presets/fx-pairs.json

# Volatility daily update, authoritative CBOE direct source
python scripts/livewire_ingest.py cboe-vol
```

Run the full manual daily cycle:

```bash
python scripts/livewire_ingest.py daily --asset-class equity
python scripts/livewire_ingest.py daily --asset-class futures
python scripts/livewire_ingest.py daily --asset-class cmdty --preset presets/cmdty-metals.json
python scripts/livewire_ingest.py daily --asset-class fx --preset presets/fx-pairs.json
python scripts/livewire_ingest.py cboe-vol
```

Common flags:

```bash
--dry-run
--force
--target-date YYYY-MM-DD
--preset presets/sp500.json
--asset-class {equity|volatility|futures|cmdty|fx}
--source {ib|massive}
--host 127.0.0.1
--port 4001
```

#### Key Behavior

* Detects missing trading days
* Fetches only gaps
* Validates OHLCV
* Atomic snapshot updates
* Fallback recovery if IB fails

### Reliability / Data Quality

Sub-A of the Livewire reliability roadmap adds source-tagged telemetry, quality-flag sidecars, a central `quality_audit.jsonl`, a per-ticker IB orchestrator, and a daily rollup email marker. Design details live in `docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md`; the execution plan is `docs/superpowers/plans/2026-05-18-reliability-foundation-plan.md`.

Common quality commands:

```bash
# Data-lake health report
python scripts/livewire_quality.py health

# Include intraday checks
python scripts/livewire_quality.py health --intraday --timeframe 5m

# Daily coverage report with auto-recovery enabled
python scripts/livewire_quality.py coverage

# Report-only coverage check
python scripts/livewire_quality.py coverage --no-recover

# Quality rollup for the last 24 hours
python scripts/livewire_quality.py report --view summary --since 24h

# Send the quality rollup by email
python scripts/livewire_quality.py report --view summary --since 24h --email

# Weekly summary, self-skips on non-Sunday
python scripts/livewire_quality.py weekly

# Scheduled-job watchdog
python scripts/livewire_quality.py watchdog
```

The scheduled runner executes equities, futures, and CBOE volatility:

```bash
python scripts/livewire_ops.py run-daily-job
```

Run `cmdty` and `fx` daily updates explicitly until they are added to the scheduled runner:

```bash
python scripts/livewire_ingest.py daily --asset-class cmdty --preset presets/cmdty-metals.json
python scripts/livewire_ingest.py daily --asset-class fx --preset presets/fx-pairs.json
```

---

### Other storage commands

```bash
# Sync lake files to R2 when R2 env vars are configured
python scripts/livewire_store.py sync-r2

# Migrate old parquet filenames to the current layout
python scripts/livewire_store.py migrate-parquet
```

---

### Rebuild Postgres

Postgres is optional and replayable. It is not the ingestion source of truth, and live ingestion scripts do not write to it.

```bash
export MDW_POSTGRES_DSN="postgresql://user:password@localhost:5432/livewire"
export MDW_POSTGRES_SCHEMA="md"

# Connectivity and table-count smoke check
python scripts/livewire_store.py smoke-postgres --ensure-schema

# Rebuild equity daily rows
python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe 1d

# Rebuild all available equity timeframes; missing optional 1h/5m data is skipped
python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe all

# Rebuild futures and volatility when their bronze parquet exists
python scripts/livewire_store.py rebuild-postgres --asset-class futures
python scripts/livewire_store.py rebuild-postgres --asset-class volatility

# Import reliability telemetry and quality flags
python scripts/livewire_store.py rebuild-postgres --include-reliability
```

The Postgres schema contains `md.symbols`, `md.equities_daily`, `md.futures_daily`, `md.equities_1h`, `md.equities_5m`, `md.telemetry_events`, and `md.quality_flags`. The rebuild command streams local parquet/JSONL through Python and `psycopg`; it does not require server-side parquet extensions or database access to the local filesystem.

Rollback is to drop or truncate the target schema and replay from canonical bronze Parquet and JSONL artifacts:

```sql
DROP SCHEMA IF EXISTS md CASCADE;
```

Then rerun the smoke and rebuild commands above. Futures and intraday commands are conditional on corresponding bronze parquet existing under `~/market-warehouse/data-lake/bronze/asset_class=futures` or `asset_class=equity/symbol=*/{1h,5m}.parquet`.

---

## Scheduling

### macOS (`launchd`)

```bash
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update.plist
```

### Schedule

* **Daily sync**: 13:05 PT (4:05 PM ET)
* **Watchdog**: 18:30 PT

---

## Alerts & Monitoring

* Automatic retries
* Email alerts on failure
* Optional AI-generated summaries

### Setup

```bash
npm install
```

Example config:

```bash
MDW_ALERT_EMAIL_TO="you@example.com"
MDW_ALERT_SMTP_URL="smtp://user:pass@mail.example.com:587"
```


## Testing

### Run Tests

```bash
python -m pytest tests/ -v
```

### Coverage

```bash
python -m pytest tests -q --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing
```

* ✅ **100% coverage enforced**

### RuntimeWarning Gate

Run this after changes that touch async script runners or tests that mock `ib.ib.run(...)`:

```bash
python -m pytest tests -q -W error::RuntimeWarning
```

### Focused Asset-Class Tests

```bash
python -m pytest tests/test_bronze_client.py tests/test_daily_update.py tests/test_fetch_ib_historical.py -q
```

---

## Security

### Pre-commit Hook

```bash
ln -sf ../../tools/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Detects:

* API keys
* credentials
* private keys
* `.env` leaks

---

## Data Model Notes

### Split-Adjusted Volume

All volume is **split-adjusted** to ensure consistency across time.

---

## ClickHouse (Optional)

Used for:

* Benchmarking
* Concurrency
* Production simulation

### Commands

```bash
~/market-warehouse/scripts/start_clickhouse.sh
~/market-warehouse/scripts/init_clickhouse.sh
~/market-warehouse/scripts/stop_clickhouse.sh
```

---

## Sample Data

```bash
scripts/setup_market_warehouse.sh --with-sample-data

# After setup, the generated helper is available under the warehouse tree:
python ~/market-warehouse/scripts/write_sample_parquet.py
```

---

## Recommended Workflow

1. Store all data in **Parquet (bronze)**
2. Use **Postgres** for replayable local SQL queries when needed
3. Use **ClickHouse** for:

   * large-scale queries
   * production-like workloads

---

## Troubleshooting

### ClickHouse Issues

```bash
clickhouse-client --query "SELECT version()"
```

---

## Recommended Command

```bash
scripts/setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
```

---

## License

MIT
