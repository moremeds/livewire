# Livewire

A **local-first financial data warehouse** for universe-scale market data.

---

## Overview

Livewire is a market data warehouse designed for storing and analyzing historical **OHLCV data across equities, futures, volatility indices, spot commodities, and FX pairs**, with a clear path from **daily bars → intraday data → production analytics**.

### Core Stack

* **Parquet data lake** → canonical storage
* **DuckDB** → local analytics, research, backtesting
* **Postgres (optional)** → replayable analytical publish target
* **ClickHouse (optional)** → large-scale aggregation & concurrency

### Current Capabilities

* Daily ingestion for:

  * **Equities (IB)**
  * **Futures (IB)**
  * **Volatility indices (CBOE API)**
  * **Spot commodities (IB CMDTY MIDPOINT)**
  * **FX pairs (IB Forex MIDPOINT, with reverse-pair inversion when needed)**
* Per-ticker **bronze Parquet snapshots**
* **Atomic writes + validation**
* **Fallback recovery pipeline** for missing data
* On-demand **DuckDB rebuilds** from Parquet
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
* **Local engine**: DuckDB
* **Analytical publish target (optional)**: Postgres
* **Warehouse (optional)**: ClickHouse

Live ingestion writes bronze Parquet only. Postgres and DuckDB are derived analytical targets and can be dropped or rebuilt from bronze Parquet plus reliability JSONL artifacts.

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
│   │   └── asset_class=fx/symbol=USDEUR/1d.parquet
│   ├── silver/
│   └── gold/
├── duckdb/
│   └── market.duckdb
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
* DuckDB
* [Interactive Brokers](https://ibkr.com/referral/joseph5632) account
* Docker (recommended for IB Gateway)
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
| `scripts/livewire_ops.py` | Operations | Scheduled daily job, alert sending, native IBC install/start helpers |
| `scripts/livewire_store.py` | Storage maintenance | DuckDB/Postgres rebuilds, Postgres smoke checks, R2 sync, parquet filename migration |

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
livewire_ingest.py   daily | historical | robust | cboe-vol | intraday-backfill | intraday-status | probe-intraday | universe | backfill-all
livewire_quality.py  health | coverage | report | weekly | watchdog
livewire_ops.py      run-daily-job | ibc-install | ibc-start | send-alert
livewire_store.py    rebuild-duckdb | rebuild-postgres | smoke-postgres | sync-r2 | migrate-parquet
```

---

## [Interactive Brokers](https://ibkr.com/referral/joseph5632) Gateway

You need a running IB Gateway for ingestion.

---

### Option 1 (Recommended): Docker

Uses [`gnzsnz/ib-gateway-docker`](https://github.com/gnzsnz/ib-gateway-docker)

#### Quick Start

```bash
cd docker/ib-gateway
cp .env.example .env
mkdir -p secrets
echo "YOUR_IB_PASSWORD" > secrets/ib_password.txt
docker compose up -d
```

* Complete 2FA via:

  * VNC (`localhost:5900`)
  * IBKR mobile app

#### Ports

| Host | Purpose   |
| ---- | --------- |
| 4001 | Live API  |
| 4002 | Paper API |
| 5900 | VNC       |

---

### Option 2 (Cloud): Hetzner VPS + Tailscale

Run the same Docker Compose setup on a cloud VPS with Tailscale for secure, WireGuard-encrypted access. No public ports exposed — all traffic flows through a private mesh VPN.

#### Why

* IB Gateway runs 24/5 without tying up a local machine
* Accessible from any Tailscale-enrolled device (Mac, iPhone, other servers)
* ~$4-6/mo (Hetzner) + free (Tailscale)

#### Architecture

```text
Hetzner VPS (all public ports blocked)
├── Tailscale (MagicDNS hostname: ib-gateway)
├── socat proxy: tailscale-ip:4001 → localhost:4001
└── Docker: same docker-compose.yml, unmodified
        └── IB Gateway (127.0.0.1:4001)

Clients connect to ib-gateway:4001 via Tailscale tunnel
```

#### Setup

1. **Provision a VPS** (Hetzner CPX11 or similar, Ubuntu 24.04, US East region)
2. **Harden the host**: non-root user, SSH key-only, disable root login, unattended-upgrades
3. **Install Docker and Tailscale** on the VPS
4. **Configure Tailscale**:
   * Authenticate with a preauth key tagged for the gateway
   * Enable MagicDNS for stable hostname resolution
   * Set up ACLs to restrict which devices can reach port 4001
5. **Bridge Tailscale to Docker**: a socat systemd service forwards traffic from the Tailscale interface to Docker's localhost-bound ports (`tailscale serve --tcp` adds TLS, which is incompatible with IB's raw TCP protocol)
6. **Firewall**: ufw denies all incoming except on the Tailscale interface
7. **Deploy**: `scp` the `docker-compose.yml` and `.env` to the VPS, create the password secret, `docker compose up -d`
8. **2FA**: Approve via IBKR mobile app or VNC through Tailscale

#### Client Configuration

```bash
# Set in .env or export in shell
MDW_IB_HOST=ib-gateway    # Tailscale MagicDNS hostname
MDW_IB_PORT=4001
```

The ingest commands under `scripts/livewire_ingest.py` read `MDW_IB_HOST`/`MDW_IB_PORT` automatically.

#### Phone Access

SSH from iOS/Android (Termius, Blink) to `mdw@ib-gateway` via Tailscale:

```bash
cd ~/ib-gateway
docker compose stop    # stop gateway
docker compose start   # start gateway
docker compose ps      # check status
```

#### Rollback to Local

```bash
unset MDW_IB_HOST      # falls back to 127.0.0.1
```

See [`docker/ib-gateway/README.md`](docker/ib-gateway/README.md) for the full step-by-step provisioning guide, Tailscale ACL policy, client enrollment, 2FA reauth runbook, and volume backup procedures.

---

### Option 3: Native macOS (IBC)

IBC provides:

* Auto login
* Session recovery
* Daily restarts

#### Install

```bash
python scripts/livewire_ops.py ibc-install
```

#### Commands

```bash
~/ibc/bin/start-secure-ibc-service.sh
~/ibc/bin/stop-secure-ibc-service.sh
~/ibc/bin/status-secure-ibc-service.sh
```

> The IBC service is **machine-level**, not repo-scoped.

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
```

Notes:

* `cmdty` and `fx` use IB `MIDPOINT` daily bars and store volume as `0`.
* FX pairs are six-letter local symbols. If IB supports only the reverse cross, the fetcher requests the supported pair and stores inverted OHLC rows. Example: `USDEUR` fetches `EURUSD`, then stores inverted `USDEUR`.
* CBOE direct sync is the authoritative daily source for volatility indices.

---

### Equity Preset Backfill

The consolidated equity preset backfill entrypoint runs `sp500`, `ndx100`, and `r2k` daily-bar normal fetches, then older-history backfills, with stall detection and cursor resume:

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

### Rebuild DuckDB

```bash
# Rebuild equities, including daily/intraday tables when present
python scripts/livewire_store.py rebuild-duckdb

# Rebuild only daily equity rows
python scripts/livewire_store.py rebuild-duckdb --asset-class equity --timeframe 1d

# Rebuild futures daily rows
python scripts/livewire_store.py rebuild-duckdb --asset-class futures

# Rebuild volatility daily rows
python scripts/livewire_store.py rebuild-duckdb --asset-class volatility
```

DuckDB rebuild currently supports `equity`, `futures`, and `volatility`. `cmdty` and `fx` are canonical in bronze Parquet and do not yet have DuckDB rebuild targets.

Other storage commands:

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
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
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
2. Use **DuckDB** for:

   * research
   * backtesting
3. Use **ClickHouse** for:

   * large-scale queries
   * production-like workloads

---

## Troubleshooting

### DuckDB Errors

* Use inline `PRIMARY KEY`
* Avoid reserved keywords (`right` → `option_right`)

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
