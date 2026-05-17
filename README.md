# Market Data Warehouse

![Market Data Warehouse](https://raw.githubusercontent.com/joemccann/market-data-warehouse/main/.github/github-banner.png)

A **local-first financial data warehouse** for universe-scale market data.

---

## Overview

Market Data Warehouse is designed for storing and analyzing historical **OHLCV data across equities, futures, volatility indices, spot commodities, and FX pairs**, with a clear path from **daily bars → intraday data → production analytics**.

### Core Stack

* **Parquet data lake** → canonical storage
* **DuckDB** → local analytics, research, backtesting
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

> **In one sentence:**
> A local-first, production-ready market data warehouse for serious quantitative workflows.

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
* **Warehouse (optional)**: ClickHouse

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
├── scripts/
├── logs/
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
chmod +x setup_market_warehouse.sh
./setup_market_warehouse.sh
```

### Full Bootstrap

```bash
./setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
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

All scripts (`fetch_ib_historical.py`, `daily_update.py`, etc.) read `MDW_IB_HOST`/`MDW_IB_PORT` automatically.

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
python scripts/install_ibc_secure_service.py
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
python scripts/fetch_ib_historical.py

# Specific tickers
python scripts/fetch_ib_historical.py --tickers AAPL NVDA

# Preset universe
python scripts/fetch_ib_historical.py --preset presets/sp500.json

# Futures by preset
python scripts/fetch_ib_historical.py --preset presets/futures-index.json --asset-class futures
python scripts/fetch_ib_historical.py --preset presets/futures-energy.json --asset-class futures
python scripts/fetch_ib_historical.py --preset presets/futures-metals.json --asset-class futures
python scripts/fetch_ib_historical.py --preset presets/futures-treasuries.json --asset-class futures

# Spot commodities via IB CMDTY MIDPOINT
python scripts/fetch_ib_historical.py --preset presets/cmdty-metals.json --asset-class cmdty

# FX via IB Forex MIDPOINT
python scripts/fetch_ib_historical.py --preset presets/fx-pairs.json --asset-class fx

# Volatility (CBOE direct)
python scripts/fetch_cboe_volatility.py

# Volatility historical backfill through IB Index contracts, when needed
python scripts/fetch_ib_historical.py --preset presets/volatility.json --asset-class volatility
```

Run all documented historical seed groups manually:

```bash
python scripts/fetch_ib_historical.py --preset presets/sp500.json
python scripts/fetch_ib_historical.py --preset presets/futures-index.json --asset-class futures
python scripts/fetch_ib_historical.py --preset presets/futures-energy.json --asset-class futures
python scripts/fetch_ib_historical.py --preset presets/futures-metals.json --asset-class futures
python scripts/fetch_ib_historical.py --preset presets/futures-treasuries.json --asset-class futures
python scripts/fetch_ib_historical.py --preset presets/cmdty-metals.json --asset-class cmdty
python scripts/fetch_ib_historical.py --preset presets/fx-pairs.json --asset-class fx
python scripts/fetch_cboe_volatility.py
```

Notes:

* `cmdty` and `fx` use IB `MIDPOINT` daily bars and store volume as `0`.
* FX pairs are six-letter local symbols. If IB supports only the reverse cross, the fetcher requests the supported pair and stores inverted OHLC rows. Example: `USDEUR` fetches `EURUSD`, then stores inverted `USDEUR`.
* CBOE direct sync is the authoritative daily source for volatility indices.

---

### Backfill Missing Data

```bash
# Equity backfill
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill

# Futures backfill
python scripts/fetch_ib_historical.py --preset presets/futures-index.json --asset-class futures --backfill

# Spot commodity backfill
python scripts/fetch_ib_historical.py --preset presets/cmdty-metals.json --asset-class cmdty --backfill

# FX backfill
python scripts/fetch_ib_historical.py --preset presets/fx-pairs.json --asset-class fx --backfill

# Volatility IB historical backfill, if CBOE direct history is not enough
python scripts/fetch_ib_historical.py --preset presets/volatility.json --asset-class volatility --backfill
```

* Fills only missing history
* Preserves existing data
* Independent cursor tracking

---

### Daily Updates

```bash
# Equity daily update
python scripts/daily_update.py

# Futures daily update
python scripts/daily_update.py --asset-class futures

# Spot commodity daily update
python scripts/daily_update.py --asset-class cmdty --preset presets/cmdty-metals.json

# FX daily update
python scripts/daily_update.py --asset-class fx --preset presets/fx-pairs.json

# Volatility daily update, authoritative CBOE direct source
python scripts/fetch_cboe_volatility.py
```

Run the full manual daily cycle:

```bash
python scripts/daily_update.py --asset-class equity
python scripts/daily_update.py --asset-class futures
python scripts/daily_update.py --asset-class cmdty --preset presets/cmdty-metals.json
python scripts/daily_update.py --asset-class fx --preset presets/fx-pairs.json
python scripts/fetch_cboe_volatility.py
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

The scheduled runner executes equities, futures, and CBOE volatility:

```bash
python scripts/run_daily_update_job.py
```

Run `cmdty` and `fx` daily updates explicitly until they are added to the scheduled runner:

```bash
python scripts/daily_update.py --asset-class cmdty --preset presets/cmdty-metals.json
python scripts/daily_update.py --asset-class fx --preset presets/fx-pairs.json
```

---

### Rebuild DuckDB

```bash
# Rebuild equities, including daily/intraday tables when present
python scripts/rebuild_duckdb_from_parquet.py

# Rebuild only daily equity rows
python scripts/rebuild_duckdb_from_parquet.py --asset-class equity --timeframe 1d

# Rebuild futures daily rows
python scripts/rebuild_duckdb_from_parquet.py --asset-class futures

# Rebuild volatility daily rows
python scripts/rebuild_duckdb_from_parquet.py --asset-class volatility
```

DuckDB rebuild currently supports `equity`, `futures`, and `volatility`. `cmdty` and `fx` are canonical in bronze Parquet and do not yet have DuckDB rebuild targets.

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
python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing
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
ln -sf ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit
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
python scripts/write_sample_parquet.py
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
./setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
```

---

## License

MIT
