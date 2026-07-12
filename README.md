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

  * **Equities** (Massive by default; IB available with `--source ib`)
  * **Futures** (IB)
  * **Volatility indices** (CBOE API)
  * **Spot commodities** (IB CMDTY MIDPOINT)
  * **FX pairs** (IB Forex MIDPOINT, with reverse-pair inversion when needed)
  * **Treasury yields** (FRED API)
* Intraday bars for:

  * **Equities** (Massive whole-market flat files for 1m, with local derivation to 5m/30m/1h)
  * **Volatility/Index** (IB 30m bars, with local 1h derivation)
  * **Futures** (IB)
* Per-ticker **bronze Parquet snapshots**
* **Atomic writes + validation**
* **Fallback recovery pipeline** for missing data
* Optional **Postgres analytical rebuilds** from Parquet and reliability JSONL

> **In one sentence:**
> Livewire — a local-first, production-ready market data warehouse for serious quantitative workflows.

---

## Goals

* High-performance **local quant research environment**
* Scalable **multi-asset data model**
* Clean **local → production transition**
* **Polyglot workflows** (Python, Rust, Node.js)

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

Daily and intraday bronze mutations are serialized per exact Parquet path with
blocking advisory locks. Persistent `*.parquet.lock` sidecars coordinate writers;
they are not market data and are excluded from discovery and R2 synchronization.

---

## Directory Structure

```text
~/market-warehouse/
├── data-lake/         # Can be a symlink to an external volume — see note below
│   ├── raw/
│   ├── bronze/
│   │   ├── asset_class=equity/symbol=AAPL/{1d,1m,5m,30m,1h}.parquet
│   │   ├── asset_class=volatility/symbol=VIX/{1d,30m,1h}.parquet
│   │   ├── asset_class=futures/symbol=ES_202506/1d.parquet
│   │   ├── asset_class=cmdty/symbol=XAUUSD/1d.parquet
│   │   ├── asset_class=fx/symbol=USDEUR/1d.parquet
│   │   └── asset_class=rates/symbol=DGS10/1d.parquet
│   ├── bronze-delisted/   # Symbols archived out of the daily sync universe
│   ├── silver/
│   └── gold/
├── logs/
│   ├── telemetry.jsonl
│   └── quality_audit.jsonl
├── cursors/           # Intraday backfill cursor JSON files
├── scripts/
└── .venv/
```

> **Relocating the data lake**: a full Massive intraday build projects ~600 GB.
> If `~` lives on a small SSD, you can move `data-lake/` to an external volume
> and symlink it back:
>
> ```bash
> launchctl unload ~/Library/LaunchAgents/com.livewire.*.plist
> mv ~/market-warehouse/data-lake /Volumes/YOUR_VOLUME/livewire/data-lake
> ln -s /Volumes/YOUR_VOLUME/livewire/data-lake ~/market-warehouse/data-lake
> launchctl load ~/Library/LaunchAgents/com.livewire.*.plist
> ```
>
> Atomic publish (`temp → validate → os.replace`) still works because the temp
> file is created in the same directory as the canonical file — both end up on
> the external volume. On exFAT, expect cold-cache wide scans to be 5-25×
> slower than APFS; warm-cache scans converge with APFS. macOS may spawn
> `._foo.parquet` AppleDouble sidecars; the codebase filters them in glob
> patterns where it matters.

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

## CLI Reference

Livewire has two CLI layers: the **unified CLI** (`scripts/livewire.py`) for streamlined workflows, and five **operator scripts** for granular control.

### Unified CLI

The unified CLI groups all operations under four commands with automatic source selection:

```bash
source ~/market-warehouse/.venv/bin/activate

# Daily sync — catch up all asset classes
python scripts/livewire.py sync                         # Equity + volatility + futures + rates
python scripts/livewire.py sync --asset-class equity    # Equity only
python scripts/livewire.py sync --full                  # Full daily-backfill orchestrator
python scripts/livewire.py sync --scheduled             # Scheduled job with retry + alerting

# Deep historical backfill
python scripts/livewire.py backfill --full              # Full warehouse build (all presets, all phases)
python scripts/livewire.py backfill --timeframe 1d      # Daily bars only
python scripts/livewire.py backfill --timeframe 1m 5m   # Full-market equity intraday

# Quality and health
python scripts/livewire.py check                        # Default: coverage report
python scripts/livewire.py check --health               # Bronze health check
python scripts/livewire.py check --report               # Quality rollup
python scripts/livewire.py check --weekly               # Weekly summary
python scripts/livewire.py check --universe             # Universe screener

# Publish to external targets
python scripts/livewire.py publish postgres             # Rebuild Postgres
python scripts/livewire.py publish postgres --smoke     # Smoke test only
python scripts/livewire.py publish r2                   # Sync to R2
python scripts/livewire.py publish --migrate            # Parquet schema migration
```

**Sources** — equity daily uses Massive by default (requires `MASSIVE_API_KEY`);
pass `--source ib` to force IB. Equity intraday always requires Massive flat-file
credentials. Non-equity intraday remains IB-backed.

### Operator Scripts

For granular control, use the five operator scripts directly:

| Script | Function | Typical usage |
| --- | --- | --- |
| `scripts/livewire_ingest.py` | Data ingestion | Historical seeds, daily updates, robust IB runs, CBOE volatility, intraday backfill, S3 flat files |
| `scripts/livewire_quality.py` | Quality and health reporting | Bronze health checks, HTML warehouse report, coverage reports, daily rollup, weekly summary, watchdog alerts |
| `scripts/livewire_ops.py` | Operations | Scheduled daily job, alert sending |
| `scripts/livewire_store.py` | Storage maintenance | Postgres rebuilds, Postgres smoke checks, R2 sync, parquet filename migration |
| `scripts/setup_market_warehouse.sh` | One-time bootstrap | Create `~/market-warehouse/`, venv, directories, optional ClickHouse helpers |

Use `--help` at the top level or after a subcommand:

```bash
python scripts/livewire_ingest.py --help
python scripts/livewire_ingest.py historical --help
```

Subcommand map:

```text
livewire_ingest.py   daily | historical | robust | cboe-vol | fred-rates |
                     corporate-actions |
                     intraday-backfill | flatfile-ingest | universe |
                     universe-sync | backfill-all | daily-backfill
livewire_quality.py  health | coverage | report | weekly | watchdog | warehouse
livewire_ops.py      run-daily-job | run-intraday-catchup-job | send-alert
livewire_store.py    rebuild-postgres | smoke-postgres | sync-r2 | migrate-parquet
```

---

## [Interactive Brokers](https://ibkr.com/referral/joseph5632) Gateway

You need a running IB Gateway for ingestion of non-Massive data.

IB Gateway and IBC are owned by the separate **trading-stack** project at `~/trading-stack/` — livewire only consumes the API on `127.0.0.1:4001`. The full setup runbook lives at `~/runbooks/trading-stack/ib-gateway-ibc.md`.

```bash
~/trading-stack/scripts/ibc_gateway_status.sh   # health + watchdog state
~/trading-stack/scripts/bounce_ibc_xenon.sh     # full restart cycle
tail -30 /opt/ibc/logs/ibc-watchdog.log
nc -z 127.0.0.1 4001
```

> Gateway pinned to **10.45** (10.46 incompatible). 2FA approval via IBKR Mobile is manual on every fresh login.

---

## Data Ingestion

### Corporate actions

Massive split and cash-dividend events are reconciled independently from OHLCV
bars and stored at
`data-lake/bronze/asset_class=corporate_action/symbol=<ticker>/events.parquet`.
Each provider correction creates a new canonical revision linked to the prior
`action_id`; cancelled events remain in the audit history but are excluded from
the latest active view.

```bash
# Explicit symbols or a preset
python scripts/livewire_ingest.py corporate-actions --tickers NVDA AAPL SPY
python scripts/livewire_ingest.py corporate-actions --preset presets/sp500.json

# Discover all existing equity-bronze symbols and compare without writing
python scripts/livewire_ingest.py corporate-actions --dry-run

# A complete unfiltered provider fetch may infer cancellations
python scripts/livewire_ingest.py corporate-actions --full-reconcile
```

Targeted and preset runs do not infer cancellations unless
`--full-reconcile` is explicitly supplied. The command prints aggregate JSON
counters for `inserted`, `revised`, `cancelled`, `unchanged`, and `failed`, and
returns nonzero if any symbol fails. Automated scheduling intentionally lands
with the Silver engine so corporate-action ingestion and adjusted-bar
publication are ordered together.

### Prerequisites

* IB Gateway running (`127.0.0.1:4001` by default) — only needed for non-Massive data
* Configurable via CLI flags (`--host`, `--port`) or env vars (`MDW_IB_HOST`, `MDW_IB_PORT`)

Activate the project environment before running Python commands:

```bash
source ~/market-warehouse/.venv/bin/activate
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

# Spot commodities via IB CMDTY MIDPOINT
python scripts/livewire_ingest.py historical --preset presets/cmdty-metals.json --asset-class cmdty

# FX via IB Forex MIDPOINT
python scripts/livewire_ingest.py historical --preset presets/fx-pairs.json --asset-class fx

# Volatility (CBOE direct — authoritative daily source)
python scripts/livewire_ingest.py cboe-vol

# Treasury yields from FRED
FRED_API_KEY=... python scripts/livewire_ingest.py fred-rates

# Volatility historical backfill through IB Index contracts
python scripts/livewire_ingest.py historical --preset presets/volatility.json --asset-class volatility
```

For bulk IB runs (>5 tickers), use the robust orchestrator with per-ticker retry:

```bash
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode seed
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode backfill
```

---

### Default Warehouse Backfill

The full warehouse build runs all presets through daily seed, older-history backfill, intraday backfill, CBOE volatility, FRED rates, and optional Postgres rebuild:

```bash
# Python orchestrator (recommended)
python scripts/livewire_ingest.py backfill-all
# Or via unified CLI
python scripts/livewire.py backfill --full
```

Features:
- Equity daily seed/backfill for `sp500`, `ndx100`, `r2k`
- FRED Treasury yield rates
- Maximum-entitled-history full-market Massive equity intraday (`1m`, `5m`, `30m`, `1h`) in parallel with the volatility/index lane
- CBOE daily volatility sync followed by IB-backed VIX/SPX/NDX/RUT/VXN/RVX intraday (`30m` bars, 1h derived locally)
- Optional Postgres analytical rebuild when `MDW_POSTGRES_DSN` is set
- Activity-based stall detection and retry-until-done logic

For long runs, use `tmux`:

```bash
tmux new-session -s livewire_backfill \
  'cd /path/to/livewire && source ~/market-warehouse/.venv/bin/activate && python scripts/livewire_ingest.py backfill-all'
```

### Daily Backfill

Routine daily catch-up. Uses Massive for equity daily gaps and one whole-market
flat-file catch-up across every symbol present in each target day:

```bash
python scripts/livewire_ingest.py daily-backfill
# Or via unified CLI
python scripts/livewire.py sync --full
```

Default intraday lookback: 7 calendar days (`MDW_DAILY_BACKFILL_INTRADAY_DAYS`).

---

### Backfill Missing Data

```bash
# Equity — auto picks IB for deep history, Massive for recent
python scripts/livewire_ingest.py historical --preset presets/sp500.json --backfill --source auto

# Force IB or Massive
python scripts/livewire_ingest.py historical --preset presets/sp500.json --backfill --source ib
python scripts/livewire_ingest.py historical --preset presets/sp500.json --backfill --source massive

# Futures, commodities, FX, volatility
python scripts/livewire_ingest.py historical --preset presets/futures-index.json --asset-class futures --backfill
python scripts/livewire_ingest.py historical --preset presets/cmdty-metals.json --asset-class cmdty --backfill
python scripts/livewire_ingest.py historical --preset presets/fx-pairs.json --asset-class fx --backfill
```

---

### Intraday Data

Equity intraday uses Massive whole-market flat files exclusively. Non-equity
intraday remains IB-backed.

#### 1. Massive flat files (equity)

Each daily gzip covers every U.S. stock present in Massive's SIP minute file.
Livewire discovers the maximum entitled range, stages immutable bucketed raw
Parquet, publishes every ticker's canonical `1m` history, and derives
`5m`/`30m`/`1h` locally. Runs resume from durable raw-date, ticker, and bucket
state under `~/market-warehouse/cursors/`.

The client signs S3 V4 requests against `https://files.massive.com`, bucket
`flatfiles`, under `us_stocks_sip/minute_aggs_v1/`. Flat-file modes are
whole-market operations; ticker and preset filters are intentionally unsupported.

```bash
# Read-only entitlement and capacity plan
python scripts/livewire_ingest.py flatfile-ingest discover

# Full entitled-history build (Massive's standard entitlement is a rolling 5-year window)
python scripts/livewire_ingest.py flatfile-ingest backfill --workers 4

# Routine whole-market catch-up
python scripts/livewire_ingest.py flatfile-ingest catch-up --days 7 --workers 4

# Explicit date-range repair (skips capacity preflight)
python scripts/livewire_ingest.py flatfile-ingest repair --start 2021-06-11 --end 2026-06-10 --workers 4

# Explicit date repair (single day)
python scripts/livewire_ingest.py flatfile-ingest repair --dates 2026-06-05
```

`--workers N` parallelises both phases (download+stage and per-bucket publish).
Sweet spot is 4 on a 4-core Mac mini; expect ~3-4× speedup over serial.
Each worker caps open bucket file descriptors at 64 (256 total across 4 workers,
well under launchd's default rlimit). Set `MDW_FLATFILE_WORKERS` to control the
default used by scheduled jobs (daily intraday catch-up and full-backfill).

Requires `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`. Capacity planning
uses `MDW_FLATFILE_STORAGE_MULTIPLIER` (default `8`) and preserves at least
`MDW_FLATFILE_MIN_FREE_GB` (default `25`) after a full build. Raw partitions
live under
`data-lake/raw/massive/us_stocks_sip/minute_aggs_v1/date=YYYY-MM-DD/`.

> **Entitlement window**: the standard Massive plan is a rolling 5-year window
> (today − 5 years through today). Dates outside the window return `403
> Forbidden`. The discovery output reports both the entitled range and the
> object count.

Rollback is operational, not a runtime fallback: unload the intraday-catchup
launchd job, revert the replacement PR, deploy the revert, then reload the
prior scheduled job. Do not run old and new equity-intraday writers together.

#### 2. IB (volatility/index, futures)

Volatility/index intraday covers VIX, SPX, NDX, RUT, VXN, and RVX via `presets/volatility-intraday.json`. IB fetches 30m bars only; 1h is derived locally via lossless aggregation from 30m.

```bash
# Volatility/index intraday
python scripts/livewire_ingest.py intraday-backfill --preset presets/volatility-intraday.json --asset-class volatility --timeframe 30m --source ib --skip-existing

# Futures intraday
python scripts/livewire_ingest.py intraday-backfill --preset presets/futures-index.json --asset-class futures --timeframe 1m --source ib --years 5
```

#### Timeframe aggregation

Lossless OHLCV rollup supports: `1m→5m`, `1m→30m`, `1m→1h`, `30m→1h`. Aggregation uses clock-aligned windows (`open=first, high=max, low=min, close=last, volume=sum`). Both the S3 flat file ingestion and the vol/index IB pipeline apply aggregation automatically — derived timeframes don't require separate backfill runs.

---

### Daily Updates

```bash
# Equity daily update (uses Massive by default — requires MASSIVE_API_KEY)
python scripts/livewire_ingest.py daily

# Force IB for equity instead of Massive
python scripts/livewire_ingest.py daily --asset-class equity --source ib

# Futures daily update (IB)
python scripts/livewire_ingest.py daily --asset-class futures

# Spot commodity daily update (IB)
python scripts/livewire_ingest.py daily --asset-class cmdty --preset presets/cmdty-metals.json

# FX daily update (IB)
python scripts/livewire_ingest.py daily --asset-class fx --preset presets/fx-pairs.json

# Volatility (CBOE direct — authoritative)
python scripts/livewire_ingest.py cboe-vol
```

Common flags:

```bash
--dry-run
--force
--target-date YYYY-MM-DD
--preset presets/sp500.json
--asset-class {equity|volatility|futures|cmdty|fx}
--source {ib|massive}        # default: massive for equity, ib for everything else
```

Key behavior:
* Detects missing trading days automatically
* Fetches only gaps
* Validates OHLCV integrity
* Atomic snapshot updates
* Fallback recovery if IB fails (Nasdaq stocks/ETF, then Stooq)

### Scheduled Daily Runs

The scheduled runner handles equities, futures, and CBOE volatility:

```bash
python scripts/livewire_ops.py run-daily-job
```

**macOS launchd scheduling:**

```bash
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.intraday-catchup.plist.example > ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
```

* **Daily sync**: 05:05 UTC (01:05 ET, ~9h after US RTH close)
* **Watchdog**: 10:30 UTC (06:30 ET)
* **Intraday catch-up**: 20:30 UTC (16:30 EDT Mar–Nov / 15:30 EST Nov–Mar — see CLAUDE.md for the DST-drift caveat)

> launchd has no `TimeZone` key — each plist's `Hour`/`Minute` are interpreted in the Mac's local TZ. The example plists ship with `Asia/Hong_Kong` defaults; see each plist header for the conversion table to other Mac timezones.

---

### Reliability / Data Quality

```bash
# Bronze health report
python scripts/livewire_quality.py health

# Include intraday gap detection
python scripts/livewire_quality.py health --intraday --timeframe 5m

# Static HTML warehouse report from actual bronze parquet, grouped by asset and ticker
python scripts/livewire_quality.py warehouse

# Write the report to a specific path
python scripts/livewire_quality.py warehouse --output ~/market-warehouse/reports/warehouse_health.html

# Plan repair actions for actionable report warnings/errors
python scripts/livewire_quality.py warehouse --repair --dry-run

# Run repair actions, then regenerate the report
python scripts/livewire_quality.py warehouse --repair

# Daily coverage report with auto-recovery
python scripts/livewire_quality.py coverage

# Quality rollup
python scripts/livewire_quality.py report --view summary --since 24h

# Send quality rollup by email
python scripts/livewire_quality.py report --view summary --since 24h --email

# Weekly summary (self-skips on non-Sunday)
python scripts/livewire_quality.py weekly

# Scheduled-job watchdog
python scripts/livewire_quality.py watchdog
```

---

### Rebuild Postgres

Postgres is optional and replayable. It is not the ingestion source of truth.

```bash
export MDW_POSTGRES_DSN="postgresql://USER:YOUR_PASSWORD@localhost:5432/livewire"
export MDW_POSTGRES_SCHEMA="md"

# Smoke check
python scripts/livewire_store.py smoke-postgres --ensure-schema

# Rebuild equity daily
python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe 1d

# Rebuild all equity timeframes (missing optional intraday data is skipped)
python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe all

# Rebuild futures and volatility
python scripts/livewire_store.py rebuild-postgres --asset-class futures
python scripts/livewire_store.py rebuild-postgres --asset-class volatility

# Import reliability telemetry and quality flags
python scripts/livewire_store.py rebuild-postgres --include-reliability
```

Rollback: `DROP SCHEMA IF EXISTS md CASCADE;` then rerun rebuilds from bronze parquet.

---

### Other Storage Commands

```bash
# Sync lake files to R2
python scripts/livewire_store.py sync-r2

# Migrate old parquet filenames
python scripts/livewire_store.py migrate-parquet
```

---

## Environment Variables

### Data sources

| Variable | Purpose |
| --- | --- |
| `MASSIVE_API_KEY` | Massive REST API key for optional equity daily acceleration |
| `MASSIVE_S3_ACCESS_KEY` | Massive S3 access key; required for equity intraday |
| `MASSIVE_S3_SECRET_KEY` | Massive S3 secret key; required for equity intraday |
| `FRED_API_KEY` | FRED API key for Treasury yield rates |

### IB Gateway

| Variable | Default | Purpose |
| --- | --- | --- |
| `MDW_IB_HOST` | `127.0.0.1` | IB Gateway host |
| `MDW_IB_PORT` | `4001` | IB Gateway port |

### Reliability / alerting

| Variable | Default | Purpose |
| --- | --- | --- |
| `MDW_TELEMETRY_PATH` | `~/market-warehouse/logs/telemetry.jsonl` | Telemetry JSONL append path |
| `MDW_QUALITY_AUDIT_PATH` | `~/market-warehouse/logs/quality_audit.jsonl` | Quality-flag audit JSONL |
| `MDW_ALERT_SEVERITY_THRESHOLD` | `warning` | Min severity that triggers per-flag email |
| `MDW_ALERT_RATE_LIMIT_SECONDS` | `300` | De-dup window for identical alerts |
| `MDW_LOG_LEVEL` | `INFO` | Logger root level |

### Postgres

| Variable | Default | Purpose |
| --- | --- | --- |
| `MDW_POSTGRES_DSN` | — | Postgres DSN for analytical rebuilds |
| `MDW_POSTGRES_SCHEMA` | `md` | Target analytical schema |
| `MDW_TEST_POSTGRES_DSN` | — | Disposable DSN for integration tests |

### Orchestrators

| Variable | Default | Purpose |
| --- | --- | --- |
| `MDW_ORCHESTRATOR_TIMEOUT_SECONDS` | `300` | Per-ticker hard timeout for robust IB runs |
| `MDW_ORCHESTRATOR_MAX_ATTEMPTS` | `3` | Per-ticker retry budget |
| `MDW_ORCHESTRATOR_COOLDOWN_SECONDS` | `60` | Sleep between retry attempts |
| `MDW_DAILY_BACKFILL_INTRADAY_DAYS` | `7` | Intraday recent-window lookback (calendar days) |
| `MDW_FLATFILE_LOOKBACK_DAYS` | `7` | Default direct `flatfile-ingest catch-up` lookback |
| `MDW_FLATFILE_BUCKETS` | `256` | Raw ticker buckets per Massive trading-day partition |
| `MDW_FLATFILE_STORAGE_MULTIPLIER` | `8` | Full-build storage projection multiplier |
| `MDW_FLATFILE_MIN_FREE_GB` | `25` | Required free-space reserve after a full build |
| `MDW_FLATFILE_WORKERS` | `4` (scheduled jobs); `1` (manual CLI) | Parallel worker count for download+stage and per-bucket publish |

---

## Testing

### Run Tests

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v
```

### Coverage

```bash
python -m pytest tests -q --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing
```

* **100% coverage enforced** (`fail_under = 100` in `pyproject.toml`)
* `clients/ib_client.py` and `clients/historical_provider.py` excluded from coverage gate

### RuntimeWarning Gate

Run after changes that touch async script runners or tests that mock `ib.ib.run(...)`:

```bash
python -m pytest tests -q -W error::RuntimeWarning
```

---

## Security

### Pre-commit Hook

```bash
ln -sf ../../tools/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Detects API keys, credentials, private keys, and `.env` leaks.

---

## Data Model Notes

### Split-Adjusted Volume

All volume is **split-adjusted** to ensure consistency across time.

### Timeframe Aggregation

Lossless OHLCV rollup (pure function, no I/O):
- Supported: `1m→5m`, `1m→30m`, `1m→1h`, `30m→1h`
- Clock-aligned windows: `open=first, high=max, low=min, close=last, volume=sum`
- Partial windows at end of data are dropped

---

## ClickHouse (Optional)

Used for benchmarking, concurrency testing, and production simulation.

```bash
~/market-warehouse/scripts/start_clickhouse.sh
~/market-warehouse/scripts/init_clickhouse.sh
~/market-warehouse/scripts/stop_clickhouse.sh
```

---

## Sample Data

```bash
scripts/setup_market_warehouse.sh --with-sample-data
python ~/market-warehouse/scripts/write_sample_parquet.py
```

---

## License

MIT
