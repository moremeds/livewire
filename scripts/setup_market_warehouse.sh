#!/usr/bin/env bash
set -euo pipefail

########################################
# Config
########################################
ROOT_DIR="${HOME}/market-warehouse"
DATA_LAKE_DIR="${ROOT_DIR}/data-lake"
RAW_DIR="${DATA_LAKE_DIR}/raw"
BRONZE_DIR="${DATA_LAKE_DIR}/bronze"
SILVER_DIR="${DATA_LAKE_DIR}/silver"
GOLD_DIR="${DATA_LAKE_DIR}/gold"
DUCKDB_DIR="${ROOT_DIR}/duckdb"
CLICKHOUSE_DIR="${ROOT_DIR}/clickhouse"
PY_ENV_DIR="${ROOT_DIR}/.venv"
SCRIPTS_DIR="${ROOT_DIR}/scripts"
LOG_DIR="${ROOT_DIR}/logs"
TMP_DUCKDB_DIR="${ROOT_DIR}/tmp_duckdb"

DUCKDB_FILE="${DUCKDB_DIR}/market.duckdb"

INIT_CLICKHOUSE=0
START_CLICKHOUSE=0
WITH_SAMPLE_DATA=0
SMOKE_TEST=0

########################################
# Helpers
########################################
green()  { printf "\033[32m%s\033[0m\n" "$1"; }
yellow() { printf "\033[33m%s\033[0m\n" "$1"; }
red()    { printf "\033[31m%s\033[0m\n" "$1"; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1
}

usage() {
  cat <<EOF
Usage: $0 [flags]

Flags:
  --start-clickhouse    Start ClickHouse after setup
  --init-clickhouse     Initialize ClickHouse schema after setup
  --with-sample-data    Generate sample Parquet data after setup
  --smoke-test          Run validation queries/import tests after setup
  --help                Show this help

Examples:
  $0
  $0 --with-sample-data --smoke-test
  $0 --start-clickhouse --init-clickhouse
  $0 --start-clickhouse --init-clickhouse --with-sample-data --smoke-test
EOF
}

########################################
# Parse flags
########################################
while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-clickhouse)
      START_CLICKHOUSE=1
      shift
      ;;
    --init-clickhouse)
      INIT_CLICKHOUSE=1
      START_CLICKHOUSE=1
      shift
      ;;
    --with-sample-data)
      WITH_SAMPLE_DATA=1
      shift
      ;;
    --smoke-test)
      SMOKE_TEST=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      red "Unknown argument: $1"
      echo
      usage
      exit 1
      ;;
  esac
done

########################################
# Sanity checks
########################################
if [[ "$(uname -s)" != "Darwin" ]]; then
  red "This script is for macOS only."
  exit 1
fi

ARCH="$(uname -m)"
if [[ "${ARCH}" != "arm64" ]]; then
  yellow "Warning: This script is optimized for Apple Silicon. Detected: ${ARCH}"
fi

########################################
# Install Homebrew if missing
########################################
if ! need_cmd brew; then
  yellow "Homebrew not found. Installing..."
  NONINTERACTIVE=1 /bin/bash -c \
    "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

if [[ -x /opt/homebrew/bin/brew ]]; then
  eval "$(/opt/homebrew/bin/brew shellenv)"
elif [[ -x /usr/local/bin/brew ]]; then
  eval "$(/usr/local/bin/brew shellenv)"
else
  red "Homebrew installed but brew not found in expected locations."
  exit 1
fi

########################################
# Update brew metadata
########################################
green "Updating Homebrew..."
brew update

########################################
# Install base tooling
########################################
green "Installing base packages..."
brew install \
  duckdb \
  clickhouse \
  python@3.13 \
  uv \
  jq \
  wget \
  zstd \
  cmake \
  pkg-config

########################################
# Optional dev tooling
########################################
if ! need_cmd rustc; then
  green "Installing Rust toolchain..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  # shellcheck disable=SC1090
  source "${HOME}/.cargo/env"
else
  green "Rust already installed."
fi

if ! need_cmd node; then
  green "Installing Node.js..."
  brew install node
else
  green "Node.js already installed."
fi

########################################
# Create project layout
########################################
green "Creating project layout under ${ROOT_DIR}..."
mkdir -p \
  "${RAW_DIR}/asset_class=equity" \
  "${RAW_DIR}/asset_class=option" \
  "${RAW_DIR}/asset_class=future" \
  "${BRONZE_DIR}/asset_class=equity" \
  "${BRONZE_DIR}/asset_class=option" \
  "${BRONZE_DIR}/asset_class=future" \
  "${SILVER_DIR}/asset_class=equity" \
  "${SILVER_DIR}/asset_class=option" \
  "${SILVER_DIR}/asset_class=future" \
  "${GOLD_DIR}/asset_class=equity" \
  "${GOLD_DIR}/asset_class=option" \
  "${GOLD_DIR}/asset_class=future" \
  "${DUCKDB_DIR}" \
  "${CLICKHOUSE_DIR}" \
  "${SCRIPTS_DIR}" \
  "${LOG_DIR}" \
  "${TMP_DUCKDB_DIR}"

########################################
# Python environment
########################################
green "Creating Python virtual environment..."
if [[ -x /opt/homebrew/bin/python3.13 ]]; then
  /opt/homebrew/bin/python3.13 -m venv "${PY_ENV_DIR}"
else
  python3.13 -m venv "${PY_ENV_DIR}"
fi

# shellcheck disable=SC1091
source "${PY_ENV_DIR}/bin/activate"

green "Installing Python packages..."
python -m pip install --upgrade pip wheel setuptools
pip install \
  duckdb \
  polars \
  pandas \
  pyarrow \
  "psycopg[binary]" \
  clickhouse-connect \
  numpy \
  scipy \
  python-dotenv \
  rich \
  ib-async \
  httpx \
  ipython \
  jupyterlab

########################################
# Create DuckDB bootstrap SQL
########################################
cat > "${SCRIPTS_DIR}/bootstrap_duckdb.sql" <<'SQL'
PRAGMA threads=8;
PRAGMA enable_progress_bar;
PRAGMA temp_directory='./tmp_duckdb';

CREATE SCHEMA IF NOT EXISTS md;

CREATE TABLE IF NOT EXISTS md.symbols (
    symbol_id BIGINT PRIMARY KEY,
    symbol VARCHAR,
    asset_class VARCHAR,
    venue VARCHAR
);

CREATE TABLE IF NOT EXISTS md.equities_daily (
    trade_date DATE,
    symbol_id BIGINT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT
);

CREATE TABLE IF NOT EXISTS md.futures_daily (
    trade_date DATE,
    contract_id BIGINT,
    root_symbol VARCHAR,
    expiry_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    settlement DOUBLE,
    volume BIGINT,
    open_interest BIGINT
);

CREATE TABLE IF NOT EXISTS md.options_daily (
    trade_date DATE,
    contract_id BIGINT,
    underlier_id BIGINT,
    expiry_date DATE,
    strike DOUBLE,
    option_right VARCHAR,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    open_interest BIGINT,
    implied_vol DOUBLE
);
SQL

green "Bootstrapping DuckDB..."
rm -f "${DUCKDB_FILE}"

if ! (
  cd "${ROOT_DIR}"
  duckdb "${DUCKDB_FILE}" < "${SCRIPTS_DIR}/bootstrap_duckdb.sql"
); then
  red "DuckDB bootstrap failed."
  red "Run this manually for more detail:"
  echo "  cd \"${ROOT_DIR}\" && duckdb \"${DUCKDB_FILE}\" < \"${SCRIPTS_DIR}/bootstrap_duckdb.sql\""
  exit 1
fi

########################################
# Create ClickHouse bootstrap SQL
########################################
cat > "${SCRIPTS_DIR}/bootstrap_clickhouse.sql" <<'SQL'
CREATE DATABASE IF NOT EXISTS md;

CREATE TABLE IF NOT EXISTS md.equities_daily
(
    trade_date Date,
    symbol_id UInt64,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    adj_close Float64,
    volume UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol_id, trade_date);

CREATE TABLE IF NOT EXISTS md.futures_daily
(
    trade_date Date,
    contract_id UInt64,
    root_symbol LowCardinality(String),
    expiry_date Date,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    settlement Float64,
    volume UInt64,
    open_interest UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (root_symbol, expiry_date, trade_date, contract_id);

CREATE TABLE IF NOT EXISTS md.options_daily
(
    trade_date Date,
    contract_id UInt64,
    underlier_id UInt64,
    expiry_date Date,
    strike Float64,
    option_right Enum8('C' = 1, 'P' = 2),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    open_interest UInt64,
    implied_vol Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (underlier_id, expiry_date, strike, option_right, trade_date);
SQL

########################################
# Helper scripts
########################################
cat > "${SCRIPTS_DIR}/start_clickhouse.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${HOME}/market-warehouse/clickhouse/data"
LOG_DIR="${HOME}/market-warehouse/logs"
mkdir -p "${DATA_DIR}" "${LOG_DIR}"

if pgrep -f "clickhouse server" >/dev/null 2>&1; then
  echo "ClickHouse server already running."
  exit 0
fi

clickhouse server --daemon -- --path "${DATA_DIR}"
sleep 3

if pgrep -f "clickhouse server" >/dev/null 2>&1; then
  echo "ClickHouse server started."
else
  echo "ClickHouse server failed to start."
  exit 1
fi
SH
chmod +x "${SCRIPTS_DIR}/start_clickhouse.sh"

cat > "${SCRIPTS_DIR}/stop_clickhouse.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
pkill -f "clickhouse server" || true
echo "ClickHouse server stopped."
SH
chmod +x "${SCRIPTS_DIR}/stop_clickhouse.sh"

cat > "${SCRIPTS_DIR}/init_clickhouse.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

ROOT="${HOME}/market-warehouse"
SQL_FILE="${ROOT}/scripts/bootstrap_clickhouse.sql"

if ! pgrep -f "clickhouse server" >/dev/null 2>&1; then
  "${ROOT}/scripts/start_clickhouse.sh"
fi

clickhouse client --multiquery < "${SQL_FILE}"
echo "ClickHouse schema initialized."
SH
chmod +x "${SCRIPTS_DIR}/init_clickhouse.sh"

cat > "${SCRIPTS_DIR}/activate_env.sh" <<SH
#!/usr/bin/env bash
source "${PY_ENV_DIR}/bin/activate"
echo "Activated Python env: ${PY_ENV_DIR}"
SH
chmod +x "${SCRIPTS_DIR}/activate_env.sh"

########################################
# Sample Parquet writer
########################################
cat > "${SCRIPTS_DIR}/write_sample_parquet.py" <<'PY'
from pathlib import Path
import polars as pl
from datetime import date, timedelta

root = Path.home() / "market-warehouse" / "data-lake" / "bronze" / "asset_class=equity" / "year=2025" / "month=01"
root.mkdir(parents=True, exist_ok=True)

rows = []
start = date(2025, 1, 1)
symbols = [1001, 1002, 1003]

for sid in symbols:
    px = 100.0 + sid % 10
    for i in range(20):
        d = start + timedelta(days=i)
        rows.append({
            "trade_date": d,
            "symbol_id": sid,
            "open": px + i * 0.1,
            "high": px + i * 0.2,
            "low": px + i * 0.05,
            "close": px + i * 0.15,
            "adj_close": px + i * 0.15,
            "volume": 1_000_000 + i * 1_000
        })

df = pl.DataFrame(rows)
out = root / "part-0001.parquet"
df.write_parquet(out)
print(f"Wrote {out}")
PY

########################################
# DuckDB query demo
########################################
cat > "${SCRIPTS_DIR}/query_parquet_duckdb.sql" <<'SQL'
SELECT
  symbol_id,
  avg(close) AS avg_close,
  sum(volume) AS total_volume
FROM read_parquet('~/market-warehouse/data-lake/bronze/asset_class=equity/year=*/month=*/*.parquet')
GROUP BY symbol_id
ORDER BY symbol_id;
SQL

########################################
# README
########################################
cat > "${ROOT_DIR}/README.md" <<'MD'
# Market Warehouse Setup

## What this installs
- DuckDB for local analytics
- ClickHouse for production-like local benchmarking
- Python environment with Polars, Pandas, PyArrow, DuckDB, ClickHouse Connect
- Canonical Parquet-based data lake layout

## Flags
- `--start-clickhouse`
- `--init-clickhouse`
- `--with-sample-data`
- `--smoke-test`

## Examples
```bash
./setup_market_warehouse.sh
./setup_market_warehouse.sh --with-sample-data --smoke-test
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse --with-sample-data --smoke-test
```

## Troubleshooting

### macOS Gatekeeper blocks ClickHouse

If you see an error like **"clickhouse-macos-aarch64" Not Opened** — Apple could not
verify the binary is free of malware — click **Done** (do not move to trash), then
remove the quarantine attribute:

```bash
xattr -d com.apple.quarantine $(which clickhouse)
```

If `which clickhouse` returns nothing or the above gives a "no such xattr" error,
try removing it from the Homebrew Cellar directly:

```bash
xattr -dr com.apple.quarantine /opt/homebrew/Cellar/clickhouse
```
MD

########################################
# Execute optional steps based on flags
########################################
if [[ "${START_CLICKHOUSE}" -eq 1 ]]; then
  green "Starting ClickHouse..."
  bash "${SCRIPTS_DIR}/start_clickhouse.sh"
fi

if [[ "${INIT_CLICKHOUSE}" -eq 1 ]]; then
  green "Initializing ClickHouse schema..."
  bash "${SCRIPTS_DIR}/init_clickhouse.sh"
fi

if [[ "${WITH_SAMPLE_DATA}" -eq 1 ]]; then
  green "Generating sample Parquet data..."
  python "${SCRIPTS_DIR}/write_sample_parquet.py"
fi

if [[ "${SMOKE_TEST}" -eq 1 ]]; then
  green "Running smoke tests..."

  # DuckDB: verify tables exist
  DUCKDB_TABLES=$(duckdb -csv -noheader "${DUCKDB_FILE}" <<< "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'md';")
  if [[ "${DUCKDB_TABLES}" -eq 0 ]]; then
    red "FAIL: DuckDB has no tables in schema 'md'."
    exit 1
  fi
  green "  DuckDB schema OK (tables found in md)."

  # DuckDB: if sample data was generated, query it
  if [[ "${WITH_SAMPLE_DATA}" -eq 1 ]]; then
    PARQUET_COUNT=$(duckdb -csv -noheader "${DUCKDB_FILE}" <<< "SELECT count(*) FROM read_parquet('${DATA_LAKE_DIR}/bronze/asset_class=equity/year=*/month=*/*.parquet');")
    if [[ "${PARQUET_COUNT}" -eq 0 ]]; then
      red "FAIL: Sample Parquet data returned 0 rows."
      exit 1
    fi
    green "  DuckDB Parquet query OK (${PARQUET_COUNT} rows)."
  fi

  # ClickHouse: if started, verify schema
  if [[ "${INIT_CLICKHOUSE}" -eq 1 ]]; then
    CH_TABLES=$(clickhouse client --query "SELECT count() FROM system.tables WHERE database = 'md';")
    if [[ "${CH_TABLES}" -eq 0 ]]; then
      red "FAIL: ClickHouse has no tables in database 'md'."
      exit 1
    fi
    green "  ClickHouse schema OK (${CH_TABLES} tables in md)."
  fi

  green "All smoke tests passed."
fi

green "Setup complete. Market warehouse is at ${ROOT_DIR}"
