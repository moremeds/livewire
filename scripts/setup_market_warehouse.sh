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
PY_ENV_DIR="${ROOT_DIR}/.venv"
SCRIPTS_DIR="${ROOT_DIR}/scripts"
LOG_DIR="${ROOT_DIR}/logs"

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
  --with-sample-data    Generate sample Parquet data after setup
  --smoke-test          Run validation queries/import tests after setup
  --help                Show this help

Examples:
  $0
  $0 --with-sample-data --smoke-test
EOF
}

########################################
# Parse flags
########################################
while [[ $# -gt 0 ]]; do
  case "$1" in
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
  "${SCRIPTS_DIR}" \
  "${LOG_DIR}"

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

green "Installing Python packages via uv..."
if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found. Install with: brew install uv  (or https://docs.astral.sh/uv/)"
  exit 1
fi
uv pip install --python "${PY_ENV_DIR}/bin/python" \
  polars \
  pandas \
  pyarrow \
  "psycopg[binary]" \
  numpy \
  scipy \
  python-dotenv \
  rich \
  ib-async \
  httpx \
  ipython \
  jupyterlab

########################################
# Helper scripts
########################################
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
# README
########################################
cat > "${ROOT_DIR}/README.md" <<'MD'
# Market Warehouse Setup

## What this installs
- Python environment with Polars, Pandas, PyArrow, psycopg
- Canonical Parquet-based data lake layout

## Flags
- `--with-sample-data`
- `--smoke-test`

## Examples
```bash
./setup_market_warehouse.sh
./setup_market_warehouse.sh --with-sample-data --smoke-test
```
MD

########################################
# Execute optional steps based on flags
########################################
if [[ "${WITH_SAMPLE_DATA}" -eq 1 ]]; then
  green "Generating sample Parquet data..."
  python "${SCRIPTS_DIR}/write_sample_parquet.py"
fi

if [[ "${SMOKE_TEST}" -eq 1 ]]; then
  green "Running smoke tests..."

  if [[ "${WITH_SAMPLE_DATA}" -eq 1 ]]; then
    PARQUET_COUNT=$(python - <<PY
from pathlib import Path
import pyarrow.parquet as pq

root = Path("${DATA_LAKE_DIR}/bronze/asset_class=equity")
print(sum(pq.ParquetFile(path).metadata.num_rows for path in root.glob("year=*/month=*/*.parquet")))
PY
)
    if [[ "${PARQUET_COUNT}" -eq 0 ]]; then
      red "FAIL: Sample Parquet data returned 0 rows."
      exit 1
    fi
    green "  Parquet sample data OK (${PARQUET_COUNT} rows)."
  fi

  green "All smoke tests passed."
fi

green "Setup complete. Market warehouse is at ${ROOT_DIR}"
