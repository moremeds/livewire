#!/usr/bin/env bash
# Daily backfill runner — lightweight catch-up path for routine warehouse updates.
#
# Uses Massive for equity daily gaps and equity intraday recent windows. Keeps
# non-equity sources aligned with the full warehouse runner: FRED for rates,
# CBOE for daily volatility, IB for VIX/SPX volatility intraday, and optional
# Postgres rebuild from canonical parquet.

set -euo pipefail

VENV="$HOME/market-warehouse/.venv/bin/activate"
SCRIPT="scripts/livewire_ingest.py"
LOG_DIR="$HOME/market-warehouse/logs"
ENV_FILE=".env"
INTRADAY_DAYS="${MDW_DAILY_BACKFILL_INTRADAY_DAYS:-7}"
INTRADAY_CONCURRENT="${MDW_DAILY_BACKFILL_INTRADAY_CONCURRENT:-20}"
TARGET_DATE="${MDW_DAILY_BACKFILL_TARGET_DATE:-}"
RUN_FAILURES=()

source "$VENV"
if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

timestamp() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] $*"; }

format_command() {
    local limit=24
    if [ "$#" -le "$limit" ]; then
        printf "%s " "$@"
        return
    fi
    local shown=0
    while [ "$shown" -lt "$limit" ]; do
        printf "%s " "$1"
        shift
        shown=$((shown + 1))
    done
    printf "... [%d more args]" "$#"
}

latest_complete_trading_day() {
    python3 - <<'PY'
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from livewire_scripts.daily_update import is_trading_day, previous_trading_day, session_close_time

et_now = datetime.now(ZoneInfo("America/New_York"))
today = et_now.date()
if not is_trading_day(today):
    print(previous_trading_day(today).isoformat())
else:
    close_time = session_close_time(today)
    close_dt = et_now.replace(
        hour=close_time.hour,
        minute=close_time.minute,
        second=0,
        microsecond=0,
    )
    complete_day = today if et_now >= close_dt + timedelta(minutes=30) else previous_trading_day(today)
    print(complete_day.isoformat())
PY
}

equity_ticker_union() {
    python3 - "${PRESETS[@]}" <<'PY'
import json
import sys

tickers = set()
for preset in sys.argv[1:]:
    with open(preset, encoding="utf-8") as fh:
        payload = json.load(fh)
    tickers.update(str(ticker).upper() for ticker in payload.get("tickers", []))
print(" ".join(sorted(tickers)))
PY
}

preset_tickers() {
    python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    payload = json.load(fh)
print(" ".join(str(ticker).upper() for ticker in payload.get("tickers", [])))
PY
}

run_logged() {
    local label="$1"
    shift
    local allow_completed_summary=0
    if [ "${1:-}" = "--allow-completed-summary" ]; then
        allow_completed_summary=1
        shift
    fi
    local log_file="$LOG_DIR/${label}.log"
    log "CMD $label: $(format_command "$@")"
    set +e
    "$@" >> "$log_file" 2>&1
    local status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        if [ "$allow_completed_summary" -eq 1 ] && grep -q "Daily Update Complete" "$log_file"; then
            log "WARN $label exited with code $status after a completed summary; continuing"
            return
        fi
        log "WARN $label exited with code $status; continuing"
        RUN_FAILURES+=("${label}:${status}")
    fi
}

mkdir -p "$LOG_DIR"

if [ -z "$TARGET_DATE" ]; then
    TARGET_DATE="$(latest_complete_trading_day)"
fi

log "============================================================"
log "DAILY BACKFILL START"
log "Target complete trading day: ${TARGET_DATE}"
log "Intraday recent window: ${INTRADAY_DAYS} calendar days"
log "Massive intraday concurrency: ${INTRADAY_CONCURRENT}"
log "============================================================"

PRESETS=("presets/sp500.json" "presets/ndx100.json" "presets/r2k.json")
EQUITY_TICKERS=($(equity_ticker_union))

log "── PHASE 1: Massive equity daily recent backfill union (${#EQUITY_TICKERS[@]} tickers) ──"
run_logged "daily_backfill_equity_union" \
    --allow-completed-summary \
    python "$SCRIPT" daily --asset-class equity --source massive --tickers "${EQUITY_TICKERS[@]}" \
    --target-date "$TARGET_DATE" --force

log "============================================================"
log "PHASE 1 COMPLETE"
log "============================================================"

log "── PHASE 2: FRED Treasury rates ──"
run_logged "daily_backfill_fred_rates" python "$SCRIPT" fred-rates

log "── PHASE 3: CBOE volatility daily ──"
run_logged "daily_backfill_volatility_cboe" \
    python "$SCRIPT" cboe-vol --preset presets/volatility.json

log "============================================================"
log "PHASE 3 COMPLETE"
log "============================================================"

for timeframe in 1m 5m 1h; do
    log "── PHASE 4: Massive equity ${timeframe} intraday recent backfill union ──"
    run_logged "daily_backfill_intraday_${timeframe}_equity" \
        python "$SCRIPT" intraday-backfill --tickers "${EQUITY_TICKERS[@]}" --timeframe "$timeframe" \
        --source massive --asset-class equity --days "$INTRADAY_DAYS" \
        --max-concurrent "$INTRADAY_CONCURRENT"
done

log "============================================================"
log "PHASE 4 COMPLETE"
log "============================================================"

VOL_PRESET="presets/volatility-intraday.json"
VOL_NAME=$(python3 -c "import json; print(json.load(open('$VOL_PRESET'))['name'])")
VOL_TICKERS=($(preset_tickers "$VOL_PRESET"))
for timeframe in 5m 1h; do
    log "── PHASE 5: IB volatility ${timeframe} intraday recent backfill $VOL_NAME (${#VOL_TICKERS[@]} tickers) ──"
    run_logged "daily_backfill_intraday_${timeframe}_${VOL_NAME}" \
        python "$SCRIPT" intraday-backfill --tickers "${VOL_TICKERS[@]}" --timeframe "$timeframe" \
        --source ib --asset-class volatility --days "$INTRADAY_DAYS"
done

log "============================================================"
log "PHASE 5 COMPLETE"
log "============================================================"

if [ -n "${MDW_POSTGRES_DSN:-}" ]; then
    log "── PHASE 6: Postgres analytical rebuild ──"
    run_logged "daily_backfill_postgres_equity" \
        python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe all --include-reliability
    run_logged "daily_backfill_postgres_volatility" \
        python scripts/livewire_store.py rebuild-postgres --asset-class volatility --timeframe 1d
else
    log "── PHASE 6: Postgres analytical rebuild skipped — MDW_POSTGRES_DSN is not set ──"
fi

log "============================================================"
log "DAILY BACKFILL COMPLETE"
if [ "${#RUN_FAILURES[@]}" -gt 0 ]; then
    log "Completed with lane failures: ${RUN_FAILURES[*]}"
    log "============================================================"
    exit 1
fi
log "============================================================"
