#!/usr/bin/env bash
# run_backfill_all.sh — Auto-restarting backfill runner for all presets.
#
# Monitors cursor file modification time (not DB row count, which locks).
# Restarts with cooldown if cursor hasn't updated in STALL_TIMEOUT seconds.
#
# Usage:
#   source ~/market-warehouse/.venv/bin/activate
#   bash tools/run_backfill_all.sh

set -euo pipefail

VENV="$HOME/market-warehouse/.venv/bin/activate"
SCRIPT="scripts/livewire_ingest.py"
LOG_DIR="$HOME/market-warehouse/logs"
ENV_FILE=".env"
STALL_TIMEOUT=600    # seconds of no cursor update before killing (10 min)
COOLDOWN=300         # seconds to wait after stall/failure (5 min IB cooldown)
MAX_CONCURRENT=10
BATCH_SIZE=5

source "$VENV"
if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

timestamp() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] $*"; }

# Get mtime of a file in epoch seconds (0 if missing)
file_mtime() {
    if [ -f "$1" ]; then
        stat -f %m "$1" 2>/dev/null || echo 0
    else
        echo 0
    fi
}

# Get completed count from cursor file
cursor_completed() {
    local cursor_file="$1"
    if [ -f "$cursor_file" ]; then
        python3 -c "import json; print(len(json.load(open('$cursor_file')).get('completed',[])))" 2>/dev/null || echo 0
    else
        echo 0
    fi
}

# Run a single preset with stall detection. Monitors cursor file changes.
# Returns 0 on clean completion, 1 if killed due to stall.
run_preset() {
    local label="$1"
    local cursor_file="$2"
    shift 2
    local cmd=("$@")

    local start_completed
    start_completed=$(cursor_completed "$cursor_file")
    log "START $label — cursor completed: $start_completed"
    log "CMD: ${cmd[*]}"

    # Launch fetch
    "${cmd[@]}" >> "$LOG_DIR/${label}.log" 2>&1 &
    local pid=$!
    log "PID: $pid"

    local last_mtime
    last_mtime=$(file_mtime "$cursor_file")
    local last_check
    last_check=$(date +%s)

    # Monitor via cursor file mtime
    while kill -0 "$pid" 2>/dev/null; do
        sleep 30

        local current_mtime
        current_mtime=$(file_mtime "$cursor_file")

        if [ "$current_mtime" != "$last_mtime" ]; then
            local completed
            completed=$(cursor_completed "$cursor_file")
            log "PROGRESS $label — completed: $completed"
            last_mtime="$current_mtime"
            last_check=$(date +%s)
        else
            local now
            now=$(date +%s)
            local stall=$((now - last_check))

            if [ "$stall" -ge "$STALL_TIMEOUT" ]; then
                log "STALL $label — no cursor update for ${stall}s, killing pid $pid"
                kill "$pid" 2>/dev/null || true
                sleep 3
                kill -9 "$pid" 2>/dev/null || true
                wait "$pid" 2>/dev/null || true
                return 1
            fi
        fi
    done

    wait "$pid" 2>/dev/null
    local exit_code=$?
    local end_completed
    end_completed=$(cursor_completed "$cursor_file")
    log "EXIT $label — code=$exit_code, completed: $start_completed → $end_completed"
    return "$exit_code"
}

# Retry a preset until it completes all tickers or no progress after MAX_STALE attempts
MAX_STALE=3  # move on after this many attempts with no new completions

run_until_done() {
    local label="$1"
    local cursor_file="$2"
    local total="$3"
    shift 3
    local cmd=("$@")

    local stale_count=0

    while true; do
        local completed
        completed=$(cursor_completed "$cursor_file")

        if [ "$completed" -ge "$total" ]; then
            log "COMPLETE $label — $completed/$total tickers done"
            return 0
        fi

        log "ATTEMPT $label — $completed/$total done, $(($total - $completed)) remaining (stale=$stale_count/$MAX_STALE)"

        local before_completed="$completed"

        if run_preset "$label" "$cursor_file" "${cmd[@]}"; then
            completed=$(cursor_completed "$cursor_file")
            if [ "$completed" -ge "$total" ]; then
                log "COMPLETE $label — $completed/$total tickers done"
                return 0
            fi

            if [ "$completed" -gt "$before_completed" ]; then
                log "PROGRESS $label — $before_completed → $completed. Cooling down ${COOLDOWN}s..."
                stale_count=0
            else
                stale_count=$((stale_count + 1))
                log "NO PROGRESS $label — still $completed/$total (stale $stale_count/$MAX_STALE). Cooling down ${COOLDOWN}s..."
            fi

            if [ "$stale_count" -ge "$MAX_STALE" ]; then
                log "GIVING UP $label — $completed/$total done, $((total - completed)) tickers unfetchable. Moving on."
                return 0
            fi
            sleep "$COOLDOWN"
        else
            stale_count=$((stale_count + 1))
            if [ "$stale_count" -ge "$MAX_STALE" ]; then
                log "GIVING UP $label — $completed/$total after $stale_count stalls. Moving on."
                return 0
            fi
            log "RESTART $label — stale $stale_count/$MAX_STALE. Cooling down ${COOLDOWN}s..."
            sleep "$COOLDOWN"
        fi
    done
}

# ── Main ─────────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"

log "============================================================"
log "BACKFILL RUNNER START"
log "Stall timeout: ${STALL_TIMEOUT}s, Cooldown: ${COOLDOWN}s"
log "Batch: $BATCH_SIZE, Concurrent: $MAX_CONCURRENT"
log "============================================================"

PRESETS=("presets/sp500.json" "presets/ndx100.json" "presets/r2k.json")

# Phase 1: Finish normal fetches
for preset in "${PRESETS[@]}"; do
    name=$(python3 -c "import json; print(json.load(open('$preset'))['name'])")
    total=$(python3 -c "import json; print(len(json.load(open('$preset'))['tickers']))")
    cursor_file="$LOG_DIR/cursor_${name}.json"

    log "── PHASE 1: Normal fetch $name ($total tickers) ──"
    run_until_done "normal_${name}" "$cursor_file" "$total" \
        python "$SCRIPT" historical --preset "$preset" --years 0 --skip-existing \
        --batch-size "$BATCH_SIZE" --max-concurrent "$MAX_CONCURRENT"
done

log "============================================================"
log "PHASE 1 COMPLETE"
log "============================================================"

# Phase 2: Backfill older data
for preset in "${PRESETS[@]}"; do
    name=$(python3 -c "import json; print(json.load(open('$preset'))['name'])")
    total=$(python3 -c "import json; print(len(json.load(open('$preset'))['tickers']))")
    cursor_file="$LOG_DIR/cursor_backfill_${name}.json"

    log "── PHASE 2: Backfill $name ($total tickers) ──"
    run_until_done "backfill_${name}" "$cursor_file" "$total" \
        python "$SCRIPT" historical --preset "$preset" --backfill \
        --batch-size "$BATCH_SIZE" --max-concurrent "$MAX_CONCURRENT"
done

log "============================================================"
log "PHASE 2 COMPLETE"
log "============================================================"

log "── PHASE 3: FRED Treasury rates ──"
log "CMD: python $SCRIPT fred-rates"
python "$SCRIPT" fred-rates >> "$LOG_DIR/backfill_fred_rates.log" 2>&1
log "PHASE 3 COMPLETE"

log "============================================================"
log "ALL DONE"
log "============================================================"
