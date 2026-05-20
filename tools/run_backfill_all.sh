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
STALL_TIMEOUT="${MDW_BACKFILL_STALL_TIMEOUT:-600}"  # seconds of no activity before killing
STALL_COOLDOWN="${MDW_BACKFILL_STALL_COOLDOWN:-${MDW_BACKFILL_COOLDOWN:-300}}"
SUCCESS_COOLDOWN="${MDW_BACKFILL_SUCCESS_COOLDOWN:-0}"
NO_PROGRESS_COOLDOWN="${MDW_BACKFILL_NO_PROGRESS_COOLDOWN:-30}"
POLL_INTERVAL="${MDW_BACKFILL_POLL_INTERVAL:-30}"
MAX_CONCURRENT=10
BATCH_SIZE=5
RUNNING_PIDS=()

source "$VENV"
if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

timestamp() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] $*"; }

track_pid() {
    RUNNING_PIDS+=("$1")
}

untrack_pid() {
    local target="$1"
    local kept=()
    local pid
    for pid in "${RUNNING_PIDS[@]:-}"; do
        if [ "$pid" != "$target" ]; then
            kept+=("$pid")
        fi
    done
    RUNNING_PIDS=("${kept[@]}")
}

kill_tree() {
    local root="$1"
    local signal="${2:-TERM}"
    local children
    children=$(pgrep -P "$root" 2>/dev/null || true)
    local child
    for child in $children; do
        kill_tree "$child" "$signal"
    done
    kill "-$signal" "$root" 2>/dev/null || true
}

cleanup_children() {
    local status=$?
    trap - INT TERM EXIT
    local pid
    for pid in "${RUNNING_PIDS[@]:-}"; do
        kill_tree "$pid" TERM
    done
    exit "$status"
}

trap cleanup_children INT TERM EXIT

# Get mtime of a file in epoch seconds (0 if missing)
file_mtime() {
    if [ -f "$1" ]; then
        stat -f %m "$1" 2>/dev/null || echo 0
    else
        echo 0
    fi
}

max_mtime() {
    local first
    local second
    first=$(file_mtime "$1")
    second=$(file_mtime "$2")
    if [ "$first" -gt "$second" ]; then
        echo "$first"
    else
        echo "$second"
    fi
}

sleep_if_needed() {
    local seconds="$1"
    if [ "$seconds" -gt 0 ]; then
        sleep "$seconds"
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
    local last_completed="$start_completed"
    local log_file="$LOG_DIR/${label}.log"
    log "START $label — cursor completed: $start_completed"
    log "CMD: ${cmd[*]}"

    # Launch fetch
    "${cmd[@]}" >> "$log_file" 2>&1 &
    local pid=$!
    track_pid "$pid"
    log "PID: $pid"

    local last_mtime
    last_mtime=$(max_mtime "$cursor_file" "$log_file")
    local last_check
    last_check=$(date +%s)

    # Monitor cursor and child log activity. Massive intraday can spend a long
    # time on a ticker before marking cursor completion; log writes still prove
    # the process is alive.
    while kill -0 "$pid" 2>/dev/null; do
        sleep "$POLL_INTERVAL"

        local current_mtime
        current_mtime=$(max_mtime "$cursor_file" "$log_file")

        if [ "$current_mtime" != "$last_mtime" ]; then
            local completed
            completed=$(cursor_completed "$cursor_file")
            if [ "$completed" != "$last_completed" ]; then
                log "PROGRESS $label — completed: $completed"
                last_completed="$completed"
            else
                log "ACTIVITY $label — child log updated; cursor completed: $completed"
            fi
            last_mtime="$current_mtime"
            last_check=$(date +%s)
        else
            local now
            now=$(date +%s)
            local stall=$((now - last_check))

            if [ "$stall" -ge "$STALL_TIMEOUT" ]; then
                log "STALL $label — no cursor/log activity for ${stall}s, killing pid $pid"
                kill_tree "$pid" TERM
                sleep 3
                kill_tree "$pid" KILL
                set +e
                wait "$pid" 2>/dev/null
                set -e
                untrack_pid "$pid"
                return 1
            fi
        fi
    done

    set +e
    wait "$pid" 2>/dev/null
    local exit_code=$?
    set -e
    untrack_pid "$pid"
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
                log "PROGRESS $label — $before_completed → $completed. Cooling down ${SUCCESS_COOLDOWN}s..."
                stale_count=0
                sleep_if_needed "$SUCCESS_COOLDOWN"
            else
                stale_count=$((stale_count + 1))
                log "NO PROGRESS $label — still $completed/$total (stale $stale_count/$MAX_STALE). Cooling down ${NO_PROGRESS_COOLDOWN}s..."
                sleep_if_needed "$NO_PROGRESS_COOLDOWN"
            fi

            if [ "$stale_count" -ge "$MAX_STALE" ]; then
                log "GIVING UP $label — $completed/$total done, $((total - completed)) tickers unfetchable. Moving on."
                return 0
            fi
        else
            stale_count=$((stale_count + 1))
            if [ "$stale_count" -ge "$MAX_STALE" ]; then
                log "GIVING UP $label — $completed/$total after $stale_count stalls. Moving on."
                return 0
            fi
            log "RESTART $label — stale $stale_count/$MAX_STALE. Cooling down ${STALL_COOLDOWN}s..."
            sleep_if_needed "$STALL_COOLDOWN"
        fi
    done
}

# ── Main ─────────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"

log "============================================================"
log "BACKFILL RUNNER START"
log "Stall timeout: ${STALL_TIMEOUT}s, Stall cooldown: ${STALL_COOLDOWN}s"
log "Success cooldown: ${SUCCESS_COOLDOWN}s, No-progress cooldown: ${NO_PROGRESS_COOLDOWN}s"
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
        python "$SCRIPT" historical --preset "$preset" --backfill --source auto \
        --batch-size "$BATCH_SIZE" --max-concurrent "$MAX_CONCURRENT"
done

log "============================================================"
log "PHASE 2 COMPLETE"
log "============================================================"

log "── PHASE 3: FRED Treasury rates ──"
log "CMD: python $SCRIPT fred-rates"
python "$SCRIPT" fred-rates >> "$LOG_DIR/backfill_fred_rates.log" 2>&1
log "PHASE 3 COMPLETE"

# Phases 4-6: Build default equity intraday bronze from Massive.
run_equity_intraday() {
    for timeframe in 1m 5m 1h; do
        case "$timeframe" in
            1m) phase=4 ;;
            5m) phase=5 ;;
            1h) phase=6 ;;
        esac
        for preset in "${PRESETS[@]}"; do
            name=$(python3 -c "import json; print(json.load(open('$preset'))['name'])")
            total=$(python3 -c "import json; print(len(json.load(open('$preset'))['tickers']))")
            cursor_file="$HOME/market-warehouse/cursors/cursor_intraday_${timeframe}_${name}.json"

            log "── PHASE ${phase}: Massive ${timeframe} intraday $name ($total tickers, 5y) ──"
            run_until_done "intraday_${timeframe}_${name}" "$cursor_file" "$total" \
                python "$SCRIPT" intraday-backfill --preset "$preset" --timeframe "$timeframe" \
                --source massive --asset-class equity --years 5 --skip-existing
        done
        log "============================================================"
        log "PHASE ${phase} COMPLETE"
        log "============================================================"
    done
}

# Phases 7-8: Build volatility daily via CBOE, then VIX/SPX volatility/index intraday via IB.
run_volatility_intraday() {
    log "── PHASE 7: CBOE volatility daily ──"
    python "$SCRIPT" cboe-vol --preset presets/volatility.json >> "$LOG_DIR/volatility_cboe.log" 2>&1

    log "============================================================"
    log "PHASE 7 COMPLETE"
    log "============================================================"

    VOL_PRESET="presets/volatility-intraday.json"
    VOL_NAME=$(python3 -c "import json; print(json.load(open('$VOL_PRESET'))['name'])")
    VOL_TOTAL=$(python3 -c "import json; print(len(json.load(open('$VOL_PRESET'))['tickers']))")
    for timeframe in 5m 1h; do
        cursor_file="$HOME/market-warehouse/cursors/cursor_intraday_${timeframe}_${VOL_NAME}.json"
        log "── PHASE 8: IB volatility intraday ${timeframe} ($VOL_TOTAL tickers) ──"
        run_until_done "intraday_${timeframe}_${VOL_NAME}" "$cursor_file" "$VOL_TOTAL" \
            python "$SCRIPT" intraday-backfill --preset "$VOL_PRESET" --timeframe "$timeframe" \
            --source ib --asset-class volatility --skip-existing
    done

    log "============================================================"
    log "PHASE 8 COMPLETE"
    log "============================================================"
}

log "Starting Massive equity intraday and IB/CBOE volatility lanes in parallel"
run_equity_intraday &
equity_intraday_pid=$!
track_pid "$equity_intraday_pid"
run_volatility_intraday &
volatility_intraday_pid=$!
track_pid "$volatility_intraday_pid"

set +e
wait "$equity_intraday_pid"
equity_intraday_status=$?
untrack_pid "$equity_intraday_pid"
wait "$volatility_intraday_pid"
volatility_intraday_status=$?
untrack_pid "$volatility_intraday_pid"
set -e

if [ "$equity_intraday_status" -ne 0 ] || [ "$volatility_intraday_status" -ne 0 ]; then
    log "Parallel intraday phase failed: equity=$equity_intraday_status volatility=$volatility_intraday_status"
    exit 1
fi

# Phase 9: Rebuild Postgres analytical publish target when configured
if [ -n "${MDW_POSTGRES_DSN:-}" ]; then
    log "── PHASE 9: Postgres analytical rebuild ──"
    python scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe all --include-reliability
    python scripts/livewire_store.py rebuild-postgres --asset-class volatility --timeframe 1d
else
    log "── PHASE 9: Postgres analytical rebuild skipped — MDW_POSTGRES_DSN is not set ──"
fi

log "============================================================"
log "PHASE 9 COMPLETE"
log "============================================================"

log "============================================================"
log "ALL DONE"
log "============================================================"
