# Livewire Intraday Catch-up Scheduler Design

## Goal

The equity intraday warehouse (1m, 5m, 30m, 1h) needs a scheduled refresh. The existing `com.livewire.daily-update` launchd job calls `scripts/livewire_ingest.py daily`, which only catches up `1d` bars. A second launchd job runs the existing `daily-backfill` orchestrator once per day so equity intraday, FRED rates, and CBOE volatility stay current alongside the daily 1d sync.

## Scope

- Add a new launchd plist template `launchd/com.livewire.intraday-catchup.plist.example` that fires once daily at 16:00 PT, mirroring the install pattern of `launchd/com.livewire.daily-update.plist.example`.
- Add a `run-intraday-catchup-job` subcommand to `scripts/livewire_ops.py` that loads the same env files as `run-daily-job` (`~/.secrets`, repo `.env`, `~/market-warehouse/.env`) and invokes `scripts/livewire_ingest.py daily-backfill` as a subprocess.
- Implement the wrapper in `livewire_scripts/run_intraday_catchup_job.py`. The wrapper is single-attempt: `daily-backfill` already has retry-until-done and activity-based stall detection internally, so the wrapper does not retry.
- Extract the env-loading helper currently inlined as `_load_run_daily_env()` in `scripts/livewire_ops.py` into a shared module (`livewire_scripts/scheduled_env.py`) and reuse it from both `run-daily-job` and `run-intraday-catchup-job` so the two scheduled wrappers cannot drift on env precedence.
- On non-zero exit, the wrapper invokes `scripts/livewire_ops.py send-alert` once with `--job-name intraday_catchup`, mirroring the failure-alert behavior of `run-daily-job`.
- Log output writes to `~/market-warehouse/logs/intraday_catchup_YYYY-MM-DD.log` in append mode. The date is computed from the same `_utc_now()` semantics used by `run_daily_update_job.build_log_file()` so log naming stays consistent across both scheduled jobs. At a 16:00 PT trigger that means the UTC date is the next calendar day; this is acceptable and matches how the daily-update wrapper would already name a hypothetical late-evening rerun.
- The log directory is overridable via `MDW_INTRADAY_CATCHUP_LOG_DIR` (defaults to the same `~/market-warehouse/logs/` used by daily-update). The Python interpreter and script paths are overridable via `MDW_INTRADAY_CATCHUP_PYTHON_BIN` and `MDW_INTRADAY_CATCHUP_SCRIPT` for parity with `MDW_DAILY_UPDATE_PYTHON_BIN` / `MDW_DAILY_UPDATE_SCRIPT`.
- Update `README.md` and `CLAUDE.md` to document the second launchd job in the scheduling section.

## Out of Scope

- No watchdog plist. The default 7-day `MDW_DAILY_BACKFILL_INTRADAY_DAYS` lookback inside `daily-backfill` absorbs a single missed run; multiple consecutive misses are caught by routine coverage inspection rather than an automated watchdog.
- No external retry loop in the wrapper. `daily-backfill` owns retry-until-done.
- The scheduler delegates equity intraday to `daily-backfill`, which now invokes one `flatfile-ingest catch-up` command. `intraday-backfill` is not an equity path.
- Massive S3 credentials are mandatory for `daily-backfill`; there is no equity-intraday REST or IB fallback.
- No changes to the existing `com.livewire.daily-update` 13:05 PT job. It continues running `daily` for 1d catch-up.

## Data Flow

```
launchd 16:00 PT (com.livewire.intraday-catchup)
   │
   ▼
scripts/livewire_ops.py run-intraday-catchup-job
   │  load env via livewire_scripts.scheduled_env.load_scheduled_env()
   │   (~/.secrets → repo .env → ~/market-warehouse/.env, last-set-wins)
   │
   ▼
livewire_scripts/run_intraday_catchup_job.main()
   │  open ~/market-warehouse/logs/intraday_catchup_YYYY-MM-DD.log (append)
   │  subprocess.run(
   │    [python, scripts/livewire_ingest.py, daily-backfill],
   │    stdout=log, stderr=STDOUT
   │  )
   │       │
   │       ▼
   │  daily-backfill (livewire_scripts/sync_runner.py) handles:
   │     • equity 1d catch-up (Massive, --skip-existing)
   │     • full-market equity intraday via flatfile-ingest catch-up
   │         lookback = MDW_DAILY_BACKFILL_INTRADAY_DAYS (default 7)
   │     • FRED Treasury rates
   │     • CBOE daily volatility sync
   │     • IB VIX/SPX intraday (best effort; logs and continues if Gateway down)
   │     • Postgres rebuild (only if MDW_POSTGRES_DSN is set)
   │
   ▼
exit code captured by wrapper
   │
   ├── 0 → wrapper exits 0
   └── non-zero → wrapper invokes scripts/livewire_ops.py send-alert
                  --job-name intraday_catchup
                  --run-date YYYY-MM-DD
                  --log-file <log path>
                  --error-summary "intraday-catchup failed"
                  --exit-code <code>
                  --attempts 1
```

## Operator Behavior

- Install: copy the example plist with the repo path substitution pattern already used for daily-update:

  ```bash
  sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.intraday-catchup.plist.example \
    > ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
  launchctl load ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
  ```

- Run manually for testing: `python scripts/livewire_ops.py run-intraday-catchup-job`.
- Tune lookback via env: `export MDW_DAILY_BACKFILL_INTRADAY_DAYS=14` in `~/market-warehouse/.env` to widen the catch-up window.
- Required env for equity intraday: `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`. Optional: `MASSIVE_API_KEY` for equity daily and `MDW_POSTGRES_DSN` for analytical rebuild.
- Failure alerting requires `CEREBRAS_API_KEY` (or `CEREBRAS_API_KEY_FREE`) for the human-readable AI summary; absent it, the alert email is still sent with the raw error summary, matching the existing daily-update behavior.

## Overlap and Timing

- `com.livewire.daily-update` starts at 13:05 PT and normally completes within 30 minutes to ~2 hours, with longer tails when Nasdaq/Stooq fallback recovery is exercised.
- `com.livewire.intraday-catchup` starts at 16:00 PT, leaving ~3 hours of margin after daily-update's typical completion.
- If daily-update is still running at 16:00 PT, both jobs run in parallel. The overlap is safe because:
  - `daily-backfill`'s 1d Massive catch-up runs with `--skip-existing` and is a near-noop once daily-update has finished the 1d pass.
  - Intraday writes target different parquet files (`{1m,5m,30m,1h}.parquet`) than the daily writer (`1d.parquet`), so atomic `os.replace()` publication does not contend.
- No file lock contention is expected. If the same ticker's same-timeframe parquet were targeted by both jobs at once (it is not in the current design), the atomic `temp -> validate -> os.replace()` publish path in `BronzeClient` would make the last writer win cleanly rather than corrupt.

## Verification

Tests must prove:

- `livewire_ops.py run-intraday-catchup-job` dispatches to `livewire_scripts.run_intraday_catchup_job.main` and forwards no extra argv.
- Env loading priority is `~/.secrets` → repo `.env` → `~/market-warehouse/.env` (last-set wins), exercised from a shared `livewire_scripts/scheduled_env.py` and re-used by both `run_daily_update_job` and `run_intraday_catchup_job` without behavior drift versus the current inlined `_load_run_daily_env()`.
- The wrapper constructs the command `[<python>, <repo>/scripts/livewire_ingest.py, daily-backfill]` and streams stdout+stderr into `~/market-warehouse/logs/intraday_catchup_<date>.log` (append mode).
- On zero exit, no alert is dispatched.
- On non-zero exit, exactly one invocation of `scripts/livewire_ops.py send-alert` is made with the documented argv (`--job-name intraday_catchup`, `--attempts 1`, the resolved log path, exit code, and run date).
- The launchd plist template renders into a syntactically valid plist after `sed` path substitution and runs at 16:00 local time (`StartCalendarInterval` with `Hour=16 Minute=0`).
- Coverage stays at 100% for the new module and any refactored entry points (`pyproject.toml`'s `fail_under = 100`).
- The async-leak guard `pytest -W error::RuntimeWarning` passes for the new module's tests because the wrapper does not own an async runner.

## Rollback

Disable the new job without touching any code:

```bash
launchctl unload ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
rm ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
```

The codebase still ships the example plist, the wrapper module, and the dispatch entry — those are inert when no plist references them. Reverting the commit that introduces the wrapper is also safe because the existing `com.livewire.daily-update` job does not depend on the new helpers.
