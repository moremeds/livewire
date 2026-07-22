# Multi-Timeframe Phase 2 Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans. Steps use `- [ ]` checkboxes.

**Goal:** Ship the four items deferred from the Phase 1 plan (`docs/superpowers/plans/2026-04-06-multi-timeframe-plan.md`):

1. `health_check.py --intraday` report-only mode (spec § 9)
2. Daily coverage tracking + weekly summary (spec § 17 Layer 2)
3. Auto-recovery on coverage drop below threshold (spec § 17 Layer 2)
4. Full historical intraday backfill orchestration (spec § 11)

**Spec:** `docs/superpowers/specs/2026-04-06-multi-timeframe-design.md` §§ 9, 11, 17

**Tech stack:** Python 3.13, DuckDB, ib_async, pytest (100% coverage on `clients/`+`scripts/` except `clients/ib_client.py`).

**Phase 1 status:** Shipped through commit `d33abfd` (intraday_update chained in entrypoint). `IntradayBronzeClient`, per-tf cursor, `validate_intraday_bar`, NYSE early-close helpers, core ETFs, R2 sync, intraday DuckDB rebuild are all in place.

---

## File Structure

### New files
| File | Responsibility |
|------|---------------|
| `scripts/coverage_report.py` | Daily coverage one-liner + auto-recovery orchestration (spec § 17 Layer 2) |
| `scripts/weekly_quality_summary.py` | Sunday-only aggregation of 7 daily coverage logs into a markdown report |
| `scripts/backfill_intraday.py` | Full historical intraday backfill orchestrator using per-timeframe chunking |
| `tests/test_coverage_report.py` | Coverage counting, threshold logic, auto-recovery happy/partial/safety-cap paths |
| `tests/test_weekly_quality_summary.py` | Markdown rendering, churn detection, persistent-gap detection |
| `tests/test_backfill_intraday.py` | Cursor resume, chunking dispatch, dry-run, --skip-existing |

### Modified files
| File | Change |
|------|--------|
| `scripts/health_check.py` | Add `--intraday`, `--timeframe`, `--repair`, `--symbol`, `--since` flags; report-only by default for intraday; reuse `IntradayBronzeClient` for gap detection |
| `tests/test_health_check.py` | Tests for new intraday modes (expected-bar generation, RTH window, halt annotation, --repair gating) |
| `docker/ibroker-mkt-data/entrypoint.py` | After `intraday_update`, run `coverage_report` (every day) and `weekly_quality_summary` (Sunday only) |
| `tests/test_entrypoint.py` (if exists, else add to its peer) | Verify the new chained calls |
| `CLAUDE.md` | Document new scripts under "Data Ingestion" + "Daily updates" sections |

---

## Task 1: Intraday Health Check Mode (spec § 9)

**Files:**
- Modify: `scripts/health_check.py`
- Modify: `tests/test_health_check.py`

### Steps

- [ ] Add CLI flags: `--intraday`, `--timeframe {1h,5m}`, `--symbol SYM`, `--since YYYY-MM-DD`. **Repair is implicit**: when `--symbol` AND `--since` AND `--timeframe` are all set, the script repairs that narrow window. Otherwise it is report-only (spec § 9 — targeted, narrow, explicit).
- [ ] Add `generate_expected_intraday_timestamps(symbol_dates: list[date], timeframe: str) -> set[datetime]`. For each trading day:
  - Get `session_close_time(d)` from `daily_update` helpers
  - 1h: emit 9:30, 10:30, …, (close - 1h) ET
  - 5m: emit 9:30, 9:35, …, (close - 5m) ET
  - Convert each ET timestamp to UTC via `ZoneInfo("America/New_York")` and return as a set of tz-aware UTC datetimes.
- [ ] Add `find_intraday_gaps(symbol: str, timeframe: str, intraday_bronze: IntradayBronzeClient) -> tuple[list[datetime], list[tuple[datetime, datetime]]]`:
  - Read all `bar_timestamp` values for the symbol via a DuckDB query against `IntradayBronzeClient` parquet glob
  - Compute expected set across `[min_date, max_date]` of stored bars (interior only)
  - Return `(missing_timestamps, suspected_halts)` where halts = contiguous gap < 30 min surrounded by normal bars
- [ ] Add `report_intraday_health(timeframe: str, symbol_filter: str | None = None) -> dict`:
  - Iterate symbols; collect counts (`expected`, `actual`, `missing`, `halts_suspected`)
  - Print Rich table; return summary dict for tests
- [ ] In `main()`, branch: if `--intraday`, call the new path; else keep daily path unchanged.
- [ ] When `--symbol` + `--since` + `--timeframe` all present: call `fetch_ib_historical.py --tickers SYM --timeframe TF --start SINCE` via subprocess (no in-process IB connection — keeps health check side-effect-free unless explicitly scoped).

### Tests (`tests/test_health_check.py`)

- [ ] `test_generate_expected_intraday_timestamps_5m_full_day` — full RTH yields 78 bars (9:30→15:55)
- [ ] `test_generate_expected_intraday_timestamps_1h_full_day` — 6 bars (9:30, 10:30, …, 14:30)
- [ ] `test_generate_expected_intraday_timestamps_early_close` — half-day yields (close-9:30)/bar bars
- [ ] `test_generate_expected_intraday_timestamps_skips_holidays` — Christmas not in output
- [ ] `test_find_intraday_gaps_clean_series` — empty missing list
- [ ] `test_find_intraday_gaps_interior_missing` — synthetic 5m bronze with one missing 5m bar
- [ ] `test_find_intraday_gaps_suspected_halt_annotation` — gap < 30 min flagged
- [ ] `test_intraday_partial_repair_args_report_only` — `--symbol` without `--since` stays report-only (no subprocess)
- [ ] `test_intraday_full_scope_invokes_fetch_subprocess` — `--symbol` + `--since` + `--timeframe` → patch `subprocess.run`, verify args
- [ ] `test_intraday_default_no_subprocess` — bare `--intraday --timeframe 5m` reports only

---

## Task 2: Daily Coverage Report + Auto-Recovery (spec § 17 Layer 2)

**Files:**
- Create: `scripts/coverage_report.py`
- Create: `tests/test_coverage_report.py`

### Steps

- [ ] CLI: `python scripts/coverage_report.py [--target-date YYYY-MM-DD] [--no-recover] [--threshold 0.95]`. Default target = latest trading day.
- [ ] `compute_coverage(target_date: date) -> dict[str, CoverageResult]` returning `{timeframe: {total, present, missing_symbols}}` for each of `1d`, `1h`, `5m`. Implementation: one DuckDB query per timeframe over `~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=*/{1d|1h|5m}.parquet` selecting `symbol, max(trade_date or bar_timestamp::DATE)`. "Missing" = `latest < target_date`.
- [ ] `format_one_liner(target_date, results) -> str` matching spec § 17 format.
- [ ] `write_coverage_log(line, missing_blocks)` appending to `~/market-warehouse/logs/coverage_YYYY-MM-DD.log` (one file per day, append-safe for re-runs).
- [ ] `auto_recover(timeframe, missing_symbols, threshold, safety_cap=100) -> RecoveryOutcome`:
  - If `len(missing) > safety_cap`: return outcome with `aborted=True`, `reason="safety_cap"`. Email immediately.
  - Else subprocess call: `fetch_ib_historical.py --tickers <missing> --timeframe <tf>`
  - Re-run `compute_coverage` for that timeframe only
  - Return outcome `{recovered: int, still_missing: list[str], aborted: bool}`
- [ ] `decide_alert(outcomes) -> AlertLevel` — `INFO` if all recovered, `EMAIL` otherwise.
- [ ] `_send_email(...)` reuses `scripts/send_daily_update_failure_email.mjs` via subprocess (same pattern as daily_update). Body lists per-tf before/after; if `CEREBRAS_API_KEY` set, the existing path attaches the Cerebras summary.
- [ ] `MDW_COVERAGE_ALERT_THRESHOLD` env var overrides default `0.95`.
- [ ] `main()` orchestrates: compute → log → for each tf below threshold, recover → log → alert.

### Tests (`tests/test_coverage_report.py`)

All IB and subprocess calls mocked. Use `tmp_bronze` fixture (extend conftest if needed) to seed multi-tf parquet files.

- [ ] `test_compute_coverage_all_present` — 3 symbols × 3 tf → 100/100/100
- [ ] `test_compute_coverage_partial_missing` — 1 symbol stale at 5m → reports it
- [ ] `test_format_one_liner_matches_spec` — string equality against spec example
- [ ] `test_write_coverage_log_appends` — second call appends, doesn't truncate
- [ ] `test_threshold_above_no_recovery` — coverage 99% > 95% → no subprocess
- [ ] `test_threshold_below_triggers_recovery` — mock subprocess success, verify INFO not email
- [ ] `test_auto_recovery_partial_success` — 5 missing → 3 recovered → email lists 2 still missing
- [ ] `test_auto_recovery_safety_cap` — 150 missing → no subprocess, immediate email with `aborted=True`
- [ ] `test_env_var_threshold_override` — `MDW_COVERAGE_ALERT_THRESHOLD=0.99` → 99.5% triggers
- [ ] `test_no_recover_flag_skips_subprocess` — `--no-recover` honored

---

## Task 3: Weekly Quality Summary (spec § 17 Layer 2)

**Files:**
- Create: `scripts/weekly_quality_summary.py`
- Create: `tests/test_weekly_quality_summary.py`

### Steps

- [ ] CLI: `python scripts/weekly_quality_summary.py [--week YYYY-WW]`. Default = current ISO week.
- [ ] `parse_coverage_log(path: Path) -> CoverageEntry` — regex over the daily one-liner format produced in Task 2.
- [ ] `load_week(week: tuple[int, int]) -> list[CoverageEntry]` — reads 7 daily logs (skips missing).
- [ ] `detect_churn(entries) -> tuple[set[str], set[str]]` — diff of present-symbol sets between first and last day → (added, removed). Removed restricted to symbols absent for ≥3 consecutive days at any timeframe.
- [ ] `detect_persistent_gaps(entries) -> dict[str, dict[str, int]]` — symbol → timeframe → consecutive-missing-days; emit if `≥3`.
- [ ] `render_markdown(entries, churn, gaps) -> str` — matches spec § 17 example.
- [ ] `write_summary(md, week)` → `~/market-warehouse/logs/quality_weekly_YYYY-WW.md`.
- [ ] `main()` only runs if today is Sunday OR `--force` is passed (so the entrypoint can call it daily without side effects).

### Tests

- [ ] `test_parse_coverage_log_one_line` — parses spec example
- [ ] `test_load_week_skips_missing_days` — 5/7 files present → 5 entries
- [ ] `test_detect_churn_added_and_removed` — synthetic 7-day window
- [ ] `test_detect_persistent_gaps_threshold` — 2-day gap excluded, 3-day included
- [ ] `test_render_markdown_matches_spec` — golden compare against spec § 17 sample
- [ ] `test_main_skips_non_sunday_without_force` — patch `date.today()` Wednesday → no file write
- [ ] `test_main_force_runs_any_day` — `--force` writes file on Wednesday

---

## Task 4: Full Historical Intraday Backfill Orchestrator (spec § 11)

**Files:**
- Create: `scripts/backfill_intraday.py`
- Create: `tests/test_backfill_intraday.py`

### Steps

- [ ] CLI:
  ```
  python scripts/backfill_intraday.py --timeframe {1h,5m} [--preset PATH] [--tickers ...]
       [--years N] [--skip-existing] [--dry-run] [--max-tickers N]
  ```
- [ ] Defaults: `--timeframe` required; `--preset presets/screened-universe.json` if neither preset nor tickers; `--years` from spec § 3 (`1h=2`, `5m=1`).
- [ ] Reuses `compute_intraday_chunks` from `fetch_ib_historical.py` (already extracted in Phase 1) for per-symbol IB request windowing.
- [ ] Per-timeframe cursor: `cursor_intraday_{timeframe}_{preset}.json` storing last completed ticker. Resume on restart.
- [ ] Pacing: reuse the existing semaphore + `IB_HISTORICAL_PACE_SECONDS` from `fetch_ib_historical.py`. Cap concurrency ≤ 4 for intraday (IB pacing is stricter than daily).
- [ ] On error 162 ("HMDS query returned no data"): log + skip ticker, do not retry.
- [ ] On error 200 (ambiguous contract): same.
- [ ] Each fetched chunk validated through `validate_intraday_bar`, merged via `IntradayBronzeClient.merge_ticker_rows`.
- [ ] `--dry-run` prints planned chunks without IB calls.
- [ ] `--skip-existing` consults the cursor and the bronze parquet (skip if `min_bar_timestamp <= today - years`).
- [ ] Logs: `~/market-warehouse/logs/backfill_intraday_{timeframe}_{date}.log`.

### Tests (all IB mocked)

- [ ] `test_cli_requires_timeframe`
- [ ] `test_dry_run_no_ib_calls`
- [ ] `test_cursor_resume_skips_completed_tickers`
- [ ] `test_chunking_dispatched_per_symbol` — 5m × 1 year × 1 symbol → expected number of `compute_intraday_chunks` calls
- [ ] `test_skip_existing_when_bronze_full_history` — mock parquet with `min_bar_timestamp` covering range → ticker skipped
- [ ] `test_error_162_skips_ticker` — mocked IB raises 162 → counted in skipped, run continues
- [ ] `test_validate_intraday_bar_rejection_counted` — invalid bar dropped, counter incremented
- [ ] `test_max_tickers_caps_run`

---

## Task 5: Wire Coverage + Weekly Summary into Entrypoint

**Files:**
- Modify: `docker/ibroker-mkt-data/entrypoint.py`
- Modify or add tests under `docker/ibroker-mkt-data/tests/` (mirror existing pattern there)

### Steps

- [ ] After `intraday_update` returns successfully, call `python scripts/coverage_report.py` (subprocess, same pattern as existing chain). Failure here is a soft warning, not fatal — coverage report itself handles its own alerting.
- [ ] After `coverage_report`, call `python scripts/weekly_quality_summary.py` (it self-skips on non-Sunday).
- [ ] Test: patch subprocess to record call ordering; assert daily → intraday → coverage → weekly.

---

## Task 6: Documentation

- [ ] Update `CLAUDE.md` "Daily updates" subsection to mention `coverage_report.py` and `weekly_quality_summary.py` chained from the entrypoint.
- [ ] Add a "Backfill (intraday)" subsection under "Data Ingestion" with `backfill_intraday.py` examples and the IPO/halt caveats from spec § 11.
- [ ] Add `MDW_COVERAGE_ALERT_THRESHOLD` to env var list.
- [ ] Update `AGENTS.md` if it lists scripts.

---

## Task 7: Full E2E Verification

> Required by the user request: pick **one ETF candidate** and **one stock candidate** per new feature and walk them through end-to-end.

**Candidates:**
- ETF: `SPY` (deep history, always available, in core-etfs.json) — used for read-only feature checks
- Stock: `AAPL` (high liquidity) — used for read-only feature checks
- **Recovery rehearsal ticker:** `COST` if not currently in bronze; otherwise pick the first liquid S&P symbol absent from `~/market-warehouse/data-lake/bronze/asset_class=equity/`. The recovery test bootstraps it from nothing so we never touch existing parquet.

### Steps (run in order, against live IB Gateway on `127.0.0.1:4001` or whatever `MDW_IB_HOST` points to)

- [ ] **Pre-flight:** `python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing` — must show `100%` and zero failures.
- [ ] **Phase 1 sanity:** `python scripts/daily_update.py --dry-run` and `python scripts/intraday_update.py --dry-run` to confirm baseline still healthy.

#### Feature 1 — `health_check.py --intraday`
- [ ] `python scripts/health_check.py --intraday --timeframe 5m --symbol SPY` — report only, expect either clean or annotated halts. Capture output.
- [ ] `python scripts/health_check.py --intraday --timeframe 1h --symbol AAPL`
- [ ] Targeted repair smoke test: `python scripts/health_check.py --intraday --timeframe 5m --symbol AAPL --since 2026-04-01` — fully scoped flags imply repair; confirm subprocess fetch fires and bronze gains rows. Re-run without `--symbol` to confirm gaps closed.
- [ ] Default-mode check: `python scripts/health_check.py --intraday --timeframe 5m` — report-only, no subprocess.

#### Feature 2 — `coverage_report.py` + auto-recovery
- [ ] `python scripts/coverage_report.py --no-recover` — verify the one-line log appears in `~/market-warehouse/logs/coverage_<today>.log` and that SPY + AAPL are present in all 3 timeframes.
- [ ] Pick a fresh ticker not present in `bronze/asset_class=equity/` (default candidate `COST`; pick another liquid S&P name if `COST` already exists). Add it to a temporary one-off preset, then run `python scripts/coverage_report.py --preset /tmp/recovery_preset.json --threshold 0.99`. Expect: ticker flagged at all 3 timeframes, subprocess backfill bootstraps it from scratch, re-check passes, INFO log only (no email).
- [ ] Safety-cap path is unit-tested only — do not rehearse with 100+ real symbols.

#### Feature 3 — `weekly_quality_summary.py`
- [ ] `python scripts/weekly_quality_summary.py --force` — confirms a markdown report appears under `~/market-warehouse/logs/quality_weekly_*.md` containing both SPY and AAPL coverage lines.

#### Feature 4 — `backfill_intraday.py`
- [ ] `python scripts/backfill_intraday.py --timeframe 1h --tickers SPY --years 2 --dry-run` — verify chunk plan printed.
- [ ] `python scripts/backfill_intraday.py --timeframe 5m --tickers AAPL --years 1` — actually run; monitor log; expect bronze 5m parquet for AAPL to grow back to 1-year depth. Note runtime for the docs.
- [ ] `python scripts/backfill_intraday.py --timeframe 5m --tickers AAPL --years 1 --skip-existing` — second run should report ticker skipped via cursor.

#### Final
- [ ] `python -m pytest tests/ -v -W error::RuntimeWarning` — catches leaked coroutines.
- [ ] Re-run `coverage_report.py` to confirm everything ends green.
- [ ] Commit each task atomically (one commit per task) with the standard footer.

---

## Risks

- **IB pacing during backfill:** 5m × 1 year for 1,166 tickers from cold can take many hours. Operator should kick `backfill_intraday.py` off-hours; the cursor lets it resume.
- **Coverage report DuckDB perf:** A glob over 1,166 symbols × 3 timeframes is fine on local SSD but document the cost.
- **Auto-recovery feedback loop:** If `fetch_ib_historical.py` itself is broken, the recovery path will repeatedly fail. The safety cap + email mitigates, but watch the first production runs.
- **Health check false positives on halts:** The 30-min annotation is heuristic — operator review required.

## Sequencing

Tasks 1–4 are independent and can be implemented in any order, but the test E2E (Task 7) requires all four shipped. Recommended: T1 → T2 → T3 → T4 → T5 → T6 → T7.
