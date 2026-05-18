# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Start Sub-C planning: use Massive as a faster near-term daily equity/ETF data path and as a second-source reference so `row_count_anomaly` becomes real instead of a Sub-A stub.
- Finish the post-PR #6 launchd cleanup by replacing stale `com.market-warehouse.*` jobs with valid `com.livewire.*` user LaunchAgents.

## Success Criteria
- Old `com.market-warehouse.daily-update*` launchd labels are absent.
- `com.livewire.daily-update` and `com.livewire.daily-update-watchdog` are installed under `~/Library/LaunchAgents`, validate with `plutil`, and are bootstrapped in `gui/$UID`.
- Repo launchd templates use valid XML escaping for shell `&&`.
- Sub-C scope is narrow enough for one implementation plan:
  - request telemetry is real for `UWClient` and a new Massive client,
  - Massive daily OHLC/reference data can be fetched through a bounded client,
  - Massive can serve near-term equity/ETF daily bars faster than IB when the operator explicitly chooses it or when a narrow recent-gap recovery path calls it,
  - `detect_row_count_anomaly` compares IB bar counts with a second-source reference,
  - quality flags continue through sidecar JSON, audit JSONL, alerting, and optional Postgres import.
- Sub-D new timeframes, Sub-E options chains, and Sub-F gold/factor tables stay out of Sub-C.
- Intraday and broader multi-timeframe productionization are explicitly deferred to Sub-D.

## Dependency Graph
- T0 -> T1 -> T2
- T2 -> T3
- T2 -> T4
- T3, T4 -> T5
- T5 -> T6 -> T7

## Tasks
- [x] T0 Inspect current launchd state and installed plists
  depends_on: []
- [x] T1 Install and bootstrap `com.livewire.*` LaunchAgents
  depends_on: [T0]
- [x] T2 Fix launchd template XML escaping and reload installed plists
  depends_on: [T1]
- [ ] T3 Draft Sub-C design spec
  depends_on: [T2]
  - Recommended scope: provider request telemetry + Massive near-term daily provider + Massive daily reference provider + row-count anomaly activation.
  - Massive is allowed to accelerate near-term equity/ETF daily fetches, but bronze Parquet remains canonical and the chosen source must be visible in logs/quality metadata.
  - Explicit non-goals: intraday, canonical provider replacement, options chain capture, new timeframes, gold/factor tables, DuckDB retirement.
  - Design decision to lock: whether Massive is exposed only as an explicit `--source massive` path first, or also as a narrow recent-gap fallback after IB misses the target day.
  - Design decision to lock: whether UW in Sub-C is telemetry-only for existing endpoints or also participates in daily reference checks when a daily OHLC endpoint is reliable enough.
- [ ] T4 Verify provider contracts before implementation
  depends_on: [T2]
  - Inspect current Massive API docs or live client assumptions before naming exact endpoints.
  - Verify Massive supports the needed recent daily equity/ETF OHLC endpoint, date filters, pagination/range limits, rate-limit headers, and adjusted/unadjusted semantics.
  - Inspect existing `docs/unusual_whales_api_spec.yaml` and `clients/uw_client.py` before extending UW.
  - Define non-secret env variables in `.env.example` only after contract names are verified.
- [ ] T5 Write implementation plan for Sub-C
  depends_on: [T3, T4]
  - Add exact task slices for `clients/massive_client.py`, `clients/telemetry.py`, `clients/uw_client.py`, `clients/quality_detector.py`, `livewire_scripts/daily_update.py` or a narrow recovery helper, script wiring, docs, and tests.
  - Include an explicit-source command design such as `daily --source ib|massive` or a separate recent-gap recovery command, chosen in the spec before implementation.
  - Include a coverage-preserving test plan for `clients/`, `scripts/`, and `livewire_scripts/`.
- [ ] T6 Implement Sub-C behind explicit operator commands
  depends_on: [T5]
  - Keep bronze Parquet canonical.
  - Do not make Massive or UW an invisible write path; every Massive-sourced publish or recovery must show source/outcome counters.
  - Emit provider telemetry, near-term recovery outcomes, and row-count quality flags observably.
  - Do not touch intraday backfill/update behavior in Sub-C.
- [ ] T7 Verify and prepare PR
  depends_on: [T6]
  - Run focused tests as changes land.
  - Before PR, run `python -m pytest tests -q --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing -W error::RuntimeWarning`.

## Review
- Launchd cleanup:
  - No installed or loaded `com.market-warehouse.daily-update*` services were present.
  - Rendered and installed `/Users/moremeds/Library/LaunchAgents/com.livewire.daily-update.plist`.
  - Rendered and installed `/Users/moremeds/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist`.
  - `plutil -lint` passes for both installed plists.
  - `launchctl print gui/501/com.livewire.daily-update` shows the 13:05 trigger and command `python scripts/livewire_ops.py run-daily-job`.
  - `launchctl print gui/501/com.livewire.daily-update-watchdog` shows the 18:30 trigger and command `python scripts/livewire_quality.py watchdog`.
- Sub-C code seams identified:
  - `clients/telemetry.py` already has `UWTelemetry` and `MassiveTelemetry` request methods.
  - `clients/uw_client.py` has a retrying authenticated request layer but does not yet emit telemetry.
  - `clients/quality_detector.py::detect_row_count_anomaly` is still the Sub-A stub.
  - Postgres already imports source-tagged telemetry and quality audit JSONL through `livewire_store.py rebuild-postgres --include-reliability`.
- Scope correction after discussion:
  - Sub-C is not just passive validation. It should let Massive speed up near-term equity/ETF daily data where IB HMDS is slow or flaky.
  - Massive should not replace IB wholesale; IB remains important for long historical backfill and broker-specific futures/CMDTY/FX semantics.
  - Intraday/multi-timeframe productionization remains Sub-D even though 1h/5m storage and manual IB backfill already exist.
