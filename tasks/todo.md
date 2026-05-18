# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Build Sub-C as the **Massive near-term daily accelerator and validation layer**:
  - use Massive for fast recent U.S. equity/ETF daily OHLCV when explicitly selected or in a narrow recent-gap recovery path,
  - keep IB as the primary broker-aligned source for long historical backfills and non-equity contract semantics,
  - use Massive as the second-source reference that activates `row_count_anomaly`,
  - keep intraday and broader multi-timeframe productionization out of scope until Sub-D.

## Current Baseline
- Branch: `feat/sub-c-massive-daily-validation`
- Clean-slate commit already made: `87dfc72 chore: prepare sub-c planning slate`
- Launchd cleanup from PR #6 is complete:
  - no loaded `com.market-warehouse.daily-update*` labels were present,
  - `com.livewire.daily-update` and `com.livewire.daily-update-watchdog` are installed under `~/Library/LaunchAgents`,
  - installed plists and repo templates pass `plutil -lint`,
  - repo templates escape shell `&&` as `&amp;&amp;`.

## Scope
- Sub-C includes:
  - `clients/massive_client.py` for Massive stock daily bars.
  - `MASSIVE_API_KEY` / non-secret config surface.
  - provider request telemetry for Massive and UW.
  - an explicit Massive daily path for equity/ETF recent bars.
  - optional narrow Massive recovery for unresolved recent daily gaps after IB, if the design chooses it.
  - `detect_row_count_anomaly` implementation using Massive daily coverage as reference.
  - source/outcome counters and quality metadata that make Massive-sourced writes visible.
- Sub-C excludes:
  - intraday fetch/update/backfill changes,
  - new timeframes such as 15m/30m/4h,
  - option chains,
  - gold/factor tables,
  - DuckDB retirement,
  - making Massive or UW the canonical warehouse.

## Provider Contract Notes
- Massive official docs identify two candidate daily-equity surfaces:
  - per-ticker custom aggregate bars: `/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}`,
  - grouped daily bars for all U.S. stocks on one date.
- Current docs say per-ticker custom bars:
  - return ET aggregate OHLCV records under `results`,
  - support `adjusted`, `sort`, and `limit`,
  - default to split-adjusted results unless `adjusted=false`,
  - may include `next_url`.
- Current docs say grouped daily bars:
  - use `/v2/aggs/grouped/locale/us/market/stocks/{date}`,
  - support `adjusted`,
  - support `include_otc` defaulting false.
- Authenticated live smoke was run with the user-provided key for this session only; the key was not written to repo files.
- Live smoke results:
  - `Authorization: Bearer <key>` works against `https://api.massive.com`.
  - Per-ticker custom daily bars for AAPL returned HTTP 200 with `status=OK`, `adjusted=False`, `queryCount=5`, `resultsCount=5`, no `next_url`, and sample keys `c,h,l,n,o,t,v,vw`.
  - Grouped daily bars for one date returned HTTP 200 with `status=OK`, `adjusted=False`, `queryCount=12104`, `resultsCount=12104`, no `next_url`, and sample keys `T,c,h,l,n,o,t,v,vw`.
  - No rate-limit/remaining/reset response headers were observed in that smoke response.
  - Per-ticker daily `t` resolved to the requested trade date at midnight ET; grouped daily sample `t` resolved to the requested trade date at 16:00 ET. Normalization should derive `trade_date` carefully from ET date and tests should cover both shapes.
  - `v` was returned as a number and may be non-integer, so conversion into the current bigint `volume` column needs an explicit normalization rule and tests.
- Do not hardcode undocumented endpoint assumptions into code; record the final auth style, pagination/range-limit behavior, adjusted/unadjusted choice, and rate-limit behavior in the Sub-C spec.
- UW already has `clients/uw_client.py`; Sub-C should add telemetry there first. UW daily OHLC can participate in validation only if its contract semantics are verified and useful. Otherwise UW remains telemetry-only in Sub-C.

## Success Criteria
- Massive daily bars can be fetched for explicit recent equity/ETF requests without opening IB Gateway.
- `python scripts/livewire_ingest.py daily --asset-class equity --source massive` bypasses IB Gateway preflight while default IB-backed daily commands still require preflight.
- The operator can see when a publish/recovery used Massive instead of IB.
- `row_count_anomaly` no longer raises `NotImplementedError` when a reference source is supplied.
- Daily quality detection receives `reference_source` metadata when Massive reference data is available; implementing the detector alone is not sufficient.
- Row-count anomaly flags include source names, expected/reference row count, actual row count, percent delta, missing date range detail when available, and threshold/severity.
- Existing sidecar JSON, central `quality_audit.jsonl`, daily report, alert email, and optional Postgres import paths continue to work with `source="massive"`.
- IB remains unchanged for futures, volatility-through-IB historical fallback, CMDTY, FX, and long historical backfills.
- No intraday code path changes in Sub-C.
- Coverage remains 100% for the configured source set.

## Dependency Graph
- T0 -> T1
- T1 -> T2
- T1 -> T3
- T2, T3 -> T4
- T4 -> T5 -> T6
- T6 -> T7 -> T8
- T8 -> T9
- T6, T8 -> T10
- T9, T10 -> T11

## Tasks
- [x] T0 Create clean Sub-C branch baseline
  depends_on: []
  - `main` and `feat/sub-c-massive-daily-validation` both point at `87dfc72`.
  - Worktree was clean before this plan update.

- [x] T1 Write Sub-C design spec
  depends_on: [T0]
  - Create `docs/superpowers/specs/2026-05-18-sub-c-massive-daily-validation-design.md`.
  - Lock the product boundary:
    - Massive is a fast near-term daily equity/ETF source and validation reference.
    - IB remains primary for long historical and broker-specific asset classes.
    - Intraday is deferred to Sub-D.
  - Decide whether Sub-C ships:
    - Option A: explicit `daily --source massive` only.
    - Option B: explicit `daily --source massive` plus narrow recent-gap fallback after IB misses target-day bars.
  - Recommended decision: Option B, but the fallback must be narrow, recent, equity/ETF-only, logged, and source-counted.

- [x] T2 Verify Massive provider contract
  depends_on: [T1]
  - Use the user-provided dashboard reference: `https://massive.com/dashboard/rest`.
  - Docs-confirmed candidate endpoints:
    - per-ticker custom bars: `/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}`,
    - grouped daily bars: `/v2/aggs/grouped/locale/us/market/stocks/{date}`.
  - Live-confirmed auth:
    - base URL `https://api.massive.com`,
    - `Authorization: Bearer <key>` accepted.
  - Live-confirmed per-ticker aggregate response shape for daily bars:
    - `results[].t` as Unix millisecond bar start,
    - `results[].o/h/l/c` as OHLC,
    - `results[].v` as volume,
    - optional `results[].vw/n/otc`.
  - Live-confirmed grouped daily response shape:
    - `results[].T` ticker,
    - `results[].t` Unix millisecond timestamp,
    - `results[].o/h/l/c/v`,
    - optional `results[].vw/n/otc`.
  - Grouped daily is suitable for universe-scale single-date recovery because one request returned 12,104 U.S. stock rows for the smoke date.
  - Choose `adjusted=false` unless the design explicitly decides to store adjusted Massive bars; current IB rows store `adj_close = close`.
  - Treat absent rate-limit headers as possible; retry logic must still handle HTTP 429 with `Retry-After` if present.
  - Define and test timestamp normalization separately for per-ticker and grouped shapes.
  - Define and test volume normalization because live `v` can be non-integer.
  - Record exact endpoint choices in the design spec before implementation.

- [x] T3 Verify UW Sub-C role
  depends_on: [T1]
  - Inspect `docs/unusual_whales_api_spec.yaml` and `clients/uw_client.py`.
  - Add request telemetry to `UWClient` regardless.
  - Decide whether UW daily OHLC is in validation scope. If semantics are not clean enough, explicitly mark UW as telemetry-only for Sub-C.

- [x] T4 Write implementation plan
  depends_on: [T2, T3]
  - Create `docs/superpowers/plans/2026-05-18-sub-c-massive-daily-validation-plan.md`.
  - Include exact file-level tasks and tests for:
    - `clients/massive_client.py`
    - `tests/test_massive_client.py`
    - `clients/telemetry.py`
    - `clients/uw_client.py`
    - `clients/quality_detector.py`
    - `scripts/livewire_ingest.py` preflight routing
    - daily Massive source/recovery wiring
    - Massive reference comparison wiring into quality detection metadata
    - README / CLAUDE / `.env.example`
  - Include exact verification commands and expected outcomes.

- [ ] T5 Add Massive client and telemetry
  depends_on: [T4]
  - Implement a small requests-based `MassiveClient` matching `UWClient` style:
    - env-backed token,
    - connection pooling,
    - timeout,
    - retry for 429/5xx and network timeouts,
    - typed exception hierarchy,
    - request/rate-limit telemetry.
  - Keep the client focused on stock daily bars needed by Sub-C.

- [ ] T6 Add daily bar normalization boundary
  depends_on: [T5]
  - Convert Massive daily OHLCV payloads into the same row shape used by `BronzeClient` daily equity writes.
  - Preserve current storage semantics:
    - `trade_date`,
    - stable `symbol_id`,
    - `open/high/low/close`,
    - `adj_close = close` unless the verified endpoint semantics justify an explicit adjusted mode,
    - integer `volume`.
  - Explicitly choose a volume conversion rule for Massive numeric `v` values. Recommended: require finite non-negative values and store `int(round(v))`; record if rounding changed the raw value in source metadata.
  - Normalize `trade_date` from the ET date represented by Massive `t`, and test both:
    - per-ticker aggregate timestamps at midnight ET,
    - grouped daily timestamps at market close ET.
  - Reject malformed bars before publish.

- [ ] T7 Fix ingest preflight routing for non-IB daily source
  depends_on: [T6]
  - Update `scripts/livewire_ingest.py` so `daily --source massive` does not call `assert_gateway_up()`.
  - Preserve current IB preflight behavior for:
    - default `daily`,
    - `daily --source ib`,
    - `historical`,
    - `robust`,
    - intraday commands,
    - universe commands.
  - Add tests proving Massive daily help/dry-run paths do not require IB Gateway.

- [ ] T8 Wire explicit Massive daily path
  depends_on: [T7]
  - Add the selected explicit operator surface, likely:
    - `python scripts/livewire_ingest.py daily --asset-class equity --source massive`
    - existing default remains IB.
  - Scope to equity/ETF only.
  - Print counters: requested, fetched, published, skipped, failed, source.
  - Emit provider telemetry and quality audit context.

- [ ] T9 Wire narrow Massive recent-gap recovery
  depends_on: [T8]
  - Product decision: use Option B.
  - Fallback order for unresolved recent target-day equity/ETF gaps:
    - IB first,
    - Massive second,
    - existing public fallback chain third (`nasdaq:stocks`, `nasdaq:etf`, `stooq:us`).
  - Rationale: if Massive is the paid fast path, putting it after public fallback defeats the accelerator goal.
  - Guardrails:
    - equity/ETF only,
    - recent daily bars only,
    - no intraday,
    - no futures/CMDTY/FX,
    - no silent source substitution,
    - source/outcome counters printed and logged.

- [ ] T10 Activate row-count anomaly and reference-source wiring
  depends_on: [T6, T8]
  - Replace the Sub-A stub in `clients/quality_detector.py`.
  - Define a small `SourceComparison` shape or plain dict contract with:
    - `source`,
    - `expected_count`,
    - `actual_count`,
    - optional `expected_dates`,
    - optional `actual_dates`.
  - Add the daily-update wiring that builds `SourceComparison` from Massive reference rows and passes it as `metadata["reference_source"]` into `detect_all()`.
  - Add tests proving `detect_row_count_anomaly` fires through `detect_all()` from real metadata, not only as a direct unit call.
  - Warning threshold: absolute delta > 1%.
  - Critical threshold: absolute delta > 5%.
  - Include missing-date detail when available.
  - Keep `None` return when no reference source is supplied.

- [ ] T11 Verification and PR prep
  depends_on: [T9, T10]
  - Focused tests as each slice lands.
  - Full gate before PR:
    - `python -m pytest tests -q --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing -W error::RuntimeWarning`
  - Optional live smoke only if `MASSIVE_API_KEY` is configured:
    - one recent daily fetch for `AAPL`,
    - one grouped daily fetch for a recent completed trading date,
    - one small explicit-source daily dry run,
    - one row-count anomaly comparison against a tiny fixture.

## Self-Review
- Spec coverage:
  - Massive near-term acceleration is explicit in Objective, Scope, T2, T8, and T9.
  - Cross-source validation is explicit in Objective, Success Criteria, and T10.
  - Provider telemetry is explicit in Scope, T3, T5, T8, and T9.
  - Existing quality surfaces are preserved in Success Criteria.
  - Intraday is excluded in Scope, Success Criteria, T9 guardrails, and the design spec task.
- Placeholder scan:
  - No placeholder markers, no vague deferral task, and no implementation task without a concrete file or behavior target.
  - Endpoint names are docs-confirmed as candidates; final auth, pagination, and live smoke details remain gated in T2 because authenticated verification is blocked until a key is configured.
- Type/contract consistency:
  - `MassiveClient` feeds daily bar normalization, explicit daily source wiring, and `SourceComparison`.
  - `row_count_anomaly` remains optional when no reference is supplied, preserving current `detect_all` callers.
  - `source` values stay within the existing closed set: `ib`, `uw`, `massive`.
  - Massive per-ticker and grouped daily payloads use different ticker/timestamp shapes, and T6 now requires normalization tests for both.
- Scope risks:
  - The biggest risk is accidentally expanding into intraday because Massive supports aggregate timeframes. The plan forbids intraday changes in Sub-C and defers them to Sub-D.
  - The second risk is making Massive an invisible fallback. The plan requires explicit source counters and logged recovery outcomes.
  - The third risk is adjusted-vs-unadjusted mismatch. T2 must settle endpoint semantics before any writer code lands.
  - The fourth risk is implementing `detect_row_count_anomaly` without passing reference metadata. T10 now requires the metadata wiring and integration tests.
  - The fifth risk is forcing IB Gateway preflight on Massive-only runs. T7 now requires explicit preflight routing tests.
  - The sixth risk is truncating fractional Massive volume. T6 now requires an explicit conversion rule and metadata note when rounding changes the raw value.
