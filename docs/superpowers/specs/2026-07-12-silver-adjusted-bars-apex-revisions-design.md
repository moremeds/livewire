# Silver Adjusted Bars and Apex Revisions — Design

Status: Approved for planning
Date: 2026-07-12
Owners: Livewire data plane and Apex TA service
Scope: U.S. equity split- and cash-dividend-adjusted OHLCV

## 1. Objective

Provide split- and cash-dividend-adjusted equity bars to Apex by default without
destroying Livewire's raw, replayable bronze record. Livewire owns corporate
actions, adjustment semantics, adjusted artifacts, and revision publication.
Apex remains a continuously running Docker service and must refresh already
seeded indicator state without a container restart.

The first delivery priority is the Apex revision protocol and watcher. It can be
built against fixtures before the Livewire adjustment engine is complete.

## 2. Tier Ownership

- **Bronze bars:** provider-original OHLCV. Never rewritten merely to present an
  adjusted history.
- **Bronze corporate actions:** canonical provider events for splits and cash
  dividends, including revisions and cancellations.
- **Silver:** reproducible cleaned/adjusted bars and compact adjustment-factor
  intervals derived from bronze.
- **Gold:** downstream factors, returns, analytics, and strategy-ready tables.
- **Apex:** technical analysis, subscription state, and signals. Apex consumes
  Silver semantics but does not define adjustment mathematics.

This resolves older draft language that placed adjusted data in Gold. The durable
roadmap definition is Sub-F Silver for cleaned/adjusted data and Sub-G Gold for
analytics.

## 3. Approaches Considered

### 3.1 Rewrite bronze

Rejected. It would make ingestion output convenient for Apex, but remove the
provider-original audit record and make corrections difficult to replay.

### 3.2 Materialize every adjusted timeframe

Rejected as the default. It gives consumers simple files but duplicates the
large equity-intraday warehouse.

### 3.3 Livewire-owned hybrid Silver layer

Selected. Livewire materializes adjusted daily bars and publishes compact factor
intervals for adjusted intraday reads. Apex applies the published factors during
its DuckDB intraday query. The adjustment logic and fixtures remain defined by
Livewire, not independently invented by each consumer.

## 4. Data Flow

```text
Massive OHLCV ───────────────────────────▶ Bronze equity bars
Massive splits/dividends ────────────────▶ Bronze corporate actions
                                                   │
                                                   ▼
                                          Adjustment engine
                                         ┌─────────┴─────────┐
                                         ▼                   ▼
                                Silver adjusted 1d   Silver factor intervals
                                         └─────────┬─────────┘
                                                   ▼
                                           Revision manifest
                                                   ▼
                                  Apex chart reads + subscription watcher
```

Corporate-action fetching and adjustment publication are scheduled/event-driven.
Apex's manifest watcher runs continuously inside the existing long-lived Docker
process.

## 5. Corporate-Action Contract

Corporate actions live at:

```text
data-lake/bronze/asset_class=corporate_action/symbol={encoded_symbol}/events.parquet
```

Each row contains:

| Field | Type | Meaning |
|---|---|---|
| `action_id` | string | Stable Livewire identifier |
| `provider` | string | Closed provider identifier |
| `provider_event_id` | string | Provider identity used for idempotent upsert |
| `event_revision` | integer | Monotonic revision of the logical provider event |
| `supersedes_action_id` | string nullable | Previous canonical version, when corrected |
| `symbol` | string | Security symbol |
| `action_type` | string | `split` or `cash_dividend` |
| `ex_date` | date | First session trading on the new economic basis |
| `split_from` / `split_to` | double nullable | Split ratio components |
| `cash_amount` | double nullable | Cash amount per share |
| `currency` | string nullable | Dividend currency |
| `declaration_date` | date nullable | Provider declaration date |
| `record_date` | date nullable | Provider record date |
| `pay_date` | date nullable | Provider payment date |
| `status` | string | `active`, `corrected`, or `cancelled` |
| `fetched_at` | UTC timestamp | Retrieval time |
| `payload_hash` | string | Detects provider revisions without storing secrets |

The initial provider is Massive's paginated reference splits and dividends API.
Fetching is idempotent by `(provider, provider_event_id, payload_hash)`. An
unchanged payload is a no-op. A changed payload increments `event_revision`,
creates a new `action_id`, and points `supersedes_action_id` at the previous
canonical version; it does not silently mutate the audit history. Only the
latest non-cancelled version participates in factor computation. Cancelled
actions remain retained.

## 6. Adjustment Mathematics

Silver uses the latest fully back-adjusted history. A new corporate action may
therefore revise all earlier adjusted bars for the affected symbol.

For an action with ex-date `e`, only bars with `bar_date < e` are adjusted.

### 6.1 Splits

For `split_from` old shares becoming `split_to` new shares:

```text
r = split_to / split_from
split price multiplier = 1 / r
split volume multiplier = r
```

### 6.2 Cash dividends

Let `D` be the cash amount and `C` the previous valid session close expressed on
the ex-date's split basis:

```text
dividend price multiplier = (C - D) / C
dividend volume multiplier = 1
```

If a split and dividend share an ex-date, the split basis is applied first, then
the dividend multiplier is calculated. The engine rejects a missing previous
close, `C <= 0`, `D < 0`, `D >= C`, a non-positive split ratio, or a currency
mismatch. Rejected events do not publish a new manifest revision and produce an
operator-visible failure.

For a bar at date `t`, cumulative multipliers are the products of all active
events with `ex_date > t`:

```text
adjusted OHLC = raw OHLC * cumulative price multiplier
adjusted volume = round(raw volume * cumulative split volume multiplier)
```

Cash dividends never change volume. Calculations use float64 artifacts with
decimal arithmetic while constructing factors, deterministic rounding for
integer volume, and stable event ordering by `(ex_date, action_type, action_id)`.

## 7. Silver Artifacts

### 7.1 Materialized daily bars

```text
data-lake/silver/asset_class=equity/symbol={encoded_symbol}/1d.parquet
```

The file preserves Apex's required names and types:

- `trade_date`, `symbol_id`
- adjusted `open`, `high`, `low`, `close`
- `adj_close` equal to adjusted `close`
- split-adjusted `volume`
- `price_adjustment_factor`
- `split_volume_factor`
- `adjustment_revision`

Symbols with no actions still receive factor `1.0`, allowing a uniform consumer
contract. Publication uses Livewire's temp-write, validation, and atomic replace
semantics.

### 7.2 Intraday factor intervals

```text
data-lake/silver/adjustments/asset_class=equity/symbol={encoded_symbol}/factors.parquet
```

Each non-overlapping interval contains:

- `effective_start` and `effective_end`, inclusive and nullable at the bounds
- `price_adjustment_factor`
- `split_volume_factor`
- `adjustment_revision`

Apex range-joins raw bronze intraday bars by the New York trading date. The
factor file is small because it has one interval per distinct cumulative factor,
not one row per intraday bar.

## 8. Revision Manifest Contract

Immutable manifests and a current pointer live at:

```text
data-lake/silver/revisions/revision={revision}.json
data-lake/silver/revisions/current.json
```

Example schema version 1:

```json
{
  "schema_version": 1,
  "revision": 42,
  "generation_id": "20260712T100000Z-42",
  "published_at": "2026-07-12T10:00:00Z",
  "corporate_actions_as_of": "2026-07-12T09:58:00Z",
  "affected": [
    {
      "symbol": "NVDA",
      "earliest_date": "1999-01-22",
      "timeframes": ["1d", "1m", "5m", "30m", "1h"]
    }
  ],
  "artifacts": [
    {
      "path": "asset_class=equity/symbol=NVDA/1d.parquet",
      "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    }
  ]
}
```

The digest above is illustrative. The production serializer computes and
requires the actual 64-character SHA-256 digest of each published artifact.

Contract rules:

1. `revision` increases monotonically under a Silver publication lock.
2. All adjusted artifacts are atomically published and validated first.
3. The immutable revision manifest is written second.
4. `current.json` is atomically replaced last and acts as the commit record.
5. Duplicate observation is harmless; consumers track the latest successful
   revision.
6. Consumers may skip intermediate revisions because each revision describes a
   complete latest state.
7. No-op reconciliation does not advance the revision.

## 9. Apex Integration

### 9.1 Configuration

Apex retains `APEX_LIVEWIRE_ROOT` for raw bronze and adds:

- `APEX_LIVEWIRE_SILVER_ROOT`
- `APEX_LIVEWIRE_REVISION_POLL_SECONDS`, default `30`
- `APEX_LIVEWIRE_PRICE_MODE`, default `adjusted` after cutover; `raw` remains a
  diagnostic option

Both roots are mounted read-only into the Docker container.

### 9.2 Read behavior

- Daily adjusted requests read Silver `1d.parquet`.
- Intraday adjusted requests read bronze and range-join the Silver factor file.
- Raw diagnostic requests read bronze without factors.
- Missing or invalid adjusted artifacts never silently fall back to raw, because
  that would reintroduce false discontinuities. The request returns a clear
  unavailable error while the previous in-memory subscription state remains
  active.

### 9.3 Continuous revision watcher

The watcher polls `current.json` rather than relying on filesystem events across
the Docker bind mount. On a newer valid revision it:

1. Verifies manifest schema and relevant artifact checksums.
2. Selects affected symbols that currently have subscriptions.
3. Marks those symbols as refreshing without interrupting unrelated symbols.
4. Buffers incoming Xenon ticks for each refreshing symbol.
5. Reloads adjusted history and recomputes indicator state.
6. Atomically swaps the refreshed symbol state.
7. Replays buffered ticks in event-time order with normal duplicate handling.
8. Records each successfully refreshed symbol's applied revision independently.
   `last_fully_applied_revision` advances only after every affected subscribed
   symbol succeeds.

Chart endpoints read files on demand and see atomic publications immediately.
The watcher exists because subscription warmup history and indicators are held
in memory by the continuously running Apex service.

### 9.4 Failure behavior

- Apex continues serving the last coherent in-memory state during refresh.
- A failed symbol is retried with bounded backoff and does not block unrelated
  symbols.
- Buffered ticks have a configured count/age ceiling. Crossing it marks the
  symbol unavailable instead of silently dropping ticks.
- Apex health reports current observed revision, `last_fully_applied_revision`,
  per-symbol applied revisions, refresh age, affected failures, and staleness.
- Livewire never advances `current.json` after a partial publication.

## 10. Scheduling and Operations

The Livewire engine is not a permanent daemon:

- **Pre-market:** fetch actions effective or revised since the last cursor and
  publish affected symbols before analysis begins.
- **Post-ingestion:** reconcile against completed daily bars and republish if
  factors changed.
- **Weekly:** perform a wider provider reconciliation for revisions.
- **On demand:** rebuild one symbol, an explicit ticker list, or the full Silver
  layer; support dry-run reporting.

Every run records counters for fetched, inserted, revised, cancelled, rebuilt,
failed, and unchanged events/symbols.

## 11. Delivery Roadmap

```text
R0 revision contract
 ├──▶ R1 Apex watcher and atomic reseed
 └──▶ R2 corporate-action ingestion
          └──▶ R3 daily Silver engine
                   ├──▶ R4 end-to-end canary
                   └──▶ R5 intraday adjustment
                              └──▶ R6 adjusted-by-default cutover
```

- **R0 `depends_on: []`:** freeze this manifest contract and matching fixtures in
  both repositories.
- **R1 `depends_on: [R0]`:** Apex poller, targeted invalidation, tick buffering,
  recompute, atomic state swap, health, and fixture-driven tests. This is the
  first implementation PR and does not wait for the engine.
- **R2 `depends_on: [R0]`:** Livewire Massive corporate-action client, canonical
  store, pagination, revisions, reconciliation, and CLI.
- **R3 `depends_on: [R2]`:** factor engine, adjusted daily publisher, interval
  publisher, publication lock, revision manifest, and operator commands.
- **R4 `depends_on: [R1, R3]`:** shadow-mode canary using NVDA, AAPL, SPY, and a
  no-action control symbol. Apex remains raw by default.
- **R5 `depends_on: [R4]`:** Apex intraday factor join and revision-driven
  intraday reseeding.
- **R6 `depends_on: [R5]`:** adjusted-by-default cutover, raw diagnostic mode,
  documentation, alerts, and canary removal.

The existing 17-item audit plan set does not contain Silver work. R1 can start
immediately. New Silver paths must use the same warehouse-root resolution rules
as the planned path-consolidation work, but Silver is not blocked on completion
of every audit plan. Plan overlap is resolved explicitly before each PR.

## 12. PR Boundaries

1. Apex: revision contract fixture, watcher, and reseed lifecycle.
2. Livewire: corporate-action ingestion and canonical store.
3. Livewire: adjustment engine, Silver publishers, and manifest publisher.
4. Cross-repo operational canary; no default change.
5. Apex: intraday factor application.
6. Apex: adjusted-by-default cutover.

Each PR has an independent rollback and requires explicit user approval before
merge. No direct push to `main` or `master` is permitted.

## 13. Testing and Acceptance

### Livewire unit and property tests

- Forward and reverse splits, fractional ratios, and multiple cumulative splits.
- Cash dividends, multiple dividends, and split-plus-dividend on one ex-date.
- Dividend volume invariance.
- Missing previous close, invalid ratios, currency mismatch, cancellation, and
  provider correction.
- Factor intervals are ordered, non-overlapping, exhaustive over available bars,
  and reproduce materialized daily outputs.
- Idempotent rebuild produces identical logical rows and no new revision.
- Publication failure leaves `current.json` unchanged.

### Apex tests

- Repeated and skipped manifest revisions.
- Docker-friendly polling semantics.
- Only affected subscribed symbols reseed.
- Tick buffering, ordered replay, deduplication, ceiling failure, and atomic
  state replacement.
- Chart reads observe new daily files while subscription state refreshes.
- Missing or corrupt adjusted artifacts do not fall back to raw.
- Raw diagnostic mode remains explicit and functional.

### End-to-end acceptance

- NVDA has no artificial discontinuity across its 2021 4-for-1 and 2024
  10-for-1 splits.
- AAPL and SPY total-return histories remove cash-dividend price gaps according
  to the documented formula.
- A control symbol with no actions is unchanged.
- Bronze artifacts are unchanged by Silver publication.
- A corporate-action correction causes a new revision and refreshes a live Apex
  subscription without restarting the container.
- Broad Livewire and Apex CI suites pass at their configured coverage gates.

## 14. Rollout and Rollback

R4 runs in shadow mode and compares adjusted output against independent provider
references before cutover. R6 changes the Apex default only after daily and
intraday paths pass the canary.

Rollback is configuration-only: set Apex price mode to `raw` and leave Silver
artifacts in place for diagnosis. A broken Livewire publication cannot become
current because `current.json` is the final atomic commit record. Bronze remains
the replay source throughout.

## 15. Explicit Non-Goals

- Dividend reinvestment share accounting or portfolio cash ledgers.
- Tax treatment, withholding, or currency conversion.
- Symbol rename continuity, mergers, spin-offs, rights offerings, or return of
  capital in the first release.
- Statistical bad-print correction; that remains a separate quality-control
  design.
- TA or signal persistence in Livewire Silver or Gold.
