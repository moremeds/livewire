# Full-History Adjusted-Data Validation Design

## Status

Approved direction: use Massive wherever the account has historical coverage,
then use a fresh Interactive Brokers historical query for the remaining local
history. A ticker passes only when the combined references cover every stored
session and all price, moving-average, corporate-action, and invariant checks
pass.

## Problem

Livewire now records row-level source and price basis in equity Bronze,
normalizes IB daily bars to the canonical raw basis, and derives split- and
dividend-adjusted Silver. Targeted canaries prove the transformation on known
symbols, but they do not establish that the complete local equity universe is
correct over its complete stored history.

Three properties make a whole-history validator necessary:

- Massive history is entitlement-limited and may return only part of a requested
  range. Validating only the returned intersection would incorrectly pass
  unverified history.
- IB `TRADES` history is not uniform across split events. Fresh IB bars must be
  classified per split event and normalized before they can be compared with a
  canonical series.
- A moving average can look correct even when positive and negative point errors
  cancel. Moving averages are therefore a required aggregate check, but cannot
  replace date-aligned OHLCV checks.

The validation must be read-only, resumable across a large universe, explicit
about provider coverage, and safe to run while Apex continues reading published
Silver revisions.

## Goals

- Validate every stored equity daily session, not a fixed canary list.
- Prefer Massive as the independent market-data reference where entitled.
- Fill Massive coverage gaps with a fresh IB historical request.
- Validate split-only and split-plus-dividend series separately.
- Compare every eligible 20-, 50-, and 200-session moving average.
- Fail closed on missing coverage, ambiguous IB basis, truncated responses, and
  unexplained value differences.
- Produce machine-readable evidence that can be resumed and reviewed before any
  separate repair operation.

## Non-Goals

- The validator does not mutate Bronze, Silver, corporate actions, or revision
  manifests.
- It does not automatically repair failed symbols.
- It does not claim that an IB-backed comparison independently proves IB's own
  vendor data. Fresh IB verifies retrieval, normalization, storage, and Silver
  transformation; Massive overlap supplies stronger cross-provider evidence.
- It does not validate intraday bars or indicators derived by Apex.
- It does not treat a provider's missing entitlement as a successful partial
  validation.

## Considered Approaches

### Massive-only validation

Massive adjusted aggregates are a strong independent split-only reference, and
the provider SMA endpoint is useful as a secondary calculation check. This
approach cannot cover the complete local history with the current entitlement,
so it cannot satisfy the full-history gate.

### IB-only validation

A fresh IB request can cover older history and exercises the same broker path
used operationally. It is not fully independent for rows originally sourced
from IB and still requires event-level price-basis classification. It remains a
valuable fallback, but is weaker as the sole oracle.

### Massive-first with IB fallback

This is the selected approach. Massive validates every available overlapping
session. Fresh IB supplies the remaining range. The report preserves provenance
at range and date level, so consumers can distinguish cross-provider evidence
from same-provider replay evidence.

## Architecture

Add a read-only validator behind `scripts/livewire_quality.py`. The command
discovers canonical equity Bronze symbols, reads the corresponding Silver
revision, loads effective corporate actions using one New York as-of date, and
constructs two expected local series:

1. **Split-only local series**: canonical raw Bronze with effective split price
   and volume factors.
2. **Total-return local series**: the split-only series with effective cash
   dividend price factors, matching Silver semantics.

Reference acquisition is separate from comparison:

1. Request Massive `adjusted=true` daily aggregates for the complete local date
   range, following pagination and recording the actual first/last dates.
2. Determine exact local sessions not covered by Massive.
3. Request fresh IB `TRADES` history for those ranges, expanded by the sessions
   needed to classify every adjacent split event, using a read-only runner that
   reuses the canonical robust execution model's pacing, retry, timeout, and
   per-symbol outcome semantics.
4. Classify every applicable IB split event and normalize the returned series
   onto the split-only comparison basis.
5. Mark any session absent from both references as unresolved.

Provider results are cached under a validation workspace outside Bronze and
Silver. Cached payloads include request parameters, retrieval time, source,
actual response range, and a content hash. A new run may reuse a cache only when
its identity and requested validation as-of date match.

Corporate-action evidence is acquired independently from price-bar coverage.
The validator refreshes Massive split and dividend reference records where the
account is entitled, compares their active revisions and
`historical_adjustment_factor` values with the local action snapshot, and records
the provider's actual action range. An unverified action range cannot be
described as independently validated merely because IB supplied price bars.

## Source and Coverage Contract

Coverage is evaluated against local stored sessions, because the question is
whether every value exposed from Parquet is supported by reference evidence.
Each local date receives exactly one primary reference classification:

- `massive`: Massive returned a valid adjusted aggregate;
- `ib`: Massive did not cover the date and normalized fresh IB did;
- `massive+ib`: both providers returned the date; Massive is primary and IB is
  recorded as corroborating evidence;
- `unresolved`: neither provider supplied a valid reference.

The validator must not infer complete coverage from an HTTP success, a non-empty
response, or an aggregate count. It verifies pagination, requested and actual
date bounds, unique dates, monotonic ordering, and every expected local date.
Provider dates not present locally are reported separately and do not compensate
for a missing local date.

A ticker cannot receive `pass` while any local date is unresolved. It may still
receive a diagnostic partial-range summary showing exactly which ranges were
validated.

## Basis-Specific Validation

### Massive split-only reference

Massive `adjusted=true` aggregates are split-adjusted but not dividend-adjusted.
They are compared with the local split-only reconstruction, never directly with
total-return Silver. Price OHLC fields are compared pointwise. Volume is checked
with a separate tolerance and diagnostic classification because provider trade
filters can produce legitimate volume differences.

The validator calculates 20-, 50-, and 200-session simple moving averages from
the returned Massive adjusted close series. On supported overlap it also queries
Massive's SMA endpoint as a secondary provider-calculation check. The locally
calculated Massive SMA is the reproducible primary oracle; an SMA endpoint
disagreement is reported as a provider-oracle error rather than silently choosing
one value.

### Fresh IB fallback reference

Fresh IB `TRADES` bars are classified per effective split event using the same
event-level evidence and fail-closed rules as ingestion. A symbol with an
ambiguous or uncovered applicable split cannot use IB for those affected dates.
After normalization, IB prices are compared with the local split-only series.

Because IB may also be the original Bronze source, this result proves that the
stored data and transformation are reproducible from a fresh retrieval. It is
labelled `same_provider_replay` rather than cross-provider evidence.

### Dividend-adjusted Silver

Neither Massive adjusted aggregates nor IB `TRADES` bars are assumed to be a
total-return oracle. The validator independently rebuilds dividend factors from
active effective corporate actions and pre-event reference closes, then compares
the resulting total-return OHLC values and 20/50/200-session moving averages with
persisted Silver.

Fresh Massive dividend records, including historical adjustment factors where
present, are a separate action-inventory cross-check. When the provider cannot
establish complete action coverage, the report distinguishes a successful
`transformation_check` from an `independent_action_check`. The transformation
result remains useful and explicit, but the report must not describe the action
inventory itself as independently proven.

The independent reconstruction must not call the production Silver writer or
reuse persisted Silver factors as expected values. It may reuse pure corporate
action arithmetic only when the validation tests separately exercise those
semantics. This avoids comparing an artifact with itself.

## Comparisons and Tolerances

For every covered local date, validate:

- exact symbol and trading-date alignment;
- finite, positive OHLC and standard OHLC ordering;
- pointwise open, high, low, and close percentage error;
- adjusted-close and factor consistency;
- volume semantics and split-volume factors;
- 20-, 50-, and 200-session simple moving averages at every complete window;
- focused pre-event and post-event windows for every split and dividend;
- absence of unexplained mechanical 2x, 3x, 4x, 7x, and 10x jumps, including
  reciprocal reverse-split forms.

Default price and moving-average thresholds are:

- warning when absolute relative error exceeds 1 basis point;
- failure when absolute relative error exceeds 5 basis points.

Exact provider agreement remains visible in the report. Thresholds are command
options so they can be tightened after observing whole-universe distributions,
but widening them is recorded in the report and cannot hide missing coverage,
ambiguous basis, invalid OHLC, or mechanical split jumps.

Volume differences do not use the price threshold. The report records absolute
and relative volume differences and fails only on structural split-volume errors
or a separately configured volume threshold.

## Moving-Average Semantics

Moving averages use session rows, not calendar days. A window is eligible only
after the reference series has the required number of ordered observations.
Missing sessions invalidate every affected window rather than shortening the
denominator.

The report includes, for each 20/50/200 window and reference source:

- number of eligible comparisons;
- maximum, mean, median, and percentile absolute error;
- first and worst failing dates;
- error distributions around corporate-action boundaries;
- Massive-SMA-endpoint divergence where that endpoint is available.

Pointwise checks remain authoritative. A passing moving average cannot cancel a
pointwise failure.

## CLI and Execution Model

The operator entry point is conceptually:

```bash
uv run python scripts/livewire_quality.py validate-adjusted-history \
  --all-equities \
  --massive-first \
  --ib-fallback \
  --resume
```

It also supports explicit `--tickers`, `--data-lake-root`, `--as-of-date`,
`--host`, `--port`, `--workers`, `--output-dir`, and threshold arguments.
Runs over more than five symbols use the repository's robust IB orchestration
semantics in a dedicated read-only fetch runner rather than invoking the
Bronze-writing `robust` ingestion command or using an ad hoc direct loop.

The cursor is versioned and keyed by resolved data-lake root, Bronze hash,
Silver revision, corporate-action snapshot, as-of date, provider request
identity, validator version, and thresholds. A mismatch invalidates the affected
symbol checkpoint. Completion is atomic per symbol.

The validator limits Massive concurrency, respects retry and rate-limit
responses, and isolates IB failures per ticker. Interruptions yield
`resume-pending`; they are not converted into validation failures or passes.

## Outputs

Each run writes:

- a versioned JSON manifest with run identity, configuration, source coverage,
  per-symbol outcomes, summary statistics, and artifact hashes;
- per-symbol JSON detail containing missing dates, mismatches, action-boundary
  evidence, moving-average statistics, and provider errors;
- a concise Markdown summary for operator review;
- a resumable cursor written atomically.

Ticker outcomes are:

- `pass`: complete history covered and every required check passed;
- `fail`: complete or partial evidence contains a value, moving-average,
  corporate-action, schema, or invariant failure;
- `unresolved`: at least one local date lacks usable reference evidence;
- `provider-error`: validation could not establish a usable provider response;
- `input-changed`: a Bronze, Silver, or corporate-action input changed during
  validation;
- `resume-pending`: execution stopped before the ticker reached a terminal state.

The process exits zero only when every requested ticker passes. Unresolved and
provider-error outcomes are distinct in JSON but fail the aggregate gate.

Every terminal result also carries evidence dimensions rather than compressing
all successful checks into one confidence claim:

- `price_evidence`: `cross_provider`, `hybrid`, or `same_provider_replay`;
- `action_reference_status`: `complete`, `partial`, or `unavailable`;
- `transformation_check`: `pass` or `fail`;
- `independent_action_check`: `pass`, `fail`, or `unavailable`.

Thus a complete IB-backed history can pass the value and transformation gate
without being mislabeled as fully independent validation. A fresh Massive action
mismatch is a failure; unavailable independent action evidence is reported as a
confidence limitation, not silently converted into success.

## Safety

- Bronze, Silver, corporate-action, and revision paths are opened read-only.
- The output directory must resolve outside canonical Bronze and Silver roots.
- Input hashes are recorded before validation and rechecked after each symbol.
- A changed input produces `input-changed`, invalidates the checkpoint, and
  prevents a pass.
- No validation result authorizes repair. A repair requires its own reviewed
  manifest and explicit operator command.
- API credentials are read from environment variables and never written to
  reports, caches, logs, or command arguments.

## Verification Strategy

### Unit tests

- Complete, partial, truncated, duplicated, unsorted, empty, paginated, and
  entitlement-limited Massive responses.
- IB-only, Massive-only, overlapping, and unresolved coverage maps.
- Raw, adjusted, cumulative, reverse, and ambiguous IB split histories.
- Point errors that cancel in a moving average, and moving-average errors with
  otherwise valid schemas.
- 20/50/200 session eligibility, gaps, action boundaries, and threshold edges.
- Dividend reconstruction that is independent of persisted Silver factors.
- Complete, partial, corrected, cancelled, and unavailable corporate-action
  reference coverage, including historical adjustment factors.
- Cursor invalidation for every identity input and atomic resume behavior.
- Input-root binding, changed-input detection, and output-root rejection.

### Integration tests

- Synthetic full histories containing splits and dividends, with exact expected
  result distributions.
- Mocked two-provider history where Massive covers recent dates and IB covers the
  older range.
- A deliberate Bronze point corruption that fails even when its moving-average
  effect is offset by another corruption.
- A deliberate Silver dividend-factor corruption that passes split-only checks
  and fails total-return checks.
- Read-only proof that hashes and revision manifests remain unchanged.

### Live smoke validation

- Run AAPL, MSFT, NVDA, SPY, and PLTR first to exercise raw, adjusted, split,
  dividend, and control behavior.
- Confirm exact or within-threshold Massive overlap, including provider SMA
  endpoint comparisons where entitled.
- Confirm fresh IB fills the older Massive gaps and is labelled as same-provider
  replay evidence.
- Resume an interrupted run and prove already valid checkpoints are reused.
- Run the complete local equity universe and require zero unresolved dates before
  declaring the data-quality gate successful.

### Repository gates

Run focused tests throughout development, then Ruff, Pyright, RuntimeWarning
enforcement, and the CI-equivalent suite with configured coverage at or above 95
percent.

## Rollout

1. Land the read-only engine, reports, cursor, and focused tests.
2. Run the five-symbol live smoke and review error distributions.
3. Run the full universe with Massive-first/IB-fallback coverage.
4. Review every failed or unresolved symbol; do not widen thresholds to absorb
   unexplained clusters.
5. Only after a clean validation run, use a separate explicitly approved repair
   workflow for any source data changes and rerun the complete validator.
