# Sub-C: Massive Daily Accelerator and Validation

## Decision

Sub-C adds Massive as a near-term daily U.S. equity/ETF accelerator and second-source validation reference. It does not replace Interactive Brokers as the canonical broker-aligned provider, and it does not add intraday or new timeframes.

Ship Option B:

- explicit operator-selected Massive daily path,
- narrow Massive recent-gap recovery after IB misses target daily bars,
- existing public fallback chain after Massive.

Fallback order for recent U.S. equity/ETF daily gaps is:

1. IB daily bars.
2. Massive daily bars.
3. Existing public fallback chain: Nasdaq stocks, Nasdaq ETF, Stooq U.S.

The reason for putting Massive before public fallbacks is product intent: Massive is the paid fast path for near-term daily bars. Putting it after slower public recovery would hide the accelerator behind a last-resort role.

## Boundaries

In scope:

- `clients/massive_client.py` for stock daily aggregates.
- `MASSIVE_API_KEY` as the auth environment variable.
- request and rate-limit telemetry for Massive and UW.
- `daily --source massive` for equity/ETF daily runs without IB Gateway preflight.
- narrow Massive recovery for unresolved recent equity daily gaps.
- `row_count_anomaly` using Massive reference coverage.
- source/outcome counters and metadata so Massive writes are visible.

Out of scope:

- intraday, 15m/30m/4h, or broader multi-timeframe behavior,
- futures, CMDTY, FX, CBOE volatility provider changes,
- option chains,
- gold or factor tables,
- DuckDB canonical write paths,
- making Massive or UW canonical storage.

## Massive Contract

The dashboard URL `https://massive.com/dashboard/rest` redirects to signup without an authenticated browser session, so the implementation relies on public docs plus the authenticated live smoke performed in this session.

Provider endpoints:

- Per-ticker daily custom bars:
  - `GET /v2/aggs/ticker/{stocksTicker}/range/1/day/{from}/{to}`
  - use `adjusted=false`, `sort=asc`, `limit=50000`.
  - returns `results[].t` as Unix milliseconds for the aggregate start.
  - returns `results[].o/h/l/c/v`, optional `vw/n/otc`.
- Grouped daily bars:
  - `GET /v2/aggs/grouped/locale/us/market/stocks/{date}`
  - use `adjusted=false`, `include_otc=false`.
  - returns `results[].T` plus `results[].t/o/h/l/c/v`, optional `vw/n/otc`.
  - docs describe `t` as the aggregate end timestamp.

Authentication:

- base URL `https://api.massive.com`.
- `Authorization: Bearer <token>`.
- token comes from `MASSIVE_API_KEY`.

Normalization:

- Store unadjusted bars to match current IB semantics. Current bronze rows set `adj_close = close`; Massive rows do the same.
- Derive `trade_date` from `t` in `America/New_York`.
- Test both per-ticker midnight ET timestamps and grouped market-close ET timestamps.
- Require finite positive OHLC, `high >= low`, `high >= open/close`, and `low <= open/close`.
- Require finite non-negative volume. Store `int(round(v))`.
- Keep metadata showing raw volume when rounding changes it.

Rate limits and errors:

- Live smoke did not expose rate-limit headers.
- Client still handles HTTP 429 with `Retry-After` when present.
- Retry only 429, 5xx, and transport timeouts.
- Auth, validation, not-found, and malformed payload errors should be typed and non-retryable unless the status is retryable.

## UW Role

UW remains telemetry-only for Sub-C. `clients/uw_client.py` already has an authenticated retrying request layer, but UW daily OHLC semantics are not needed for the Sub-C daily warehouse write path. Add telemetry to make UW requests observable without introducing another validation source.

## Ingest Behavior

`scripts/livewire_ingest.py` must route IB preflight based on the actual provider:

- default `daily` and `daily --source ib` still preflight IB Gateway.
- `daily --source massive` bypasses IB preflight.
- `historical`, `robust`, intraday commands, `universe`, and `backfill-all` keep existing IB preflight behavior.

The explicit Massive daily command is:

```bash
python scripts/livewire_ingest.py daily --asset-class equity --source massive
```

It is equity-only. Non-equity `--source massive` fails fast before fetching.

## Quality Behavior

`row_count_anomaly` becomes active only when metadata includes `reference_source`.

Reference contract:

```python
{
    "source": "massive",
    "expected_count": 252,
    "actual_count": 250,
    "expected_dates": ["2025-01-02", "..."],
    "actual_dates": ["2025-01-02", "..."],
}
```

Thresholds:

- warning when absolute percent delta is greater than 1%.
- critical when absolute percent delta is greater than 5%.
- no flag when no reference source exists.

Flag detail must include source, expected/reference count, actual count, percent delta, threshold, and missing-date details when dates are supplied.

## Operator Visibility

Massive-sourced actions must show:

- requested/fetched/published/skipped/failed counters,
- source counters for `ib`, `massive`, and existing public fallback providers,
- telemetry records tagged `source="massive"`,
- quality metadata source context for sidecar, audit JSONL, alerts, and Postgres reliability import.
