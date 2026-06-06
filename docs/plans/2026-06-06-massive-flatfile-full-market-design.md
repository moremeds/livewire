# Massive Flat-File Full-Market Ingestion Design

## Goal

Make Massive minute-aggregate flat files the default source for U.S. equity
intraday ingestion. Ingest every symbol present in the available daily files,
backfill the maximum history allowed by the configured Massive plan, and keep
the warehouse current with one whole-market download per trading day.

The expected entitlement is five years, but the implementation must discover
the actual available range instead of hard-coding that assumption.

## Current Problem

Livewire already has a Massive flat-file client and a `flatfile-ingest`
subcommand, but the production paths do not use them:

- `daily-backfill` invokes Massive REST separately for `1m`, `5m`, and `1h`.
- `backfill-all` invokes Massive REST separately for each timeframe and preset.
- The unified CLI can select flat files, but the scheduled orchestrators cannot.
- Documentation incorrectly says the orchestrators auto-detect S3 credentials.
- `flatfile-ingest` requires a preset or explicit ticker list, so it cannot
  publish every symbol in a whole-market file.
- The current flat-file writer merges each ticker after every downloaded day.
  Because each merge reads and rewrites the ticker's complete Parquet snapshot,
  a deep historical run performs excessive repeated I/O.

## Scope

- Ingest every symbol present in Massive's
  `us_stocks_sip/minute_aggs_v1` daily files.
- Discover and backfill the maximum accessible history.
- Preserve downloaded whole-market days as raw Parquet artifacts.
- Publish canonical per-symbol `1m` bronze Parquet in resumable batches.
- Materialize `5m`, `30m`, and `1h` from canonical `1m` for every discovered
  symbol.
- Make flat files the default equity-intraday source for `backfill-all` and
  `daily-backfill` when S3 credentials are available.
- Keep Massive REST as a narrow fallback for targeted ticker/date repairs.
- Keep equity `1d`, volatility, futures, rates, and other non-equity lanes
  unchanged.

## Out of Scope

- Replacing the equity `1d` source. The minute flat files do not replace the
  current daily-bar pipeline.
- Using Massive flat files for volatility, futures, options, forex, or rates.
- Replacing canonical per-symbol bronze Parquet with a database.
- Automatically deleting old raw files after publication.
- Building adjustment-factor or corporate-action processing.

## Architecture

The pipeline has three durable stages:

```text
Massive S3 daily .csv.gz
        |
        v
raw/massive/us_stocks_sip/minute_aggs_v1/date=YYYY-MM-DD/data.parquet
        |
        v
monthly publish batch grouped by ticker
        |
        v
bronze/asset_class=equity/symbol=<ticker>/1m.parquet
        |
        v
derive and publish 5m / 30m / 1h
```

Raw daily Parquet is a replayable provider artifact. Bronze remains the
canonical normalized store used by Livewire consumers.

## Available-History Discovery

The job must not assume exactly five years. It probes backward by month from the
latest complete U.S. trading day until it identifies the first accessible
month, then enumerates expected exchange trading days within the accessible
range.

Discovery distinguishes:

- `available`: the daily object exists and is readable.
- `not_entitled_or_before_history`: the object is outside accessible history.
- `market_closed`: no object is expected for that date.
- `transient_failure`: network, authentication, throttling, or server failure.

The discovered earliest date and probe result are persisted in a state file so
routine jobs do not repeat entitlement discovery.

## Raw Download Stage

Each accessible Massive daily gzip is downloaded once and converted to a
normalized raw Parquet file partitioned by trade date.

The raw schema retains provider ticker and minute OHLCV:

```text
ticker: string
bar_timestamp: timestamp[us, UTC]
open: float64
high: float64
low: float64
close: float64
volume: int64
```

Publication uses the existing atomic Parquet helper. A completed raw partition
is immutable unless an explicit repair command replaces it.

The downloader writes a manifest entry per date containing status, object key,
row count, symbol count, file size, checksum, and completion timestamp.

## Bronze Publish Stage

Historical publication runs in configurable calendar-month batches:

1. Read all available raw daily Parquet partitions for the month.
2. Group rows by ticker across the batch.
3. Assign the stable Livewire `symbol_id`.
4. Merge each ticker into `1m.parquet` once for the entire batch.
5. Record per-ticker and per-batch completion only after atomic publication.

This changes the historical write complexity from one complete ticker rewrite
per trading day to one rewrite per ticker per monthly batch.

Routine daily catch-up uses a one-day publish batch. It downloads the latest
complete trading-day file once, then publishes all symbols found in that file.

## Derived Timeframes

Canonical equity intraday is `1m`. After a batch publishes `1m`, the pipeline
derives `5m`, `30m`, and `1h` locally using the existing lossless OHLCV
aggregator.

For correctness at batch boundaries, derivation reads the affected timestamp
range plus the minimum overlap needed to complete the first and last target
windows. The derived writer then merges only those affected windows.

All discovered symbols receive all four intraday timeframes. This is
storage-intensive but matches the approved full-market warehouse goal and
keeps downstream queries simple.

## State And Resume

State lives under `~/market-warehouse/cursors/` and is separate from raw and
bronze data:

```text
massive_flatfile_manifest.jsonl
massive_flatfile_state.json
```

The state records:

- discovered earliest accessible date
- latest checked/downloaded raw date
- raw dates completed, unavailable, or failed
- publish batches completed
- derived-timeframe batches completed
- last successful run and aggregate counters

All state transitions occur after durable output publication. Re-running a
completed batch is idempotent.

## Orchestrator Integration

### Full Backfill

`backfill-all` uses the full-market flat-file pipeline for equity intraday when
S3 credentials are present. It performs:

1. history discovery
2. missing raw-date downloads
3. monthly `1m` bronze publication
4. derived timeframe publication
5. existing non-equity lanes and optional Postgres rebuild

If S3 credentials are absent, the job fails the equity-intraday lane with a
clear configuration error. It must not silently fall back to thousands of REST
requests for a full-market build.

### Daily Catch-Up

`daily-backfill` uses the flat-file pipeline for equity intraday when S3
credentials are present. It requests missing raw files from a configurable
recent lookback window, then publishes all symbols found.

If S3 credentials are absent, the current managed-universe REST path remains
available as a degraded fallback so scheduled catch-up still has a recovery
path.

### Targeted Repair

Massive REST remains the appropriate tool for narrow repairs where downloading
and replaying a whole-market day is unnecessary:

- explicit ticker list
- explicit short date range
- coverage repair for a small missing-symbol set

## Error Handling

- Authentication or entitlement failures stop the flat-file lane immediately.
- Transient download failures are retried with bounded exponential backoff.
- Missing objects on expected trading days are recorded and reported, not
  silently treated as market holidays.
- A raw partition is never marked complete until its Parquet validates.
- A bronze batch is never marked complete until all ticker publications for the
  batch succeed.
- Partial batch failure is resumable at ticker level without re-downloading raw
  files.
- Derived-timeframe failure does not invalidate canonical `1m`, but the batch
  remains incomplete until derivation succeeds.
- Logs and final summaries expose dates downloaded, bytes read, symbols found,
  rows published, batches completed, skipped work, and failures.

## Storage And Capacity

The pipeline intentionally retains full-market raw daily Parquet plus four
per-symbol bronze timeframes. Before a full backfill starts, it estimates:

- number of accessible trading days
- raw compressed bytes from sampled object sizes
- projected raw Parquet size
- projected bronze and derived size
- required free-space headroom

The full backfill refuses to start when free space is below the configured
safety threshold. Daily catch-up reports low space but only fails when it
cannot safely publish.

## CLI And Configuration

The existing `flatfile-ingest` command becomes the operator surface for the
full-market pipeline.

Planned modes:

```bash
python scripts/livewire_ingest.py flatfile-ingest discover
python scripts/livewire_ingest.py flatfile-ingest backfill
python scripts/livewire_ingest.py flatfile-ingest catch-up
python scripts/livewire_ingest.py flatfile-ingest repair --dates 2026-06-01
```

Relevant configuration:

```text
MASSIVE_S3_ACCESS_KEY
MASSIVE_S3_SECRET_KEY
MDW_FLATFILE_BATCH_MONTHS=1
MDW_FLATFILE_LOOKBACK_DAYS=7
MDW_FLATFILE_MIN_FREE_GB=<safety threshold>
MDW_FLATFILE_RAW_RETENTION=keep
```

## Testing

Tests must prove:

- history discovery stops at the entitlement boundary and distinguishes
  unavailable objects from transient failures
- exchange holidays are not requested
- raw daily CSV converts to validated atomic Parquet
- all symbols are retained when no ticker filter is supplied
- monthly publication merges each ticker once per batch
- interrupted downloads and interrupted publish batches resume idempotently
- stable symbol IDs are preserved
- derived bars remain correct across batch boundaries
- `backfill-all` prefers flat files and refuses silent full-market REST fallback
- `daily-backfill` prefers flat files but retains explicit degraded REST fallback
- targeted REST repair behavior remains unchanged
- dry-run and capacity estimates perform no downloads or writes
- the configured 100% coverage gate and RuntimeWarning guard pass

## Rollout

1. Implement and test history discovery, raw staging, and manifests.
2. Run a small read-only entitlement discovery and capacity estimate.
3. Ingest and verify one historical month into an isolated warehouse root.
4. Compare sampled symbols and bars against Massive REST.
5. Run a full-market one-day catch-up into the production warehouse.
6. Enable `daily-backfill` flat-file preference.
7. Run the resumable full historical backfill.
8. Enable `backfill-all` flat-file default and update operational docs.

## Success Criteria

- One Massive download per accessible trading day covers the full U.S. equity
  market.
- Every symbol present in the files is published to canonical `1m` bronze.
- `5m`, `30m`, and `1h` are materialized for every published symbol.
- A full run resumes without repeating completed downloads or batches.
- Scheduled equity intraday catch-up no longer issues ticker-by-ticker REST
  requests when S3 credentials are configured.
- Full historical publication avoids per-day complete Parquet rewrites.
