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
- Publish canonical per-symbol `1m` bronze Parquet through resumable hash
  buckets.
- Materialize `5m`, `30m`, and `1h` from canonical `1m` for every discovered
  symbol.
- Make flat files the only equity-intraday source for `backfill-all`,
  `daily-backfill`, manual backfills, and repairs.
- Remove the old ticker-filtered flat-file implementation and Massive REST
  equity-intraday implementation rather than preserving compatibility paths.
- Keep equity `1d`, volatility, futures, rates, and other non-equity lanes
  unchanged.

## Out of Scope

- Replacing the equity `1d` source. The minute flat files do not replace the
  current daily-bar pipeline.
- Using Massive flat files for volatility, futures, options, forex, or rates.
- Replacing canonical per-symbol bronze Parquet with a database.
- Automatically deleting old raw files after publication.
- Building adjustment-factor or corporate-action processing.
- Preserving compatibility with the current `flatfile-ingest --preset`,
  `flatfile-ingest --tickers`, or equity `intraday-backfill --source massive`
  command shapes.

## Architecture

The pipeline has three durable stages:

```text
Massive S3 daily .csv.gz
        |
        v
raw/massive/us_stocks_sip/minute_aggs_v1/date=YYYY-MM-DD/
  bucket=000/part.parquet ... bucket=255/part.parquet + _SUCCESS
        |
        v
symbol-oriented historical publish, one bucket at a time
        |
        v
bronze/asset_class=equity/symbol=<encoded-ticker>/1m.parquet
        |
        v
derive and publish 5m / 30m / 1h once per ticker
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
normalized raw Parquet dataset partitioned by trade date and stable ticker-hash
bucket. Bucketed staging keeps memory bounded and lets historical publication
read one subset of the full market across all available dates.

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

Provider ticker spelling and case are preserved exactly. Unsafe filesystem path
characters are encoded only when constructing bronze partition paths.

Each raw date also writes `_symbols.parquet`, containing the exact distinct
ticker set present on that trading day. Coverage uses this date-specific set so
historical or delisted symbols are not incorrectly expected on later dates.

The date partition is built in a temporary directory, validated, marked with
`_SUCCESS`, then renamed into place. Readers ignore any partition without
`_SUCCESS`. A completed raw partition is immutable unless an explicit repair
command performs a recoverable old/temp/final directory swap.

The downloader writes a manifest entry per date containing status, object key,
row count, symbol count, file size, checksum, and completion timestamp.

## Bronze Publish Stage

Historical publication is symbol-oriented:

1. Read one stable hash bucket across all available raw dates.
2. K-way merge the sorted daily bucket readers and stream/group rows by ticker
   across the complete available range.
3. Assign the stable Livewire `symbol_id`.
4. Publish each ticker's complete entitled `1m` history once.
5. Derive and publish its complete `5m`, `30m`, and `1h` history once.
6. Record per-ticker and per-bucket completion only after atomic publication.

This changes the historical write complexity from one complete ticker rewrite
per trading day to one complete write per ticker and timeframe.

Routine daily catch-up uses a one-day publish operation. It downloads the latest
complete trading-day file once, then publishes all symbols found in that file.

## Derived Timeframes

Canonical equity intraday is `1m`. After historical or catch-up publication of
`1m`, the pipeline
derives `5m`, `30m`, and `1h` locally using the existing lossless OHLCV
aggregator.

Historical derivation runs once from each ticker's complete `1m` history.
Routine catch-up and repair read the affected timestamp range plus the minimum
overlap needed to complete the first and last target windows, then merge only
those affected windows.

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
- raw date buckets completed
- historical publish buckets and tickers completed
- derived-timeframe tickers completed
- last successful run and aggregate counters

All state transitions occur after durable output publication. Re-running a
completed bucket or date is idempotent.

## Orchestrator Integration

### Full Backfill

`backfill-all` uses the full-market flat-file pipeline for equity intraday. It
performs:

1. history discovery
2. missing raw-date downloads
3. symbol-oriented `1m` bronze publication
4. derived timeframe publication
5. existing non-equity lanes and optional Postgres rebuild

If S3 credentials are absent, the job fails immediately with a clear
configuration error.

### Daily Catch-Up

`daily-backfill` uses the flat-file pipeline for equity intraday. It requests
missing raw files from a configurable recent lookback window, then publishes
all symbols found. Missing S3 credentials are a hard configuration failure.

### Targeted Repair

Targeted equity-intraday repair replays the relevant whole-market raw day. If
the raw partition is absent or explicitly marked for replacement, it downloads
that Massive daily flat file again. There is no REST repair path.

## Error Handling

- Authentication or entitlement failures stop the flat-file lane immediately.
- Provider object-listing and `head_object` behavior is verified with a
  read-only live probe before implementation locks in error classification.
- Transient download failures are retried with bounded exponential backoff.
- Missing objects on expected trading days are recorded and reported, not
  silently treated as market holidays.
- A raw partition is never marked complete until its Parquet validates.
- A historical publish bucket is never marked complete until all ticker
  publications in the bucket succeed.
- Partial bucket failure is resumable at ticker level without re-downloading
  raw files.
- Derived-timeframe failure does not invalidate canonical `1m`, but the ticker
  remains incomplete until derivation succeeds.
- Logs and final summaries expose dates downloaded, bytes read, symbols found,
  rows published, buckets completed, skipped work, and failures.
- Logs never include S3 access or secret key values.

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

The existing `flatfile-ingest` implementation is replaced by the full-market
pipeline. The old required `--preset` / `--tickers` interface is removed.

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
MDW_FLATFILE_BUCKETS=256
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
- historical publication writes each ticker once for the complete available
  range
- interrupted downloads and interrupted publish buckets resume idempotently
- stable symbol IDs are preserved
- provider ticker values that are unsafe as path components are encoded and
  decode back to the original ticker
- derived bars remain correct across catch-up and repair boundaries
- `backfill-all` requires flat files and refuses silent full-market REST fallback
- `daily-backfill` requires flat files and fails clearly without S3 credentials
- legacy ticker-filtered flat-file and equity-intraday REST command paths are
  removed
- targeted repair replays whole-market raw partitions without REST calls
- dry-run and capacity estimates perform no downloads or writes
- the configured 100% coverage gate and RuntimeWarning guard pass

## Rollout

1. Implement and test history discovery, raw staging, and manifests.
2. Run a small read-only provider-contract probe and entitlement/capacity
   discovery.
3. Ingest and verify one historical month into an isolated warehouse root,
   including a bounded-memory bucket publish.
4. Compare sampled symbols and bars against a one-off read-only provider probe
   kept outside the runtime implementation.
5. Run a full-market one-day catch-up into the production warehouse.
6. Switch `daily-backfill` to the flat-file-only path and remove its REST lane.
7. Run the resumable full historical backfill.
8. Switch `backfill-all` to the flat-file-only path, remove legacy code, and
   update operational docs.

## Rollback

There is no runtime fallback to the removed implementation. Rollback is a code
rollback through Git/PR:

1. Disable the scheduled intraday-catchup launchd job.
2. Revert the replacement PR.
3. Re-enable the prior scheduled job only after the revert is deployed.

Raw Massive partitions are additive and may remain on disk during rollback.
Bronze publication remains atomic, so rollback does not require restoring
partially written Parquet files.

## Success Criteria

- One Massive download per accessible trading day covers the full U.S. equity
  market.
- Every symbol present in the files is published to canonical `1m` bronze.
- `5m`, `30m`, and `1h` are materialized for every published symbol.
- A full run resumes without repeating completed downloads, buckets, or
  tickers.
- No equity-intraday ingestion path issues ticker-by-ticker REST requests.
- Full historical publication avoids per-day complete Parquet rewrites.
