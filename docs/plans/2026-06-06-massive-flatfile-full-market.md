# Massive Flat-File Full-Market Ingestion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace all legacy equity-intraday ingestion with a resumable full-market Massive flat-file pipeline that ingests every available symbol for the maximum entitled history.

**Architecture:** Download each Massive whole-market minute file once, normalize it into immutable date-and-ticker-bucketed raw Parquet, then stream one bucket across the full available range so each ticker's canonical `1m`, `5m`, `30m`, and `1h` snapshots are written once during the historical build. Durable manifests and atomic state make downloads and publication resumable. There is no backwards-compatible ticker-filtered flat-file or Massive REST equity-intraday path.

**Tech Stack:** Python 3.13, boto3 S3-compatible client, PyArrow/Parquet, existing atomic `publish_parquet`, existing `IntradayBronzeClient`, pytest/pytest-cov, ruff, pyright.

---

## Dependency Graph

```text
T0 -> T1 -> T2 -> T3 -> T4 -> T5
T3 -> T6
T4 -> T6
T5 -> T6 -> T7 -> T8 -> T9
```

## Replacement Boundary

This plan intentionally removes:

- `flatfile-ingest --preset ...`
- `flatfile-ingest --tickers ...`
- the current per-day/per-ticker `ingest_date()` writer
- equity `intraday-backfill --source massive`
- Massive REST intraday methods and tests
- unified CLI equity-intraday `--source massive` routing
- scheduler and full-backfill REST equity-intraday commands

It preserves Massive REST support for equity `1d` ingestion and validation.

## Complexity And Risk

| Task | Size | Primary Risk |
|---|---|---|
| T0-T1 | M | External S3-compatible error and listing semantics |
| T2-T3 | L | Crash-safe multi-file raw partitions and resume state |
| T4-T5 | L | Entitlement discovery, storage estimates, long-running downloads |
| T6 | XL | Bounded-memory full-market transpose and per-symbol publication |
| T7-T8 | L | Removing coupled legacy paths without affecting non-equity or `1d` |
| T9 | L | Live data volume, storage capacity, and interruption testing |

### Task T0: Lock the replacement contract with failing tests

depends_on: []

**Files:**
- Modify: `tests/test_script_consolidation.py`
- Modify: `tests/test_livewire_cli.py`
- Modify: `tests/test_livewire_entrypoints.py`
- Modify: `tests/test_backfill_intraday.py`
- Modify: `tests/test_ingest_flatfiles.py`

**Steps:**

1. Replace legacy flat-file CLI tests with tests asserting:
   - `flatfile-ingest` requires a mode: `discover`, `backfill`, `catch-up`, or `repair`.
   - `--preset` and `--tickers` are rejected.
   - `backfill` and `catch-up` dispatch to `livewire_scripts.ingest_flatfiles`.
2. Add tests asserting equity `intraday-backfill --source massive` is rejected.
3. Add tests asserting unified `livewire backfill` routes equity intraday to the flat-file pipeline and does not construct Massive REST intraday commands.
4. Add tests asserting `daily-backfill` and `backfill-all` fail clearly when S3 credentials are absent.
5. Run:

   ```bash
   source ~/market-warehouse/.venv/bin/activate
   python -m pytest \
     tests/test_script_consolidation.py \
     tests/test_livewire_cli.py \
     tests/test_livewire_entrypoints.py \
     tests/test_backfill_intraday.py \
     tests/test_ingest_flatfiles.py -q
   ```

   Expected: failures proving the old interfaces still exist and the new modes do not.
6. Commit the red tests:

   ```bash
   git add tests
   git commit -m "test: define full-market flat-file replacement contract"
   ```

### Task T1: Add S3 object classification and exchange-calendar enumeration

depends_on: [T0]

**Files:**
- Modify: `clients/massive_flatfile_client.py`
- Modify: `clients/trading_calendar.py`
- Modify: `tests/test_massive_flatfile_client.py`
- Modify: `tests/test_trading_calendar.py`

**Implementation:**

1. Before fixing the object-status contract, run a read-only provider probe
   using configured credentials against one known available trading day, one
   exchange holiday, one date older than expected entitlement, and one
   malformed key. Record whether Massive permits `head_object`,
   `list_objects_v2`, or both; exact error codes; object sizes; sample ticker
   ordering; and any unsafe ticker path characters. Do not print secrets. If
   credentials are not configured, this task is blocked.
2. Remove weekday-only `trading_dates_between()` from
   `clients/massive_flatfile_client.py`.
3. Add an inclusive exchange-calendar helper to `clients/trading_calendar.py`:

   ```python
   def trading_dates_in_range(start: date, end: date) -> list[date]:
       return [
           start + timedelta(days=offset)
           for offset in range((end - start).days + 1)
           if is_trading_day(start + timedelta(days=offset))
       ]
   ```

4. Add typed flat-file object states:

   ```python
   class FlatfileObjectStatus(StrEnum):
       AVAILABLE = "available"
       NOT_FOUND = "not_found"
       FORBIDDEN = "forbidden"
       TRANSIENT_ERROR = "transient_error"

   @dataclass(frozen=True)
   class FlatfileObjectInfo:
       date: date
       key: str
       status: FlatfileObjectStatus
       size_bytes: int | None = None
       etag: str | None = None
       error: str | None = None
   ```

5. Add `MassiveFlatfileClient.inspect_date(d)` using the verified supported
   provider operation. Classify unavailable, forbidden, and retryable failures
   from observed Massive behavior rather than generic AWS assumptions.
6. Add `download_date_to_path(d, destination)` that streams the gzip to a
   caller-controlled path without parsing it into memory.
7. Test holiday exclusion, object-key construction, status classification,
   streamed downloads, cleanup on failure, and secret redaction.
8. Run:

   ```bash
   python -m pytest tests/test_massive_flatfile_client.py tests/test_trading_calendar.py -q
   ```

9. Commit:

   ```bash
   git add clients/massive_flatfile_client.py clients/trading_calendar.py tests/test_massive_flatfile_client.py tests/test_trading_calendar.py
   git commit -m "feat: classify Massive flat-file objects"
   ```

### Task T2: Add atomic bucketed raw daily Parquet staging

depends_on: [T1]

**Files:**
- Create: `clients/massive_flatfile_store.py`
- Create: `clients/symbol_paths.py`
- Create: `tests/test_massive_flatfile_store.py`
- Create: `tests/test_symbol_paths.py`
- Modify: `clients/parquet_io.py`
- Modify: `tests/test_parquet_io.py`
- Modify: `clients/__init__.py`

**Implementation:**

1. Define the raw schema:

   ```python
   RAW_FLATFILE_SCHEMA = pa.schema([
       ("ticker", pa.string()),
       ("bar_timestamp", pa.timestamp("us", tz="UTC")),
       ("open", pa.float64()),
       ("high", pa.float64()),
       ("low", pa.float64()),
       ("close", pa.float64()),
       ("volume", pa.int64()),
   ])
   ```

2. Add reversible symbol path helpers. Ordinary tickers such as `AAPL`,
   `BRK.B`, and `BF-B` keep their existing directory names; unsafe path
   characters are percent-encoded and decode back to the exact provider ticker.
3. Add `publish_parquet_directory()` to `clients/parquet_io.py`. It builds and
   validates a temporary directory and writes `_SUCCESS` last. New date
   partitions rename directly into place. Explicit repair uses a recoverable
   old/temp/final swap and state reconciliation on restart. Do not use
   single-file `publish_parquet()` for composite-key raw datasets.
4. Implement `MassiveFlatfileStore` with:
   - `raw_path(d)` returning
     `raw/massive/us_stocks_sip/minute_aggs_v1/date=YYYY-MM-DD/`
   - `has_raw_date(d)`
   - `stage_gzip(d, gzip_path, bucket_count)` using streaming CSV batches,
     stable-symbol-ID hash buckets, bounded spill files, and an external merge
     rather than `gzip.decompress(...).decode(...)`
   - `scan_bucket_by_ticker(bucket, dates)` performing a bounded k-way merge of
     the sorted daily bucket files
   - `raw_stats(d)` returning row count, distinct symbol count, file size, and
     timestamp bounds
   - `_symbols.parquet` containing the exact distinct provider ticker set for
     the date
5. Externally sort each bucket by exact provider `ticker`, then
   `bar_timestamp`; validate composite keys; then publish the complete date
   directory.
6. Reject malformed numbers, naive timestamps, empty files, duplicate
   `(ticker, bar_timestamp)` keys, and non-monotonic ticker partitions.
7. Test that unfiltered staging preserves every ticker and its case, handles
   unsafe ticker path characters, stays bounded to configured chunk/spill
   sizes, k-way merges tickers correctly across multiple dates, never exposes a
   partial date partition, and recovers repair swaps interrupted at each rename
   boundary.
8. Run:

   ```bash
   python -m pytest tests/test_massive_flatfile_store.py tests/test_symbol_paths.py tests/test_parquet_io.py -q
   ```

9. Commit:

   ```bash
   git add clients/massive_flatfile_store.py clients/symbol_paths.py clients/parquet_io.py clients/__init__.py tests/test_massive_flatfile_store.py tests/test_symbol_paths.py tests/test_parquet_io.py
   git commit -m "feat: stage Massive daily flat files as raw parquet"
   ```

### Task T3: Add durable manifest and resume state

depends_on: [T2]

**Files:**
- Create: `clients/massive_flatfile_state.py`
- Create: `tests/test_massive_flatfile_state.py`

**Implementation:**

1. Add dataclasses for raw-date and publish-batch records.
2. Store append-only events in
   `~/market-warehouse/cursors/massive_flatfile_manifest.jsonl`.
3. Store the compact current snapshot in
   `~/market-warehouse/cursors/massive_flatfile_state.json`.
4. Implement atomic snapshot writes using `tmp.replace(path)`.
5. State transitions must be explicit:
   - `raw_started -> raw_completed | raw_failed | raw_unavailable`
   - `bucket_started -> bucket_completed | bucket_failed`
   - `ticker_started -> ticker_completed | ticker_failed`
6. Ignore truncated final JSONL lines during recovery, but reject malformed
   non-final records.
7. Test restart recovery, duplicate completion idempotency, failure retry,
   atomic state writes, and corrupted-state behavior.
8. Run:

   ```bash
   python -m pytest tests/test_massive_flatfile_state.py -q
   ```

9. Commit:

   ```bash
   git add clients/massive_flatfile_state.py tests/test_massive_flatfile_state.py
   git commit -m "feat: add resumable Massive flat-file state"
   ```

### Task T4: Add entitlement discovery and capacity planning

depends_on: [T3]

**Files:**
- Create: `livewire_scripts/flatfile_planner.py`
- Create: `tests/test_flatfile_planner.py`
- Modify: `livewire_scripts/ingest_flatfiles.py`

**Implementation:**

1. Add `discover_available_range()` that probes backward by month from the
   latest complete trading day, then narrows to the earliest accessible date.
2. Treat `FORBIDDEN` as a hard credential/entitlement error. Treat `NOT_FOUND`
   before the entitlement boundary as unavailable history. Retry
   `TRANSIENT_ERROR`; never reinterpret it as unavailable history.
3. Persist the earliest accessible date and discovery timestamp in state.
4. Add capacity estimation based on sampled S3 object sizes and configurable
   multipliers for raw Parquet and four bronze timeframes.
5. Add free-space gating with `MDW_FLATFILE_MIN_FREE_GB`.
6. Implement:

   ```bash
   python scripts/livewire_ingest.py flatfile-ingest discover
   ```

   It must be read-only except for discovery state and print the accessible
   range, expected trading days, sampled bytes, projected storage, and current
   free space.
7. Add `--dry-run` to `backfill`, `catch-up`, and `repair`; dry-run performs
   planning only.
8. Test all boundary/error classifications and no-write dry-run behavior.
   Include tests that formatted plans and errors do not contain credentials.
9. Run:

   ```bash
   python -m pytest tests/test_flatfile_planner.py tests/test_ingest_flatfiles.py -q
   ```

10. Commit:

   ```bash
   git add livewire_scripts/flatfile_planner.py livewire_scripts/ingest_flatfiles.py tests/test_flatfile_planner.py tests/test_ingest_flatfiles.py
   git commit -m "feat: discover Massive flat-file history and capacity"
   ```

### Task T5: Add resumable raw downloader

depends_on: [T4]

**Files:**
- Create: `livewire_scripts/flatfile_downloader.py`
- Create: `tests/test_flatfile_downloader.py`
- Modify: `livewire_scripts/ingest_flatfiles.py`

**Implementation:**

1. Implement a downloader that:
   - accepts an ordered trading-date list
   - skips raw dates already completed in state and present on disk
   - downloads to a temporary gzip
   - stages validated raw Parquet
   - records manifest/state only after raw publication
   - retries transient failures with bounded exponential backoff
   - removes temporary gzip files in all outcomes
2. Missing files on expected trading days must be recorded and reported as
   failures, not silently skipped.
3. Add aggregate counters: inspected, downloaded, skipped, unavailable,
   failed, bytes, rows, and symbols.
4. Wire:

   ```bash
   flatfile-ingest backfill
   flatfile-ingest catch-up
   flatfile-ingest repair --dates YYYY-MM-DD [YYYY-MM-DD ...]
   ```

   through the raw downloader before publication.
5. Test interrupted-run resume, retry exhaustion, expected-day missing objects,
   repair replacement, and idempotent reruns.
6. Run:

   ```bash
   python -m pytest tests/test_flatfile_downloader.py tests/test_ingest_flatfiles.py -q
   ```

7. Commit:

   ```bash
   git add livewire_scripts/flatfile_downloader.py livewire_scripts/ingest_flatfiles.py tests/test_flatfile_downloader.py tests/test_ingest_flatfiles.py
   git commit -m "feat: download Massive flat files resumably"
   ```

### Task T6: Add symbol-oriented bronze publication and bounded derivation

depends_on: [T3, T4, T5]

**Files:**
- Modify: `clients/intraday_bronze_client.py`
- Modify: `clients/bronze_client.py`
- Modify: `clients/symbol_paths.py`
- Create: `livewire_scripts/flatfile_publisher.py`
- Create: `tests/test_flatfile_publisher.py`
- Modify: `tests/test_intraday_bronze_client.py`
- Modify: `tests/test_bronze_client.py`

**Implementation:**

1. Route bronze symbol paths and symbol discovery through the reversible
   `clients.symbol_paths` helpers so every provider ticker can be stored safely.
2. Add a range-oriented bronze merge API for catch-up/repair that preserves
   rows outside the affected range.
3. For historical publication, process one configured hash bucket at a time:
   - k-way merge bounded sorted raw readers across the complete available range
   - stream/group rows by ticker without loading a full-market month
   - assign stable symbol IDs
   - publish each symbol's complete `1m` snapshot once
   - derive and publish complete `5m`, `30m`, and `1h` snapshots once
   - checkpoint each ticker and bucket
4. For catch-up/repair, derive from the affected `1m` range with enough overlap
   to produce correct first and last windows.
5. Never mark a bucket complete until every discovered ticker and derived
   timeframe succeeds.
6. Add a benchmark-style test proving maximum buffered rows are bounded by the
   configured scanner/bucket size rather than total full-market rows.
7. Test:
   - every source ticker is published
   - one complete write per ticker during historical publication
   - idempotent bucket replay
   - partial ticker failure resume
   - stable symbol IDs
   - reversible unsafe ticker paths
   - correct derived bars across catch-up/repair boundaries
   - rows outside a repair range remain unchanged
8. Run:

   ```bash
   python -m pytest tests/test_flatfile_publisher.py tests/test_intraday_bronze_client.py -q
   ```

9. Commit:

   ```bash
   git add clients/intraday_bronze_client.py clients/bronze_client.py clients/symbol_paths.py livewire_scripts/flatfile_publisher.py tests/test_flatfile_publisher.py tests/test_intraday_bronze_client.py tests/test_bronze_client.py
   git commit -m "feat: publish full-market intraday buckets"
   ```

### Task T7: Replace legacy equity-intraday runtime paths

depends_on: [T6]

**Files:**
- Rewrite: `livewire_scripts/ingest_flatfiles.py`
- Modify: `livewire_scripts/sync_runner.py`
- Modify: `livewire_scripts/backfill_runner.py`
- Modify: `livewire_scripts/backfill_intraday.py`
- Modify: `scripts/livewire.py`
- Modify: `scripts/livewire_ingest.py`
- Modify: `clients/massive_client.py`
- Modify: `clients/__init__.py`
- Modify/Delete relevant legacy tests

**Implementation:**

1. Make `ingest_flatfiles.py` the thin orchestration surface for `discover`,
   `backfill`, `catch-up`, and `repair`.
2. Change `daily-backfill` equity intraday phase to one command:

   ```bash
   python scripts/livewire_ingest.py flatfile-ingest catch-up
   ```

3. Change `backfill-all` equity intraday phase to one command:

   ```bash
   python scripts/livewire_ingest.py flatfile-ingest backfill
   ```

4. Make missing S3 credentials fail immediately before other full-market work.
5. Remove Massive REST equity-intraday code from `backfill_intraday.py`.
   Preserve IB-backed non-equity intraday behavior.
6. Remove Massive REST intraday methods from `MassiveClient`; preserve daily
   APIs used by equity `1d`.
7. Remove the unified CLI's old `--source s3`/auto-detection branch and route
   equity intraday directly to the replacement pipeline.
8. Delete tests that assert removed behavior; replace them with hard-rejection
   and new-routing tests.
9. Run:

   ```bash
   python -m pytest \
     tests/test_ingest_flatfiles.py \
     tests/test_sync_runner.py \
     tests/test_backfill_runner.py \
     tests/test_backfill_intraday.py \
     tests/test_massive_client.py \
     tests/test_livewire_cli.py \
     tests/test_livewire_entrypoints.py -q
   ```

10. Commit:

   ```bash
   git add -A
   git commit -m "refactor: replace equity intraday with flat files"
   ```

### Task T8: Update coverage, health, docs, and durable memory

depends_on: [T7]

**Files:**
- Modify: `livewire_scripts/coverage_report.py`
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `AGENTS.md`
- Modify: `.codex/project-memory.md`
- Modify: `docs/superpowers/specs/2026-06-05-livewire-intraday-catchup-scheduler-design.md`
- Modify: `tasks/lessons.md`
- Modify: relevant tests

**Implementation:**

1. Route equity-intraday coverage repair by missing trade date through
   `flatfile-ingest repair --dates ...`; do not construct ticker-level REST
   commands.
2. Update coverage semantics to use the symbol set present in the target
   trading day's `_symbols.parquet`, not the union of every historical symbol.
3. Remove all documentation claiming presets, ticker filters, REST fallback, or
   optional S3 preference for equity intraday.
4. Document:
   - required S3 credentials
   - raw partition layout
   - discovery/backfill/catch-up/repair commands
   - state and resume behavior
   - capacity gate
   - full-market storage impact
5. Update durable project memory because the canonical equity-intraday source
   and operational behavior changed.
6. Run stale-text checks:

   ```bash
   rg -n "flatfile-ingest --preset|flatfile-ingest --tickers|intraday-backfill.*source massive|equity intraday.*REST|prefers S3" README.md CLAUDE.md AGENTS.md .codex/project-memory.md tasks
   ```

   Expected: no active documentation describing removed behavior.
7. Run focused tests and commit:

   ```bash
   git add README.md CLAUDE.md AGENTS.md .codex/project-memory.md docs/superpowers/specs/2026-06-05-livewire-intraday-catchup-scheduler-design.md tasks/lessons.md livewire_scripts/coverage_report.py tests
   git commit -m "docs: document full-market flat-file operations"
   ```

### Task T9: Isolated validation, full verification, and rollout gate

depends_on: [T8]

**Files:**
- Modify: `tasks/todo.md`
- Modify only if validation exposes a defect: implementation/tests/docs above

**Steps:**

1. Run static and focused verification:

   ```bash
   source ~/market-warehouse/.venv/bin/activate
   python -m pytest \
     tests/test_massive_flatfile_client.py \
     tests/test_massive_flatfile_store.py \
     tests/test_massive_flatfile_state.py \
     tests/test_flatfile_planner.py \
     tests/test_flatfile_downloader.py \
     tests/test_flatfile_publisher.py \
     tests/test_ingest_flatfiles.py -q
   uv run ruff check .
   uv run ruff format --check .
   uv run pyright clients livewire_scripts
   git diff --check
   ```

2. Run read-only live discovery:

   ```bash
   python scripts/livewire_ingest.py flatfile-ingest discover
   ```

   Verify actual earliest entitlement date and capacity estimate. Do not start
   the full production backfill yet.
3. Validate one historical month in an isolated warehouse:

   ```bash
   MDW_WAREHOUSE_DIR=/tmp/livewire-flatfile-validation \
     python scripts/livewire_ingest.py flatfile-ingest repair \
       --start YYYY-MM-01 --end YYYY-MM-DD
   ```

4. Compare sampled symbols and timestamps against the raw partition and a
   one-off read-only provider probe kept outside the runtime implementation.
   Verify all discovered tickers published, stable IDs, no duplicate
   timestamps, and correct derived OHLCV.
5. Test interruption/resume by stopping after at least one completed raw date
   and one completed publish bucket, then rerunning.
6. Run the complete repository gates:

   ```bash
   source ~/market-warehouse/.venv/bin/activate
   python -m pytest tests -q --cov=clients --cov=livewire_scripts --cov-report=term-missing --cov-fail-under=100
   python -m pytest tests -q -W error::RuntimeWarning
   ```

   Expected: all tests pass and configured coverage remains 100%.
7. Update `tasks/todo.md` with exact verification evidence and mark all tasks
   complete.
8. Commit final validation fixes/evidence:

   ```bash
   git add -A
   git commit -m "test: validate full-market flat-file pipeline"
   ```

9. Before enabling the scheduled job, document the rollback procedure:
   disable launchd, revert the replacement PR, then re-enable the prior job
   only after the revert is deployed. Do not preserve a runtime fallback.

## Final Review Checklist

- [ ] No legacy ticker-filtered flat-file interface remains.
- [ ] No Massive REST equity-intraday runtime path remains.
- [ ] Equity `1d` Massive REST behavior remains intact.
- [ ] Full-market discovery is entitlement-aware and exchange-calendar-aware.
- [ ] Raw daily Parquet is immutable, validated, and resumable.
- [ ] Historical bronze writes occur once per ticker and timeframe.
- [ ] `1m`, `5m`, `30m`, and `1h` publish for every discovered symbol.
- [ ] Missing S3 credentials fail clearly.
- [ ] Logs and errors do not expose S3 credential values.
- [ ] Interrupted runs resume without repeating completed work.
- [ ] Isolated live validation passes before production rollout.
- [ ] Full test, coverage, warning, lint, format, and type gates pass.

## Plan Validation Outcome

Status: **ready with one execution blocker**.

The plan was validated against the current flat-file writer, S3 client,
orchestrators, coverage repair path, bronze clients, atomic Parquet helper, test
suite, and CI tooling. The original monthly/full-table approach was replaced
with bucketed raw staging plus a bounded k-way merge so the initial historical
build writes each ticker once and does not require full-market memory.

Execution blocker: Massive S3 credentials are not currently configured in the
scheduled env paths. Task T1 must stop until the read-only provider-contract
probe can run with valid credentials.
