# Intraday Bronze Multi-File Migration — Design

Status: Draft / proposed
Date: 2026-06-13
Author: livewire maintainers
Related: `2026-06-05-livewire-intraday-catchup-scheduler-design.md`, `2026-05-20-massive-equity-incremental-backfill-design.md`
Measurements: `2026-06-13-intraday-build-time-measurements.md` (build-time breakdown + phase decomposition + zstd A/B that motivate this design)

## 1. Problem

The intraday catch-up publish step takes ~2 hours per run. It is not CPU-bound
and not transpose-bound. It is bound by **re-reading and re-writing whole
per-ticker files** because the bronze layout stores one immutable Parquet file
per (ticker, timeframe):

```
bronze/asset_class=equity/symbol=AAPL/{1m,5m,30m,1h}.parquet
```

Parquet files are immutable, so appending one trading day to `AAPL/1m.parquet`
means: read the entire ~5-year file → concat the new day → sort → write the
entire file back. Across ~15K active tickers × 4 timeframes every run, that is
roughly the whole intraday lake streamed through a single HDD head twice.

### 1.1 Measured evidence (2026-06-13, this machine, real lake on the 14 TB HDD)

Cold read vs. flushed write, 150-symbol sample × 4 timeframes, extrapolated to
the full ~20K-symbol universe:

| Pass | Throughput | Extrapolated |
|---|---|---|
| Read existing (cold, HDD) | 18.1 MB/s | **~40 min** |
| Write new (snappy, HDD+sync) | 40.1 MB/s | ~18 min |
| **If the read pass is eliminated** | — | **~18 min (≈3× faster)** |

Two decisive facts:

1. **The read pass dominates (~2/3 of wall-clock).** It is *seek-bound* — a
   spinning disk does 100–150 MB/s sequential but collapses to ~18 MB/s hopping
   between thousands of scattered cold files.
2. **That read pass does no useful work.** History is re-read only because the
   immutable single file must be fully rewritten to add one day. Eliminate the
   full rewrite and the read disappears with it.

`zstd-3` (shipped separately) shrinks bytes ~28% and is worth keeping, but it is
*time-neutral on the write pass* (compression CPU offsets the smaller output) and
only partially helps the seek-bound read. It is a ~15% wall-clock win, not the
fix. **The fix is to stop rewriting whole files.**

## 2. Goals / Non-goals

**Goals**
- Make a daily intraday append O(new data), not O(history) — target the ~3×
  read-pass elimination, ideally more.
- Preserve **T+1 freshness** (a hard constraint: no multi-day staleness).
- Keep the change lossless and keep downstream working through the migration.

**Non-goals**
- Re-architecting daily (`1d`) bronze — out of scope; `BronzeClient` is untouched.
- Introducing a database (Postgres/Timescale/ClickHouse) — evaluated previously,
  deferred. This spec is the no-database path.
- Changing the bronze schema or values.

## 3. Current architecture (the frozen contract)

`IntradayBronzeClient` (`clients/intraday_bronze_client.py`) reads/writes one file
per (symbol, timeframe). The literal filename `{tf}.parquet` is a contract for:

- **Internal API consumers** (easy — go through `read_symbol_rows`):
  `backfill_runner`, `sync_runner`, `backfill_intraday`, `health_check`.
- **Internal literal-path consumers** (must be migrated):
  - `livewire_scripts/rebuild_postgres_from_parquet.py:108` — hardcoded
    `["1d.parquet","1m.parquet","1h.parquet","5m.parquet"]`.
  - `livewire_scripts/sync_to_r2.py:24-27` — uploads these literal files to R2.
  - `livewire_scripts/coverage_report.py` — via `INTRADAY_PARQUET_FILENAME`.
- **External consumers** (the real blocker — see §8):
  R2 readers and the Sift app, which read the published per-ticker files.

## 4. Proposed architecture

Replace the single per-(symbol, timeframe) file with a **directory of Parquet
files** that readers open as one logical dataset:

```
symbol=AAPL/1m/                       # directory, not a file
├── year=2021.parquet                 # compacted, immutable, never rewritten
├── year=2022.parquet
├── ...
├── year=2026.parquet                 # current-year compacted base
└── _tail/                            # fresh, appended daily, never re-read to append
    ├── 2026-06-11.parquet
    ├── 2026-06-12.parquet
    └── 2026-06-13.parquet
```

Two layers:

- **Year files** — the compacted base. Written only by periodic compaction.
- **Daily tail files** — one small file per trading day, holding that day's bars
  for that ticker. Written by the daily catch-up. **Append = write one small new
  file. No read of history. No rewrite.**

A reader's logical view of the symbol is the **union** of year files + tail
files (pyarrow reads a directory as a single dataset natively).

### 4.1 Why this shape

`★ Insight` The earlier analysis showed two escapes were blocked: weekly
amortization (killed by the T+1 freshness rule) and the database (deferred). The
daily-tail layout dissolves the conflict that made those mutually exclusive.
Freshness lives in the **tail** (written every day, T+1), while the expensive
full rewrite becomes **compaction** of tail→year, which can run weekly because it
is no longer on the freshness path. We recover the ~7× amortization win *and*
keep T+1 — the two we previously thought we had to choose between.

### 4.2 Write path (daily catch-up)

For each (ticker, day) in the new raw buckets:

1. Build the day's bars (already done — `scan_bucket_by_ticker` + aggregation).
2. Write `symbol=X/1m/_tail/<day>.parquet` atomically (temp + `os.replace`).
3. Derive 5m/30m/1h from *just those rows* (already done) and write their tail
   files the same way.

No `read_table` of history. No full-file rewrite. Cost ≈ writing one trading
day's worth of small files for the active universe — minutes, not hours.

### 4.3 Read path

`IntradayBronzeClient.read_symbol_rows(symbol)` reads the whole `symbol=X/{tf}/`
directory as a dataset (year files + tail files), de-duplicates on
`bar_timestamp` (tail wins over year on overlap), sorts, and returns rows. Same
method signature → internal API consumers are unchanged.

### 4.4 Compaction

`flatfile-ingest compact` (new mode), run on a schedule (e.g. weekly, or when a
ticker accumulates > N tail files):

1. Read `symbol=X/1m/year=<current>.parquet` (if any) + all `_tail/*.parquet`.
2. Merge, de-dupe, sort, write `year=<current>.parquet` via the atomic
   temp-dir-swap pattern already used in `MassiveFlatfileStore.stage_gzip`.
3. Delete the compacted tail files.

This is the *only* full-rewrite, and it is amortized and off the freshness path.
A tail file spanning a year boundary is split into the correct year on compaction
(calendar-day boundaries, same rule as the timeframe aggregator).

### 4.5 Layout decision

| Option | Daily append | File count (per sym/tf) | Compaction needed | Verdict |
|---|---|---|---|---|
| One file per day (no compaction) | O(delta) ✅ | ~250/yr → ~100M total ❌ | no | rejected — file explosion is the exact HDD problem |
| Year-bucketed only | ~5× (rewrite current year) | ~6 | no | simple fallback |
| **Year + daily tail + compaction** | O(delta) ✅ | year files + ≤ ~7 tail | yes (weekly) | **recommended** |

Year-bucketed-only is the low-complexity fallback (~5× win, no compaction daemon,
rewrites only the ~98K-row current-year 1m file instead of the ~490K-row full
file). The tail+compaction layout is the full win (≈3×+ and amortized) at the
cost of a compaction job and union-reads.

## 5. Atomicity & correctness

- Tail writes: temp file in the same dir + `os.replace` (atomic on one FS) — same
  guarantee as `publish_parquet` today.
- Compaction: write a new year file to a temp name, fsync, `os.replace`, then
  delete tails — crash-safe (a crash leaves old year + intact tails; reader still
  sees correct union; next compaction retries).
- De-dup rule: on overlapping `bar_timestamp`, tail rows win (mirrors today's
  `overwrite_existing=True`).
- Validation: reuse `validate_parquet_file` per written file.

## 6. Performance expectation

- Daily catch-up: ~2 h → **~15–20 min** (write tail files only; no history read).
- Weekly compaction: one ~current-cost pass amortized over 7 days (~17 min/day
  equivalent, off the freshness path).
- Storage: unchanged in total (same rows) + zstd-3's ~28% reduction already
  applied. File count rises (year + tail) but is bounded by compaction.

## 7. Migration / cutover plan

Each phase is independently shippable and reversible.

- **Phase 0 (done):** zstd-3 on all bronze + raw writes. Lossless, no path change.
- **Phase 1 — dataset-aware client.** Add directory read + tail write + `compact`
  to `IntradayBronzeClient` behind a flag, defaulting OFF. Full tests, 95% gate.
- **Phase 2 — dual-write + verify.** Catch-up writes both the legacy
  `{tf}.parquet` and the new `symbol=X/{tf}/` layout. A verifier asserts the two
  read paths return identical rows for a sample. No consumer changes yet.
- **Phase 3 — migrate internal literal-path consumers.** Point
  `rebuild_postgres_from_parquet`, `coverage_report`, and `sync_to_r2` at the
  dataset/`read_symbol_rows`. Internal API consumers already work.
- **Phase 4 — one-time backfill conversion.** Convert existing per-ticker
  `{tf}.parquet` into `symbol=X/{tf}/year=*.parquet` (read once, split by year,
  write). Resumable via a cursor like the other flatfile modes.
- **Phase 5 — flip + retire legacy.** Stop writing the legacy single file once
  all consumers (incl. external) read the dataset. Keep the dual-write window long
  enough to validate.

## 8. Risks & open questions

1. **External R2 / Sift readers are the real blocker.** They read literal
   per-ticker files. Options, needs a decision:
   - (a) Keep emitting a **consolidated single-file snapshot** to R2 on the weekly
     compaction (so external readers see one file, refreshed weekly) while
     internal pipeline uses the dataset daily. Preserves the external contract.
   - (b) Migrate external readers to read the directory / an API.
   - (c) Keep dual-writing the legacy single file to R2 indefinitely (simplest,
     but keeps a slice of the rewrite cost for whatever is R2-synced).
   **Decision needed before Phase 5.**
2. **File-count growth on HDD.** Year + tail multiplies inodes; full-history
   reads touch more files. Mitigations: compaction keeps tails ≤ ~7; delete the
   ~117K `._*` resource-fork junk (doubles every directory scan today).
3. **`sync_to_r2` semantics.** Must learn to sync a directory and prune deleted
   tail files (mirror, not just upload).
4. **Compaction scheduling.** New launchd job or a phase in the existing
   orchestrator; must not overlap the daily catch-up on the single HDD head.

## 9. Decision requested

- Approve the **year + daily-tail + compaction** layout (vs. the simpler
  year-bucketed-only fallback)?
- Resolve the external R2/Sift contract (§8.1 a/b/c) — this gates Phase 5.

Until those are decided, Phases 0–2 are safe to build (additive, dual-write,
verified) without touching any consumer.
