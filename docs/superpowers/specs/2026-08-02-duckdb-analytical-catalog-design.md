# DuckDB analytical catalog (replacing Postgres)

**Date:** 2026-08-02
**Status:** implemented on `feat/duckdb-analytical-catalog`

## Why

Two things were true at once:

1. **Postgres was dead code in production.** `MDW_POSTGRES_DSN` was unset in
   `~/market-warehouse/.env`, and both orchestrators gate their Postgres lane on
   `if os.getenv("MDW_POSTGRES_DSN")`. Fourteen days of nightly logs contain no
   `postgres` line at all. It cost ~1,800 lines and a hard `psycopg[binary]`
   dependency to run nothing.
2. **Coverage and freshness had no owner.** The warehouse could not answer "which
   tickers are stale" or "what is in bronze but missing from silver" at
   interactive speed, because answering meant opening 13,270 parquet footers one
   at a time. So nobody asked, and real gaps went unreported for weeks.

DuckDB was retired in 2026-05 (`c9d5d86`) for a good reason — it had grown into a
second copy of the lake (`db_client.py` + `rebuild_duckdb_from_parquet.py`). It
returns here in a deliberately different shape: a **query layer**, not a
warehouse.

## Measurements that shaped the design

All measured 2026-08-02 against the real lake (13,270 equity 1d files,
19,753,203 rows) on APFS internal SSD, DuckDB 1.5.5.

| Operation | Time |
|---|---|
| Open one known parquet file | 0.04s |
| `CREATE VIEW` over the equity `1h` glob | **221.04s** |
| `glob()` enumeration of equity 1d | 85.48s |
| Whole-universe `count(*)`, cache warm | 0.86s |
| The same query after cache eviction | **283.84s** |
| `parquet_metadata()` over equity 1d | 471s |
| Aggregate scan building coverage (bronze+silver) | 117.8s |
| Materialising `silver_daily` (18.6M rows) | 102.7s → **0.73 GB** |
| 4 concurrent `read_only` readers | 0.00s each |
| A writer while a reader holds the file | **fails** |

Three findings drove everything:

**Glob enumeration dominates, and it is not the data.** Binding a view costs
221s while reading the resulting file costs 0.04s. Any design that eagerly
registers views is unusable.

**The filesystem cache does not survive the night.** The same query measured
0.86s and 283.84s forty minutes apart, because the intraday benchmark in between
evicted the metadata. The nightly job writes 23.57 GB of intraday every night and
does exactly that. **Cold is the normal morning state** — so a view-only design
pays ~284s on the first query of each day. This is why a durable coverage table
is required rather than merely nice.

**Intraday cannot be materialised.** Equity `1m` is 23.57 GB against ~20 GiB of
free disk (90% full). Not a trade-off — a wall.

## Design

Three layers, each with one job. Parquet stays the system of record.

| Layer | What it holds | Cost |
|---|---|---|
| **Views** | every asset class and timeframe, incl. all intraday | 0 bytes; bind on demand |
| **Direct-path reads** | `symbol=<T>/<tf>.parquet` resolved by construction | 0 bytes |
| **Coverage table** | per-symbol `n_rows/first_date/last_date` per view | 536 KB |

### Rules the implementation enforces

- **`connect()` registers no views by default.** Callers name what they need;
  `duckdb sql` registers only the views its query text mentions.
- **`read_symbols()` never touches a view.** The lake layout is the contract and
  Apex already resolves symbols by construction, so building the path list is
  both legitimate and ~2000× cheaper. Measured 0.53s for a two-symbol query
  against >5 min through the glob.
- **Coverage is daily-only.** An intraday pass would enumerate and scan 23.57 GB.
  `parquet_metadata()` is not the shortcut it appears to be (471s vs 118s).
- **`build_coverage` publishes by `os.replace()`.** Required, not stylistic:
  DuckDB is single-writer, so an in-place rebuild fails whenever a reader is
  connected. Mirrors the temp → validate → replace contract bronze already uses.
- **A missing asset class is not a failure.** `cmdty`/`fx` may legitimately have
  no parquet; the build fails only if *every* source is empty.

### What it answers, and how fast

```
duckdb freshness   0.36s    per-view staleness buckets
duckdb lag         0.25s    silver trailing or absent vs bronze
duckdb stale       0.25s    symbols with no recent bar
```

Previously the equivalent required a 250–284s full-lake pass, and the cross-tier
questions were not asked at all.

## Findings this immediately surfaced

Persisted to `logs/probes/2026-08-02-coverage-freshness-gaps.json` and
`logs/probes/2026-08-02-silver-lag-and-absent.json`.

- **241 symbols are in bronze but absent from Silver entirely** — and they are
  blue chips with full history and current data: ROL, WY, AIG, LEN, HON, CMCSA,
  MMM, MSI, ECL, WST, each with ~11,000+ rows through 2026-07-31. Consistent with
  the documented `unknown price_basis` + split → quarantine path. **Deferred, not
  fixed, by explicit decision.**
- **419 symbols have Silver trailing Bronze**, but 372 of those trail by exactly
  one trading day, which is ordinary nightly ordering. The real tail is ~47
  symbols stuck 2–3 weeks (SMH, POM, RVI, VIVO, XLE, WW, OPI, SKK at 2026-07-13).
- **134 bronze symbols stale >30 days**, likely delisted and due for
  `bronze-delisted/` archival — unverified individually.
- **All 4 FRED rate series are behind** (`bronze_rates_1d`: 0 current).

### A measurement that was wrong, recorded so it is not repeated

An interior-gap count against the full NYSE calendar reported 8,261 symbols with
1,279,479 missing bars. **That number is not trustworthy and was discarded.** A
stock that does not trade has no bar — `no_trade` is not a gap — and the top
"offenders" were all illiquid small caps. Distinguishing the two requires the
day's actually-traded set (Massive `day_aggs` `_symbols.parquet`), which is what
`coverage_report.py` already uses. Counting against the calendar cannot do it.

## Deferred

- **Materialising `silver_daily`** (102.7s, 0.73 GB — cheap and viable). Deferred
  because the coverage table addresses the stated priority and this does not; it
  buys constant-time whole-universe *data* scans, which is a backtest concern.
  Add it when a real backtest measures the view path as too slow.
- **Intraday coverage.** Blocked on cost, not on design.
- **The 241 absent blue chips.** A data-repair project, tracked separately.
