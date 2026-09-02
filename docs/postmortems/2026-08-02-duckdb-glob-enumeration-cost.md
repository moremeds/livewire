# Glob enumeration dominates the cost of the whole lake

**Rule:** Register DuckDB views on demand only, bypass views for symbol-scoped reads, and keep the coverage table durable because the filesystem cache is not.

**Incident / measurement:**

⚠️ **Glob enumeration is the dominant cost of the whole lake, and it dwarfs
reading data.** Measured 2026-08-02 against 13,270 equity 1d files / 19.75M rows:

| Operation | Time |
|---|---|
| Open one known parquet file | 0.04s |
| `CREATE VIEW` over the equity `1h` glob | **221.04s** |
| Whole-universe `count(*)`, filesystem cache warm | 0.86s |
| The same query after the cache was evicted | **283.84s** |
| `parquet_metadata()` over equity 1d (the "footer-only shortcut") | 471s |

Three rules follow, and breaking any of them makes the catalog unusable:

- **Views are registered on demand, never eagerly.** `CREATE VIEW` binds the
  schema, and binding enumerates the glob — so registering all 13 views costs 13
  full enumerations before a single query runs. `connect()` registers nothing by
  default; `duckdb sql` registers only the views its query text names.
- **Symbol-scoped reads bypass views entirely.** `read_symbols()` /
  `duckdb bars` construct `symbol=<TICKER>/<tf>.parquet` paths directly. A
  two-symbol query took 0.53s that way against >5 min through the glob.
- **The coverage table is durable because the cache is not.** The nightly job
  writes 23.57 GB of intraday, which is exactly what evicted the cache between
  the 0.86s and 283.84s readings. Cold is the normal morning state, so freshness
  questions get a table rather than being re-derived from 13,270 footers.

Coverage is **daily-only**. A pass over the intraday tier would enumerate and
scan 23.57 GB, and intraday cannot be materialised at all — equity `1m` alone is
23.57 GB against ~20 GiB of free disk.

`build` publishes by writing a staging database and `os.replace()`-ing it into
place. This is required, not stylistic: DuckDB is single-writer, so an in-place
rebuild fails outright whenever a reader is connected. Concurrent `read_only`
readers are fine — four simultaneous readers measured 0.00s each.

**Postgres was removed** (2026-08-02). It had been dead in production for
months: `MDW_POSTGRES_DSN` was unset in `~/market-warehouse/.env`, so both
orchestrators skipped the lane every night and 14 days of nightly logs contain
no `postgres` line at all. `tests/test_duckdb_containment.py` now holds the
line that got DuckDB retired in 2026-05 — DuckDB may be imported only by the
catalog modules, and no command may materialise bars out of bronze.


**Source:** CLAUDE.md section "DuckDB analytical catalog" (moved 2026-09-02)
