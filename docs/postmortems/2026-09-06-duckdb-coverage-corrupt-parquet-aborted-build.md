# A corrupt parquet aborted the whole duckdb catalog build, three nights running

**Rule:** `duckdb_catalog.build_coverage` catches `duckdb.IOException` (no files
behind a view yet) but not `duckdb.InvalidInputException` (a corrupt/truncated
file inside the glob), so the second exception class propagates and kills the
build for **every** view — not just the one holding the bad file.

**Incident / measurement:**

`daily_backfill_duckdb_coverage`, the intraday-catchup phase that runs
`livewire_store.py duckdb build`, exited 1 on 2026-09-03, -04 and -05 —
verified on the mini in `~/market-warehouse/logs/daily_backfill_duckdb_coverage.log`,
which accumulates across runs and holds all three tracebacks, identical file, identical error:

```
_duckdb.InvalidInputException: Invalid Input Error: No magic bytes found at end of file
'/Users/moremeds/market-warehouse/data-lake/bronze/asset_class=equity/symbol=OII/1d.parquet'
```

Neither the intraday-catchup job log nor the launchd stdout/stderr redirects
captured this — they only show `exited with code 1` with no traceback, because
the phase's own subprocess stderr goes to this separate, undated log file, not
to either of those. That gap cost real diagnosis time: the 2026-09-05 run
looked at first like a possible deadlock (12,116s runtime) before this log was
found; it was neither a deadlock nor a hang, it was three nights of the same
uncaught exception.

**Why it took down every other view too:** `build_coverage` loops over
`COVERAGE_SOURCES` and builds one DuckDB view per source with a single
`read_parquet(glob, hive_partitioning=1)` call. DuckDB 1.5 has no per-file skip
for a corrupt file inside a glob read — `ignore_errors` is not a real parameter
for `read_parquet` (verified directly: passing it raises `BinderException`,
"Invalid named parameter"). So one torn equity file failed the whole
`bronze_equity_1d` view's read, and because the per-view exception wasn't
caught, it also aborted `bronze_futures_1d`, `bronze_fx_1d`, `silver_equity_1d`,
and the shepherd-coverage insert in the same call — the entire nightly catalog,
not the one symbol.

**This is the same file class as [pm:2026-09-01-coverage-aborted-on-corrupt-parquet](2026-09-01-coverage-aborted-on-corrupt-parquet.md)
and [pm:2026-07-14-corrupt-parquet-aborted-publish](2026-07-14-corrupt-parquet-aborted-publish.md),
recurring on a third reader.** The publisher quarantines on write; `coverage_report.py`
was fixed to count a torn file missing instead of aborting; `duckdb_catalog.py`
never got the same treatment because it reads through DuckDB's own glob rather
than iterating files in Python, so the exception type is different
(`InvalidInputException`, not `pyarrow.lib.ArrowInvalid`) and the existing
`except duckdb.IOException` clause didn't happen to catch it.

**What the fix does and does not do:** `build_coverage` now catches
`duckdb.InvalidInputException` alongside `duckdb.IOException` and leaves that
view out of the build, same as an absent asset class — every _other_ view still
publishes. It does **not** give per-symbol granularity the way
`coverage_report.py` does (that reader iterates files one at a time in Python;
this one reads a whole view in one DuckDB call, by design —
pm:2026-08-02-duckdb-glob-enumeration-cost). Until the corrupt file is
quarantined off disk, its whole view stays absent from that night's catalog
rather than just the one symbol. `OII/1d.parquet` was moved to
`data-lake/quarantine/20260905T174307Z/symbol=OII/1d.parquet` on the mini as
part of this fix (only `1d` was corrupt — last 8 bytes all zero, not just a
missing footer; `1h`/`1m`/`30m`/`5m` for OII all have valid `PAR1` magic bytes.
OII's silver daily is valid too, `249236` bytes with a good footer, published
2026-08-30 — it predates the bronze corruption and was not derived from it).

**Source:** new incident, 2026-09-06. Fix and test in PR (this branch).
