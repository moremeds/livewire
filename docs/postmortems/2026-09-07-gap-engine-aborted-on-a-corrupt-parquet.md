# One unreadable parquet aborted the gap scan — fourth recurrence of the same class

**Rule:** A corrupt per-symbol parquet is quarantined by the publisher and counted **missing** by every reader. A read-only detector never raises on one file, and never repairs one either. `clients/gap_engine.py::actual_sessions` was a reader that had not been told.

**Incident / measurement (host: macmini, 2026-09-07):**

`actual_sessions` promised in its own docstring that "a missing file is an empty
set, not an error", but only guarded `path.exists()`. A file that exists and is
torn raised `pyarrow.lib.ArrowInvalid` straight out, through
`scan_findings` (`livewire_scripts/coverage_report.py:627`) and up to the
`try/except` in `_scan_and_write_artifacts`, which degrades the whole gap scan
to one line: `scan: FAILED (...)`.

Verified on the mini:

- `bronze/asset_class=equity/symbol=RJF/1d.parquet`, 217,059 bytes, fails with
  `Parquet magic bytes not found in footer`.
- RJF's `1h`/`1m`/`5m`/`30m` parquet files all read fine. **The bug is one file,
  not one symbol** — and the file class, not the symbol, is what recurs.

**What it cost:** the Tier A/B repair queue — the entire point of the gap engine
— was not produced that night. Nothing else broke: the rest of the coverage job
completed normally, so the run looked healthy and the only trace was a single
`scan: FAILED` line in an otherwise green report. A detector with no output is
dead, not healthy.

**Fourth recurrence.** The write path was fixed 2026-07-14
([pm:2026-07-14-corrupt-parquet-aborted-publish](2026-07-14-corrupt-parquet-aborted-publish.md));
the coverage read path was fixed 2026-09-02 after nine aborts
([pm:2026-09-01-coverage-aborted-on-corrupt-parquet](2026-09-01-coverage-aborted-on-corrupt-parquet.md));
the DuckDB catalog build was fixed after three nights down
([pm:2026-09-06-duckdb-coverage-corrupt-parquet-aborted-build](2026-09-06-duckdb-coverage-corrupt-parquet-aborted-build.md)),
one day before this one. Three fixes each named the general rule and none swept
the remaining reader. "Fix the twin" is not one grep at fix time: it is a
standing obligation on every reader of a per-symbol parquet, and the fact that
this recurred *the day after* the previous fix is the measurement that says the
sweep is still not being done.

**Why empty and not a new failure channel:** an empty set is the conservative
direction and needs no plumbing. `classify` reads the series as `G3` — nothing
on disk — so the symbol enters the Tier A/B queue exactly as an absent file
would. It over-reports a gap; it cannot hide one. The ERROR names the exact path
so an operator can quarantine or refetch it. Nothing is downgraded to silence.

**Why the engine does not quarantine:** moving a file aside belongs to the write
path that owns the data (`flatfile_publisher.quarantine_corrupt_parquet`). Phase
1 of the gap engine is read-only and mutates nothing; a read-only scan that
writes bronze is a worse bug than the one it fixes.

**Related, not fixed here:** the same RJF directory holds macOS AppleDouble
files (`._1d.parquet` … `._5m.parquet`, 4096 bytes each, none valid parquet).
`actual_sessions` builds an explicit path so it never sees them, but
`livewire_scripts/warehouse_health_report.py:147` globs
`asset_class=*/symbol=*/*.parquet` with no `._` filter and would pick them up as
a timeframe named `._1d`. `clients/duckdb_catalog.py:163` already filters them.
Out of scope for this diff; recorded so it is not rediscovered.

**Source:** new incident, 2026-09-07. Fix and test on `fix/gap-engine-corrupt-parquet`.
