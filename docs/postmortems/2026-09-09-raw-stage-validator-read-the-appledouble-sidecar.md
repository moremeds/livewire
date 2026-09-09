# The raw-stage validator decoded an AppleDouble sidecar

Rule: any code that enumerates files on the exFAT lake filters names starting
with `._`. macOS writes one AppleDouble sidecar per file there, so a directory
holding `bucket=000.parquet` also holds a 4096-byte `._bucket=000.parquet` that
is not Parquet. Readers already glob `bucket=*.parquet`, which cannot match a
`._` name; a bare `*.parquet` glob can, and does.

Observed on `ssh macmini`, 2026-09-09.

`validate_and_fsync_raw_stage` (new in #125) globbed `*.parquet` over the
staging directory, which `stage_gzip` creates with
`tempfile.mkdtemp(prefix=".date=<day>.", dir=self.raw_root)` — on the lake
volume, not on internal disk. Every staged file therefore had a sidecar beside
it. `pq.ParquetFile(handle).read()` on the first one raised
`ArrowInvalid: Parquet magic bytes not found in footer`, the `finally` branch
removed the staging directory, and no Massive raw date could be published.

It fired on the first new trading date after the cutover: 2026-09-08,
downloaded 10:03Z on 2026-09-09. It took down the intraday-catchup phases
`daily_backfill_intraday_equity_flatfiles` and `daily_backfill_equity_day_aggs`,
and the coverage job's auto-recover republish of 1m for 2026-09-08 —
14,837 symbols. No data was lost; nothing new could land.

The validator now skips `._` names, and the "staged raw date has no parquet
files" error is computed on the filtered list, so a directory holding only
sidecars still fails closed rather than passing validation. `publish_raw_date`
and `recover_raw_date` need no guard: they rename and remove whole
directories and never enumerate their contents.

The precedent was already in the repo twice — pm:2026-08-10-appledouble-sweep-cost
and `clients/duckdb_catalog.py::_ledger_files`, which filters `._` for exactly
this reason. The cost of the third instance was one night of ingest.
