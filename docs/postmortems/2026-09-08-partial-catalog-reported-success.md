# A corrupt source published an incomplete catalog with exit zero

Rule: an unreadable source aborts catalog publication and leaves the previous
database untouched. A build may skip a genuinely absent optional source, not an
I/O error. The production status reader compares against declared views rather
than inferring completeness from whatever rows remain.

Observed on `ssh macmini`, 2026-09-08, from the previous UTC day's artifacts:

- `logs/daily_backfill_duckdb_coverage.log` reports an unreadable
  `bronze/asset_class=equity/symbol=RJF/1d.parquet`, followed by
  `bronze_equity_1d: 0 symbols` and a successful database publication.
- Intraday run `intraday-catchup-20260907T100001Z-3088` recorded its catalog
  phase exit 0. The latest daily run `daily-update-20260907T152128Z-71747`
  also closed OK, but did not rebuild the catalog after Silver.
- The status reader graded only views returned by the partial catalog. The
  completely absent equity view was invisible to that grading.
- The latest Silver measurements still reported 269 failed symbols and 53
  window regressions. An unchanged failure count was graded OK, conflating
  lack of deterioration with absence of unresolved failures.

The earlier fix caught DuckDB `InvalidInputException` alongside `IOException`
and skipped the affected view. It prevented one exception from stopping a build,
but replaced that failure with a published omission. The revised build catches
only DuckDB's no-files binding error after filesystem checks prove absence,
lets corruption and other I/O errors fail,
and never replaces the destination on that path. It does not repair source data.

Daily-update now builds the catalog after Silver through the existing locked
lane runner and includes catalog failures in its final result. Status grades
missing/empty production views BAD and positive Silver failure counts WARN,
even when shrinking. A completed Silver lane is labeled as completion, not proof
that its revision advanced. Non-trading target dates do not add a stale session.
The digest reads the latest terminal catalog lane directly, before the enclosing
run closes, so a retained old database cannot hide a new build failure.

Regression evidence uses real truncated Parquet bytes, verifies an existing
catalog byte-for-byte after rejection, covers a first build with no destination,
and restores the source fixture to prove retry. Scheduler checks verify command
ordering and failure propagation; no test uses the production lake.

This fixes publication and reporting semantics. Existing bad source files and
Silver residual failures remain explicit production verification items. It does
not establish why the bad file arose or solve Silver batch atomicity.
