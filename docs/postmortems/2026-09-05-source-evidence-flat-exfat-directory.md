# One flat CAS directory on exFAT timed out the corporate-actions lane

**Rule:** A content-addressed store on exFAT is sharded (`sha256/<d[0:2]>/<d[2:4]>/<digest>`),
takes no per-artifact lock file, and fsyncs its directory once per commit — never once per write.
Readers fall back to the legacy flat path; nothing is migrated on a hot path.

**Date:** 2026-09-05 (measured on the mini, `macmini`)

**Incident:**

- The nightly `corporate-actions` lane hit its 10800s budget three nights running
  (2026-09-03, 09-04, 09-05) and was SIGKILLed by the lane runner each time.
  Silver is gated on that lane, so it was skipped on all three nights.
- `SourceEvidenceStore.persist_raw` wrote every provider response (~29.6k a
  night) into **one** directory, `<lake>/raw/shepherd/sha256/`. Measured there
  on 2026-09-05: **275,006 entries, 25 GB**, of which **137,504 were orphan
  `.<digest>.lock` files** — `_exclusive_lock` created them and never unlinked
  them. exFAT has no directory index, so every lookup, create and rename is a
  linear scan of that entry list. Per response the store did: create the lock
  file, `exists()`, temp write + `fsync`, read-back rehash, `os.replace`, and an
  `fsync` of the 275k-entry directory. That is the 3 hours.
- PR #108 (the in-process digest cache) fixed a workload that does not exist:
  it only helps when response bodies repeat, and in production every body is
  distinct. The fixture that "proved" it — `_stub_endpoints` — returns one
  byte-identical empty body for every ticker, so every write after the first was
  a cache hit and the flat-directory cost was invisible in tests.
- Because the lane was SIGKILLed, the `finally` in `sync_corporate_actions.py`
  never ran: `evidence.flush()` and `_emit_provider_measurements` were skipped,
  so the night's manifest rows were lost for bytes already on disk. Lane
  subprocesses also ran with block-buffered stdout, so each 3h lane left an
  **empty log**.

**Cost:** 3 nights of corporate-actions, 3 nights of Silver, 25 GB and 275,006
directory entries to clean up, and no log to diagnose any of it from.

**Fix:** shard the CAS two levels; drop the per-artifact lock file (a
content-addressed write is idempotent — racing writers produce identical bytes
and `os.replace` is atomic); fsync the shard directories once per manifest
commit; skip the read-back rehash in `record_many` for digests this process
already wrote; `PYTHONUNBUFFERED=1` in every lane subprocess env; commit the
evidence manifest every 500 symbols instead of only at the end;
`housekeeping --evidence-locks` sweeps the orphan locks (opt-in — listing a
275k-entry directory is minutes, and `main()` plans everything before deleting
anything, so a glob that blows the nightly 600s tail budget would delete nothing).

**Superseded 2026-09-06:** the orphan-lock sweep is not how the flat directory
gets retired. Listing 275k entries on exFAT to unlink 137k of them costs more
than it saves; the operator renames the whole directory aside in one `mv`
(`raw/shepherd/sha256` → `raw/shepherd/sha256-legacy`) and `raw_path` resolves
artifacts there as a third fallback, so nothing that cannot be refetched becomes
unreadable.

**Tests:** `tests/test_source_evidence.py::TestShardedCas`,
`tests/test_sync_corporate_actions.py::TestDistinctResponseBodies`,
`tests/test_housekeeping.py::TestEvidenceLockSweep`,
`tests/test_run_daily_update_job.py::TestLaneSubprocessesRunUnbuffered`.
