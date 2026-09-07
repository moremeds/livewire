# Two scheduled jobs wrote the same exFAT volume all night

**Rule:** Every lane that touches the lake holds one `fcntl.flock` at
`<warehouse>/locks/lake-io.lock` for the length of that lane, in **both**
scheduled runners. The lock lives on the internal disk, never in the lake. A
lane that cannot get it within its own budget is recorded
`outcome='blocked', blocker='lake_lock'` and skipped -- never run anyway, never
waited on forever. Silver is gated on `outcome='failed'` only: a timeout is slow
data, not wrong data.

**Date:** 2026-09-06 (measured on the mini, `macmini`, from the ledger)

**Incident:**

- The `corporate-actions` lane hit its 10800s budget four nights running and was
  SIGKILLed each time: 2026-09-03 (10800s), 09-04 (10886s), 09-05 (10872s),
  09-06 (10811s).
- On 2026-09-04 at 14:09Z the same lane was run by hand with no other job
  touching the lake. It finished all **14,839 symbols in 39 minutes** (2,340s) --
  4.6x under its budget. The lane was never slow; it was sharing a disk.
- The other writer was `intraday-catchup` (05:00Z), whose
  `daily_backfill_intraday_equity_flatfiles` phase runs to its 21600s (6h)
  budget every single day. It therefore wrote the exFAT volume continuously
  through the entire 06:00Z daily-update window, every night. exFAT has no
  directory index, so every concurrent create and rename is a linear scan
  (pm:2026-09-05-source-evidence-flat-exfat-directory).
- By 09-06 the contention had reached the short lanes: `cboe` timed out at
  1804s (30m budget), `fx` at 1811s, `equity` at 7210s (2h budget).
- `silver_is_blocked()` gated on `exit_code != 0`, and a timeout is exit 124, so
  a merely-slow upstream lane withheld the adjusted series for the whole ~13.3k
  universe. Silver did not run on any of the four nights.

**Cost:** four nights of corporate-actions, four nights of Silver for the whole
equity universe, three additional lanes timed out on the fourth night, and an
operator stopgap (launchd edited by hand on the mini, 2026-09-06: daily-update
06:00Z -> 05:00Z, intraday-catchup 05:00Z -> 10:00Z; backups in
`~/market-warehouse/backups/launchd-20260906/`) that lived in two plist files
and in no test.

**Fix:** one lake-io lock, taken in all three lane bodies
(`run_daily_update_job.run_with_retries`, `._run_scheduled_lane`,
`sync_runner.run_phase`) via the flock helper already in
`clients/parquet_io.py`; the wait recorded per lane as
`measurements(name='lake_lock_wait_s', scope=<lane>)` and graded against a
declared value; priority expressed as the poll interval (daily 1s, intraday
60s) rather than as a scheduler; `silver_is_blocked()` narrowed to
`outcome='failed'`; a `Lanes blocked` check on the graded surface so a deferred
lane is visible rather than a silent exit 0.

**Also found:** the `silver` lane budget was never measured. 7200s was a guess,
and the first run it actually bounded was killed at 7201s (2026-09-07). Four
measured full rebuilds on the mini -- 2026-08-27 2h12m, 08-28 2h15m, 08-30
2h51m, 08-31 1h18m -- put the budget at 4h.

**Not fixed here:** the `digest` tail (which spawns `housekeeping --apply`),
`vol_1h_derive`, and the separate 11:00Z `coverage` job stay outside the lock --
none of them is a lane body. Coverage is untimed by design
(pm:2026-08-02-coverage-budget-expired-silently), so its runtime will rise where
it now overlaps intraday-catchup; that number is measured, not assumed.

**Tests:** `tests/test_job_runner_common.py::TestTheLakeLock`,
`tests/test_run_daily_update_job.py::TestTheLakeLock`,
`::TestTheSilverGateOnlyBlocksOnFailure`,
`tests/test_sync_runner.py::TestTheIntradayPhasesWaitForTheLake`,
`tests/test_run_intraday_catchup_job.py::TestBothRunnersTakeOneLock`,
`tests/test_status.py`, `tests/test_launchd_templates.py::test_the_two_lake_writers_start_five_hours_apart`.
