# 2026-09-08 — the failure email named the lane, never the error

**Rule.** Every failure surface leads with the error text itself: the failing
lane/phase/check, its exit code, the log that holds the full trace, and the
exception. Generic prose ("Probable cause: see the raw error summary below",
"Impact: … limited to the ledger evidence above") is deleted, not softened. One
shared extractor (`livewire_scripts/daily_outcomes.extract_error_lines` /
`last_failed_section` / `read_log_tail`) feeds the daily runner, the intraday
runner, the watchdog and the status surface, so the twins cannot drift.

**What happened.** One corrupt parquet —
`bronze/asset_class=equity/symbol=RJF/1d.parquet`, "Parquet magic bytes not
found in footer" — failed three surfaces on the same night, and none of the
three said so:

- **Intraday catch-up email:** `Intraday catchup failed — phases failed:
  daily_backfill_equity_union, daily_backfill_duckdb_coverage`. `sync_runner`
  streams each phase into `<log_dir>/<label>.log`, so the runner log the email
  quoted said only "daily_backfill_equity_union exited with code 1". The
  traceback was one file away.
- **Watchdog email:** `launchd jobs:; Daily update ran:; Intraday catch-up ran:;
  Catalog build:; DuckDB catalog: incomplete` — five check names, joined with
  `; `, and not one value, while `status.collect()` held the evidence rows.
- **Daily lane email:** `Daily update failed — updated=0, no_trade=0, partial=0,
  errors=0, target_date=?, source=?, asset_class=?`. The extractor took the last
  `SUMMARY_JSON` in the whole file, which belonged to the Silver lane that had
  already succeeded; the DuckDB catalog lane that actually died was ignored.
- **Nightly digest:** 138 lines, up from 72 on 09-04. Six identical ritual lines
  per non-OK check, `fix` printed twice (once as "Next action:"), three lines per
  passing check, no cause anywhere.

**What it cost.** Five emails on 2026-09-08 that an operator could not act on
without ssh'ing to the mini and reading three separate logs. The user's summary:
"所有的email 成功的失败的完全不make sense 我完全have no idea what went wrong."

**Enforced by.** `tests/test_daily_outcomes.py::TestTheSharedErrorExtractor`,
`tests/test_run_daily_update_job.py::TestHelpers::test_extract_error_summary_names_the_failing_lane_and_quotes_its_exception`,
`tests/test_run_intraday_catchup_job.py::TestExtractErrorSummary::test_quotes_the_failed_phases_own_log`,
`tests/test_check_daily_update_watchdog.py::test_the_page_carries_every_bad_checks_evidence_not_just_its_name`,
`tests/test_status.py::TestANonOkCheckCarriesItsCause`,
`tests/test_status.py::TestTheSurfaceIsShortEnoughToRead`,
`tests/test_nightly_digest.py::TestTheDigestSaysWhatWentWrong`,
`tests/node/send_daily_update_failure_email.test.mjs` (the boilerplate assertions
are now negative).
