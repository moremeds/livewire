# One lane failure sent two pages

**Rule:** A surface that pages does not restate a page another surface already
sent for the same run. The watchdog is the safety net for a failure nobody
reported, not a second reporter of the failures that did.

**Incident / measurement:** 2026-09-16, host **macmini**, reviewing the alert
history since notify became a ledger row on 2026-09-13.

Five pages in three days. Two of them were the same event:

| time (UTC)  | subject                                  |
| ----------- | ---------------------------------------- |
| 09-14 10:36 | `lane intraday_catchup failed (exit 1)`  |
| 09-14 12:00 | `Intraday catch-up ran` **BAD**          |

One FRED 502. The lane runner paged at failure time from `_page_failure`, and
84 minutes later the watchdog read the same closed run out of the ledger, graded
its `Intraday catch-up ran` check BAD, and paged again.

Neither dedup was broken; they were simply in unrelated namespaces:

- `notify.page_for_lane` fingerprints `("page", [run_id, lane])`.
- `notify.page_from_sections` fingerprints `("page", sorted(section keys))` —
  one fingerprint for the whole set of BAD sections, with no run in it.

Two schemes that cannot collide, so `already_sent` correctly found nothing.
Each surface deduped perfectly against itself and not at all against the other.

**Cost:** double the alert volume for every lane failure, which is the exact
mechanism that trains an operator to stop reading pages. Compare
pm:2026-07-19-interior-day-warning-email-storm — the failure mode of an alert
system is being ignored, not being silent.

**What was *not* the cause:** the 24h dedup window, the Resend transport, and
the `notify` executions rows were all working. Nothing was mis-sent; two
different questions were both answered truthfully.

**Fix:** `executions.run_id` already records the run each send ran under —
the one identity both surfaces share, and it was already being written.
`notify.paged_for_run(run_id)` asks whether a page already left the machine
under that run, and `page_from_sections` drops a BAD section whose own
`run_id` answers yes. `status.Section` carries the `run_id` its rows name, set
only when the check's rows name exactly one.

Deliberately narrow, in three ways:

- only a send with `exit_code = 0` and `skipped = false` suppresses, so a page
  that failed to leave the machine leaves the watchdog as the safety net;
- a BAD section belonging to no run (coverage, disk, catalog) always pages,
  because nothing else will;
- suppression is per section, so one suppressed section never silences the
  others in the same notice.

→ test: `tests/test_check_daily_update_watchdog.py::TestTheWatchdogDoesNotRestateALanesOwnPage`
(four cases: suppressed, failed-send falls through, no-run still pages, and one
suppressed section does not silence its neighbours)
