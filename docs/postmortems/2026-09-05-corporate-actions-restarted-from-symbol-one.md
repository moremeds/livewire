# The corporate-actions lane restarted from symbol one every night

**Rule:** A lane that keeps a per-symbol cursor always resumes. `--resume` is
unconditional on the scheduled command, so every reason the cursor cannot be
continued — a refreshed universe, a finished pass, an unreadable file — starts a
fresh pass with one line on stderr instead of failing the lane.

**Date:** 2026-09-05 (measured on the mini, `macmini`)

**Incident:**

- `sync_corporate_actions.py` has kept a durable per-symbol cursor since it was
  written (`corporate_action_cursor.py`, `mark_completed` after every symbol) and
  has had a `--resume` flag the whole time. `build_corporate_action_command` in
  `run_daily_update_job.py` never passed it.
- The lane hit its 10800s budget on 2026-09-03, 09-04 and 09-05 and was SIGKILLed
  by the lane runner each night. Each night it had restarted at symbol 1 of the
  ~13.3K equity universe and got roughly as far as the night before, so the tail
  of the universe was never reconciled at all — the same head was re-fetched three
  times while the tail aged three days.
- Silver is gated on this lane (`silver_inputs_ok = action_code == 0`), so it was
  skipped all three nights.
- Blindly adding `--resume` would have made it worse: `open_cursor(resume=True)`
  raised `ValueError` on a cursor whose identity did not match (root, ticker-set
  hash, count, `full_reconcile`, `dry_run`) and on a cursor already marked
  complete. A weekly `universe-refresh` changes the ticker-set hash, and a night
  that finishes marks the cursor complete — so the flag alone would have failed
  the lane on the first quiet night and on every night after a preset change.

**Cost:** 3 nights × 3h of provider time spent re-fetching the head of the
universe, the tail never refreshed, and Silver blocked 3 nights.

**Fix:** pass `--resume` on the scheduled command, Sunday's `--full-reconcile`
included. `open_cursor(resume=True)` degrades to a fresh cursor and prints why
instead of raising. `run()` does at most two passes per invocation: finish the
resumed tail, and — only if that tail completes and there is budget left — open a
fresh cursor and run this night's own full pass, so a normal night still does a
complete reconciliation. A SIGKILL mid-pass leaves the cursor resumable. The
lane also heartbeats `measurements(name='progress'|'progress_total',
scope='corporate-actions')` every 500 symbols, so a lane killed at its budget
still says how far it got; `status` renders it as `Corporate-action progress`.

**Note:** `started_on_ny` is written to the cursor and read back on resume, but
nothing branches on it — no expiry, no "is this cursor from tonight" check. Under
cross-night resume it now means "the NY date the current pass started", not "the
night this run belongs to". Nothing depends on the difference today; a future
reader must not treat it as a freshness check.

**Tests:**
`tests/test_run_daily_update_job.py::test_build_commands_with_and_without_optional_alert_fields`
(the built command carries `--resume`),
`::TestSilverScheduledLanes::test_sunday_action_sync_requests_full_reconciliation`,
`tests/test_corporate_action_cursor.py::TestResumeNeverFailsTheLane`,
`tests/test_sync_corporate_actions.py::test_a_resumed_pass_finishes_its_tail_then_opens_a_new_cycle`,
`::test_a_resumed_pass_that_does_not_finish_stays_resumable`,
`::test_progress_heartbeats_to_the_ledger_at_every_flush`.
