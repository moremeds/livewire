# The lane runner must never run the alert

**Rule:** Keep `_page_failure` free of any runner parameter and let `send_failure_alert` default to `subprocess.run` late so the seam stays patchable.

**Incident / measurement:**

- ⚠️ **The lane runner must never run the alert.** `_run_in_own_process_group`
  is keyword-only on `stdout/env/timeout` and returns a `CompletedProcess` with
  **no stdout** (a lane streams into the log file). Threading it into
  `_page_failure` made every page raise `TypeError` *out of `main()`* — so on
  2026-08-02 one failed symbol out of 14,577 in corporate-actions killed the
  whole nightly job, and equity, futures, cmdty, CBOE, FX and Silver never ran.
  No alert was sent either; only the watchdog noticed, 4.5h later. `_page_failure`
  therefore takes **no runner parameter**, and `send_failure_alert` defaults to
  `subprocess.run` *late* so the seam stays patchable. The reason 95% coverage
  missed this: every fake runner in the tests swallows `**kwargs` and every test
  reaching the alert patches `send_failure_alert` itself, so the real pairing was
  never executed. `TestTheLaneRunnerNeverRunsTheAlert` uses the real signature.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
