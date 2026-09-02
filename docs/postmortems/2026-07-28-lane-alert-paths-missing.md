# Every lane pages, and the timeout pages too

**Rule:** Reach `send_failure_alert` from every terminal failure path — no early return, no private copy of the lane body.

**Incident / measurement:**

- **Every lane pages, and the timeout pages too.** `send_failure_alert` sits at
  the *end* of `run_with_retries` and is reachable only by falling out of the
  retry loop — an early `return` for a new failure mode silently skips it, so
  the timeout branch `break`s. `_run_scheduled_lane` had **no alert path at
  all**, which is why the 2026-07-28 corporate-action wedge produced no alert
  from this job; corporate-actions, CBOE, FX and Silver all run through it.
  `run_cboe_volatility_sync` also carried a byte-identical private copy of the
  lane body, so it silently missed every fix made to the shared one — it now
  calls `_run_scheduled_lane` like the rest. A down Gateway stays silent:
  degraded is not failed.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
