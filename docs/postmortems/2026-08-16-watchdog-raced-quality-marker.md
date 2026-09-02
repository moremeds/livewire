# The watchdog's quality-marker check raced the tail it was checking

**Rule:** Treat an absent quality marker as failure only once `run_post_success_quality` has logged `=== Job complete <ts> ===`.

**Incident / measurement:**

- ⚠️ **The watchdog's quality-marker check raced the tail it was checking.**
  Lanes must finish inside `MDW_DAILY_JOB_DEADLINE_SECONDS` (06:00 + 4h = 10:00
  UTC), but `run_post_success_quality` runs *after* that with budgets of its own
  — the Sunday interior gap scan alone is 3600s — and the digest, which writes
  `quality_summary_<date>.marker`, is second-to-last in it. The watchdog checks
  at 10:30. Measured 2026-08-16: check 10:30:00Z, marker 10:36:49Z. Same on
  2026-07-29, 2026-08-04 (digest 10:49Z) and 2026-08-06 (digest 10:39Z) — four
  pages whose entire content was "not yet". `run_post_success_quality` now logs
  `=== Job complete <ts> ===` as its last act and the watchdog treats an absent
  marker as failure only once that line exists. The marker is deliberately not
  of the form `=== Done <scope> ===`, which `completed_scopes` would read as a
  phantom lane. A tail that never finishes is logged, not paged — `status`
  grades digest and coverage freshness independently and owns that case.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
