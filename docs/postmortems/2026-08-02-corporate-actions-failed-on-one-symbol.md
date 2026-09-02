# corporate-actions fails on a rate, not on one symbol

**Rule:** Fail the corporate-actions lane on the failure rate alone (FAILURE_RATE_TOLERANCE, 5%) with no absolute floor.

**Incident / measurement:**

- **corporate-actions fails on a rate, not on one symbol.** `main()` gates the
  Silver rebuild on `action_code == 0`, and the lane returned `1 if failed` — so
  one flaky provider response blocked the adjusted rebuild for the whole ~13K
  equity universe (2026-08-02, `TGNA: Response ended prematurely`, 1 of 14,577).
  A symbol that fails simply keeps the actions already in the store. The rule is
  the rate alone (`FAILURE_RATE_TOLERANCE`, 5%) with no absolute floor, so a
  targeted 2-ticker run that loses one still fails; `resolve_exit_code`'s
  `max(50, …)` floor is calibrated for the equity universe and does not fit here.
  Exit 0 with failures still prints a WARNING naming the count.
- **The watchdog requires the `silver` scope** and reads the equity
  `SUMMARY_JSON`: `=== Done equity ===` with `updated=0` is not healthy.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
