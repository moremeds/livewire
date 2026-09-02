# The Massive floor is derived from the scan date, not hardcoded

**Rule:** Compute massive_floor_for(as_of) = as_of - 1827 days from the scan date, never a constant and never the target date.

**Incident / measurement:**

- ⚠️ **The Massive floor is derived from the scan date, not hardcoded.**
  `massive_floor_for(as_of) = as_of − 1827 days` (measured 2026-07-29:
  `2021-07-27` → 403, `2021-07-28` → OK, exactly 5.00 years). A constant here
  rots one day per day and silently mis-tiers — and so does passing the *target
  date*: on a Monday run for Friday's session the two differ by three days.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)
