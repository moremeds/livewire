# coverage/weekly/digest run once, after Silver

**Rule:** Run coverage, weekly and digest exactly once per night, after Silver — never inside each asset class's success branch.

**Incident / measurement:**

- **coverage/weekly/digest run once, after Silver.** They used to fire inside
  each asset class's success branch — four digests a night, all before Silver,
  so `_silver_section` parsed a log that could not yet contain Silver's
  summary and the `window_regressions` warning was structurally unreachable.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
