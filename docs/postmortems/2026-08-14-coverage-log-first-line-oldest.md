# Coverage logs are append-mode, so the FIRST line is the OLDEST run

**Rule:** Select the LAST line matching `coverage:` and grade an all-timeframe 0/0 as UNKNOWN, not OK.

**Incident / measurement:**

- ⚠️ **Coverage logs are append-mode, so the FIRST line is the OLDEST run.**
  `status._coverage_section` took the first non-blank line. On 2026-08-14 that
  file held an aborted run writing `1d=0/0 (100.00%)` for every timeframe and,
  three lines below, the real `1d=13375/13385 (99.93%)`. It reported the 0/0 —
  and `_coverage_ratios` maps `0/0 → 1.0`, so it graded **OK**. Per-timeframe
  that mapping is right (an asset class with no files is not a gap); across
  *every* timeframe it means the run enumerated nothing, which is now UNKNOWN.
  Selection is the last line matching `coverage:` — the `1d missing:` and
  `MISSING_JSON` detail lines are not measurements, and the `non-equity 1d:`
  line matches the timeframe regex but is not one either.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
