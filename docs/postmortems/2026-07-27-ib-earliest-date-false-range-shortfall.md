# IB_EARLIEST_DATE is IB's floor, never an instrument's inception

**Rule:** Never default `expected_start` to `IB_EARLIEST_DATE`; no known inception means no expectation to test.

**Incident / measurement:**

- ⚠️ **`IB_EARLIEST_DATE` is IB's floor, never an instrument's inception.**
  `backfill_ticker` defaulted `expected_start` to it, asserting every ticker
  should carry history back to 1993-01-29. On 2026-07-27 that mailed CRITICAL
  `range_shortfall` for BIL (listed 2007), GLD (2004), IEF (2002), TLT (2002),
  EEM (2003), EFA (2001) and MDY (1995) — seven instruments, all false, and it
  would fire for every instrument younger than 1993 reaching that path without
  an `ib_head_timestamp` to excuse it. There is now no default: `detect_all`
  already skips the detector when `expected_start` is None, which is correct —
  no known inception means no expectation to test.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
