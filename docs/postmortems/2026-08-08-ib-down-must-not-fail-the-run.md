# A down Gateway skipped all nine phases, and exit 86 must read as degraded

**Rule:** Preflight inside the phase that needs IB, classify an IB phase's exit 86 as degraded by phase-set membership, and fall the equity lane back to Massive.

**Incident / measurement:**

- ⚠️ **A preflight belongs to the phase that needs it, not to the orchestrator.**
  `daily-backfill` and `backfill-all` sat in `IB_COMMANDS`, and `main()`
  preflights before dispatching — so a down Gateway exited 86 ten seconds in and
  none of the nine phases ran, including the Massive `equity_day_aggs` lane that
  owns the ~20K SIP daily universe. Measured 2026-08-08 and 2026-08-09; Friday
  2026-08-07 is absent from bronze warehouse-wide (equity 0/13311, futures 0/14,
  rates 0/4 — only CBOE and FX, the two non-IB lanes of the *other* job, have it).
  This is the same invariant as "IB is not a single point of failure", which was
  implemented in `run_daily_update_job` and never checked against `sync_runner`.
  **It is not a weekend pattern** — 3 of 16 logged days, and the previous weekend
  ran fine. Phase 5 still shells out to `intraday-backfill`, which preflights
  itself, so no check was removed.
- **A phase exiting 86 is degraded, not failed.** `SUMMARY_JSON` carries a
  `degraded` list disjoint from `failed`, and eligibility is **membership of the
  IB phase set, not the exit code** — 86 is livewire's own preflight code and a
  Massive/FRED/CBOE/DuckDB phase returning it for an unrelated reason must still
  fail the run. `_phases_section` renders it as `DEGRADED (IB down)`: a field
  nobody renders changes nothing, and the orchestrator returning 0 while the
  nightly email reads `FAILED (exit 86)` is the same exit-code-versus-summary
  disagreement this runner was already fixed for once.
- **The equity lane falls back to Massive on a down Gateway.** Silver reads
  equity bronze and the corporate-action store, both Massive-backed, but the
  equity lane runs on IB — so `silver_inputs_ok` gated the rebuild for the whole
  universe on a dependency Silver does not have. Futures and cmdty get no
  fallback: Massive does not carry them, and a fallback there would manufacture
  a success out of missing data. If both providers are down the lane leaves
  `degraded` for `failed` and the job pages, which is correct.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
