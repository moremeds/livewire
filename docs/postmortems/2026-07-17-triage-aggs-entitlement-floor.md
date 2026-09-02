# /v2/aggs is entitled for a rolling ~5 years, so triage verdicts are durable

**Rule:** Never delete the triage verdict store at <data-lake-root>/repairs/triage/current.json — a verdict obtainable today may be unobtainable next year.

**Incident / measurement:**

- **`/v2/aggs` is entitled for a rolling ~5 years only** (floor measured
  **2021-07-12** on 2026-07-17). Every older break is `inconclusive` — always. A
  large `inconclusive` count is the expected shape, not a failure.
- **The floor rolls, so the verdict manifest is durable and default-loaded** from
  `<data-lake-root>/repairs/triage/current.json`. The nightly job passes no flags;
  without the verdicts at that path every confirmed `real_move` is re-read as an
  unexplained break and trimmed the next night. Never delete the verdict store to
  "force a re-triage" — a verdict obtained today may be unobtainable next year.
- Transient provider failures (rate-limit, 5xx, timeout, a wrapped connection
  failure) **abort the run and are never checkpointed**; `--resume` re-asks them.
- The run probes the credentials against an entitled date first: a bad key 401s on
  every request, which is indistinguishable from the entitlement floor and would
  otherwise trim the whole population silently.


**Source:** CLAUDE.md section "Break triage — keeping real market moves" (moved 2026-09-02)
