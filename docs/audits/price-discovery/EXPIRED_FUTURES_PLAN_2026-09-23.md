# Expired futures history backfill plan

## Goal

Backfill only `CL_202610` and `OJ_202609` into their own daily futures Bronze
files, retaining all existing rows. Use IB `includeExpired` only on these
two contracts. Do not alter the rolling universe or BZ/COIL handling.

## Dependency graph

```text
Task 1 (Mini preflight) -> Task 2 (opt-in path + tests) -> Task 3 (scoped IB write + verify)
```

## Tasks

- [x] Task 1 `depends_on: []` — lead completed read-only Mini checks; see
      `EXPIRED_FUTURES_PREFLIGHT_2026-09-23.md`.
- [x] Task 2 `depends_on: [1]` — add and test a default-off `includeExpired`
      option on robust futures historical requests. No production deploy.
- [x] Task 3 `depends_on: [2]` — use a manifest limited to these two symbols,
      task-specific run state, then verify source rows and preservation. See
      `EXPIRED_FUTURES_RUN_2026-09-23.md`; Mini readback reconfirmed 2026-09-23
      (OJ_202609 746/746 dates 2023-10-03..2026-09-22, CL_202610 2137/2137
      2018-01-24..2026-09-22).

## Acceptance

- Write only the two listed `1d.parquet` targets in
  `EXPIRED_FUTURES_MANIFEST_2026-09-23.json`.
- Keep contract identities separate; preserve existing `CL_202610` rows.
- Record exact commands, return codes, IB outcomes, row counts/date bounds, and
  any API-unavailable result.
- Do not deploy code, alter shared cursors, change schedules, or restart IB.

## Herd work log

- `expired-futures-scout` (Devin SWE-2 Max, Mini pane `w1:p4`) was assigned the
  initial read-only preflight. It confirmed loopback Gateway reachability and
  launchd/process state but did not return a report/evidence. Requests to read
  the production daily log and a broad agent roster were denied as unnecessary.
  The lead independently completed and saved the preflight evidence.
- `implementer` (Devin SWE-2 Max, local pane `w2:pE`) completed Task 2; lead
  acceptance and any corrections are recorded below after review.
