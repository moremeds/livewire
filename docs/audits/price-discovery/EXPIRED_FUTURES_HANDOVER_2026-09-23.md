# Commodity and expired futures handover — 2026-09-23

## Reactivation prompt

We are continuing from this handover. Read this document first, inspect the
current repo state, verify what still applies, and continue from the next steps
without assuming the old chat context is available.

## Goal

The expired-contract plan says:

> “Backfill only `CL_202610` and `OJ_202609` into their own daily futures
> Bronze files, retaining all existing rows.”

This was a follow-up to the rolling commodity daily backfill. Do not broaden the
expired-contract task to other expiries or intraday bars.

## Prior rolling commodity backfill

The earlier approved manifest covered 103 live qualified contracts across 20
roots. The Task 2 report records 100 new contracts seeded and three existing
contracts checked for older history; 103/103 Bronze files were readable with
131,436 total rows. SI's two contracts needed a trading-class-specific retry.
This evidence is in the sibling worktree at
`/Users/chenxi/projects/livewire/.worktrees/price-discovery/docs/audits/price-discovery/TASK2_COMMODITY_BACKFILL_2026-09-23.md`.
It is daily data only; it does not establish intraday history. BZ was retired
and excluded, and its history remains separate from COIL.

## Current worktree and delivery state

- Repo/worktree: `/Users/chenxi/projects/livewire/.worktrees/expired-futures-backfill`
- Branch: `feat/expired-futures-backfill`; HEAD/task base:
  `65a0d96df1e35937ac99b2c042bb6fddaeb40592`.
- The implementation, tests and evidence are uncommitted. No upstream is
  configured; `origin` currently has no branch with this name; GitHub reports
  no PR for this branch.
- The `--include-expired` flag is default-off and futures-only. Robust passes it
  to its historical child; the child sets IB `Contract.includeExpired` before
  qualification. The change was not deployed as code.
- At the expired-contract run, production `current` was release
  `65a0d96df1e35937ac99b2c042bb6fddaeb40592`. A later status check observed
  `current` at `c47c9d220ccec3faeeb01c2cdd2364aec4f65b58`; verify before any
  production operation.
- Latest Mini process check showed an active `daily-backfill` with an
  equity/Massive child. No expired-futures runner was active. Do not interrupt
  that equity job; recheck current process state before any production write.

## Expired-contract result

- `OJ_202609`, robust `seed`, exit 0: `ok`, +746 rows. Exact-file PyArrow
  readback found 746 unique dates from 2023-10-03 through 2026-09-22.
- `CL_202610`, robust `backfill`, exit 0: `ok-noop`; IB returned no older
  history. Its 2,137 existing unique dates from 2018-01-24 through
  2026-09-22 remained unchanged.
- Only those contracts' daily `1d.parquet` paths were write targets. No
  intraday requests or other-symbol writes were part of this run.
- Full commands, outcomes, verification and run deviations:
  `EXPIRED_FUTURES_RUN_2026-09-23.md`.
- Implementation and recorded targeted tests:
  `EXPIRED_FUTURES_IMPLEMENTATION_2026-09-23.md`.
- Manifest and preflight:
  `EXPIRED_FUTURES_MANIFEST_2026-09-23.json` and
  `EXPIRED_FUTURES_PREFLIGHT_2026-09-23.md`.
- Targeted tests recorded in the implementation report: 202 passed with
  `-W error::RuntimeWarning`; 265 related tests passed; both modified modules
  reported 100% coverage. A full-suite result of 3,103 passed / 95.07% was
  reported in chat but is not recorded in these task evidence files. Capture
  or reverify it before using it as durable delivery evidence.

## Deviations and known limits

- The expired-futures runner reused a task-specific log directory that was
  present but empty at preflight, instead of choosing a new path. It also
  listed up to 50 futures Bronze directory names. Both deviations are
  recorded in the run report; no data collision or out-of-scope write was
  observed.
- The implementation worker's Task 3 report says no deviations. That report
  covers implementation work, not the separate production-run deviations.
- The plan still marks production Task 3 unchecked although the run report and
  Parquet readback exist. Reconcile this stale checkbox before calling the
  plan fully closed.
- This task verifies file readability, date bounds and row preservation. It
  does not validate `settlement` or `open_interest` as actual exchange values.
- No claim is made that IB has history older than the returned CL window, or
  that other expired contracts or intraday periods were backfilled.

## Dirty files to preserve

Tracked implementation/test changes:

- `livewire_scripts/fetch_ib_historical.py`
- `livewire_scripts/run_ib_fetch_robust.py`
- `tests/test_fetch_ib_historical.py`
- `tests/test_run_ib_fetch_robust.py`

Untracked task files:

- `.devin/config.local.json` — preserve; do not inspect or remove without need.
- `EXPIRED_FUTURES_IMPLEMENTATION_2026-09-23.md`
- `EXPIRED_FUTURES_MANIFEST_2026-09-23.json`
- `EXPIRED_FUTURES_PLAN_2026-09-23.md`
- `EXPIRED_FUTURES_PREFLIGHT_2026-09-23.md`
- `EXPIRED_FUTURES_RUN_2026-09-23.md`
- This handover file.

`tasks/todo.md` is unchanged. Preserve unrelated changes in the main checkout
and `.worktrees/price-discovery`.

## Next steps

1. Recheck the worktree and evidence; reconcile the Task 3 checkbox only if the
   two target-path acceptance evidence still matches.
2. If preparing code delivery, review the four-file diff and capture fresh full
   CI output. Keep local tests distinct from deployment status.
3. Stop before commit, push, PR creation/merge, deployment, another production
   write, or Gateway restart; none is authorized by this handover.

No commit, push, PR, merge, deployment or Gateway restart was performed for
this task.

## Reverification — 2026-09-23 (follow-up session)

- MacBook, this worktree: `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning`
  → 3,103 passed, exit 0 (coverage gate met; exact percentage not captured).
- Mini read-only readback: `OJ_202609` 746 rows / 746 unique dates
  2023-10-03..2026-09-22; `CL_202610` 2,137 / 2,137 2018-01-24..2026-09-22.
  `current` → `c47c9d22…`; no expired-futures runner active.
- Plan Task 3 checkbox reconciled to done.
