# Fix/improvement plans — 2026-07-04 repo audit

> **STATUS 2026-07-22 → see [CONSOLIDATED-STATUS-2026-07-22.md](CONSOLIDATED-STATUS-2026-07-22.md).**
> PR #60 closed the entire blocker+high tier. **DONE (rows 1–8: B1, B2, H1, H2, H5,
> H3+I2, H4, M1)** — those 8 plan files can be deleted. **Still open:** rows 9–17
> (all medium/low): M2, M3, M4 (Postgres half only), M5, M6, L1, I3, L2, I6.

One plan per item. Severity from the audit; suggested order groups by theme and
respects the dependencies noted inside each plan.

| # | Plan | Item | Severity |
|---|------|------|----------|
| 1 | [fix-sync-runner-success-detection](fix-sync-runner-success-detection.md) | B1 | blocker |
| 2 | [fix-watchdog-env-loading](fix-watchdog-env-loading.md) | B2 | blocker |
| 3 | [fix-digest-marker-after-send](fix-digest-marker-after-send.md) | H1 | high |
| 4 | [fix-watchdog-per-asset-completion](fix-watchdog-per-asset-completion.md) | H2 | med-high |
| 5 | [fix-watchdog-utc-dates](fix-watchdog-utc-dates.md) | H5 | high |
| 6 | [unify-warehouse-path-resolution](unify-warehouse-path-resolution.md) | H3+I2 | high |
| 7 | [fix-massive-trade-date-conversion](fix-massive-trade-date-conversion.md) | H4 | high |
| 8 | [add-bronze-merge-locking](add-bronze-merge-locking.md) | M1 | medium |
| 9 | [fix-gap-aware-registry-bounds](fix-gap-aware-registry-bounds.md) | M2 | medium |
| 10 | [scope-intraday-coverage-recovery](scope-intraday-coverage-recovery.md) | M3 | medium |
| 11 | [add-30m-timeframe-parity](add-30m-timeframe-parity.md) | M4 | medium |
| 12 | [harden-r2-sync-error-handling](harden-r2-sync-error-handling.md) | M5 | medium |
| 13 | [add-jsonl-retention](add-jsonl-retention.md) | M6+I4 | medium |
| 14 | [enable-ib-connect-retry](enable-ib-connect-retry.md) | L1 | low-med |
| 15 | [dedupe-cli-dispatch](dedupe-cli-dispatch.md) | I3 | low |
| 16 | [fix-docs-drift](fix-docs-drift.md) | L2+I5 | low |
| 17 | [misc-config-knobs](misc-config-knobs.md) | I6 | low |

Sequencing constraints (from the plans + the 2026-07-05 Opus review cycle):

- 3 before 16 (docs describe post-fix marker semantics).
- 2 before 15 before 17 (same files; avoid parallel churn).
- 13's sync_runner log-dating composes with 1 — land 1 first.
- 7 starts with a ground-truth check against real Massive data before any code.
- **6 (path resolution) lands LAST of the code plans.** It sweeps ~20 files and
  mechanically overlaps 1, 3, 4, 5, 9, 10, 11, 13, 17 (same files). Landing the
  targeted fixes first and rebasing the sweep once is one rebase instead of nine.
- On `nightly_digest.py`: 3 before 5 (3 reorders `main()`; 5 then adjusts defaults).
- `run_daily_update_job.py` is touched by 4, 5, 13, 17 — land in that order, serially.
- 14's owner decision: APPROVED 2026-07-05 (typed transient-only retry) — recorded
  in the plan's `## Decision` section.
- 16's docs/architecture decision: APPROVED 2026-07-05 (commit the drafts) —
  recorded in the plan's `## Decision` section.

Review status: all 17 plans validated against source by an Opus review cycle on
2026-07-05 (verdict: 0 flawed; 8 had corrections, all applied in place).

Executor-hardening pass (2026-07-05, second Opus review): every plan now carries —

- **Preconditions / STOP-if-drift guards**: exact file:line content the executor must
  confirm before editing; any mismatch = STOP and report, never improvise.
- **Per-step verification**: a runnable command + expected output after each change.
- **Global gates**: `uv run pytest tests/ -v -m "not integration"` (the 2 time-bomb
  integration tests hang the full suite — always exclude), the 95% coverage gate, and
  `-W error::RuntimeWarning` where async runners are mocked.
- **Explicit defaults** replacing every "decide at implementation time" (each now has
  a measurable trigger or a picked default).
- **Owner-decision gates**: plans 14 and 16 instruct the executor to STOP unless a
  `## Decision` section records explicit approval.

Material corrections from that pass: plan 11 was missing `clients/postgres_schema.py`
(no `equities_30m` table → runtime missing-relation error); plan 7's Step 0 pointed at
staged parquet that is post-conversion (circular evidence) — redirected to REST `t` /
raw S3 `window_start`; plan 12's return-contract change breaks ~16 existing test
assertions (now enumerated); plan 4's legacy-marker rule now pins the exact regex
(`fromisoformat` cannot parse the trailing `Z`); plan 6 now carries the authoritative
26-file sweep list.

**Executor concurrency rule:** do NOT run two plans in parallel if they touch the same
file (see the orderings above); one plan = one branch = one PR, serialized on shared
files.

Item names (B/H/M/L/I) refer to the 2026-07-04 audit report in the session that
produced these plans.
