# IB-outage isolation, a sendable alert, a coverage job that finishes, and resource housekeeping

**Date:** 2026-08-09
**Status:** approved, pending implementation plan

## Why

The 2026-08-03 fixes (#76, #77, #78) held: seven nights ran with zero crashes and
Silver rebuilt nightly, revisions 20 → 24. Reviewing that week surfaced four
defects, three of them variants of a failure mode this repo keeps re-learning —
**a check that still runs, still reports, and is still green, while measuring the
wrong thing.**

### 1. A down IB Gateway kills seven non-IB phases

`scripts/livewire_ingest.py` lists the two *orchestrators* in `IB_COMMANDS`:

```python
IB_COMMANDS = {
    "daily", "historical", "robust", "intraday-backfill",
    "universe", "backfill-all", "daily-backfill",
}
```

`main()` calls `assert_gateway_up()` before dispatching (`livewire_ingest.py:111-114`),
so a down Gateway exits 86 before phase 1. `daily-backfill` has nine phases and
only **two** use IB (30m/5m volatility intraday). The Massive `equity_day_aggs`
lane, the Massive flat-file intraday lane, FRED rates and CBOE — none of which
touch IB — never ran.

Measured 2026-08-08:

```
=== Intraday Catchup 2026-08-08T05:00:04Z ===
ERROR: IB Gateway not reachable on 127.0.0.1:4001
=== Failed 2026-08-08T05:00:14Z (exit_code=86) ===
```

Ten seconds. Nine phases, zero run.

**Cost:** Friday 2026-08-07 equity daily bars are absent from bronze. NVDA, AAPL,
SPY, MSFT and JPM all end at 2026-08-06. Both jobs that could have ingested that
session were stopped by the same down Gateway — `daily-update` skipped its equity
lane by design (it is an IB lane), and `intraday-catchup` never reached its
Massive lane because of this bug.

**This is not a weekend pattern.** Over sixteen logged days, exit 86 occurred on
2026-07-27 (Mon), 2026-08-08 (Sat) and again on 2026-08-09 (Sun). The previous
weekend — 2026-08-01 (Sat) and 2026-08-02 (Sun) — both completed normally. IB
outages are sporadic; what makes them expensive is that the blast radius is total.

The 2026-08-09 coverage pass measured the damage warehouse-wide for the
2026-08-07 session: **equity 0/13311, futures 0/14, rates 0/4.** Only
volatility (42/43) has it, because CBOE and FX are the two lanes of the *other*
job that do not touch IB.

CLAUDE.md already states the invariant this violates: *"IB is not a single point
of failure."* That invariant was implemented in `run_daily_update_job` and never
checked against `sync_runner`.

### 2. The one alert that should have fired could not be sent

```
WARNING: failure alert returned non-zero exit code 1.
Failed to send daily update failure alert: Missing value for --error-summary
```

`livewire_node/send_daily_update_failure_email.mjs:79-83`:

```js
const key = token.slice(2);
const value = argv[index + 1];
if (value == null || value.startsWith("--")) {
  throw new Error(`Missing value for --${key}`);
}
```

There is no `--key=value` parsing. Any flag value beginning with `--` throws. The
error summary is log-derived text and on 2026-08-08 began with
`--- Runbook: ...`. The 05:00 page never went out; the watchdog caught it at
10:30 — **5.5 hours late**.

This is not specific to `--error-summary`: every flag is affected.

### 3. Coverage still exceeds its budget

PR #78 raised the budget 600s → 1800s and added `FOOTER_READ_WORKERS = 16`. Five
of six nights still timed out:

```
Quality jobs: 1 FAILED
  coverage report: ... timed out after 1800 seconds
```

Only 2026-08-04 produced a coverage log. Weekly reports remain 83-byte stubs and
week 31 was never generated at all — `weekly` is a pure parser over coverage
logs, so it is downstream, not separately broken.

**The 1800s number was a guess and the guess was wrong.** The measurement it
rested on (~1061s, taken 2026-08-02) was made against a warm filesystem cache.
The structural fact underneath:

| Condition | Wall clock |
|---|---|
| equity `1d`, 13,270 footers, warm, 16 threads | 29.2s |
| equity `1d`, 13,270 footers, warm, 1 thread | 154.0s |
| **full five-timeframe pass, cold, 16 threads** (2026-08-09, 03:43:54Z → 04:31:32Z) | **2858s** |

2858s against an 1800s budget — 1.6× over, with `--no-recover` so no recovery
subprocess was included.

The lake lives on an external exFAT volume
(`/Volumes/DATA_LAKE`, 13 TiB). Metadata traversal there is orders of magnitude
slower than the internal disk — a plain `du -sh` over `bronze` also ran for
minutes without returning. The nightly intraday job writes 23.57 GB, which
evicts the cache before coverage starts, so **cold is the normal condition**.
No thread count fixes a cold metadata walk on that volume.

### 4. The nightly disk check watches the wrong volume

```
data-lake -> /Volumes/DATA_LAKE/livewire/data-lake     13Ti   49%   6.6Ti avail
~/market-warehouse (releases, logs, venv, cursors)    228Gi   93%    15Gi avail
```

`nightly_digest._disk_section` calls `shutil.disk_usage(data_lake)` and prints
`Disk: 6752.4 GiB free (48% used)` every night. The volume that actually holds
`releases/` (3 × 422 MB), `logs/` (395 files, 306 MB, oldest 2026-06),
`cursors/` (176 MB) and the venv sits at **14.7 GiB free — already below the
25 GiB `MDW_FLATFILE_MIN_FREE_GB` reserve — and nothing reports it.** Each
`release promote` consumes another 422 MB.

One symlink was enough to silently swap the monitored object.

**Correction to an earlier framing of this section.** livewire's total footprint
on that volume is ~2.5 GB out of 173 GB used, so the 93% is not livewire filling
the disk and housekeeping will not meaningfully change it. This is a *monitoring*
gap: the warehouse writes to a volume whose headroom nothing measures, and the
next `promote` is the operation that would hit the wall. The reclaimable bulk —
26 GB in `data-lake/repairs/`, 21 GB of it 12,636 verbatim `.parquet.bak` files
from the 2026-07-15 cutover — sits on the *other* volume, which has 6.6 TiB free.
Nothing forces that call, so it stays an operator decision and out of the
automated policy.

## Design

### Part 1 — IB isolation

**1.1 Move the preflight to phase granularity.** Remove `backfill-all` and
`daily-backfill` from `IB_COMMANDS`. Nothing is lost: `sync_runner.py:316-332`
invokes the IB phases as `livewire_ingest.py intraday-backfill --source ib …`,
and `intraday-backfill` is itself in `IB_COMMANDS` with
`_requires_ib_preflight` returning `True` unconditionally for it. The check moves
to where the dependency actually is.

**1.2 A phase exiting 86 is degraded, not failed.** Mirror what
`run_daily_update_job` already does: record it as degraded, keep it out of
`failures`, and do not turn the orchestrator red for it.

**1.3 The equity lane falls back to Massive.** When the equity lane's preflight
returns 86, re-run it once with `--source massive`. `_requires_ib_preflight`
explicitly exempts that path, so the retry cannot hit the preflight again. Silver
then rebuilds on IB-outage nights, because Silver depends only on equity bronze
and the corporate-action store — both Massive-backed.

**Futures and cmdty deliberately get no fallback.** Massive does not carry those
asset classes. They stay degraded, which is correct.

### Part 2 — A sendable alert

Add `--key=value` parsing to `parseArgs` (split once on `=`), and change the
Python call sites to pass `--error-summary=<text>` as a single token. No value
can then be mistaken for a flag, whatever it contains.

Rejected: sanitising leading dashes out of the summary. That edits the message to
suit the parser — an alert that lies about what happened.

### Part 3 — Coverage as its own job, made incremental

**3.1 Its own launchd job.** Remove coverage from `_spawn_post_success_quality`;
add `com.livewire.coverage` pointing at `<warehouse>/current`, with **no budget**.
An arbitrary timeout is what produced this bug twice.

**3.2 Incremental footer reads.** Persist `(symbol, timeframe) -> (mtime,
latest_date)`. Each run `os.stat`s the file and re-opens the footer only when
mtime changed. The first night stays slow; subsequent nights touch only what
actually changed.

**3.3 Break the ordering coupling.** `nightly_digest._coverage_section` looks up
`coverage_<target_session>.log`, which an independently scheduled coverage job
will always miss. Change it to read the most recent coverage log and print that
log's own date. A one-day lag is irrelevant to a freshness trend, and this
removes the run-order dependency instead of pushing it onto the watchdog's
schedule.

### Part 4 — Housekeeping

**4.1 Fix the disk check.** `_disk_section` reports **both** volumes — the lake
volume and the warehouse volume — and warns when either falls under the reserve.
This is the only item in Part 4 that is a defect rather than maintenance.

**4.2 `scripts/livewire_ops.py housekeeping [--dry-run]`**, dry-run by default:

| Target | Retention |
|---|---|
| `logs/*.log` | 60 days |
| `releases/` | reuse the existing `release prune`, keep 3 |
| `silver/evicted/<rev>` | keep the 2 most recent (present: 10, 12, 14, 19, 21; current revision is 24) |
| AppleDouble `._*` under the lake | delete all — exFAT artifacts that also pollute symbol discovery |

**Never touched, asserted in code and in tests:**

- `raw/` flat-file partitions — older than the rolling GET floor means permanent loss
- `repairs/triage/` — a verdict obtained today may be unobtainable next year
- `repairs/*/backup/` — the only basis for `rollback-legacy-basis`
- the release `current` points at — deleting it leaves `current` dangling and
  `promote` refuses to rebuild

**4.3** Runs after the digest. A manual `--dry-run` pass is reviewed before
anything is deleted for the first time.

## Testing

- **1.1** — a test asserting `daily-backfill` and `backfill-all` do *not* require
  preflight, and that `intraday-backfill` still does.
- **1.2** — a `sync_runner` test where one IB phase exits 86: the orchestrator
  exit code is not a failure and `SUMMARY_JSON["failed"]` excludes it.
- **1.3** — a `run_daily_update_job` test where the equity lane's first invocation
  exits 86: assert the retry command contains `--source massive`, and that
  futures/cmdty get **no** such retry.
- **2** — a `parseArgs` test with a value beginning with `--`, plus a test that
  the Python call site emits the single-token `=` form. The existing
  `TestTheLaneRunnerNeverRunsTheAlert` pattern applies: use the real signature,
  no `**kwargs`-swallowing fake.
- **3.2** — an incremental test: unchanged mtime reuses the cached date without
  opening the file (assert the footer reader is not called); changed mtime re-reads.
- **3.3** — a digest test with a coverage log whose date differs from the target
  session: it is found and its date is printed.
- **4.1** — a `_disk_section` test with two volumes where only the non-lake one is
  under reserve: the warning fires.
- **4.2** — a housekeeping test asserting each protected path survives a run with
  aggressive retention.

## Out of scope

- **The 2026-08-07 gap.** Part 1.1 lets the day_aggs 7-day lookback heal it; no
  backfill code. If the Gateway is still down near **2026-08-15** the gap leaves
  that window and needs a manual
  `flatfile-ingest-daily repair --dates 2026-08-07`.
- **Silver `window_regressions` drifting 34 → 40** across five nights. Worth its
  own investigation, not this change.
- Tasks #4 (241 symbols missing from Silver), #5 (stale-symbol archival), #6 (30m
  in the weekly report) are unaffected.

## One PR

All four parts ship together. They are four faces of one property: a scheduled
job should not be taken down by a dependency it is not using, its alert must be
sendable, its detector must be able to finish, and its resource use must be
observed. Splitting them makes each harder to review, not easier.
