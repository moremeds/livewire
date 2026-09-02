# Livewire ledger — design

**Date** 2026-09-02 · **Status** design, unimplemented · **Overlays**
`2026-08-31-livewire-gap-autoheal-design.md`

This spec changes **where outputs go, who reads them, and who executes**. The
08-31 spec's registry, denominator and G1/G3/G14 semantics are unchanged and
still authoritative; nothing here re-opens them.

---

## 0. Diagnosis

Taxonomy of the 50 files in `docs/postmortems/`. Eight mechanism buckets:

| Bucket | Mechanism                                                        | Count             |
| ------ | ---------------------------------------------------------------- | ----------------- |
| A      | denominator drawn from the artifact it audits                    | 7                 |
| B      | absent or unreadable measurement graded OK                       | 6                 |
| C      | operating constant assumed, not measured                         | 11 (7 prose-only) |
| D      | one member's failure escalated to the population                 | 5                 |
| E      | gate placed above the phase that needs it; orchestrator coupling | 5                 |
| F      | wrong artifact or wrong host executed                            | 6                 |
| G      | the failure-reporting channel itself broken                      | 4                 |
| H      | derived record published on ambiguous input                      | 6                 |

A and H are largely closed by the Phase 1 registry (08-31 §1, §10).

**B, E, F and G — 21 post-mortems — share one structural defect: the system has
no ledger.** "What happened last night" exists only as log text, and is
reconstructed three separate times: `livewire_scripts/status.py` (865 lines, 19
`_*_section` textual matches across 10 section functions), the watchdog
(`check_daily_update_watchdog.py`, its own parser: `stale_equity_summary:47`,
`determine_watchdog_error:113`, `job_tail_complete` at
`run_daily_update_job.py:116`), and the digest. Three parsers over one prose
artifact is why a fix in one place leaves the other two broken.

Bucket C is the same defect one level down: constants live in code with no date,
scope or source — `FAILURE_RATE_TOLERANCE = 0.05`
(`livewire_scripts/sync_corporate_actions.py:27`),
`MDW_FLATFILE_MIN_PUBLISH_RATIO` default `"0.9"`
(`livewire_scripts/ingest_flatfiles.py:159`), `MDW_DAILY_JOB_DEADLINE_SECONDS`
default `4*60*60` (`run_daily_update_job.py:225`).

The residue is 20+ JSON state files with no schema and overwrite semantics. Two
exceptions carry a version and a history and are the pattern to generalize:
`clients/silver_revision.py` (`schema_version` at `:40`, rejected at `:195`) and
`clients/corporate_action_store.py` (pyarrow schema at `:125`, durable cursor
identities at `:63`/`:174`).

Recurrence is the evidence that log-text state does not hold a fix:

- corrupt parquet aborted `coverage` **9 times** after the publisher was fixed 2026-07-14.
- IB-as-SPOF was fixed at the lane, then recurred at the orchestrator on 2026-08-07.
- the alert channel broke **4 distinct ways in 19 days** (07-28 → 08-16).
- deadline starvation: 2026-07-28, 2026-09-01, 2026-09-02.

---

## 1. Ledger schema and write protocol

Chosen approach **A — append-only parquet, DuckDB read-only.**

Rejected **B** (SQLite WAL + DuckDB extension): a second store, and WAL over
`fcntl.flock` on exFAT is untested here. Rejected **C** (DuckDB native tables):
single-writer lock, and `.wal` residue is already treated as a publish refusal
(`clients/duckdb_catalog.py:497-499`, `refusing to publish: uncheckpointed
WAL`). A keeps the rule "DuckDB is never a second store".

**Location** `<lake>/ledger/<table>/date=<YYYY-MM-DD>/<run_id>.parquet`.
`"ledger"` is added to `PROTECTED_LAKE_DIRS`
(`livewire_scripts/housekeeping.py:46`, today `{"raw", "repairs"}`).

`clients/ledger.py` resolves its root from env `LW_LEDGER_ROOT` (default
`<lake>/ledger`) — same code, same schemas, same file layout whatever the root.

**Append-only.** A file is never rewritten. Corrections are new rows.

`clients/ledger.py` exposes exactly two functions:

- `emit(table, rows, *, run_id) -> Path` — publishes through
  `clients/parquet_io.publish_parquet` (temp → validate → `os.replace` at `:72`,
  under `fcntl.flock` at `:43`). The pyarrow schema per table is hard-declared;
  extra or missing columns raise.
- `query(sql)` — in-memory DuckDB, each table registered as a view over
  `read_parquet('<lake>/ledger/<t>/*/*.parquet', union_by_name=true)`.

`run_id = <job>-<utc-ts>-<pid>`, minted by the orchestrator and passed to
children via env `LW_RUN_ID`. **Children never mint one.** All timestamps UTC.

Keys are content-addressed; there is no `UNIQUE` constraint. Readers dedupe by
`GROUP BY <hash>` taking the latest row.

### Six tables

| Table          | Key                              | Columns                                                                                                              | Replaces                                                                                    |
| -------------- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `runs`         | `run_id`                         | job, host, release_sha, presets_sha, registry_sha, started, ended, exit_code, verdict ∈ {OK,DEGRADED,FAILED,UNKNOWN} | the `=== Job complete` marker (`run_daily_update_job.py:113`)                               |
| `lane_results` | (run_id, lane)                   | started, ended, exit_code, budget_s, elapsed_s, outcome ∈ {done,skipped,timeout,failed,blocked}, blocker             | `completed_scopes:388` / `skipped_scopes:403` log parsing                                   |
| `measurements` | (name, scope, measured_at)       | value, unit, source ∈ {measured,declared}, run_id                                                                    | bare constants; SUMMARY_JSON numeric fields                                                 |
| `findings`     | finding_hash                     | gap_class, symbol, asset_class, timeframe, sessions[], tier, source, run_id                                          | `repairs/tier_a_<date>.json`, `repairs/decisions_<date>.json`, `repairs/unresolved.json`    |
| `evidence`     | evidence_hash                    | kind, subject, payload_json, source_url, fetched_at, proposer ∈ {cron,agent,human}, run_id                           | home for `HashedRef` content (`clients/shepherd_repair.py:72`)                              |
| `executions`   | (evidence_hash, script, attempt) | args_json, release_sha, started, ended, exit_code, receipt_json                                                      | four shepherd receipt files (`shepherd_repair.py:152,168,215`), both undelivered-alert dirs |

`release_sha` on `runs` and `executions` closes bucket F: the artifact that ran
is recorded, not inferred.

The unresolved ledger becomes `findings` with `tier='unresolved'` plus one
`evidence` row `kind=unsourceable` carrying the reason — the key stays
(symbol, asset_class, timeframe, session), unchanged from
`clients/gap_engine.py:204`.

A failed alert send is an `executions` row with `script=send_alert, exit_code≠0`.
That is the whole replacement for `alerts_undelivered/` and
`MDW_UNDELIVERED_DIR` (`clients/quality_flags.py:123`, `status.py:589-590`).

**Not in the ledger:** bars; cursors (process resume state, not fact); presets
and registry — git is their history, and their sha256 goes on the `runs` row.

---

## 2. Orchestrator — `livewire_scripts/run_daily_update_job.py`

`_run_scheduled_lane` (`:713`) stays the single lane body (rule: every lane
pages, one shared body). It emits a `lane_results` row on entry and on terminal.
The `=== ... ===` lines are still printed for humans; **nothing parses them.**

**Replace the total deadline with per-lane budgets.** `JobDeadline` (`:198`,
`:223`) enforces `MDW_DAILY_JOB_DEADLINE_SECONDS` as a 4h total across seven
sequential lanes, so one slow lane starves the rest — corporate-actions ran
8h39m (2026-08-29) and 3h57m (2026-09-01) and killed equity, skipping Silver.
Instead: `LANE_BUDGET_S` per lane (declared measurements, §4). Over budget →
kill the process group → `outcome=timeout` → **the next lane starts normally.**
Closes bucket E for 07-28 / 09-01 / 09-02.

The Silver gate reads this run's equity `lane_results` row instead of the
in-process `lane_codes` dict (`:864`, `:919`).

`main()` (`:846`) emits the `runs` row at start — `release_sha` from
`readlink <warehouse>/current`, plus host, `presets_sha`, `registry_sha` — and
the terminal verdict at exit. Unexplained inter-lane gaps (the 5h18m on
2026-09-02) become a query: `runs.started` vs the first `lane_results.started`.

**Deleted:** `JobDeadline` and its threaded `remaining`; `completed_scopes:388`,
`skipped_scopes:403`, `log_has_completion_marker:421`, `job_tail_complete:116`,
`JOB_COMPLETE_MARKER:113` and their tests; `record_undelivered_alert:649` and
the second undelivered directory.

`sync_runner.py` is **not** deleted in L1 (verified 2026-09-02): it is live
production code reached by `launchd/com.livewire.intraday-catchup.plist.example:14`
→ `run_intraday_catchup_job.py:73` → `livewire_ingest.py:75`, and two modules
import from it (`run_daily_update_job.py:21`, `ingest_daily_flatfiles.py:27`).
L1 adds the `lane_results` emit to its `run_phase:126`; folding its phases into
`_run_scheduled_lane` would couple two schedules. Deleting its duplicated preset
constants (`:34-38`) is L2 scope.

**Test:** three lanes, the middle one sleeps past its budget; assert the third
lane's `outcome='done'` and that three `lane_results` rows exist. Delete
`test_a_lane_started_past_the_deadline_never_runs` — under per-lane budgets its
assertion is inverted.

---

## 3. status / watchdog / digest — one reader

`status.collect()` (`livewire_scripts/status.py:786`) runs `CHECKS`: a list of
`(name, sql)` tuples, each returning `(name, verdict, detail)`. **No rows →
UNKNOWN.** The existing `Verdict` IntEnum (`:51`, OK < UNKNOWN < WARN < BAD) is
kept unchanged. A new check is one SQL row plus one test — never another
`_foo_section` function.

Starting checks:

1. daily-update ran today (`runs` row, job='daily-update', started today).
2. every lane reached a terminal outcome (no NULL `outcome` for the last run).
3. Silver advanced (`lane_results` for lane='silver', outcome='done').
4. no undelivered alerts (`executions` where script='send_alert' and exit_code≠0 today).
5. `release_sha` on the last run == `origin/main` (the caller supplies the main sha; `status` never shells out to git).
6. declared vs measured drift (§4).
7. open requests (§5).

The **watchdog becomes a caller of `collect()`**: any BAD not yet alerted today
→ one email. The `.alerted` marker file (`record_alert_marker:143`) is kept —
it is idempotence state, not a fact about the run. `quality_summary_<date>.marker`
and its race (`run_watchdog:159-186`, `tail_pending:172`) go away: absence of a
lane row is UNKNOWN by construction, which is what the race was groping for.

The digest is unchanged — it renders `collect()`, so a check reaches both
surfaces or neither.

The coverage job emits `measurements(name='coverage_pct', scope=<view>)`
instead of printing `coverage:` lines for `_coverage_section:326` to re-parse
(and to pick the wrong one — 2026-08-14).

**Deleted:** the 10 section functions and all log regex in `status.py`
(~600 of 865 lines; keep `Verdict`, `collect`, `render:816`); the watchdog's log
parsing and `tail_pending`; `quality_summary_<date>.marker`; the regex tests.
`coverage_footer_cache.json` is **kept**: it is a per-parquet `(mtime,size)`
footer cache for the cold exFAT walk (`coverage_report.py:225-256`), not parsed
state; deleting it re-opens pm:2026-08-02-coverage-budget-expired-silently.

**Stated risk:** on cutover night a lane that forgets to emit reads UNKNOWN, not
green. That is intended (rule 9: a detector with no output is dead, not
healthy). The first nights are noisier.

**§2 and §3 land in ONE PR, promoted the same night.** There is no
half-old/half-new window in which two readers disagree.

---

## 4. Constants become measurements

`clients/constants.py` holds `DECLARED = {name: (value, unit, scope)}` and is
**the only place a number lives**:

```python
DECLARED = {
    "massive_flatfile_floor_days": (1827, "d", "flatfile"),
    "massive_rest_fx_rpm":         (5, "rpm", "fx"),
    "flatfile_min_publish_ratio":  (0.9, "ratio", "equity-intraday"),
    "failure_rate_tolerance":      (0.05, "ratio", "corporate-actions"),
    "lane_budget_s/<lane>":        (..., "s", "<lane>"),
}
```

Every value carries its scope, because every rate-limit and floor number in this
repo is scope-bound (the 5 req/min is FX-only).

The orchestrator emits the whole dict as `measurements(source='declared')` at
run start. Lanes emit `source='measured'` for what they actually observe:
coverage elapsed seconds, lane `elapsed_s`, corporate-actions per-symbol cost,
Massive's actual earliest available date.

**One check:** declared value vs the 14-day p95 of measured rows with the same
name and scope; differ by more than 2× → WARN, printing both numbers. This is
bucket C's closure — the 600s and then 1800s coverage budgets, each guessed and
each silently expired against a 1400–2860s cold scan, would have been flagged on
night one.

Env override collapses to one prefix: `LW_DECLARED_<name>`. Deleted: the
scattered constants and their per-module `os.getenv` reads
(`MDW_DAILY_JOB_DEADLINE_SECONDS`, `MDW_FLATFILE_MIN_PUBLISH_RATIO`, …) and the
corresponding env-var paragraphs in `docs/runbook.md`.

**Test:** changing a declared value without emitting it fails.

---

## 5. The agent evidence channel

**Contract.** Agents write only `evidence`. Scripts read only `evidence`. The
lake is written only by scripts. This is 08-31 §9's invariant with a table
instead of a file drop.

**Flow.** `findings` → agent searches (SEC 8-K, exchange notice, Polygon
reference) → `emit evidence(proposer='agent')` → a new final lane
`apply-evidence` in daily-update: for each `evidence` row with no `executions`
row at `exit_code=0`,

- `POLICY[kind] == "auto"` → run the existing script with `--evidence <hash>`;
- `POLICY[kind] == "human"` → wait for an `evidence` row
  `kind='approval', subject=<hash>, proposer='human'`. That wait is livewire's
  own: helium has no approval-wait primitive — its cron runs fail closed on
  `approval/request` (helium design.md:141) — so nothing here defers to helium.

The script verifies `sha256(payload) == hash` before acting — `shepherd_repair`
already requires `source_evidence` as `HashedRef`
(`clients/shepherd_repair.py:72`) — then emits `executions`.

`POLICY` lives in `constants.py`, e.g.
`{"corporate_action": "auto", "delisting": "auto", "price_basis": "human"}`.

### The inbox — agents never write the lake

One command, `livewire_ops.py ledger emit --table <t> --json '{…}'` (a new key
in `COMMANDS`, `scripts/livewire_ops.py:23`). What differs per caller is
`LW_LEDGER_ROOT`, nothing else.

A helium tenant sets `LW_LEDGER_ROOT=<worktree>/outputs/ledger`, so the agent's
`ledger emit` produces a normal `evidence/date=<d>/<run_id>.parquet` inside the
sandbox's only persistent path (helium design.md:312, 317-322) — schema-validated
by the same code, never touching `~/market-warehouse/`. helium's `guard.ts`
deny-lists every write under the warehouse _after_ the allow-list
(design.md:324-337); routing through `outputs/` satisfies that with **no
exception in `guard.ts`**.

The `apply-evidence` lane's first step is **ingest inbox**: for each parquet
under `LW_EVIDENCE_INBOX` (a livewire-cron-configured path), re-emit its rows
into `<lake>/ledger/evidence/` with `proposer` forced to `agent` — an inbox file
cannot claim `human` or `cron` — then delete the inbox file. Ingest is
idempotent by `evidence_hash`.
**PRECONDITION:** helium #72 (durable `outputs/` path, `LW_LEDGER_ROOT`
passthrough, ledger-emit tool) — `LW_EVIDENCE_INBOX` is named once #72 (1) is
decided.

Humans and livewire cron keep the default root and write the lake directly; they
are inside the trust boundary.

The inbox **is** a ledger with a different root: no second format, no second
validator, no import path — that is the consolidation argument.

### helium boundary

- helium's `span` table (`~/.helium/audit.db`, design.md:353-371) is token/cost
  telemetry. It is **not** `executions` and does not become it.
- No push from ledger to helium. helium polls on its own launchd cron
  (design.md:566-569); livewire's `apply-evidence` lane is the sole executor.
- livewire's side of §5 — inbox ingest, `POLICY`, chains, delisting, requests —
  ships and is useful with **zero agents**: human requests and cron-proposed
  delistings exercise every path.
- The agent role itself is helium M3-M5 (design.md:534-536; issues
  moremeds/helium #67 #68 #69, `on-hold` as of 2026-09-02). A stated
  dependency, not a blocker for L3.
- The three things livewire needs from helium are tracked as moremeds/helium
  #72; everything above ships without them.

### Delisting takes the same route

`livewire_scripts/universe_sync.py` stops doing the in-band archive: the
`shutil.move` to `bronze-delisted/` at `:143` and the `--skip-dead` branches at
`:273` / `:295` are removed. It emits
`evidence(kind='delisting', proposer='cron', source_url=<Polygon ref>)`; the
move becomes an `executions` row. The `--skip-dead` flag (`:151`) is deleted.

### Requests are evidence too

Human intent enters the same table:
`evidence(kind='request', subject='silver:TSLA',
payload={"target": "full-history-silver-grade"}, proposer='human')`.

`POLICY` maps a request kind to a **chain of existing scripts whose last step is
a verify**:

1. `ingest daily --symbols {s} --source ib` — below the Massive floor only IB can serve, so this is Tier B by definition.
2. `ingest corporate-actions --symbols {s} --full-reconcile`
3. `store resolve-yahoo-basis --symbols-file <tmp json> --apply --ib-verify`
4. `store rebuild-silver --tickers {s}`
5. verify: the silver revision contains `{s}` with `first_date <= inception`.

One `executions` row per step, same `evidence_hash`, `attempt` incrementing. The
request is complete **iff** the verify step has `exit_code=0`; otherwise `status`
shows `open requests: silver:TSLA blocked at step N (<blocker>)`. IB exit 86 →
`outcome='blocked'`, the request stays open and is retried the next night — Tier
B by definition, and consistent with "IB down is DEGRADED, never failed". Steps
already at `exit_code=0` are skipped on rerun, so corporate-actions is not
re-pulled when IB returns.

**VERIFIED flags:** `--tickers` at `livewire_scripts/rebuild_silver.py:50`;
`--symbols-file` at `livewire_scripts/resolve_yahoo_basis.py:57`.
**UNVERIFIED precondition:** whether `ingest daily` and
`ingest corporate-actions` accept a per-symbol scope. Check at implementation;
if they do not, adding it is part of L3, not a new script.

**Deleted in L3:** `universe_sync`'s in-band move, `--skip-dead`, and the
dead-ticker branch; `write_tier_a_manifest` (`coverage_report.py:539`) and
`write_decision_requests` (`:544`) with their call sites (`:1002-1003`);
`load_unresolved` / `record_unresolved` file I/O (`clients/gap_engine.py:213`,
`:236`); the four shepherd receipt files; the in-file capped changelog in
`clients/tag_registry.py` (`:45`, `:97`).

---

## 6. Ordering

| Phase  | Contents                                                                                                   | Note                       |
| ------ | ---------------------------------------------------------------------------------------------------------- | -------------------------- |
| **L1** | §1 + §2 + §3 — ledger, orchestrator, readers                                                               | one PR, promoted one night |
| **L2** | §4 — constants become measurements                                                                         |                            |
| **L3** | §5 — delisting first (it unblocks the Sunday universe-refresh risk), then corporate actions, then requests |                            |

House rule this spec is bound by: **consolidate — every addition deletes.**
Nothing here is added without naming what it removes.

---

## 7. Acceptance criteria

Checkable, on the mini, after promote:

1. `select count(*) from lane_results where run_id = <last> and outcome is null` → `0`.
2. `select release_sha from runs order by started desc limit 1` equals `git rev-parse origin/main`.
3. `select count(*) from runs where date(started) = current_date and job = 'daily-update'` → `1`.
4. `grep -c '_section' livewire_scripts/status.py` → ≤ 2.
5. `grep -n 're\.compile' livewire_scripts/status.py livewire_scripts/check_daily_update_watchdog.py` → no match against daily-update log text.
6. After L3: `ls <lake>/repairs/tier_a_*.json` shows no file newer than the L3 promote.
7. A lane sleeping past its budget does not delay the next lane — new test, three lanes, asserts the third `outcome='done'`.
8. `grep -n 'shutil.move' livewire_scripts/universe_sync.py` → no match.
9. `grep -rn 'MDW_DAILY_JOB_DEADLINE_SECONDS\|MDW_FLATFILE_MIN_PUBLISH_RATIO' --include='*.py' .` → no match outside `clients/constants.py`.
10. Declared/measured drift check emits a WARN row when a declared value is edited to 2× its measured p95.
11. `grep -rn 'market-warehouse' <helium repo>/plugins/livewire-shepherd/` → no write path; agent evidence reaches the lake only via the `apply-evidence` ingest step (test: an inbox parquet with `proposer='human'` lands as `proposer='agent'`).

---

## 8. Defaults chosen without explicit approval

- `payload_json` is a string column, not a struct — `evidence` payloads differ by kind (corporate action vs delisting vs request) and a union schema would churn.
- No `stdout_tail` column on `executions`. Logs stay logs; the ledger holds facts.
- `run_id` format `<job>-<utc-ts>-<pid>`.
- The ledger root is an env var, `LW_LEDGER_ROOT`, not a flag; inbox ingest forces `proposer='agent'` rather than rejecting a mismatched value.
- Dedupe by `GROUP BY <hash>` taking the latest, rather than any uniqueness enforcement at write time.
- Partitioning by `date=` only (no asset-class partition) until a scan is measured slow.
- The `.alerted` watchdog marker file survives as a file rather than becoming a ledger row.
- `analytics.duckdb`'s coverage table is left in place (see Non-goals).

---

## 9. Non-goals

- Not migrating historical logs into the ledger.
- Not touching bar or silver formats.
- Not choosing the agent runtime.
- Not building the helium tenant/tool (`plugins/livewire-shepherd`) — that is helium M3-M5.
- Not replacing the `analytics.duckdb` coverage table — it stays, still a rebuildable cache.

---

## 10. Evidence index

Bucket → post-mortem files in `docs/postmortems/`.

**A — denominator from the audited artifact (7)**
`2026-09-01-gap-engine-disk-glob-denominator` · `2026-08-17-preset-overlap-duplicate-denominator` ·
`2026-09-01-xnys-only-denominator` · `2026-09-01-registry-row-resolving-to-no-symbols` ·
`2026-08-17-interior-gap-scan-measures-liquidity` · `2026-09-01-futures-expiry-vs-scan-window` ·
`2026-09-01-g14-terminus-fails-closed`

**B — absent measurement graded OK (6)** → closed by §1, §3
`2026-08-02-coverage-budget-expired-silently` · `2026-08-16-watchdog-raced-quality-marker` ·
`2026-08-16-status-surface-grading` · `2026-08-14-coverage-log-first-line-oldest` ·
`2026-07-26-no-trade-paged-quiet-weekends` · `2026-09-01-g2-g13-not-emitted`

**C — constant assumed, not measured (11)** → closed by §4
`2026-07-29-flatfile-get-floor-list-lies` · `2026-07-29-massive-floor-derived-from-scan-date` ·
`2026-07-22-flatfile-min-publish-ratio` · `2026-07-28-daily-job-deadline-is-a-total` ·
`2026-07-27-ib-earliest-date-false-range-shortfall` · `2026-07-27-fx-dxy-provider-floors` ·
`2026-07-17-triage-aggs-entitlement-floor` · `2026-08-10-appledouble-sweep-cost` ·
`2026-08-02-duckdb-glob-enumeration-cost` · `2026-08-31-source-evidence-per-response-cost` ·
`2026-07-22-sync-phase-timeout`

**D — one member escalated to the population (5)**
`2026-07-14-corrupt-parquet-aborted-publish` · `2026-09-01-coverage-aborted-on-corrupt-parquet` ·
`2026-08-02-corporate-actions-failed-on-one-symbol` · `2026-07-19-interior-day-warning-email-storm` ·
`2026-07-19-cancellation-inference-provider-scoped`

**E — gate above the phase; orchestrator coupling (5)** → closed by §2
`2026-08-08-ib-down-must-not-fail-the-run` · `2026-07-22-ib-not-a-single-point-of-failure` ·
`2026-07-22-coverage-weekly-digest-ordering` · `2026-07-28-lane-alert-paths-missing` ·
`2026-07-18-ib-gateway-lan-ip-silent-timeout`

**F — wrong artifact or host executed (6)** → closed by `release_sha`/`host` on `runs` and `executions`
`2026-07-27-launchd-pointed-at-worktree-no-env` · `2026-07-29-promote-runs-checkout-builder` ·
`2026-07-29-rm-rf-release-current-dangling` · `2026-07-29-release-missing-node-modules` ·
`2026-09-01-universe-refresh-runs-from-repo` · `2026-09-01-gap-scan-launchagent-not-removed-by-deletion`

**G — reporting channel broken (4)** → closed by `executions(script='send_alert')`
`2026-08-16-quoted-printable-corrupted-digest` · `2026-08-08-alert-value-starting-with-dashes` ·
`2026-08-02-lane-runner-ran-the-alert` · `2026-08-10-nightly-disk-line-wrong-volume`

**H — derived record on ambiguous input (6)**
`2026-07-18-unknown-price-basis-population` · `2026-07-18-silver-seed-floor-blind-heuristic` ·
`2026-07-18-window-regressions-withheld` · `2026-08-02-two-active-splits-one-ex-date` ·
`2026-09-01-tier-follows-repair-source` · `2026-09-01-unresolved-ledger-key`
