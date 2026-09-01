# Livewire gap auto-heal — design

**Date:** 2026-08-31 (Asia/Hong_Kong)
**Status:** design, pending review
**Phase 1 scope:** Livewire only, zero model calls, no Helium dependency
**Phase 2 scope:** Helium judgment lane (interface defined here, not built here)

## 1. Why this is not another coverage project

Coverage work has recurred in this repo for five months:

| Date             | Artifact                                                              |
| ---------------- | --------------------------------------------------------------------- |
| 2026-04-05       | `livewire_scripts/health_check.py` (16 commits since)                 |
| 2026-04-06       | `livewire_scripts/coverage_report.py` (18 commits since)              |
| 2026-06-07       | `livewire_scripts/warehouse_health_report.py`                         |
| 2026-07-10/11/16 | three audits under `docs/audits/`                                     |
| 2026-08-09/10    | `ib-isolation-coverage-housekeeping`, `graded-status-surface` designs |

That table understates it. A full read-only audit of all 64 scripts (§15) found
**ten** scripts, **4,810 lines**, that each independently decide what "expected"
or "healthy" means:

`check_gaps` · `coverage_report` · `data_quality_report` · `health_check` ·
`shepherd_actions` · `shepherd_daily` · `validate_adjusted_history` ·
`validate_silver_canary` · `warehouse_health_report` · `weekly_quality_summary`

Each project completed. Each was followed by another.

**Including the newest one.** `shepherd_daily.py` and `shepherd_actions.py` were
added in 2026-08 by Livewire PR #90 as the fix for this problem; the audit
classifies them as detectors 7 and 8 — they independently re-plan and re-verify
expected coverage exactly like the six before them. The most recent attempt to
end the recurrence is itself the most recent instance of it. Any design that does
not explain why it is not number eleven is not worth implementing.

The reason is recorded in this repo's own audit,
`docs/audits/2026-07-16-silver-correctness-gap-from-apex.md`:

> **coverage is not correctness.** At least 165 of the published symbols are
> corrupt — double-adjustment garbage — and Apex is serving them live [...] This
> population is **absent from the 593 failure record** and has **no phase in the
> current plan**.
>
> a missing symbol fails visibly (HTTP 500, fail-closed); **a corrupt symbol
> renders a plausible, wrong chart.**

Three facts held at once: the coverage accounting was correct, the population
that actually hurt was not in it, and the finder was the downstream consumer
(Apex), not any of the three detectors.

**Recurrence mechanism:** every project measures _what it set out to fix_, not
_what "unhealthy" means_. When a new failure dimension appears, the output is a
new script and a new audit document. Documents do not run at 03:00.

**This design's one job:** make the definition of "unhealthy" a persistent,
growable, data-driven artifact, so that each incident becomes a permanent check
rather than a new project.

### 1.1 Success test for this design

> The next time a new gap type appears, is the output **one registry row plus one
> test**, or **a new script / a new audit / a new project**?

Former: this was the last coverage project. Latter: recurrence again.

Corollary constraint: **everything is data (registry rows), not code (detector
scripts).** Three detectors exist precisely because each new requirement became
new code.

## 2. Root causes and what fixes each

| #   | Cause                                                                                     | Fix                                                   | Needs agent? |
| --- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------- | ------------ |
| 1   | Three detectors, three "expected" definitions, no authoritative denominator               | Single denominator (§4)                               | No           |
| 2   | The denominator itself drifts (renames, delistings, new listings, index changes)          | Agent-maintained (§9)                                 | **Yes**      |
| 3   | Each repair round exposes a new gap type, spawning a new project                          | Full taxonomy written once (§3)                       | No           |
| 4   | A finished project is unwatched; it breaks again months later                             | Standing cron with a hard failure signal              | No           |
| 5   | The same unsourceable symbols are re-litigated every round, and giving up is not recorded | Persistent unresolved denominator with reasons (§4.4) | No           |

Four of five causes are structural. Cause 2 is the only one that never stops,
because the market never stops changing — that is where the agent belongs, and
only there.

**Honest limit:** this design cannot guarantee no new failure mode is ever
discovered (unknown unknowns). It guarantees that **a failure mode that has
happened once cannot silently happen twice.**

## 3. Gap taxonomy

Completeness gaps:

| ID  | Gap                     | Definition                                                 |
| --- | ----------------------- | ---------------------------------------------------------- |
| G1  | Tail gap                | Latest expected session missing                            |
| G2  | Interior gap            | Session missing inside an existing series                  |
| G3  | Missing symbol          | In the denominator, no file on disk                        |
| G4  | Missing class/timeframe | Whole class or timeframe absent                            |
| G5  | Intraday partial        | Session present, bar count below expected for that session |
| G13 | Head gap                | Series starts later than the expected history horizon (§13, default 1995-01-01) — distinct from G2, which is bounded by existing bars on both sides |

Correctness gaps (the population that hurt in 2026-07-16):

| ID  | Gap                            | Definition                                                                                                                                                   |
| --- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| G6  | Structural corruption          | OHLC violation (`h<l`, `c∉[l,h]`), non-positive price, duplicate rows, out-of-order timestamps                                                               |
| G7  | Source revision drift          | Row exists but the source has since restated it                                                                                                              |
| G8  | CA-induced silver staleness    | Silver derived from pre-event bronze after a corporate action                                                                                                |
| G9  | Identity break                 | Rename / ticker reuse glued into one series                                                                                                                  |
| G10 | Silver derivation gap          | Bronze present, silver missing or derived from an older bronze revision                                                                                      |
| G11 | Missing corporate-action event | Mechanical price jump with no matching event in the CA store                                                                                                 |
| G12 | **Silent mispricing**          | Adjusted series deviates from an independent reference beyond a threshold (double adjustment, wrong basis) — every row individually legal, all of them wrong |

G12 is the class that produced the 2026-07-16 incident. It is invisible to G6:
double-adjusted rows satisfy every structural constraint.

## 4. The denominator

Today, `livewire_scripts/coverage_report.py:274` and `:379` derive the
denominator by globbing the disk (`glob("symbol=*/1d.parquet")`). A symbol that
never landed is therefore **undetectable by construction**, and
`coverage_report.py:363` hardcodes `NON_EQUITY_ASSET_CLASSES = ("volatility",
"futures", "rates")`, omitting `fx` and `cmdty`.

**Replacement:**

```
expected = presets × trading_calendar × timeframe × asset_class
gap      = expected − actual
```

Every gap type in §3 falls out of one comparison. No per-class bespoke detector.

### 4.1 Existing parts reused (no new sources of truth)

| Input                  | Source                                                                                                                              | Status                                              |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| Symbol universe        | `presets/*.json` (`sp500.json` 4,197 lines, `ndx100`, `r2k`, `etfs`, `adrs`, `futures-*`, `fx-pairs`, `cmdty-metals`, `volatility`) | exists                                              |
| Expected sessions      | `clients/trading_calendar.py`                                                                                                       | exists                                              |
| Class list             | `clients/duckdb_catalog.py:62` `_DAILY_ASSET_CLASSES`                                                                               | exists                                              |
| Repair executor        | `clients/shepherd_repair.py` (own `fcntl.flock(LOCK_EX)` at `:905`)                                                                 | exists, proven                                      |
| Correctness comparison | `clients/adjusted_history_validation.py` — `compare_series:235`, `find_mechanical_split_jumps:355`, `_relative_error_bps:163`       | **exists as a library, wired to nothing scheduled** |
| Backup + rollback      | per-file backup with `backup_sha256` (`CLAUDE.md:770`)                                                                              | exists                                              |
| Scheduling             | five `launchd/com.livewire.*` jobs                                                                                                  | exists                                              |

The recurring pattern in this repo: the parts exist, nothing joins them into one
standing line. This design is mostly wiring, not new machinery.

### 4.2 Futures: expiry-aware denominator

Futures symbols are composite (`ES_202506`) and expire. The expected set is a
function of the as-of date, not a static list. `presets/futures-*.json` defines
roots; the denominator expands roots × active contracts for the date.

### 4.3 Delisted

`bronze-delisted/asset_class=equity/` (`CLAUDE.md:929`) is the archive for
symbols excluded from future syncs. Archived symbols leave the live denominator
and enter the frozen-history denominator (§6).

**The terminus date already has a schema and two independent producers** (source
read 2026-09-01):

| Part           | Location                                                                            | Gated on                        |
| -------------- | ----------------------------------------------------------------------------------- | ------------------------------- |
| Provider field | `clients/universe_client.py:40,156` `delisted_utc` (Polygon `/v3/reference/tickers`) | `MASSIVE_API_KEY`               |
| Stored field   | `clients/tag_registry.py:27` `delisted_at`, written by `:172 mark_delisted()`         | —                               |
| Date producer  | `livewire_scripts/universe_sync.py:273`–`274` — the **only** `mark_delisted` caller   | `MASSIVE_API_KEY`, else skipped |
| Archive move   | `livewire_scripts/universe_screener.py:380`                                           | IB Gateway (scanner sweeps)     |

They are **not one chain.** `universe_sync` is Polygon-backed and writes the
date; `universe_screener` is IB-Scanner-backed and moves the directory. Nothing
joins them, so `bronze-delisted/` can gain a symbol that has no `delisted_at`,
and vice versa. Whichever one L4 builds on, it inherits that provider's outage
mode — and joining the two is itself part of L4, not a given.

What matters for the denominator: the terminus does **not** have to come from
"last bar on disk", so the circular no-bar-vs-no-trade problem (`CLAUDE.md`,
interior gap scan) stays out of it.

**Neither producer can run on the development host, and this was not measured
against production.** Checked 2026-09-01 on the dev machine: no
`com.livewire.*` plist installed, newest warehouse log `2026-05-31`, no
`releases/`, no `current`, no `~/market-warehouse/.env`, IB port 4001 closed,
and `MASSIVE_API_KEY` set in no env file on the box. `registry.json`,
`bronze-delisted/` and `security_master/` are all absent **here** — which is
consistent with a stale dev copy and is **not evidence about the production
warehouse.** Production state must be re-checked on the host that actually runs
the jobs before any of this is treated as a finding.

Consequence for Phase 1 regardless of which way that check comes out:
`build_denominator` has no delisted branch, and the branch cannot be written
against a store whose row count is unknown — it would pass its tests and change
nothing, the vacuous-test failure this plan already hit once in the
futures-expiry test written against a preset containing no expired contract.
**L4's first step is a measurement on the production host, then a producer run;
the boundary definition was never the blocker.**

### 4.4 Unresolved denominator (cause 5)

A symbol/session that cannot be sourced from any provider is recorded once, with
reason and as-of date, and **stops being retried**. It remains visible in
coverage output as unresolved — never silently dropped, never re-litigated.

### 4.5 The registry row

This is the artifact the whole design turns on. "Coverage" is a set of rows, and
growing coverage means adding a row, not writing a detector.

```yaml
- id: g12-equity-adjusted-deviation
  gap: G12 # from the §3 taxonomy
  scope:
    asset_class: equity
    timeframe: 1d
    layer: silver
    universe: [sp500, ndx100, r2k] # preset names, not symbol lists
  check: adjusted_deviation_bps # names an existing check kind
  params:
    reference: yahoo_adjusted
    warning_bps: <calibrated>
    failure_bps: <calibrated>
  tier: B # A = auto-repair, B = escalate
  repair: null # executor name when tier is A
  since: 2026-07-16 # the incident that created this row
  test: tests/registry/test_g12_equity_adjusted_deviation.py # mandatory
```

Rules that make this work:

- **A row without a `test` is rejected.** This is what makes "one row plus one
  test" (§1.1) the literal acceptance path for new coverage.
- **`since` is the anti-recurrence ledger.** Every row traces to the incident
  that justified it, so nobody re-opens a settled question.
- **Honest limit:** new check _kinds_ are code; new check _rows_ are data. §3
  fixes the kinds (G1–G12) and rows parameterize them across universes, classes
  and thresholds. A genuinely novel failure mode may still need a new kind — that
  should be rare, and proposing it is the agent's job 2 (§9.2).

### 4.6 Identity key

`ExpectedSeries.symbol` and the unresolved-ledger key are ticker strings, while
`clients/security_master.py` exists precisely to end ticker-string joins (the
2,345 reused tickers). On a reuse the ledger records the predecessor company's
"unresolvable" against the successor — the disease the CA store already has.

`security_master/events.parquet` is also empty (§4.3), so `resolve_symbol()`
cannot answer today. Split it: **the key type becomes `security_id` now**
(signature-only, cheap), **resolution keeps a ticker fallback** until the store
has rows. Deferring both halves is what makes the migration expensive later.

## 5. Two deadlines

| Deadline           | Trigger                                                                                                                                                                            | Consequence of missing it                                                                                                                                                          | Class           |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| **Massive window** | `/v2/aggs` is entitled for a rolling ~5 years (`CLAUDE.md:719`; measured floor 2021-07-12 in `docs/audits/2026-07-11-daily-bronze-repair.md:58`) and the floor rolls forward daily | Repair becomes IB-only and 2FA-gated; for delisted symbols possibly unobtainable                                                                                                   | Cost            |
| **PIT revision**   | Every `rebuild-silver` publish                                                                                                                                                     | The published revision permanently carries the hole. PIT means "as of that date it looked like this" — a later backfill cannot retroactively correct an already-published revision | **Correctness** |

Consequence for the engine: **`heal_by` (days of remaining Massive-window
headroom) is a first-class field on every G1/G2/G3 finding, and the repair queue
is ordered by it.** Gaps nearest the rolling floor are repaired first.

## 6. Deep history is a rebuild-silver precondition, not a frozen asset

Deep-history bronze is immutable as bytes but is an **active input to every
silver rebuild**:

- `CLAUDE.md:100` — silver is _fully back-adjusted_; a split today recomputes the
  entire historical adjusted series.
- `CLAUDE.md:211` — `rebuild-silver` reads equity bronze **and** the
  corporate-action store.
- `CLAUDE.md:791` — `window_regressions` is every symbol that would _lose
  published history_; today a deep hole silently trims silver.
- `CLAUDE.md:680` — unequal ratios fail closed and quarantine the symbol.

So every deep-history hole causes fresh damage on every corporate action, and the
damage presents as "silver quietly got shorter", not "a day is missing".

**Rule:** a deep-history completeness check runs **before** `rebuild-silver`, and
its hole set is an input to publication (§7).

### 6.1 Source split for equity 1d

| Range                         | Source                          | Handling                                                                              |
| ----------------------------- | ------------------------------- | ------------------------------------------------------------------------------------- |
| Within rolling Massive window | Massive REST (`CLAUDE.md:1117`) | Tier A, fully unattended                                                              |
| Older than the window         | IB only (`CLAUDE.md:908`)       | One-time attended backfill; per-symbol hash + rowcount frozen afterwards              |
| Frozen deep history           | none needed                     | Tier A regression guard: hash/rowcount shrink or change → alert, requires no provider |

IB can never be fully unattended: `CLAUDE.md:764` — 2FA-gated, never auto-retries;
`livewire_scripts/status.py:197` — "2FA and IBKR maintenance are **not this
repo's to fix**". IB-sourced findings queue and execute opportunistically when a
session happens to be up, otherwise park as `AWAITING_USER`
(`livewire_scripts/shepherd_daily.py:424`) **without blocking any other lane**
(`livewire_scripts/sync_runner.py:337` already treats a down Gateway as degraded,
never failed).

## 7. `INCOMPLETE_HISTORY` publication contract

When deep history has holes that cannot be repaired now, `rebuild-silver`
**publishes with an explicit marker** rather than trimming (decision: option b).

- Silver revision and `clients/pit_silver_revision.py` manifests gain
  `history_status: COMPLETE | INCOMPLETE_HISTORY` and `known_holes[]` (symbol,
  session range, reason, as-of).
- `window_regressions` (`CLAUDE.md:791`) changes from a warning that precedes a
  silent trim into the input that populates `known_holes[]`.
- **Additive only.** A consumer that ignores the fields behaves exactly as today,
  so Apex requires no coordinated change to adopt this.
- PIT manifests must carry `known_holes[]`, otherwise a historical PIT query
  result cannot be known to be hole-free.

## 8. Coverage matrix

Tiers: **A** = detect, repair, verify unattended · **B** = detect automatically,
escalate the decision · **C** = explicit non-goal.

| Class                   | Denominator                             | Source                            | 1d                                                        | Intraday | G6    | G12                     | G7–G11 |
| ----------------------- | --------------------------------------- | --------------------------------- | --------------------------------------------------------- | -------- | ----- | ----------------------- | ------ |
| equity (in window)      | `presets/sp500,ndx100,r2k,etfs,adrs`    | Massive                           | **A**                                                     | B        | **A** | **A** detect / B repair | B      |
| equity (deep)           | same                                    | IB                                | one-time + frozen guard (**A**)                           | C        | **A** | **A** detect / B repair | B      |
| volatility              | `presets/volatility.json`               | CBOE public API (`CLAUDE.md:964`) | **A** all history                                         | B (IB)   | **A** | —                       | B      |
| rates                   | FRED DGS3/5/10/30                       | FRED                              | **A** all history                                         | —        | **A** | —                       | B      |
| fx                      | `presets/fx-pairs.json`                 | Yahoo (sole writer)               | **A** all history                                         | —        | **A** | —                       | B      |
| futures                 | `presets/futures-*.json` (expiry-aware) | IB                                | one-time + frozen guard                                   | B        | **A** | —                       | B      |
| cmdty                   | `presets/cmdty-metals.json`             | IB                                | one-time + frozen guard                                   | —        | **A** | —                       | B      |
| corporate_action        | event-based, no calendar                | Massive                           | **A** detect (revision reconciliation + jump cross-check) | —        | —     | —                       | B      |
| silver                  | bronze revision                         | derived                           | **A** detect (G10)                                        | B        | —     | **A** detect            | B      |
| options / crypto / tick | —                                       | —                                 | **C**                                                     | **C**    | **C** | **C**                   | **C**  |

**General rule replacing per-cell reasoning:** _IB-sourced cells cannot be fully
unattended; all others can._

**G6 auto-repair boundary (must not widen):** only `h<l`, `c∉[l,h]`, `price<=0`,
exact duplicate rows, out-of-order timestamps. Price jumps and volume anomalies
are **always B** — `CLAUDE.md:787` already warns against trimming real market
moves as corruption.

### 8.1 Freshness SLA (Tier A)

| Class                                | Expected complete by                 | Overdue ⇒ |
| ------------------------------------ | ------------------------------------ | --------- |
| equity 1d                            | T+1 06:00 HKT                        | G1        |
| volatility / futures / cmdty / fx 1d | T+1 08:00 HKT                        | G1        |
| rates 1d                             | T+2 08:00 HKT (FRED publication lag) | G1        |

## 9. Architecture

```
Livewire cron (deterministic, zero model calls)
   denominator = presets × trading_calendar × timeframe
   diff → classify G1..G12
    ├── Tier A → clients/shepherd_repair.py → verify → backup + sidecar
    └── Tier B → write decision-request file
                          ↓
              Helium agent (judgment, hard budget caps)   ← Phase 2
                          ↓
                    decision file
                          ↓
              Livewire consumes it on the next cycle
```

**Invariant: Helium never writes the data lake.** The agent produces decisions;
Livewire executes them. This removes the need for signed cross-repo mutation
authority, and decouples harness releases from lake releases.

Justification for placing execution in Livewire: writer exclusion
(`shepherd_repair.py:905` `fcntl.flock`), pre-mutation backup with
`backup_sha256`, rollback, audit JSONL, `.meta.json` sidecars, fail-closed
publication and five `launchd` schedules **already live here**. The only thing
Helium adds for Tier A is signed authority — protecting a boundary that does not
exist, since `com.livewire.daily-update` writes the same bronze files nightly
under the same identity with no signature at all.

### 9.1 Correctness test for the split

**With Helium switched off, the system must still detect all of G1–G12 and repair
every Tier A finding**; Tier B decisions simply queue. If detection ever depends
on the agent, the split is wrong and must be redrawn.

### 9.2 The agent's two jobs (Phase 2)

1. **Keep the denominator true** — but only its ambiguous subset. The audit (§15)
   found that `universe_sync.py` (updates registry/presets) and
   `shepherd_universe.py` (fetches S&P 500 / NDX-100 constituents, reconciles into
   the security master) both already exist and are both **`scheduled: no`**. The
   routine half of cause 2 — index adds/drops, new listings — is therefore not an
   agent problem at all: **schedule the two scripts that already do it.** What is
   left for the agent is genuinely ambiguous: rename vs. new listing, delisting
   vs. a broken feed, ticker reuse. Phase 1 schedules the scripts; the Phase 1
   queue depth then measures how much true ambiguity remains.
2. **Convert an incident into a permanent registry row** — diagnose a new failure
   mode, propose a check with threshold and test case, human approves, coverage
   grows by one row. This is the loop that has been leaking for five months: on
   2026-07-16 the output was an audit document; had it been a registry row
   (`adjusted_deviation_bps`, thresholds fed to the existing
   `_severity(warning_bps, failure_bps)` in `clients/adjusted_history_validation.py:169`
   and calibrated per §11.4), G12 could not recur.

Ambiguity adjudication (G11 real split vs. real crash, G9 identity, source
disagreement) also lands here, but jobs 1 and 2 are the ones that stop
recurrence.

### 9.3 Agent output contract

Three constraints on anything the Phase 2 agent produces. Each comes from a
failure mode already observed in this repo, not from principle.

1. **The agent produces evidence, never facts.** `SecurityMaster.__init__`
   requires an `evidence_verifier` and `_validate_event` rejects an unverified
   append; `shepherd_repair` requires `source_evidence: tuple[HashedRef, ...]`.
   An agent's search result therefore lands as a hashed reference in the review
   queue and is adjudicated through the same fail-closed path as
   `resolve-yahoo-basis`'s `ib_mismatch`. There is no code path by which a model
   assertion becomes a stored row.
2. **Liveness is checked on disk, never in code.** "`mark_delisted()` exists" and
   "the registry has delisted rows" are different claims. The corollary bit twice
   on 2026-09-01: the three §4.3 stores were absent on the dev host, *and* that
   absence was nearly written up as a production finding — a dev checkout is not
   the deployment. An agent finding must name the host it measured. Any finding —
   that a gap exists, or that a repair worked — must cite an on-disk artifact at
   the path the code resolves. **A green test suite is not evidence that a
   producer ran.**
3. **Budget is per job, and exhaustion queues rather than guesses.** Unchanged
   from §10, restated here because 1 and 2 are exactly what fails open under time
   pressure.

**First agent task: L4.** Whether a ticker delisted, when, and whether it was a
delisting or a rename (VSCO→VSXY, `tasks/todo.md:380`) is external fact-finding
at a scale nobody enumerates by hand, and it already has an evidence gate. Its
own prerequisite is §4.3's producer run, which produces the list to adjudicate —
against an empty registry the agent has nothing to work on.

## 10. Phase boundary

**Phase 1 — Livewire only.** Fixes causes 1, 3, 4, 5. No model calls, no Helium
dependency. Tier B findings are written to the decision-request queue and left
unconsumed — **the queue depth after one month is the measurement that decides
whether Phase 2 is worth building**, instead of assuming it.

Deliverables:

1. Denominator module (`presets × trading_calendar × timeframe`, expiry-aware,
   delisted-aware, unresolved-aware). Expiry- and unresolved-aware shipped;
   **delisted-aware is blocked on §4.3's producer run** and is the first
   follow-on, not a Phase 1 core deliverable.
2. One registry-driven engine; checks are rows, not scripts.
3. Detector convergence — **no eleventh detector allowed.** `coverage_report.py`
   is rewired onto the denominator (`:274`, `:363`, `:379`) and becomes the
   engine's reporting surface. Each of the **other nine** ENGINE-classified
   scripts (§15) gets an explicit written disposition inside Phase 1: **folded
   into the engine** or **retired**. No third state. Leaving them running beside a
   new engine fails §1.1 by construction.

   Three constraints found by the audit:

   - `health_check.py` is a detector that **writes the lake** ("gap detection +
     backfill"). Its detection half folds into the engine; its backfill half
     becomes an executor. It cannot be moved in one piece.
   - `daily_bronze_repair.py` and `shepherd_repair.py` are **two repair paths for
     the same job** (bronze daily vs. Massive). Phase 1 picks one and retires the
     other; shipping both keeps the duplication this design exists to remove.
   - `weekly_quality_summary.py` **consumes `coverage_report.py` log output**, so
     retirement has an order: the engine must emit the same log surface, or the
     weekly summary converges first.

4. Schedule `universe_sync.py` and `shepherd_universe.py` (§9.2) — the denominator
   cannot be authoritative if nothing refreshes it.
5. Tier A repair path via existing `shepherd_repair.py`, ordered by `heal_by`.
6. G12 wiring: schedule the existing `adjusted_history_validation` comparisons.
7. `rebuild-silver` precondition gate + `INCOMPLETE_HISTORY` / `known_holes[]`.
8. Decision-request file format (written, not consumed) — **adopt the existing
   `triage_breaks.py` verdict vocabulary** (`real_move | bad_data |
missing_action | inconclusive`, atomically checkpointed) rather than inventing
   a schema. See §15.

**Phase 2 — Helium decision lane.** Reads the queue, applies judgment under
job-level budget caps, writes decisions back. Reuses Helium's provider routing
(cheap triage → strong adjudicator). Needs no signed mutation authority, no
write-ahead intent, no cross-repo release binding. `com.helium.livewire-opsd` is
stopped during Phase 1 and returns decision-only in Phase 2.

## 11. Acceptance criteria

1. **Injection:** on a copy, delete one session / one whole symbol file → found
   within one cycle, repaired, hash-verified.
2. **Denominator validity:** add a never-ingested symbol to a preset → reported
   as G3. _(This is exactly what the 2026-08 MUNJ case did not prove.)_
3. **Ten consecutive sessions:** every finding is either auto-repaired and
   verified, or escalated with a reason. No silent drops.
4. **Historical replay (and threshold calibration):** the G12 check must flag the
   165 double-adjusted symbols recorded in
   `docs/audits/2026-07-16-silver-correctness-gap-from-apex.md`. That population
   was repaired by the rev-3 bootstrap (`CLAUDE.md:752`–`805`), so the live lake
   is not a valid test input. Run the replay against, in order of preference:
   (a) the retained pre-rev-3 silver artifacts if still on disk, else (b) a
   fixture built by re-applying a known real split to that symbol's real bronze
   bars a second time — a deterministic corruption of real data, labeled as a
   fixture, not invented prices. `warning_bps` / `failure_bps` are calibrated on
   this set: it must separate the 165 from a clean control group (the audit's
   `INTC` fail-closed control) with no overlap. **A design that cannot re-detect
   the incident that motivated it does not ship.**
5. **Publication contract:** a symbol with an unrepairable deep hole publishes
   with `INCOMPLETE_HISTORY` and a populated `known_holes[]` — never a silent
   trim.
6. **Degradation:** with Helium absent, criteria 1–5 still pass.
7. **Convergence:** at the end of Phase 1 there is one engine, not a fourth
   detector.
8. **Producer liveness:** every store this design reads has a non-empty artifact
   at the path the code resolves, or the section reading it states it is
   unpopulated and names the branch that is consequently untestable. Re-checked
   at the start of each phase, not once. §4.3 is the standing counter-example.

## 12. Non-goals

- Options, crypto, tick data (options do not exist in the lake).
- History earlier than the one-time IB backfill horizon.
- Fully automatic silver full rebuild (B tier: alert, human triggers).
- Any repair requiring "is this real market movement or bad data?" judgment.
- Retroactive correction of already-published PIT revisions (impossible by
  definition; recorded via `known_holes[]` instead).

## 13. Defaults chosen without explicit approval

These were decided while drafting and need confirmation during review:

| #   | Default                                                                                                                                                                                                                                    | Alternative                           |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------- |
| 1   | ~~2000-01-01~~ → **confirmed 1995-01-01**. Daily bars are small enough that the extra five years cost little, and the horizon must cover the dot-com build-up and 2000–2002 crash. Unobtainable symbols recorded as permanently unresolved | settled                               |
| 2   | `heal_by` is a first-class field and the repair-queue sort key                                                                                                                                                                             | drop it                               |
| 3   | futures + cmdty follow "one-time + frozen guard" rather than waiting on IB auto-repair                                                                                                                                                     | queue on IB instead                   |
| 4   | equity intraday stays Tier B                                                                                                                                                                                                               | promote to A (adds IB 2FA dependency) |
| 5   | SLA times in §8.1                                                                                                                                                                                                                          | adjust                                |

## 14. Evidence index

Verified against the working tree at `28242e9`:

- `livewire_scripts/coverage_report.py:274,363,379` — disk-glob denominator, hardcoded class list
- `clients/duckdb_catalog.py:62` — authoritative six daily classes
- `clients/shepherd_repair.py:905` — `fcntl.flock(LOCK_EX)`
- `clients/adjusted_history_validation.py:163,235,355` — correctness comparison library
- `clients/pit_silver_revision.py:77,85,93,169` — PIT revision publish/verify
- `clients/trading_calendar.py` — session calendar
- `CLAUDE.md:100,211,627,680,719,764,770,787,791,908,910,929,964,1117`
- `docs/audits/2026-07-11-daily-bronze-repair.md:58` — measured Massive floor 2021-07-12
- `docs/audits/2026-07-16-silver-correctness-gap-from-apex.md` — the 165-symbol incident
- `livewire_scripts/status.py:197`, `sync_runner.py:337`, `shepherd_daily.py:424` — IB degradation handling

## 15. Appendix — script disposition audit (2026-08-31)

Method: four parallel read-only agents over all 64 scripts (`livewire_scripts/*.py`
plus `scripts/*.py`), classified from docstrings, imports and targeted greps
without reading whole files. `__init__.py` and `paths.py` excluded as trivial.

| Disposition | Count | Lines | Meaning                                                                   |
| ----------- | ----: | ----: | ------------------------------------------------------------------------- |
| ENGINE      |    10 | 4,810 | independently defines "expected"/"healthy" → engine subsumes, then retire |
| EXECUTOR    |     8 | 2,142 | repair logic → keep, wrap as Tier A executor                              |
| ONEOFF      |     5 |   643 | spent one-time migration → deletion candidate                             |
| INGEST      |    20 |     — | provider acquisition → keep, out of scope                                 |
| OPS         |    16 |     — | schedule/release/status/housekeeping → keep, out of scope                 |
| BUILD       |     4 |     — | derivation/publication → keep                                             |
| UNCLEAR     |     1 |     — | needs a human decision                                                    |

### ENGINE — consolidation target

| Script                         | writes lake | scheduled | Role                                                      |
| ------------------------------ | ----------- | --------- | --------------------------------------------------------- |
| `check_gaps.py`                | no          | no        | core expected-vs-actual gap detection                     |
| `coverage_report.py`           | no          | **yes**   | named detector; `com.livewire.coverage`                   |
| `data_quality_report.py`       | no          | no        | aggregates telemetry into quality/coverage views          |
| `health_check.py`              | **yes**     | no        | gap detection **and** backfill — must be split            |
| `shepherd_actions.py`          | no          | no        | PIT corporate-action evidence for coverage verification   |
| `shepherd_daily.py`            | no          | no        | independently plans/verifies PIT-scoped expected coverage |
| `validate_adjusted_history.py` | no          | no        | validates adjusted history vs Massive / fresh IB          |
| `validate_silver_canary.py`    | no          | no        | read-only canary over silver factors and bars             |
| `warehouse_health_report.py`   | no          | no        | static HTML health report                                 |
| `weekly_quality_summary.py`    | no          | **yes**   | aggregates 7 days of `coverage_report` logs               |

### EXECUTOR — keep and wrap

The Tier A executor interface is **not to be invented**; two proven shapes already
exist and should be copied:

- `resolve_*` (read-only, emits manifest) → `repair_*` (applies, backs up first) → `rollback_*` (restores from backup)
- `shepherd_repair.py`: `preflight → stage → publish → rollback`

| Script                     | writes lake | Role                                                  |
| -------------------------- | ----------- | ----------------------------------------------------- |
| `daily_bronze_repair.py`   | yes         | audits/repairs bronze daily rows against Massive      |
| `repair_legacy_basis.py`   | yes         | manifest-driven IB re-derivation of mixed-basis rows  |
| `repair_split_basis.py`    | yes         | applies/rolls back approved split-basis manifest      |
| `repair_yahoo_splits.py`   | yes         | evidence-gated split add/cancel in the action store   |
| `resolve_split_basis.py`   | no          | resolves ambiguous split boundaries via IB → manifest |
| `resolve_yahoo_basis.py`   | no          | read-only classifier → basis-correction manifest      |
| `rollback_legacy_basis.py` | yes         | restores bronze from pre-mutation backup              |
| `shepherd_repair.py`       | yes         | staged reversible repair; the MUNJ path               |

### ONEOFF — deletion candidates

`audit_legacy_basis.py` · `audit_split_basis.py` · `calibrate_daily_basis.py` ·
`migrate_equity_price_basis.py` · `migrate_parquet_filename.py`

Residue of the legacy/split/yahoo basis campaigns and two completed file
migrations. Confirm each has no remaining caller before deleting.

### Out of scope — unchanged by this design

**INGEST (20):** `adjusted_history_sources` `backfill_intraday` `backfill_runner`
`corporate_action_cursor` `daily_update` `fetch_cboe_volatility` `fetch_fred_rates`
`fetch_fx` `fetch_ib_historical` `flatfile_downloader` `flatfile_planner`
`flatfile_publisher` `ingest_daily_flatfiles` `ingest_flatfiles` `run_ib_fetch_robust`
`shepherd_universe` `sync_corporate_actions` `sync_to_r2` `universe_screener`
`universe_sync`

**BUILD (4):** `daily_flatfile_publisher` `duckdb_catalog_cli` `rebuild_silver`
`shepherd_silver`

**OPS (16):** `archive_otc_symbols` `check_daily_update_watchdog` `daily_outcomes`
`housekeeping` `nightly_digest` `release` `run_daily_update_job`
`run_intraday_catchup_job` `scheduled_env` `status` `sync_runner` and the five
`scripts/livewire*.py` CLI routers

Ingestion is 20 of 64 scripts and does a different job: it is what the engine
_calls_, not what the engine _becomes_. Converting it to shepherd form would be
churn on working code.

### `triage_breaks.py` — the Tier B contract already exists

Re-audited separately. It neither detects nor repairs: it consumes an existing
audit manifest, cross-checks each flagged break against Massive, and emits an
atomically checkpointed **verdict manifest** whose vocabulary is
`real_move | bad_data | missing_action | inconclusive`, consumed downstream by the
silver-window resolver. It is registered as `triage-breaks` in
`scripts/livewire_quality.py` and covered by `tests/test_triage_breaks.py`; it is
not scheduled.

That vocabulary **is** the Tier B decision schema this design needs — "is this a
real market move or bad data" is precisely the G6/G11/G12 judgment call (§8), and
`inconclusive` is the escalate-to-agent bucket. Phase 1 adopts this verdict
format for the decision-request queue instead of inventing one, and rewires its
input from the legacy-basis audit manifest to the engine's findings.

**Unresolved:** the two audit passes disagree on whether it writes the lake
(`writes_lake` no vs. yes). Confirm from the code before finalizing its
disposition — do not carry this ambiguity into implementation.
