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
| 5   | The same unsourceable symbols are re-litigated every round, and giving up is not recorded | Persistent unresolved denominator with reasons (§4.5) | No           |

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
| G14 | Terminus                | Symbol absent from the raw traded set for **every** session from date X through the as-of date, with no corporate action explaining it — distinct from a no-trade day, which is bounded by presence on both sides |

G14 is not repairable and does not belong to the same family as G1–G3: the bars
are absent because the instrument stopped appearing on the tape, and no provider
can supply them. It is listed here because **G1 and G3 cannot be evaluated
without it.** Measured 2026-09-01 (§4.4): all four true findings on the
`sp500 + ndx100` universe were G14, and every one was emitted as a G1/G3 Tier A
repair against Massive — the store whose own tape is what lacks them.

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

**Measured on the production host** (`moremeds-Mini`, Mac16,10, over ssh,
2026-09-01). The dev checkout this design is written in has none of this — no
plists, warehouse logs stopped 2026-05-31, no `.env`, no `MASSIVE_API_KEY` — so
every number below comes from the mini, not from the repo host:

| Store                                    | Production state                        |
| ---------------------------------------- | --------------------------------------- |
| `data-lake/bronze-delisted/…=equity/`    | **8,620 symbols**                       |
| `data-lake/security_master/events.parquet` | **1 row** (schema proven, no population) |
| `registry.json`                          | **absent**; `universe_sync` has never logged |
| `data-lake/bronze/asset_class=equity/`   | 14,811 symbols (live universe)          |

Scheduling is healthy — 5 plists installed, 6 jobs loaded, logs current through
2026-08-31 — and `MASSIVE_API_KEY` is present in the mini's warehouse `.env`.
IB port 4001 is closed while the Gateway process runs, i.e. the ordinary
2FA-pending state, not a livewire fault.

So the two producers have diverged as far as they can: **the archive half has
run 8,620 times and the date half has never run once.** 8,620 archived symbols
carry no `delisted_at`, against a live universe of 14,811 — 37% of everything
ever ingested sits in an archive with no terminus. That is L4's actual input
size, and it settles the open question in the follow-on plan: a population that
large is not adjudicated by hand, so the agent lane is the only path that
finishes.

**The archive is not authoritative, and must never subtract from the
denominator.** Cross-checking the 8,620 archived symbols against the presets
(2026-09-01): **234 of them are still claimed by a preset** — `BK`, a current
S&P 500 member listed in `sp500.json` and two sector presets; 63 ADRs including
`ORAN`, `TEF`, `ERJ`, `ABB`, `TTM`, `VEDL`; and 157 ETFs. `KALV` and `MAPS`
exist in the live tree *and* the archive simultaneously.

`BK` decides the question. It has no `1d.parquet` in live bronze, so it is a
real G3 hole today — the engine reports it precisely because the denominator is
preset-driven. An archive-driven filter would have removed `BK` from the
denominator and hidden that hole permanently: the invisible-gap failure this
whole design exists to remove, reintroduced by the feature meant to improve it.

So the earlier split of L4 was wrong. There is no cheap membership half:

1. ~~**Membership** — archived symbols leave the live denominator.~~ **Rejected.**
   Presets are the universe; subtracting the archive from it deletes 234 live
   claims, 1 of them S&P 500. Pinned by
   `tests/test_coverage_denominator.py::test_an_archived_symbol_a_preset_still_claims_stays_expected`,
   a guard that fails if a subtracting branch is ever added.
2. **Terminus** — unchanged, and now the *only* half. Each archived symbol needs
   a `delisted_at`, and rename must be separated from delisting. `universe_sync`
   has never run, so all 8,620 lack one.

**Why the terminus half has never run** (traced 2026-09-01 on the mini).
`universe-sync --dry-run` exits **1** after two log lines:

```
INFO  S&P 500: 503 constituents
ERROR Failed to fetch Nasdaq-100: Nasdaq-100: no constituent table found
```

`fetch_ndx100` reads the Wikipedia article `Nasdaq-100`, whose section list is
now History / Selection criteria / Performance / Record values / … — **there is
no Components section any more**, and the four remaining `wikitable`s are index
history (`['Year', 'Closing level', …]`). It is not a moved page: `Nasdaq-100
Index`, `List of Nasdaq-100 companies` and `Nasdaq-100 components` all 404, and
a Wikipedia search returns only the one article. **The upstream source is
gone**, so this is a source replacement, not a selector fix.

Two consequences, and the second is worse:

- `universe_sync.py:178` is `sys.exit(1)` inside the fetch loop, so one dead
  source kills the whole sync — S&P 500 movements, registry seeding and the
  entire delisting path included. This is the "IB is not a single point of
  failure" lesson (`CLAUDE.md`) reproduced in a lane nobody checked.
- Even after that is fixed, the Polygon check runs over
  `removed_tickers + orphan_tickers`, both drawn from the registry, which is
  seeded only from *current* index members. Archived symbols are in no live
  index, so they are never seeded and never checked. **`universe_sync` cannot
  assign `delisted_at` to the 8,620 by design**; L4b needs a separate pass that
  feeds the archived list straight to `check_tickers_bulk`.

The 234 conflicts are themselves the first tranche of work: two universes
disagree about them in writing, and one of the two is wrong per symbol. That
population is small enough to adjudicate and large enough to be worth it, which
makes it the natural first batch for the agent lane (§9.3) — ahead of the
remaining 8,386, which no preset contradicts.

### 4.4 The no-trade exemption is the other half of the denominator

`livewire_scripts/coverage_report.py:322` exempts a symbol that is **absent from
the day's raw `_symbols.parquet` traded set** — no-trade is not missing. The rule
is load-bearing: it is the only second source separating "no bar" from "no trade"
(`CLAUDE.md`), and without it the interior scan flags 96.6% of the universe. It
must not be removed.

It is also the *second* mechanism hiding the population the disk-glob denominator
hides, and it hides them independently. Measured on the production host
2026-09-01 over the live lake — full method, scripts and output in
`docs/audits/2026-09-01-terminus-vs-no-trade.md`:

| Symbol | last on `minute_aggs_v1` | last on `day_aggs_v1` | in `bronze-delisted/` | latest corporate action |
| ------ | ------------------------ | --------------------- | --------------------- | ----------------------- |
| BK  | never (0 of 43 days from 2026-07-01) | never (0 of 21) | **yes** | 2026-04-27 cash dividend |
| EA  | 2026-08-04 | 2026-08-04 | no | 2026-05-27 cash dividend |
| AVB | 2026-08-14 | 2026-08-14 | no | 2026-06-30 cash dividend |
| EQR | 2026-08-17 | 2026-08-17 | no | 2026-06-29 cash dividend |

Two independently published tapes give the same terminus for each symbol, so this
is not a minute-file artifact, and the tape is healthy on the last of those
sessions (11,913 tickers; `AAPL`, `SPY`, `MSFT`, `NVDA` all present).

Consequences, in order of how much they change the design:

1. **The exemption exempts all four, on every one of their missing sessions**
   (21 / 19 / 11 / 10 of 21 / 19 / 11 / 10). Coverage therefore grades them
   present. Combined with the disk-glob denominator, which never sees BK at all,
   coverage is blind to this population through two independent mechanisms —
   **fixing either one alone still reports green.** This, not the denominator
   alone, is why a delisted S&P 500 member survived weeks under a detector
   reporting 99.93%.
2. **The distinction is a suffix test, not a threshold.** Absent for one session
   with presence on both sides is a no-trade day; absent from date X through the
   as-of date with no return is a terminus. Both readings use files coverage
   already opens, and the test costs 20 reads against coverage's ~13,000 footer
   reads. No threshold to tune — which is the same property that made
   `classify_seed_boundary` succeed where the blind heuristic failed (`CLAUDE.md`).
3. **Signal-to-noise, measured: 515 members → 4 findings → 0 false positives.**
   The no-trade population the 96.6% disease is made of does not appear, because
   those symbols return to the tape inside the window.

BK is additionally the proof for §4.3's claim that the two delisting producers
are not one chain: it sits in `bronze-delisted/` **and** in `presets/sp500.json`.
The archive move ran; nothing removed it from the universe the denominator is
built from. A delisted-aware denominator must therefore reconcile the archive
against the presets, not simply read one of them.

### 4.5 Unresolved denominator (cause 5)

A symbol/session that cannot be sourced from any provider is recorded once, with
reason and as-of date, and **stops being retried**. It remains visible in
coverage output as unresolved — never silently dropped, never re-litigated.

### 4.6 The registry row

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

### 4.7 Identity key

`ExpectedSeries.symbol` and the unresolved-ledger key are ticker strings, while
`clients/security_master.py` exists precisely to end ticker-string joins (the
2,345 reused tickers). On a reuse the ledger records the predecessor company's
"unresolvable" against the successor — the disease the CA store already has.

`security_master/events.parquet` is also empty (§4.3), so `resolve_symbol()`
cannot answer today. Split it: **the key type becomes `security_id` now**
(signature-only, cheap), **resolution keeps a ticker fallback** until the store
has rows. Deferring both halves is what makes the migration expensive later.

## 5. Three deadlines

| Deadline           | Trigger                                                                                                                                                                            | Consequence of missing it                                                                                                                                                          | Class           |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| **Massive window** | `/v2/aggs` is entitled for a rolling ~5 years (`CLAUDE.md:719`; measured floor 2021-07-12 in `docs/audits/2026-07-11-daily-bronze-repair.md:58`) and the floor rolls forward daily | Repair becomes IB-only and 2FA-gated; for delisted symbols possibly unobtainable                                                                                                   | Cost            |
| **Ingestion**      | The job filling session S starts 06:00 UTC on S+1 and must complete inside `MDW_DAILY_JOB_DEADLINE_SECONDS` (4h, so 10:00 UTC) | Session S is *closed* but not yet *due on disk*. A denominator that expects S the moment it closes manufactures a tail gap for every symbol in the universe | **Correctness of the signal** |
| **PIT revision**   | Every `rebuild-silver` publish                                                                                                                                                     | The published revision permanently carries the hole. PIT means "as of that date it looked like this" — a later backfill cannot retroactively correct an already-published revision | **Correctness** |

The ingestion deadline is why `build_denominator`'s `d < as_of` predicate is
wrong: `as_of` is a date, and closing is not delivery. Measured 2026-09-01, a run
at 04:21 UTC produced **497 phantom tail gaps out of 501 findings** — one for
every sp500 member — purely because session 2026-08-31 had closed and its job had
not yet started. The rule is `expected(S) ⇔ as_of >= (S + 1 day) 10:00 UTC`,
reusing the existing job deadline rather than introducing a second constant, and
`as_of` becomes a `datetime`. `com.livewire.coverage` never had this bug because
it is scheduled at 11:00 UTC, after the deadline; anything reading the
denominator earlier must apply the rule explicitly.

⚠️ **The deadline exists only where a lane wrapper forwards it, and three of the
seven do not.** Read 2026-09-01 in `livewire_scripts/run_daily_update_job.py`:
`main()` passes `deadline=deadline` to every lane (`:861`, `:900`, `:904`,
`:921`), and `run_fx_sync:764`, `run_corporate_action_sync:790` and
`run_silver_rebuild:810` accept the parameter, are typed for it, and drop it at
the call to `_run_scheduled_lane`. Only `run_cboe_volatility_sync:709` forwards
it. Silver is the **last** lane, so an unbudgeted job can run past 10:00 UTC with
nothing killing it — and this rule would then declare session S due while the
lane filling it is still writing. **Until issue #94 is fixed, `expected(S)` rests
on a guarantee the code does not provide**, and the engine must treat the
deadline as an assumption to verify (§11 criterion 8), not as an enforced bound.
The 3.27h healthy peak this 4h budget was sized against was measured over
2026-07-01..28, and the budget landed 2026-07-29 — the peak is a measurement of a
job nothing was killing, so it cannot by itself show 4h is enough once the three
wrappers are wired.

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

**The rule is about the provider's availability, not about the symbol.** A cell
marked **A** means the named store *can* be asked without a human; it does not
mean the store holds every symbol in the denominator. Measured 2026-09-01 (§4.4),
four `sp500` members are absent from Massive's own tape entirely, and an engine
reading this matrix alone emitted them as Tier A Massive repairs that can never
succeed. Per-finding tier is therefore this matrix **and** §9.3 rule 4: a finding
demotes to B when the cell's store does not carry the target session. A G14
terminus is always B in every cell — no source can supply bars for an instrument
that left the tape.

**G6 auto-repair boundary (must not widen):** only `h<l`, `c∉[l,h]`, `price<=0`,
exact duplicate rows, out-of-order timestamps. Price jumps and volume anomalies
are **always B** — `CLAUDE.md:787` already warns against trimming real market
moves as corruption.

### 8.1 Freshness SLA (Tier A)

| Class                                | Expected complete by                                | Overdue ⇒ |
| ------------------------------------ | --------------------------------------------------- | --------- |
| equity 1d                            | T+1 10:00 UTC (18:00 HKT)                           | G1        |
| volatility / futures / cmdty / fx 1d | T+1 10:00 UTC (18:00 HKT)                           | G1        |
| rates 1d                             | T+2 10:00 UTC (FRED publication lag)                | G1        |

Every row is the **same** existing constant: `run-daily-job` starts at 06:00 UTC
on T+1 and `MDW_DAILY_JOB_DEADLINE_SECONDS` (4h) puts its deadline at 10:00 UTC,
shared across all seven lanes (`CLAUDE.md`). The lanes are sequential, so there
is no per-class time to stagger — an earlier draft of this table read
`T+1 06:00 HKT` for equity, which is 22:00 UTC on T, **eight hours before the job
that fills the session even starts**. Any SLA earlier than the deadline
manufactures the 497 phantom findings of §5 by construction. One constant, read
from the environment, not three written down here.

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
4. **A tier is a claim about a store, and the store must be asked.** Tier A means
   "repairable unattended by a named source". `gap_engine._finding()` assigns the
   source from the asset class, and nothing verifies that the source carries the
   symbol on the target session. Measured 2026-09-01: all four true findings were
   emitted Tier A `source: massive`, `heal_by_days: 1798`, when Massive's own tape
   is exactly what lacks them — a repair that fetches nothing, forever, and
   reports either a permanent failure or a silent no-op. **Tier A requires
   positive evidence that the named store holds the target session; without that
   evidence the finding is Tier B.** This is rule 2 applied to the repair side:
   liveness of a producer is checked on disk, not asserted from a class map.

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
2. One registry-driven engine; checks are rows, not scripts. **Scoped by what the
   data supports:** on the first production run (§4.4) G2 and G13 produced zero
   true findings out of 501, and 497 of the rest were an artifact of §5's
   ingestion deadline. Phase 1 ships the denominator, G1, G3 and G14. G2 and G13
   stay unimplemented until a measurement asks for them — an unexercised branch in
   a detector is the thing this design exists to stop shipping.
3. Detector convergence — **no eleventh detector allowed.** `coverage_report.py`
   is rewired onto the denominator (`:274`, `:363`, `:379`) and becomes the
   engine's reporting surface. Each of the **other nine** ENGINE-classified
   scripts (§15) gets an explicit written disposition inside Phase 1: **folded
   into the engine** or **retired**. No third state. Leaving them running beside a
   new engine fails §1.1 by construction.

   **`livewire_scripts/gap_scan.py` is the violation this clause was written to
   prevent, and it is retired.** Written 2026-08-31 as a standalone subcommand
   with two `launchd` templates, it stands beside `coverage_report.py` answering
   the same question with a different denominator, and it carries neither the
   no-trade exemption (§4.4) nor the ingestion deadline (§5) — so on the full
   14,811-symbol universe it would reproduce the 96.6% interior-scan disease. The
   `sp500` spike did not expose that because every sp500 member is liquid. Its one
   earned part, `clients/coverage_denominator.py`, moves into
   `coverage_report.py`; the script, both `.plist.example` templates and the
   `scripts/livewire_quality.py` subcommand registration are deleted. Net effect
   of Phase 1 on the script count is **negative**.

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
   a schema. See §15. **G14 does not fit that vocabulary** and is the one place a
   term is added: a terminus asks *delisted / renamed / stale universe*, none of
   which is a statement about a price. It gets `terminus` as a fifth verdict with
   its own sub-field, not a strained reading of `inconclusive` — which would make
   the queue unsortable exactly where §9.2's first agent task reads it.

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
   detector. **Measured by deletion, not by intent:** `livewire_scripts/gap_scan.py`,
   `launchd/com.livewire.gap-scan.plist.example`,
   `launchd/com.livewire.universe-refresh.plist.example` and the `gap-scan`
   subcommand registration are absent from the tree, and no `launchd` job exists
   that a rewired `com.livewire.coverage` does not already cover.
8. **Producer liveness:** every store this design reads has a non-empty artifact
   at the path the code resolves, or the section reading it states it is
   unpopulated and names the branch that is consequently untestable. Re-checked
   at the start of each phase, not once. §4.3 is the standing counter-example.

   **Presence is not completion, and the artifact's own timestamp is not
   evidence.** `_run_scheduled_lane:730` writes `=== Done <scope> <ts> ===`, and
   `run_corporate_action_sync:797` passes `now_fn=lambda: now` — a clock frozen
   at lane start for a legitimate reason (the Sunday `--full-reconcile` weekday
   decision), so the marker reports the **start** time as the completion time
   (issue #94). A lane that wedged for 85 minutes leaves a present, non-empty,
   correctly-formatted artifact that satisfies this criterion as originally
   worded. The criterion therefore requires that a producer's completion be
   established by **differencing against an independent later timestamp** — the
   next lane's `=== <label> <ts> ===` header, or the cursor file's mtime — never
   by reading the producer's own Done stamp. This is §9.3 rule 2 applied to time:
   an on-disk artifact can exist and still lie.

   The corporate-action store is the case that makes this load-bearing rather
   than hygienic. It is the second source G14 rests on — §4.4's finding is
   literally "no corporate action explains the terminus" — and it is written by
   lane 1, the unbudgeted one that already wedged on 2026-07-28 (`CLAUDE.md`). A
   frozen store turns every unreconciled delisting into a false G14, so the
   engine must read the store's freshness before emitting G14 at all.
9. **Terminus separation:** a symbol absent from the raw traded set for a single
   session with presence on both sides is **not** reported; a symbol absent from
   date X through the as-of date is reported as G14, Tier B. Verified against
   §4.4's four symbols as the positive set and the remaining 511 members of that
   universe as the negative set — **the negative set must produce zero findings.**
   A detector that cannot pass this is the 96.6% scan again.
10. **Tier honesty:** no finding is emitted Tier A unless the named store is shown
    to carry the target session (§9.3 rule 4). The four §4.4 symbols are the
    regression case: each must land Tier B, not Tier A `source: massive`.
11. **Due, not merely closed:** a run at any hour before `(S + 1 day) 10:00 UTC`
    reports no tail gap for session S. The 2026-09-01 04:21 UTC run, which
    produced 497 phantom findings, is the regression case.

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
| 5   | ~~SLA times in §8.1~~ → **settled**: all classes inherit `MDW_DAILY_JOB_DEADLINE_SECONDS`, so the SLA is `(S + 1 day) 10:00 UTC` and there is no second constant to keep in step                                                          | settled                               |
| 6   | **Settled 2026-09-01 (user):** the ingestion deadline reuses the existing job deadline rather than a new `MDW_GAP_*` variable; `as_of` becomes a `datetime`                                                                                | a dedicated constant                  |
| 7   | G14 terminus is Tier B in every cell and never auto-repaired                                                                                                                                                                              | attempt an IB re-ask first            |

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

Added 2026-09-01, measured on the production host `moremeds-Mini` against the
live lake (not the dev checkout — §9.3 rule 2):

- `livewire_scripts/coverage_report.py:105,261,282,322` — `_raw_symbols_for_date`,
  the traded set, the disk-glob universe, and the no-trade exemption. Neither
  `clients/gap_engine.py` nor `clients/coverage_denominator.py` nor
  `livewire_scripts/gap_scan.py` contains any of them (grep, zero hits).
- `docs/audits/2026-09-01-terminus-vs-no-trade.md` — the full measurement behind
  §4.4, §5's ingestion deadline, §9.3 rule 4 and acceptance criteria 9–11.
  Production spike: 501 findings over sp500 equity `1d` in 38.5s; 497 ingestion
  lag, 4 terminus, 0 G2, 0 G13. Universe pass: 515 members → 4 findings → 0 false
  positives.
- Acceptance criterion 2 is **met on the real lake**: BK, an `sp500.json` member
  with no `1d.parquet`, is reported as G3 in production. This is the case the
  disk-glob denominator cannot express, and it is the one result that justifies
  the denominator replacement.

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
