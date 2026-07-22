# Consolidated plan status — 2026-07-22

Single source of truth reconciling every planning doc in the repo against
**main @ `f491524`** and the actual data lake. Supersedes the scattered plan
files as the status view. Verified by a 4-cluster read-only audit + direct lake
reads, not by trusting checkboxes (older plans ship work but never tick boxes).

Reconciled PRs: **#57** (silver-grade window per symbol), **#58** (re-manifest
orphaned silver under `--full`), **#60** (stop nightly jobs failing silently),
**#63** (`resolve-yahoo-basis` — IB-anchor-verified unknown-basis reconstruction).

---

## Headline reality

- **Silver is published.** Data lake `silver/revisions/current.json` = **rev 10,
  2026-07-19, full universe** (`corporate_actions_as_of` 2026-07-19). The
  handover doc's "silver never published / `silver/` empty" is **stale** — do not
  trust it.
- **But Apex production still serves Bronze (raw mode).** The cutover is a flag
  flip in the *separate* `apex` repo (`APEX_LIVEWIRE_SILVER_ROOT` unset,
  `APEX_LIVEWIRE_PRICE_MODE=raw` in prod), gated on shadow acceptance. That's the
  only "cutover" still pending, and it's not in this repo.
- **Equity intraday is stale ~1 week** (1m/5m/30m/1h stuck at 2026-07-10) because
  `equity/NULG/1m.parquet` is **corrupt** (truncated: `PAR1` header, no footer).
  PR #60's quarantine fix (now live on disk) auto-evicts it on tonight's 05:00 UTC
  run, which should unstick the lane. This is `tasks/todo.md` F2/F3.
- **All daily lanes are fresh** (equity/fx/cmdty/vol/rates at 07-17…07-21).

---

## The ONLY genuinely-open engineering

Everything else below is either done, ops, or low-priority polish. This is the
one item that needs design work:

**Dividend-currency / foreign-basis error class.** ~594 `error`-population
symbols, of which 61 `dividend_currency` + 14 `dividend_magnitude`. The plan
written for it — `2026-07-16-silver-full-universe-residual-resolution.md`
(R1–R8) — is **dead on premise**: R3/R4 depend on a `historical_adjustment_factor`
field that **does not exist** in Massive's dividend response
(`normalize_dividend` reads `None`). Needs a fresh approach (IB `ADJUSTED_LAST`,
or accept-and-quarantine as foreign-basis). These symbols simply don't publish to
Silver today — they fail closed, which is correct, just incomplete coverage.

---

## Open backlog (deduplicated, prioritized)

### Now — data freshness (`tasks/todo.md`, operational)
- **F2** recover corrupt `equity/NULG/1m.parquet` (preserve original; reject
  short-tail repair). *Mitigated tonight by the merged quarantine fix; F2 makes it
  a clean recovery instead of an eviction.*
- **F3** run equity intraday catch-up 07-10 → 07-17 (1m/5m/30m/1h).
- **F1/F6** are just the pre-capture and post-verify wrappers around F2/F3.
- ~~F4 daily lanes~~, ~~F5 FRED/non-equity intraday~~ — **already fresh, close them.**

### Next — reliability audit tail (7 of 17 open, all medium/low)
No blockers or highs remain — PR #60 closed the entire blocker+high tier. Open:
| Item | Sev | Remaining change |
|---|---|---|
| M5 `harden-r2-sync-error-handling` | med | per-file try/except, return `(uploaded, failed)`, kill dead-code exit (~16 test call-sites) |
| M6 `add-jsonl-retention` | med | net-new `jsonl_retention.py` + `prune-jsonl` cmd + dated sync_runner logs |
| M2 `fix-gap-aware-registry-bounds` | med | subtract `n_no_bounds`; all-unbounded escape hatch (`backfill_runner.py:206-218`) |
| M4 `add-30m-timeframe-parity` | med | **Postgres half only** (weekly done): `equities_30m` table + DDL + 3 whitelists |
| M3 `scope-intraday-coverage-recovery` | med | **owner call** — PR #60 documented day-scope as by-design; only the `--tickers` scoped-publish optimization + test remain. May already be "closed enough" |
| L1 `enable-ib-connect-retry` | low-med | ~4-line typed transient retry (approved 2026-07-05, never done) |
| I3 `dedupe-cli-dispatch` | low | extract 5 `_dispatch_module` copies → `cli_dispatch.py` |
| L2 `fix-docs-drift` | low | README `fail_under=100`→95, `.env.example` CEREBRAS, plist header times, `git add docs/architecture/` |
| I6 `misc-config-knobs` | low | `MDW_COVERAGE_SAFETY_CAP`, `resolve_node_bin`, pyproject exclude |

### Also open — post-lift cleanups (`tasks/todo.md` bottom, 4 small independent fixes)
Untouched by the silver work, confirmed unfixed in code:
- `BronzeClient.__init__` orphan `.tmp` sweep (~5 lines; same for intraday client).
- `warehouse_health_report.py:147` glob needs `._*` AppleDouble filter.
- `flatfile_planner.py:44` `disk_usage` targets `warehouse_dir`, not the
  `data-lake` symlink → wrong free-space in backfill mode.
- `renames.json` / MassiveClient normalization map (VSCO→VSXY manual re-archive;
  KFS/SLNO/… still fail daily union).

### Filed as GitHub issues (root causes, not instance fixes)
- **#61** — invert the exit-0 default (Pattern 1: ~127 sites, Pattern 2: 18/35
  orchestrator units, Pattern 3: 35 detector blind spots). The 21 PR-#60 fixes
  were instances; this is the contract.
- **#62** — nightly 1m merge rewrites full multi-year history per symbol per
  night. **This is the documented "Per-day publish cost is HDD-bound (2026-06-11)"
  note in `tasks/todo.md`** — a re-discovery, not new. ~88-min HDD floor;
  per-month partition (option 1) is the written fix. Link them.

---

## DONE — safe to archive/delete

### Reliability audit plans, fully shipped (delete — untracked notes):
`fix-sync-runner-success-detection`, `fix-watchdog-env-loading`,
`fix-digest-marker-after-send`, `fix-watchdog-per-asset-completion`,
`fix-watchdog-utc-dates`, `unify-warehouse-path-resolution`,
`fix-massive-trade-date-conversion`, `add-bronze-merge-locking` (8 files).

### Silver line, fully realized (archive):
silver-engine, corporate-actions, bronze-price-basis,
resumable-corporate-action-reconciliation, silver-causal-canary,
silver-future-action-cutoff, full-history-adjusted-validation,
**full-universe-silver-grade** (#57/#58), **unknown-basis-ib-verified-reconstruction** (#63).

### Historical superpowers plans, all deliverables in code (archive, 13):
health-screener-rename, multi-timeframe (+phase2), postgres-analytical-layer,
reliability-foundation, sub-c-massive-daily-validation,
massive-equity-incremental-backfill, universe-sync-and-tag-registry,
intraday-catchup-scheduler, observability-uplift (findings + implementation),
script-consolidation, massive-flatfile-full-market (+design).

---

## SUPERSEDED / stale — keep only as record, do NOT execute

| Plan | Why |
|---|---|
| `2026-07-16-silver-full-universe-residual-resolution` (R1–R8) | dead premise (no Massive adjustment-factor field); replaced by #57 window + #63 reconstruction; rev-10 already publishes the universe |
| `2026-07-16-silver-legacy-basis-full-repair` | superseded by `2026-07-17-full-universe-silver-grade` (same branch/PR #57) |
| `2026-07-16-silver-full-coverage-apex-adjusted-promotion` | Livewire half absorbed into #57/#58; Apex half = the pending cutover (separate repo) |
| `2026-05-30-cli-consolidation-and-max-coverage` | equity-intraday portion intentionally dropped for the flatfile plan (self-declared superseded) |
| `docs/plans/silver-production-cutover-handover.md` | command sequence is the *old* split-basis path (pre #57/#63); its runbook §4 predates rev-10. Retire or rewrite before anyone follows it |
| `tasks/todo.md` R1–R8, D2–D4, P6.2–P7 | superseded by the silver-grade-window + IB-anchor approach + rev-10 publish. Only crumb: P6.3's 4 invalid-OHLC rows, which the window trims around and no longer block |

---

## Two things I could not verify from this repo
- The `com.livewire.intraday-catchup` launchd plist and seeded futures data live
  outside version control (host `~/Library/LaunchAgents` + data plane).
- Apex-side state lives in the separate `apex` repo.
