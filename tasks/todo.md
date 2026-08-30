# Todo

Active task lists live here. Completed sections move to [archive.md](archive.md).

## Livewire Shepherd execution (2026-08-31)

Goal: periodically verify current S&P 500 and Nasdaq-100 market-data coverage,
preserve point-in-time evidence, investigate ambiguous gaps without a global
blocker, and permit only exact reversible repairs. Parquet remains canonical;
DuckDB remains a verified, rebuildable query surface.

Dependency graph:

```text
LS-01.4 -> LS-02.1 -> LS-02.2a -> LS-02.2b --+
                                              +-> LS-02.3 -> LS-03.1 -> LS-03.2
LS-07.1 -> LS-07.2 ---------------------------+       |          |
                                                      |          +-> LS-06.1
                                                      +-> LS-04
                                                      +-> LS-05.1 -> LS-05.2 -> LS-06.1
LS-06.1 -> LS-06.2a -> LS-06.2b -> LS-06.2c -> LS-06.3a -> LS-06.3b -> LS-07.3
                                                   |
                                                   +-> LS-08.1 -> LS-08.2

LS-01.4 + LS-02.3 + LS-03.2 + LS-05.2 + LS-06.3b + LS-07.3 -> LS-GATE
LS-04 and LS-08 continue after the first working-system gate.
```

- [x] **LS-01.4** (`depends_on: []`, Helium): Provide the strict read-only
  Livewire bridge and standalone Shepherd daemon shell through Helium PR #49.
- [x] **LS-02.1** (`depends_on: [LS-01.4]`): Preserve exact source bytes and
  MediaWiki revision metadata before parsing current index constituents.
- [x] **LS-02.2a** (`depends_on: [LS-02.1]`): Freeze the stable-security-identity
  evidence, priority, and collision policy as executable fixtures.
- [x] **LS-02.2b** (`depends_on: [LS-02.2a]`): Add append-only security identity
  and index-membership events with strict PIT queries.
- [x] **LS-07.1** (`depends_on: [LS-01.4]`, Helium): Define Shepherd team
  manifests, claim contracts, and deterministic verifier boundaries.
- [x] **LS-07.2** (`depends_on: [LS-07.1]`, Helium/Argon): Add cost-aware source
  tools and provider-owned quota recovery without daemon-wide blocking.
- [x] **LS-02.3** (`depends_on: [LS-02.2b, LS-07.2]`): Reconcile verified
  current S&P 500 and Nasdaq-100 membership into the query surface.
- [x] **LS-03.1** (`depends_on: [LS-02.3]`): Produce exact current-member daily
  coverage work units from identity intervals, not discovered files.
- [x] **LS-03.2** (`depends_on: [LS-03.1]`): Connect deep daily retrieval while
  keeping IB 2FA/session failure local and resumable. The manifest-bound
  command performs one forced-IB attempt, verifies the resulting Parquet, and
  returns typed exit 75 without full-bounce or command-level retry. Helium
  accepts that wait only with `AWAITING_USER` and continues other ready units;
  mutating receipts remain handoffs until LS-06's certified action boundary.
- [ ] **LS-04** (`depends_on: [LS-02.3]`): Expand verified historical membership
  and PIT Silver daily bars without delaying the current working gate.
- [ ] **LS-05.1** (`depends_on: [LS-02.3]`): Verify Massive intraday partitions
  and source completeness from canonical raw evidence.
- [ ] **LS-05.2** (`depends_on: [LS-05.1]`): Publish verified intraday coverage
  and DuckDB parity metadata without wide cold-lake scans.
- [ ] **LS-06.1** (`depends_on: [LS-03.2, LS-05.2]`): Freeze exact targeted
  repair manifests and mutation receipts on a disposable lake.
- [ ] **LS-06.2a** (`depends_on: [LS-06.1]`): Characterize existing repair
  scripts and their rollback/locking boundaries before adaptation.
- [ ] **LS-06.2b** (`depends_on: [LS-06.2a]`): Add exact scoped repair adapters
  without introducing a second canonical writer.
- [ ] **LS-06.2c** (`depends_on: [LS-06.2b]`): Prove crash recovery, rollback,
  and no wider-than-manifest publication.
- [ ] **LS-06.3a** (`depends_on: [LS-06.2c]`, Helium): Bind signed authority to
  the exact Livewire mutation identities and postconditions.
- [ ] **LS-06.3b** (`depends_on: [LS-06.3a]`): Run controlled autonomous repair
  and rollback drills with replayable evidence.
- [ ] **LS-07.3** (`depends_on: [LS-06.3b]`): Add periodic launchd operation,
  issue lifecycle, deadman observation, and cold-restart verification.
- [ ] **LS-08.1** (`depends_on: [LS-06.2c]`): Maintain the explicit historical
  denominator and unresolved PIT intervals.
- [ ] **LS-08.2** (`depends_on: [LS-08.1]`): Periodically expand and independently
  verify historical S&P 500/Nasdaq-100 PIT Silver coverage.
- [ ] **LS-GATE** (`depends_on: [LS-01.4, LS-02.3, LS-03.2, LS-05.2, LS-06.3b, LS-07.3]`):
  Promote the first unattended working system; seven-day observation is not a
  prerequisite to start it, while LS-04/LS-08 continue by coverage.

## Warehouse freshness backfill (2026-07-19)

Dependency graph: `F1 -> F2 -> F3 -> F4 -> F5 -> F6`

- [ ] **F1** (`depends_on: []`): Confirm no active warehouse writers, verify
  provider credentials through the main checkout, and capture the pre-backfill
  Bronze/raw/Silver cutoffs.
- [ ] **F2** (`depends_on: [F1]`): Identify a full-history-safe recovery path
  for corrupt `equity/NULG/1m.parquet`; preserve the current artifact before
  any replacement and reject any repair that would publish only a short tail.
- [ ] **F3** (`depends_on: [F2]`): Run the credentialed Massive equity-intraday
  catch-up through the canonical orchestrator and require completion through
  `2026-07-17` for `1m`, `5m`, `30m`, and `1h`.
- [ ] **F4** (`depends_on: [F3]`): Run the credentialed daily job to reconcile
  corporate actions and catch up equity, futures, commodity, FX, and CBOE
  volatility daily lanes through their expected `2026-07-17` cutoff.
- [ ] **F5** (`depends_on: [F4]`): Catch up FRED rates and any remaining
  non-equity intraday lane not owned successfully by the preceding jobs.
- [ ] **F6** (`depends_on: [F5]`): Verify raw and Bronze freshness, validate the
  published Silver revision and full carried-symbol coverage, and report any
  provider no-trade or irrecoverable exceptions without hiding them.

## Silver full-universe residual resolution (2026-07-16)

Dependency graph: `R1 -> R2 -> R3 -> R4 -> R5 -> R6 -> R7 -> R8`

- [ ] **R1** (`depends_on: []`): Land the case-preserving, evidence-grade,
  history-bounded foundation with atomic failure-report tests and both reviewed
  execution plans through a PR; stop for explicit merge approval.
- [ ] **R2** (`depends_on: [R1]`): Freeze writers and capture a fresh immutable,
  credentialed resolver/audit run with root- and hash-bound evidence.
- [ ] **R3** (`depends_on: [R2]`): Persist provider dividend factors with
  lineage, provenance, validation state, and legacy-schema compatibility.
- [ ] **R4** (`depends_on: [R3]`): Allow Silver to consume only independently
  validated provider factors and retain strict cash-derived fallback behavior.
- [ ] **R5** (`depends_on: [R4]`): Add dividend-normalized IB adjusted history
  and credentialed Massive reference lanes for remaining split ambiguity.
- [ ] **R6** (`depends_on: [R5]`): Build deterministic residual/mutation
  manifests, completeness-gate reconciliation, and close every blocker through
  an evidence-backed adjudication lane.
- [ ] **R7** (`depends_on: [R6]`): Freeze writers idempotently, verify backups,
  obtain destructive approval, apply approved repairs, and prove zero failures.
- [ ] **R8** (`depends_on: [R7]`): Atomically publish and independently verify
  Silver coverage for the discovered full Bronze universe before Apex handoff.

## Silver production failure classification (2026-07-16)

Dependency graph: `D1 -> D2 -> D3 -> D4`

- [x] **D1** (`depends_on: []`): Establish the clean Silver worktree and focused
  rebuild/Bronze baseline without touching the dirty main checkout.
- [ ] **D2** (`depends_on: [D1]`): Add evidence-grade, atomic rebuild failure
  reporting with canonical Bronze paths, source hashes, date bounds, and active
  corporate-action identities.
- [ ] **D3** (`depends_on: [D2]`): Verify and land the diagnostic through a PR
  with explicit merge approval.
- [ ] **D4** (`depends_on: [D3]`): Run a read-only production dry run, classify
  every residual failure, and revise the cutover plan from captured evidence.

## Silver case-preserving publication paths (2026-07-15)

Dependency graph: `C1 -> C2 -> C3`

- [x] **C1** (`depends_on: []`): Reproduce the `BCPC`/`BCpC` Silver artifact
  checksum collision with a focused regression test.
- [x] **C2** (`depends_on: [C1]`): Preserve provider-significant case before
  filesystem-safe encoding in daily and factor paths.
- [x] **C3** (`depends_on: [C2]`): Run focused and full Silver verification,
  then resume the production revision publish.

## Adjusted-history pointwise resolution and production readiness (2026-07-13)

Dependency graph: `P1 -> P2 -> P3 -> P4 -> P5 -> P6 -> P7`

- [x] **P1** (`depends_on: []`): Reproduce every SPY and PLTR pointwise failure
  from saved evidence and trace Bronze, Silver, Massive, IB, normalization, and
  corporate-action values at each failing date without mutating production.
- [x] **P2** (`depends_on: [P1]`): Classify each mismatch by root cause and prove
  the authoritative value using complete provider context, split boundaries,
  neighboring sessions, and raw-versus-adjusted return continuity.
- [x] **P3** (`depends_on: [P2]`): Add regression coverage and implement only the
  root-cause fix required by the evidence; keep validation read-only and keep
  ambiguous provider differences as explicit blockers.
- [x] **P4** (`depends_on: [P3]`): Run focused, RuntimeWarning, static, and full
  CI-equivalent verification, then repeat the SPY/PLTR live validation and
  require explained or eliminated pointwise failures.
- [x] **P5** (`depends_on: [P4]`): Dry-run the production Bronze basis migration,
  reconcile a complete corporate-action inventory for the Bronze equity
  universe, and run the full split-boundary audit; persist root-bound hashes,
  counts, and proposed repairs before changing price values.
  - Corporate-action cursor: 13,099 requested/completed, zero failed/pending,
    ticker hash `ad8bfdf9d908a489d5f09438910f08e9cfe26a06e16102f2cc0cf5a8ea6038fd`.
  - Structural scan: 8,753 Parquet artifacts, 335,873 rows, zero schema/read/symbol
    failures, 335,871 active revisions, and two cancelled revisions.
  - Split audit: 13,099 symbols, 11,280 eligible, 1,815 ambiguous, four invalid
    OHLC blockers, 1,097 symbols with proposed replacements, and zero approvals.
    Manifest SHA-256: `9a30e07ee9e2872d738710be051dd0bf52a63d3c0ad7170cd7380096fedfa6c2`.
- [ ] **P6** (`depends_on: [P5]`): Apply only evidence-confirmed Bronze repairs
  through the stale-safe rollback-capable manifest, then rerun the complete
  audit and structural/schema validation.
  - [x] **P6.1** (`depends_on: [P5]`): Exclude split actions at or before the
    first stored session from basis classification because no stored row can be
    affected; retain actions after the last stored session as pending evidence.
    - Corrected full audit: 13,099 symbols, 12,584 eligible, 511 ambiguous
      symbols / 759 events, and four invalid-OHLC errors. Manifest SHA-256:
      `3df837f61f588015f15b51d5934dcedd27cbf251a40e8e4a92eb5b2937388ce3`.
  - [ ] **P6.2** (`depends_on: [P6.1]`): Resolve each remaining in-history
    ambiguous boundary from repeated overlapping IB windows and persist the
    provider rows, action factor/date, continuity metrics, and terminal outcome.
  - [ ] **P6.3** (`depends_on: [P6.2]`): Repair the four invalid OHLC rows and
    approve only manifest entries supported by reproducible provider evidence.
  - [ ] **P6.4** (`depends_on: [P6.3]`): Apply the root-bound manifest, rerun the
    full audit twice, and require zero unresolved in-history classifications,
    zero invalid OHLC rows, unchanged second-run hashes, and readable schemas.
- [ ] **P7** (`depends_on: [P6]`): Build production inputs into disposable Silver,
  run the full-history gate plus representative Apex reads, self-review all
  evidence, and stop before advancing production `silver/revisions/current.json`.

## Full-history adjusted validation (2026-07-13)

Dependency graph: `V1 -> V2 -> V3 -> V4 -> V5`

- [x] **V1** (`depends_on: []`): Implement pure date coverage, split-only and
  total-return reconstruction, pointwise comparison, and 20/50/200-session SMA
  validation.
- [x] **V2** (`depends_on: [V1]`): Add paginated Massive adjusted aggregate and
  SMA evidence with strict normalization and same-origin pagination.
- [x] **V3** (`depends_on: [V2]`): Add read-only Massive/IB provider acquisition,
  split-boundary context, terminal outcomes, and content-addressed atomic caches.
- [x] **V4** (`depends_on: [V3]`): Add the full-universe quality command, strict
  coverage gate, cursor, evidence dimensions, JSON/Markdown reports, and CLI tests.
- [x] **V5** (`depends_on: [V4]`): Document operation, run focused/static/CI and
  live read-only verification, self-review, and record the final evidence.

Verification evidence:

- Focused validation suite: 100 passed with RuntimeWarning enforcement.
- CI-equivalent suite: 1,599 passed, 1 skipped, 98.29% coverage.
- Ruff check/format: clean. Pyright: 0 errors; 25 established warnings outside
  the new validation modules.
- Live disposable-Silver smoke: SPY and PLTR had complete Massive+IB coverage,
  exact Massive 20/50/200 SMA agreement, exact Silver factor/volume
  reconstruction, and unchanged production hashes. Both correctly failed the
  strict pointwise gate on existing OHLC differences. AAPL/MSFT/NVDA remained
  blocked before disposable Silver publication by split-affected unknown Bronze
  basis, and production Silver was absent.

## Bronze price-basis normalization and repair (2026-07-13)

Dependency graph: `T1 -> T2 -> T3 -> T4 -> T5 -> T6`

- [x] **T1** (`depends_on: []`): Add strict row-level equity Bronze source and
  price-basis metadata across every daily producer.
- [x] **T2** (`depends_on: [T1]`): Classify IB treatment per split event and
  selectively normalize incorporated adjustments to canonical raw rows.
- [x] **T3** (`depends_on: [T2]`): Add atomic, resumable legacy schema migration
  to `legacy/unknown` with hashes and dry-run support.
- [x] **T4** (`depends_on: [T3]`): Add read-only split-basis audit plus stale-safe,
  rollback-capable manifest repair and rehearse it in a disposable lake.
- [x] **T5** (`depends_on: [T4]`): Make Silver and its canary basis-aware and
  verify known AAPL, NVDA, and MSFT split boundaries through local Apex.
- [x] **T6** (`depends_on: [T5]`): Run all gates, update durable/operator docs,
  self-review, and prepare a separate follow-up PR without production repair.

## Silver causal canary hardening (2026-07-13)

Dependency graph: `T1 -> T2 -> T3 -> T4`

- [x] **T1** (`depends_on: []`): Cover future splits, exact ex-date activation,
  calendar gaps, and shared multi-symbol cutoffs.
- [x] **T2** (`depends_on: [T1]`): Expose rebuild and validation as-of,
  effective-action, and future-action counters.
- [x] **T3** (`depends_on: [T2]`): Recompute causal factor expectations in the
  canary and reject contaminated but internally consistent artifacts.
- [x] **T4** (`depends_on: [T3]`): Run repository gates and the five-symbol
  local sweep, self-review, commit, and update PR #50 without merging.

## Silver future-action cutoff (2026-07-13)

Dependency graph: `T1 -> T2 -> T3`

- [x] **T1** (`depends_on: []`): Add failing engine and rebuild regression
  coverage for announced-but-not-yet-effective dividends.
- [x] **T2** (`depends_on: [T1]`): Apply one explicit New York as-of cutoff to
  every factor calculation in a Silver rebuild.
- [x] **T3** (`depends_on: [T2]`): Run focused and CI-equivalent gates, repeat
  the real MSFT/Apex smoke test, self-review, and commit the fix.

## Silver adjustment engine R3 (2026-07-13)

Dependency graph: `T1 -> T2 -> T3 -> T4 -> T5`

- [x] **T1** (`depends_on: []`): Implement the pure Decimal-based adjustment
  engine and exhaustive factor intervals with example/property tests.
- [x] **T2** (`depends_on: [T1]`): Publish adjusted daily bars and compact factor
  intervals as validated, checksummed Silver Parquet artifacts.
- [x] **T3** (`depends_on: [T2]`): Publish locked, monotonic, transactional Silver
  revision manifests with `current.json` as the final commit record.
- [x] **T4** (`depends_on: [T3]`): Add targeted/full/dry-run Silver rebuild CLI,
  batch validation, no-op detection, counters, and manifest publication.
- [x] **T5** (`depends_on: [T4]`): Integrate scheduled reconciliation, implement
  the four-symbol canary, update durable/operator docs, and run all gates.

## Corporate-action ingestion R2 (2026-07-13)

Dependency graph: `T1 -> T2 -> T3 -> T4`

- [x] **T1** (`depends_on: []`): Add strict Massive split/dividend models,
  bounded same-origin pagination, validation, exports, and focused tests.
- [x] **T2** (`depends_on: [T1]`): Add the revision-aware canonical corporate
  action store with correction lineage, safe cancellation semantics, atomic
  publication, and focused tests.
- [x] **T3** (`depends_on: [T2]`): Add targeted/universe reconciliation CLI,
  dry-run behavior, telemetry counters, dispatch coverage, and focused tests.
- [x] **T4** (`depends_on: [T3]`): Document operation and scheduling boundaries,
  run focused and CI-equivalent verification, and prepare the implementation PR.

## Daily partial-bar audit and repair (2026-07-11)

Dependency graph: `T1 -> T2 -> T3 -> T4 -> T5 -> T6`

- [x] **T1** (`depends_on: []`): Confirm QQQ against staged day aggregates,
  summed minute data, REST, and ingestion logs; identify the pre-close target-date
  root cause and verify the June 24 prevention fix.
- [x] **T2** (`depends_on: [T1]`): Add tested tooling to audit staged `day_aggs`
  against every existing equity `1d.parquet`, write a rollback-capable mismatch
  manifest, and apply only manifest rows through locked atomic merges.
- [x] **T3** (`depends_on: [T2]`): Run the complete read-only overlap audit and
  review mismatch counts, date range, field distribution, and symbol scope.
- [x] **T4** (`depends_on: [T3]`): Repair only audited mismatches from staged
  authoritative rows and preserve the original values in the manifest.
- [x] **T5** (`depends_on: [T4]`): Re-run the complete audit and require zero
  remaining staged-overlap mismatches; validate QQQ and a liquid-ticker sample.
- [x] **T6** (`depends_on: [T5]`): Validate full-history structure and REST overlap
  for common and deterministic uncommon ticker samples, write the repair report,
  run repository gates, and prepare the PR.

## Post-lift cleanups (2026-06-11)

These were surfaced during the move of `data-lake/` from internal SSD to `/Volumes/DATA_LAKE` (exFAT, symlinked back).

### Atomic-publish orphan sweep on BronzeClient init

`BronzeClient` writes per-ticker parquet via temp → validate → `os.replace()`. If a writer is killed mid-publish, the temp file (`.<filename>.parquet.<pid>.<nanotime>.tmp`) is orphaned. On 2026-06-11 we found 19 such orphans (~135 MB) across equity bronze, traced to two killed-batch events (PIDs 8479, 44972). One-shot manual sweep cleaned them, but the underlying gap remains: `BronzeClient.__init__` should sweep its own asset-class dir on construction. ~5 lines.

Path: `clients/bronze_client.py` (and `clients/intraday_bronze_client.py` — same pattern).

### AppleDouble sidecar tolerance in glob patterns

On exFAT, macOS creates `._foo.parquet` sidecars (AppleDouble format) for any file with xattrs. These sidecars are pure metadata, not parquet — DuckDB / PyArrow will crash if it tries to read them as data.

Already fixed:
- `clients/massive_flatfile_store.py:110` — spill glob filters `._*`.

Still vulnerable (read-only diagnostic, low priority):
- `livewire_scripts/warehouse_health_report.py:149` — `bronze_root.glob("asset_class=*/symbol=*/*.parquet")`. The `*.parquet` leaf matches `._foo.parquet`. Add `if not p.name.startswith("._")` filter.

Operational mitigation in place: a `find … -name '._*' -delete` sweep is safe to run anytime. Done on 2026-06-11 (7,758 sidecars cleaned from equity bronze).

### Planner uses wrong filesystem for capacity check

`livewire_scripts/flatfile_planner.py:42` calls `shutil.disk_usage(warehouse_dir)` on `~/market-warehouse/` (internal SSD). After the lift, the actual data target is the symlinked `data-lake/` subdir (exFAT). Result: planner reports ~63 GiB free instead of the real 10 TiB. `repair` mode is harmless (skips `require_capacity()`), but `backfill` mode would falsely reject any future full-history run.

Fix:

```python
target = warehouse_dir / "data-lake"
if not target.exists():
    target = warehouse_dir if warehouse_dir.exists() else warehouse_dir.parent
usage = shutil.disk_usage(target)
```

### Ticker renames need a rename map (or re-archive cron)

Surfaced by VSCO → VSXY (Victoria's Secret rename, early June 2026). On 2026-06-11 we archived `symbol=VSCO/` to `bronze-delisted/` so daily-update stops failing on it. But the running 5-year backfill will recreate `symbol=VSCO/` with pre-rename intraday (Massive's flat-files use ticker-as-of-trade-date, which is data-correct).

Two follow-ups:
- **Short term**: re-archive `symbol=VSCO/` after the backfill completes.
- **Long term**: a `renames.json` config consulted at publish/query time. Equivalent to a primary-key remap table. Lets continuous-history queries `SELECT * FROM bronze WHERE symbol IN (rename_chain('VSXY'))` work cleanly.

### Equity union "exit 1 after completed summary"

`daily_backfill_equity_union` exits non-zero when any ticker fails after all retries, even if 1,548 succeeded. The orchestrator treats this as recoverable ("exited 1 after completed summary; continuing") — good — but the underlying tickers stay failing.

Persistent set across multiple days: KFS, SLNO, AMWD, BK, CVGW, FFIC, MCW, THR, TSEOF, VRE. All actively traded; not delisted. Most likely a symbol-normalization mismatch in Massive's SIP feed (suffixes, dot-class shares, ADR variants). Need to inspect Massive's actual ticker for each name and add a normalization map in `MassiveClient`.

## Per-day publish cost is HDD-bound (2026-06-11)

The DATA_LAKE volume is a spinning HDD with ~73 MB/s sustained sequential throughput. The current per-ticker monolithic intraday layout (`symbol=AAPL/{1m,5m,30m,1h}.parquet`) means every single-day append touches ~16 MB per ticker × ~12K tickers = ~192 GB read + 192 GB write. Disk-floor wall-clock = ~88 min for one day.

Real-world numbers from 2026-06-11:
- 5-year backfill staging: ~5 s/day (pyarrow-native, in `260cbf1`).
- 5-year backfill publish: completed inside the staged window — bound by the same HDD throughput.
- One-day repair for 2026-06-10: **48 min** with `--workers 4` and the new pyarrow-native merge (`95182c2`).

Three paths to actually break the ~88-min floor, none of which are in PR #28:

1. **Per-month partition layout** for intraday parquet: `symbol=AAPL/1m/year=2026/month=06.parquet`. Appending within a month rewrites only that month (~22 days, ~5 % of touch surface) → projected ~5 min/day at current HDD throughput. Requires updating every reader that opens intraday parquet by path (Postgres rebuild, warehouse health report, quality checks, any analytical script in this repo or downstream). Plus a one-time migration script that splits each existing `1m.parquet` into per-month files atomically. Multi-day effort.

2. **Hot/cold storage tier**: recent N months on internal SSD (`~/market-warehouse-hot/`), historical archive on the HDD volume. Daily catch-up only writes the hot tier; a separate compaction step rolls aging data into cold. Read path has to union both. Same downstream-touch surface as option 1, plus the operational complexity of two locations.

3. **Per-day delta sidecar**: keep the monolithic `1m.parquet` for reads as-is, but write incremental updates to `1m.delta/date=YYYY-MM-DD.parquet`. A separate compaction job folds the deltas into the monolith periodically (weekly?). Every reader has to union the monolith with whatever's in the delta dir at read time. Less invasive than option 1 — the monolithic file's API stays — but adds a non-trivial compaction service and a read-time correctness invariant.

Recommendation: option 1, when we're ready to coordinate the reader updates. Until then, daily catch-up takes ~50–90 min on this hardware and that is the actual ceiling, not a software bug.
