# IB-verified Yahoo true-raw reconstruction for the unknown-basis population

**Date:** 2026-07-19
**Status:** Approved (brainstorming), pending spec review
**Author:** operator + Claude
**Related:** `2026-07-16-silver-legacy-basis-full-repair-design.md`, `2026-07-18-repair-0618-single-bar.md`

## Problem

~90% of the equity universe carries `price_basis='unknown'` (`source='legacy'`).
Such a symbol stages into Silver fine **only while no split touches it**. The
moment a split does, `build_factor_intervals` raises
`unknown price_basis for split-affected row` and the symbol is quarantined and
evicted (fail-closed). At silver rev-9 this is **172 realized failures** —
symbols where a split already lands on an `unknown` row — out of a broader
unknown-basis population of ~12K.

The standing goal is **silver-grade full universe, no downgrade**: every symbol
publishes, and everything published is correct. To publish these symbols we must
flip their basis `unknown → raw` — but only after independently confirming the
true-raw values, because publishing a wrong series is worse than the current
fail-closed eviction (which serves nothing wrong).

## What already exists (this is not a new system)

`livewire_scripts/resolve_yahoo_basis.py` + `clients/yahoo_basis.py` already
implement ~90% of the reconstruction, and are covered by
`tests/test_resolve_yahoo_basis.py` and `tests/test_yahoo_basis.py`. It has been
run in apply mode already across prior batches (relabel-batch1: 221 completed;
apply-batch2: 123 completed; plus a split-repair batch), all reflected in rev-9.
It already does:

- Yahoo split-adjusted fetch + splits (`YahooClient.get_daily`).
- True-raw reconstruction `raw = yahoo_adj × Π(split multipliers for ex_date > bar_date)`
  (`reconstruct_raw_closes`); volume divides by the same product.
- Reconcile Yahoo splits against our corporate-action store; **fail closed**
  (`split_mismatch`) if they disagree (`reconcile_splits`). Reconciliation is
  bounded to in-history splits (`ex_date > first stored bronze date`): a split
  on/before the first stored row folds into no stored row (both reconstruction
  and Silver apply only `ex_date > bar_date`), so a disagreement there is
  immaterial and does not fail the symbol.
- Per-row classification relabel / rewrite / mismatch / unmatched
  (`classify_existing_basis`).
- Ticker-reuse / wrong-entity guard: **fail closed** (`high_mismatch`) when
  >5% of rows disagree with Yahoo.
- Staging self-check: the corrected series must pass `build_factor_intervals`
  without raising `unknown price_basis`, else `stage_fail`.
- Verbatim parquet backup + write-ahead intent sidecar + `rollback-legacy-basis`
  compatibility.
- Dry-run by default; `--relabel-only` (zero value change) vs `--allow-rewrite`
  (rewrite adjusted deep rows to raw). Cursor with `data_lake_root` identity.
- **Never overwrites bronze on Yahoo's word alone; failure is fail-closed and
  reported, not written** — already matches the no-downgrade constraint.

This design adds exactly two things it lacks: **(1) an IB cross-verification
gate**, and **(2) resumable priority batching + subcommand registration**.

## Design

### 1. IB verification — anchor gate on the post-last-split window (Option A)

**The trap:** IB's own daily TRADES history has inconsistent basis across split
boundaries (this is *why* `unknown` exists, and why the old single-day-step IB
classifier went 10/10 ambiguous). Comparing the full reconstructed true-raw
series against full IB history would produce spurious mismatches at every split
boundary → false rejections → withholding good symbols → **that is itself a
downgrade**.

**The gate:** IB is *definitionally raw* in the window **after the most recent
split ex-date** (no future split remains to adjust for). Compare the
reconstructed true-raw series against IB **only in that window**, penny
tolerance.

- A symbol with **no splits** → the whole history is the post-last-split window
  → full-history IB comparison, unambiguous. **Every symbol still gets an IB
  fetch and passes the anchor**, satisfying "IB cross-verify every symbol."
- A symbol **with splits** → only the most-recent segment is compared. That
  segment is enough to catch wrong-entity / ticker-reuse / a broken
  reconstruction (a wrong entity's recent prices will not match). Deep history is
  verified by the existing triad: Yahoo reconstruction (validated penny-for-penny
  vs Massive raw), corporate-action-store split reconciliation, and the staging
  self-check.
- **IB is a gate, never a data source.** We never write IB values into bronze —
  that would re-import IB's deep-history basis ambiguity. IB only confirms or
  rejects the Yahoo reconstruction.

**Honest boundary:** deep pre-split history cannot be independently re-checked
against IB (IB's deep history is the very thing that is unreliable). It rests on
the Yahoo-reference + action-store + staging triad. This spec does not claim
every deep bar is IB-confirmed.

**Rejected alternatives:**
- **Full-history IB comparison** — IB's own basis inconsistency causes false
  rejections = downgrade. Rejected.
- **`fetch_ib_evidence` per-boundary-normalized IB view** (the
  `repair_legacy_basis` path) — 10/10 ambiguous in practice; unreliable and
  over-engineered for a gate. Rejected.

**Tolerance:** the anchor window compares **close only**, within a small relative
tolerance (default matches the existing `_close_match` penny/ratio tolerance in
`clients/yahoo_basis.py`; a `--ib-tolerance` flag allows override). Close-only is
deliberate: `validate_adjusted_history` already treats IB open/high/low
differences as diagnostic rather than failures, because provider filters and IB
request shape legitimately move them — gating on OHLC would manufacture false
rejections, which is itself the downgrade this design exists to avoid. The write
stays safe without it because `_scale_row` multiplies every price field by the
same positive factor, so it cannot introduce an OHLC inconsistency that bronze
did not already have.
The window length is the shorter of (bars since last split ex-date) and a bounded
cap (`--ib-window-cap`, default 250 ≈ one trading year). **The IB request is
bounded to that same window**, not to the series start — a full-history request
would chunk into roughly one IB call per year of history for a 250-day
comparison and hit pacing limits on a batch of this size.

**Mount point:** `livewire_scripts/adjusted_history_sources.IBHistoryFetcher`
(`__call__(symbol, start, end) -> list[dict]`), already imported by
`repair_legacy_basis`. The gate runs *after* the staging self-check
(`status == would_resolve`) and *before* the write.

### 2. Subcommand + flags

Register the existing module as `livewire_store.py` subcommand
`resolve-yahoo-basis` (today only `repair-legacy-basis` is registered).

New flags on `resolve_yahoo_basis.run`:
- `--ib-verify` (+ `--ib-host` / `--ib-port`, default `127.0.0.1:4001`): enable
  the anchor gate. **`--apply` requires `--ib-verify`** — no publish without IB
  confirmation. `--ib-tolerance` optional override.
- `--resume`: read the existing `cursor.json {completed}` and skip completed
  symbols. (The cursor is already written on apply; this adds skip-on-read.)
- `--priority-order` (+ `--presets-dir`): order symbols
  sp500 → ndx100 → r2k → tail via `repair_legacy_basis._order_symbols`.
  **`--presets-dir` missing is an error**, never a silent zero-symbol run
  (the CLAUDE.md cwd-relative-presets trap; copy the fix already in
  `repair_legacy_basis`).
- `--failure-manifest`: accept a `rebuild-silver --failure-output` JSON as the
  symbol source, filtered to reason `unknown price_basis for split-affected row`.
  Regenerated fresh each batch — never a stale list.
- `--limit N`: process at most N not-yet-completed symbols this session (2FA
  batching).

### 3. Per-symbol verdicts (the review queue)

`published` is the only success. Everything else leaves **bronze untouched** and
lands in the review queue:

| verdict | meaning | bronze |
|---|---|---|
| `published` | staged + IB-anchor-matched, written | rewritten/relabeled |
| `ib_mismatch` | IB anchor disagrees with reconstruction | untouched |
| `ib_insufficient_overlap` | fewer than `--ib-min-overlap` window dates came back from IB. This is the expected verdict for a delisted / reused ticker: ib_async's `qualifyContracts` returns empty rather than raising, so IB simply yields no bars | untouched |
| `ib_error: …` | any other per-symbol exception out of the fetcher (caught broadly — it drives ib_async directly, so the surface is not enumerable) — re-asked on `--resume` | untouched, not checkpointed |
| `high_mismatch` | >5% rows disagree with Yahoo (ticker reuse / wrong entity) | untouched |
| `split_mismatch` | Yahoo splits do not reconcile with action store | untouched |
| `yahoo_missing` / `yahoo_empty` / `yahoo_error` | no usable Yahoo data | untouched |
| `stage_fail` | corrected series still fails `build_factor_intervals` | untouched |

### 4. No-downgrade / fail-closed invariants

- **Any non-`published` verdict → zero bronze bytes change → the symbol keeps
  serving its current state.** The 172 are currently evicted (serving nothing);
  "unresolvable" keeps them evicted — never a downgrade. Clean-but-unknown
  symbols touched in later batches keep staging as before on any non-publish.
- **IB unreachable** (2FA not approved / gateway down) → abort the run;
  nothing checkpointed for the in-flight symbol; `--resume` re-asks. **Never**
  converted to a withhold. (Matches `repair_legacy_basis`: never auto-retry a
  connection failure — a failure usually means 2FA/maintenance/session conflict.)
- **IB transient error / no-data** → per-symbol verdict above; transient errors
  are not checkpointed done, so `--resume` re-asks (same rule as `triage-breaks`).
- **Write safety:** verbatim backup + sha256 + write-ahead sidecar *before* any
  mutation; `rollback-legacy-basis --output-dir [--tickers …]` undoes a batch or
  one symbol. Re-running a batch into an existing `--output-dir` without
  `--resume` is **refused**, because re-backing-up an already-mutated parquet
  would overwrite the pristine backup and turn rollback into a no-op.
- **Do not overlap the apply phase with the nightly writers.** `symbol_lock`
  makes each individual write atomic, but the resolver reads the series *before*
  the lock and `replace_ticker_rows` replaces the whole snapshot, so a bar the
  nightly job merges in between would be dropped by the replace. It is not
  corruption (the next catch-up re-detects the gap) but it is avoidable: run
  apply outside the 05:00/06:00 UTC windows.
- **Writer freeze** applies only to the final `rebuild-silver --full` publish,
  per the existing rev-3 operator pattern (unload the three writers, publish,
  reload regardless of exit code).
- **Window regression:** flipping `unknown → raw` only *extends* a window. If a
  window start nonetheless moves later, the default guard withholds and keeps
  serving the previous window → no downgrade. **The batch rebuild runs WITHOUT
  `--allow-window-regression`** (that was a one-time rev-3 bootstrap).

### 5. Testing

Real frozen fixtures only, no synthetic market values. Mocking the IB/Yahoo
clients is allowed; the *values* they return are real tickers at real prices with
an as-of date (per the no-synthetic-data rule).

- IB anchor gate: reconstruction matching the IB recent window → `published`;
  a deliberately-mismatched anchor → `ib_mismatch`.
- No-split symbol → full-history anchor comparison.
- `--resume` skips completed symbols; `--priority-order` orders correctly;
  `--presets-dir` missing errors.
- IB unreachable → abort/resume, no false withhold; `ib_insufficient_overlap` /
  `ib_error` verdicts and their checkpoint behavior.
- Idempotency: an already-`raw` symbol re-run is a no-op (relabel-all/rewrite-0).
- Coverage ≥95% (repo gate).

### 6. Rollout

1. **batch-1 = the 172 residual.** Regenerate the list
   (`rebuild-silver --full --dry-run --failure-output`, filter the split-affected
   unknown reason) → dry-run (self-gate only, count how many clear the gate) →
   operator review → `--apply --ib-verify` (2FA-gated, `--limit`/`--resume`
   across sessions) → freeze writers → `rebuild-silver --full` (no
   `--allow-window-regression`) → reload writers → apex adjusted-mode canary on a
   sample of the newly published symbols.
2. **STOP GATE.** Measure batch-1 `published` vs review-queue ratio. A large
   review tail means the population is dominated by genuinely-hard cases
   (ticker reuse / delisted / Yahoo gaps) — reassess before spending IB time on
   the full 12K. A large review tail is an expected, correct outcome, not a
   failure; the 5 rows deleted in the 06-18 repair are exactly this shape.
3. **batch-2:** sp500 / ndx100 / r2k members of the unknown-basis population
   (audit `mixed=238` plus split-carrying `clean`/`error`).
4. **batch-3…N:** the ~12K tail, `--limit`-chunked across 2FA sessions, strung
   together by `--resume`; same dry-run → apply → rebuild → canary rhythm per
   chunk.

### Out of scope

The 61 `dividend currency does not match bronze currency` and 4
`cash dividend ≥ previous close` failures at rev-9 are a **separate
corporate-action-store fix**, not this reconstruction pipeline.

## Success criteria

- `resolve-yahoo-basis` registered, `--apply` refuses to run without
  `--ib-verify`.
- Every published symbol's post-last-split window matches IB within tolerance.
- Every non-published symbol leaves bronze untouched and is recorded in the
  review queue.
- batch-1 applied, rev-N published, apex adopts it with `consecutive_failures=0`,
  and the previously-evicted 172 either publish correctly or stay evicted — none
  serve a wrong series.
- Tests pass at ≥95% coverage.
