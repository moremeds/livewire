# Silver Full-Universe Residual Resolution Implementation Plan

> ## ⚠️ CALIBRATED 2026-07-17 — DO NOT EXECUTE FROM R1. READ THIS FIRST.
>
> This plan was written 2026-07-16, committed, and **never executed** (53 tasks,
> 0 done). Re-verified 2026-07-17 against production data and the live provider:
>
> | Task | Status | Evidence |
> |---|---|---|
> | **R1** | ✅ **already landed** — do not redo | `3e436a0`, `a89d572`, `aae8d24` are all ancestors of `main` |
> | **R2** | ⚠️ **redo** — baseline is stale | PR #57 changed `rebuild-silver` semantics (seed trim, window trim, eviction). Any pre-#57 baseline describes a rebuild that no longer exists |
> | **R3** | ❌ **IMPOSSIBLE — the premise is false** | `historical_adjustment_factor` **does not exist in Massive's response.** `/v3/reference/dividends` returns exactly: `cash_amount, currency, declaration_date, dividend_type, ex_dividend_date, frequency, id, pay_date, record_date, ticker`. Live check 2026-07-17: factor present on **0/92** (BMO), **0/11** (RACE), **0/132** (CNQ). `MassiveClient.normalize_dividend` parses it with `_optional_decimal`, so it silently reads `None` forever — which is why nobody noticed |
> | **R4** | ❌ **IMPOSSIBLE** — dead branch | Gates on `factor_validation_status == "validated"`, unreachable given R3 |
> | **R5** | ⚠️ **needs redesign** | Its `ib_adjusted` tier says *"reject the reference if any dividend lacks validated math"* — no dividend can ever have validated math, so that tier always rejects for any dividend payer |
> | **R6–R8** | ⚠️ **blocked** | `depends_on` chain through R5 |
>
> **Also dead:** deriving the dividend factor from Massive's `adjusted÷raw` ratio.
> `/v2/aggs?adjusted=true` is **split-adjusted only** — verified `adj/raw == 1.000000`
> across BMO's 2026-04-29 ex-date *and* across plain-USD payer KO's 2026-06-15
> ex-date. It does not adjust dividends for anyone.
>
> **Corrected residual taxonomy** (measured from the failure strings of a real
> `rebuild-silver --full --dry-run`, 2026-07-17 — supersedes the Evidence Baseline
> below, which says 594 and invents a nonexistent "1 AVBH non-positive close"):
>
> ```text
> 593 = 518 split_basis_unknown + 61 dividend_currency + 14 dividend_magnitude
> ```
>
> The 61 `dividend_currency` do **not** decompose as this plan's sibling claims
> ("52 genuinely foreign / 9 stray"). Measured against the real action store:
> **13** genuinely all-foreign · **47** currency-swing (same dividend stream, Massive
> sporadically reports the FX-converted USD amount instead of the declared amount) ·
> **1** (CNQ) with 40 duplicate ex-dates carrying both a CAD and a USD record.
> BMO — cited elsewhere as the example of "genuinely foreign" — is `{CAD: 68, USD: 24}`,
> i.e. the counter-example. None of the 61 is legitimately excludable: all trade in USD
> on US exchanges (Massive `us_stocks_sip` is US-listing by definition; bronze BMO
> matches Massive BMO at ratio 1.0000 on every overlapping date). They need FX
> conversion at ex-date, which is the **only** surviving path.
>
> The 14 `dividend_magnitude` are **10** terminal liquidating distributions (ex-date
> strictly *after* the symbol's last bronze bar) + **4** in-history anomalies — of which
> DBRG is a bronze-price defect (`div 1.1635` vs `prev_close 0.0004`, 2908×), not a
> dividend defect, and MCHB is a constant `85.00` repeated 23 times against ~13–16
> closes. The sibling plan's "11 terminal + 3 ticker reuse" is not what the data shows.
>
> **The "cheaper 518 path" was TESTED against IB 2026-07-17 and FALSIFIED.** The
> structural inference held — `repair_legacy_basis` is indeed indifferent to the old
> `unknown` label; it re-classifies freshly-fetched IB rows via
> `prepare_ib_rows_for_publish`, never reads `break_date` (0 refs) or the existing
> `price_basis`. A 10-symbol dry-run (relabel `mixed`, `--dry-run`) ran the whole path
> cleanly. But the **outcome was 10/10 `ambiguous`, 0 `would-repair`** — the machinery
> runs and then refuses every symbol. Root cause: `clients/price_basis.py`
> `classify_split_events` infers each series' basis from the **single-day price step**
> across each split ex-date (`observed = following[0].close / previous[-1].close`,
> line 88-92), and that step is contaminated by real market movement on the ex-date.
> AMC's 2023-08-24 10:1 reverse split: IB `TRADES` shows 19.60→14.37 (a real −27% day,
> no split jump — IB `TRADES` was already split-adjusted), so `observed=0.733` matches
> neither `factor` nor `1` within the 0.15 log-tolerance → ambiguous. INTC is ambiguous
> at exactly one split, 1987-10-29, ten days after Black Monday. **The single-day step
> is a broken validator and must be scrapped, not tuned.**
>
> **Correct fix (the real R5, redesigned): confirm basis against an authoritative
> raw/adjusted reference, never infer it from price.** Two same-date series divided
> cancel the real move and leave only the adjustment factor (`ADJUSTED_LAST÷TRADES ≡ 1.0`
> across AMC's split proved IB `TRADES` is adjusted, zero ambiguity). IB `TRADES` is NOT
> reliably raw; IB `ADJUSTED_LAST` is a deep-limited one-shot; Massive raw is entitled
> only ~5y. **Yahoo `Close`+`Adj Close` is the authoritative deep reference** (free,
> covers INTC-1987 / AIG-2009); `Adj Close/Close` is the exact cumulative
> split+dividend factor per date. Anchor at the known-raw recent end, walk back applying
> known split factors, and label each bronze row raw/adjusted by direct comparison.
> Genuinely unreachable history (delisted from every provider) fails closed — a
> data-availability limit, not the current self-inflicted ambiguity.
>
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the production Silver rebuild from 593 failures to zero, publish readable and revision-consistent Silver daily/factor artifacts for all 13,141 production Bronze equity symbols, and hand the complete revision to the Apex promotion plan.

**Architecture:** Preserve the already-proven first-session boundary fix. Re-run split/OHLC resolution against an immutable Bronze view with both credentials and dividend-normalized adjusted references. Persist provider dividend factors with provenance, but allow Silver to consume only factors independently validated against boundary prices. Apply only hash-bound replacement manifests. Publication remains blocked until a full dry run reports `failed=0` and an independent verifier proves complete universe coverage.

**Tech Stack:** Python 3.13, PyArrow/Parquet, `Decimal`, Massive reference API, IB Gateway `TRADES` and `ADJUSTED_LAST`, pytest/Hypothesis, atomic JSON/Parquet publication.

## Global Constraints

- Bronze equity Parquet and the revision-aware corporate-action store remain canonical.
- Silver is replayable; no adjusted-to-raw fallback is permitted.
- Use `/Users/moremeds/projects/livewire/.worktrees/silver-rehearsal` until the branch PR is merged; production runtime must then use a clean checkout of the merged commit.
- Production root must resolve to `/Volumes/DATA_LAKE/livewire/data-lake`; Silver root must resolve to `/Volumes/DATA_LAKE/livewire/data-lake/silver`.
- Preserve distinct `BCPC` and `BCpC` identities and paths.
- Never push to `main`; never merge without explicit approval for that PR.
- Never apply Bronze or corporate-action mutations without a writer freeze, verified backups, current source hashes, and fresh explicit destructive approval.
- Never infer provider-factor eligibility from presence alone; eligibility requires a recorded validation method, evidence digest, and `validated` state.
- Never treat an adjusted-price reference as split-only until known dividend effects in its comparison window have been removed or ruled out.
- A full reconciliation may cancel absent actions only when the provider response proves complete pagination for the requested symbol and date range.
- Resolver evidence is valid only when audit SHA-256, data-lake root, symbol, detail SHA-256, and Bronze source SHA-256 all match.
- Full-universe readiness requires `failed=0`; the partial-publish threshold is not a completion criterion.

## Evidence Baseline

```text
run root: /Volumes/DATA_LAKE/livewire/data-lake/repairs/silver-full-coverage/20260716-044638
post-fix rebuild: 13,141 symbols; 593 failed; 3,350 rebuilt; 9,198 unchanged
rebuild residuals: 518 split basis; 61 dividend currency; 14 dividend magnitude
audit2: 12,893 eligible; 248 ineligible
audit2 ineligible: 245 ambiguous split basis; BKTI, CBRE, CNC invalid OHLC
resolver: 277 resolved; 248 ambiguous
audit2 SHA-256: 677de387e25c28a39b0ba0b6f08b6b5d0a0331ab862018f251138761ff2768e6
rebuild failure SHA-256: 5eefe72bccce5bdd5f17cc5f0152132304fd91ecbc203c7d2606651b909c1439
```

The first resolver did not load `/Users/moremeds/projects/livewire/.env`.
Every inspected ambiguous fallback records `MASSIVE_API_KEY environment variable
is not set`; the same omission affected Massive OHLC fallback for BKTI, CBRE,
CNC, and NEOG. This invalidates any conclusion that Massive could not resolve
those cases; it does not invalidate the saved IB evidence.

## File Map

- Modify `clients/corporate_action_store.py`: retain provider dividend factors with revision lineage and legacy-schema reads.
- Modify `clients/adjustment_engine.py`: consume an explicitly validated provider dividend factor before cash/currency reconstruction.
- Modify `livewire_scripts/adjusted_history_sources.py`: parameterize IB daily reference type.
- Modify `livewire_scripts/resolve_split_basis.py`: use explicit `ADJUSTED_LAST`, then credentialed Massive fallback.
- Modify `livewire_scripts/rebuild_silver.py`: keep evidence-grade failure reporting and unchanged publication semantics.
- Modify `tests/test_corporate_action_store.py`, `tests/test_adjustment_engine.py`, `tests/test_adjusted_history_sources.py`, `tests/test_resolve_split_basis.py`, and `tests/test_rebuild_silver.py`.
- Create `livewire_scripts/build_silver_residual_manifests.py` and `tests/test_build_silver_residual_manifests.py`: deterministically construct dividend, mutation, and closed-residual artifacts.
- Create `livewire_scripts/verify_silver_revision.py` and `tests/test_verify_silver_revision.py`: independently verify full-universe Silver structure, manifests, and checksums.
- Create `scripts/silver_writer_freeze.sh`: capture, unload, and idempotently restore the exact launchd writer set.
- Create runtime artifacts only below the evidence baseline run root and new timestamped cutover roots.

## Dependency Graph

```text
R1 -> R2 -> R3 -> R4 -> R5 -> R6 -> R7 -> R8
```

### Task R1: Land the proven diagnostic and boundary foundation

**depends_on:** `[]`

**Interfaces:** Produces merged case-preserving paths, schema-v2 rebuild failure reporting, and `first_trade_date < ex_date <= as_of_date` action filtering.

- [ ] Copy this plan and the revised promotion plan into the clean feature worktree with `apply_patch`.
- [ ] Add the missing rebuild-report tests for empty failure output and atomic replacement; assert Bronze and Silver hashes remain unchanged in dry-run mode.
- [ ] Run:

  ```bash
  UV_CACHE_DIR=/tmp/livewire-uv-cache uv run ruff check clients livewire_scripts tests
  UV_CACHE_DIR=/tmp/livewire-uv-cache uv run pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing
  UV_CACHE_DIR=/tmp/livewire-uv-cache uv run pytest tests -q -W error::RuntimeWarning
  git diff --check origin/main...HEAD
  ```

  Required: zero failures, coverage at least 95%, no RuntimeWarning, no whitespace errors.
- [ ] Push `fix/silver-case-preserving-paths`, open one PR containing commits `3e436a0`, `a89d572`, `aae8d24`, tests, and both plans. Stop for explicit merge approval.
- [ ] After approved merge, create a clean detached runtime worktree at `origin/main`, assert an empty status, and record its exact commit in the next run root.

### Task R2: Capture an immutable diagnostic run with real fallback credentials

**depends_on:** `[R1]`

**Interfaces:** Produces a fresh `audit-resolver-v2.json`, `ib-evidence-v2/`, `audit3.json`, and a current eligible replacement set. It does not trust `audit1.json` after another scheduled Bronze write.

- [ ] Resolve the exact daily-update, intraday-catch-up, and watchdog launchd labels from their installed plists. Record `launchctl print gui/$(id -u)/<label>` success/failure and program path in `$RUN/writer-state-before.json`.
- [ ] Install an `EXIT HUP INT TERM` restoration trap before unloading anything. Unload only labels recorded as loaded, verify no `livewire_ingest`, `livewire_ops`, `rebuild-silver`, or repair process remains, and keep the freeze active through R2. Restore all originally loaded labels exactly once when R2 exits.
- [ ] Load credentials without printing them, explicitly bind the production root, and assert both providers are available:

  ```bash
  set -a
  source /Users/moremeds/projects/livewire/.env
  set +a
  test -n "$MASSIVE_API_KEY"
  export MDW_DATA_LAKE=/Volumes/DATA_LAKE/livewire/data-lake
  test "$(python -c 'from clients.paths import data_lake_dir; print(data_lake_dir())')" = "$MDW_DATA_LAKE"
  nc -zv 127.0.0.1 4001
  ```

- [ ] Add a resolver test in `tests/test_resolve_split_basis.py` proving an ambiguous IB result becomes resolved when credentialed Massive adjusted rows classify the same event.
- [ ] Create a new timestamped run root; do not place new evidence beneath the old run metadata and never overwrite evidence:

  ```bash
  RUN=/Volumes/DATA_LAKE/livewire/data-lake/repairs/silver-full-coverage/$(date -u +%Y%m%d-%H%M%S)
  test ! -e "$RUN"
  mkdir -p "$RUN/ib-evidence"
  ```

- [ ] Generate a new source-hash-bound audit immediately before resolution:

  ```bash
  /Users/moremeds/market-warehouse/.venv/bin/python scripts/livewire_quality.py audit-split-basis \
    --full \
    --data-lake-root /Volumes/DATA_LAKE/livewire/data-lake \
    --output "$RUN/audit-resolver.json"
  ```

- [ ] Run the resolver against localhost with the environment still loaded:

  ```bash
  /Users/moremeds/market-warehouse/.venv/bin/python scripts/livewire_quality.py resolve-split-basis \
    --audit-manifest "$RUN/audit-resolver.json" \
    --output-dir "$RUN/ib-evidence" \
    --data-lake-root /Volumes/DATA_LAKE/livewire/data-lake \
    --host 127.0.0.1 --port 4001 --resume
  ```

- [ ] Re-audit with `ib-evidence` into `audit-final.json`. Verify audit/root/source/detail hashes and report exact resolved, ambiguous, pending, error, invalid-OHLC, and replacement counts.
- [ ] Do not classify a Massive 403/empty response as “no repair.” It remains unresolved evidence.

### Task R3: Persist provider dividend adjustment factors

> ❌ **DEAD — DO NOT IMPLEMENT.** Massive does not return
> `historical_adjustment_factor`; see the calibration banner at the top of this file.
> Implementing R3 persists a column that is `None` for every row that will ever exist.

**depends_on:** `[R2]`

**Files:** `clients/corporate_action_store.py`, `tests/test_corporate_action_store.py`

**Interfaces:** Appends defaulted fields `historical_adjustment_factor: float | None = None`, `factor_validation_status: Literal["unvalidated", "validated", "rejected"] = "unvalidated"`, `factor_validation_method: str | None = None`, and `factor_evidence_sha256: str | None = None`; `CorporateActionStore.latest_active()` returns safe defaults for legacy schemas.

- [ ] Write failing tests proving:

  ```text
  legacy corporate-action parquet without the column reads factor=None
  MassiveDividend(historical_adjustment_factor=Decimal("0.9975")) persists 0.9975 as unvalidated
  a previously active event with the same provider ID but a newly available factor creates revision+1
  the prior revision becomes corrected and the new revision becomes active
  unchanged provider factor does not create another revision
  ```

- [ ] Append defaulted factor/provenance fields to the dataclass and nullable Parquet columns to the schema. In `_read`, inject safe legacy defaults before constructing `CorporateAction`.
- [ ] In `_from_provider`, copy the Massive dividend factor as `float` and use `None` for splits.
- [ ] Replace payload-hash-only equality with material equality over provider payload, persisted factor, validation status, method, and evidence digest. A newly available or changed factor must create a normal lineage-preserving revision through `reconcile`; never rewrite old revisions in place. Add a schema-version migration test for an old row whose payload hash already covered a factor that was formerly discarded.
- [ ] Run `tests/test_corporate_action_store.py`, `tests/test_sync_corporate_actions.py`, Ruff, and `git diff --check`; commit as `feat(actions): retain provider dividend factors`.

### Task R4: Use provider factors safely in Silver adjustment math

> ❌ **DEAD — DO NOT IMPLEMENT.** The entire branch here gates on
> `action.historical_adjustment_factor is not None and factor_validation_status == "validated"`.
> That factor is `None` for every dividend Massive will ever return (see the calibration
> banner at the top of this file), so this branch is unreachable and the `validated` status
> can never be produced. Nothing to build.

**depends_on:** `[R3]`

**Files:** `clients/adjustment_engine.py`, `tests/test_adjustment_engine.py`, `tests/test_rebuild_silver.py`

**Interfaces:** `build_factor_intervals(...)` uses `historical_adjustment_factor` only when `factor_validation_status == "validated"` and the evidence digest is present; otherwise it retains current cash/currency validation.

- [ ] Write failing tests proving:

  ```python
  factor = Decimal("0.9975")
  assert interval.price_adjustment_factor == factor
  ```

  Cover a CAD action against USD Bronze, a cash amount greater than previous close, multiple dividends on different dates, same-day split/dividend ordering, a missing factor, zero factor, negative factor, and factor greater than one.
- [ ] Implement the cash-dividend branch:

  ```python
  if action.historical_adjustment_factor is not None and action.factor_validation_status == "validated":
      price_factor = _decimal(
          action.historical_adjustment_factor,
          "historical_adjustment_factor",
      )
      if price_factor <= Decimal("0"):
          raise ValueError("historical adjustment factor must be positive")
      if not action.factor_evidence_sha256:
          raise ValueError("validated historical adjustment factor lacks evidence")
      factors_by_action[action.action_id] = (price_factor, ONE)
      continue
  ```

  Treat factors greater than one as suspicious evidence during validation, not as parser-invalid values. Keep same-day split handling for the cash-derived fallback. The validation step must compare the provider factor with pointwise adjusted/raw boundary behavior after removing known split and other dividend effects, then record the method and evidence digest.
- [ ] Add rebuild tests showing the former currency and magnitude fixtures stage successfully after reconciliation supplies a factor, while the same fixtures still fail without one.
- [ ] Run focused and full CI-equivalent suites; commit as `fix(silver): use provider dividend factors`.

### Task R5: Add explicit adjusted IB reference for residual splits

> ⚠️ **NEEDS REDESIGN — DO NOT IMPLEMENT AS WRITTEN.** The `ib_adjusted` tier here is
> supposed to supply the "validated" dividend factor R4 consumes, but that validated
> factor can never exist (dead adjusted÷raw path — see the calibration banner). An
> `ADJUSTED_LAST` IB fetch is *total-return* adjusted (splits **and** dividends folded in),
> which is the opposite of what Silver's raw-basis engine needs and cannot be decomposed
> back into a per-dividend factor without the very reference the plan lacks. The 518
> `split_basis_unknown` symbols — the bulk of the residual — are **not a dividend problem
> at all**; the cheaper path is to split the audit's `except Exception` so those
> `unknown price_basis` symbols feed the existing `repair-legacy-basis` IB re-derivation
> (currently unverified — needs a 2FA IB dry-run to confirm `would-repair`).

**depends_on:** `[R4]`

**Files:** `livewire_scripts/adjusted_history_sources.py`, `livewire_scripts/resolve_split_basis.py`, `tests/test_adjusted_history_sources.py`, `tests/test_resolve_split_basis.py`

**Interfaces:** `IBHistoryFetcher(client, *, what_to_show: str = "TRADES", price_basis: str = "split_adjusted")`; resolver provider values become `ib`, `ib_adjusted`, or `massive`.

- [ ] Write failing fetcher tests asserting `ADJUSTED_LAST` is passed unchanged to `IBClient.get_historical_data`. Do not label it `split_adjusted`; label it `total_adjusted` until dividend normalization succeeds.
- [ ] Write resolver tests for this closed decision order:

  ```text
  saved valid resolved evidence
  IB TRADES classification
  dividend-normalized IB ADJUSTED_LAST when TRADES is ambiguous
  Massive adjusted reference when IB ADJUSTED_LAST is ambiguous/unavailable
  terminal ambiguous when no reference reaches one treatment
  ```

- [ ] Parameterize `IBHistoryFetcher`. Before using `ib_adjusted`, reconstruct and remove every active dividend adjustment in the comparison window; reject the reference if any dividend lacks validated math or overlaps the split boundary ambiguously. Treat Massive as adjusted only according to its explicit endpoint contract; retain basis inference for IB TRADES.
- [ ] Add same-day split/dividend and nearby-dividend fixtures proving raw `ADJUSTED_LAST` cannot directly decide split basis and the normalized series can decide it only when all dividend inputs are validated.
- [ ] Persist both reference type and provider rows. Update `_replay_resolved_detail` to accept `{"ib", "ib_adjusted", "massive"}` and reproduce the same classification under current code.
- [ ] Run focused tests, full coverage, RuntimeWarning checks, and commit as `feat(silver): resolve splits with adjusted IB history`.
- [ ] Open a PR for R3–R5, stop for explicit merge approval, then deploy the merged commit to the clean runtime worktree.

### Task R6: Reconcile actions and build mutation-only repair manifests

**depends_on:** `[R5]`

**Interfaces:** `python -m livewire_scripts.build_silver_residual_manifests` produces schema-versioned `dividend-residuals.json`, `audit-final-mutations.json`, and `closed-residuals.json`; every input/output carries a SHA-256 and production-root identity.

- [ ] Implement and test `build_silver_residual_manifests.py`. Its `dividends` command reads the failure manifest, selects the two exact dividend errors, preserves symbol case, rejects duplicates, and writes an atomic preset plus source digest. Require the captured count, not a hard-coded 75; explain any drift from 75.
- [ ] With `/Users/moremeds/projects/livewire/.env` loaded, run the targeted dry run and record inserted, revised, cancelled, unchanged, and failed counts:

  ```bash
  /Users/moremeds/market-warehouse/.venv/bin/python scripts/livewire_ingest.py corporate-actions \
    --preset "$RUN/dividend-residuals.json" \
    --full-reconcile --dry-run --require-complete-provider-inventory --workers 4 \
    --cursor "$RUN/dividend-actions-dry-run-cursor.json"
  ```
- [ ] Extend reconciliation tests and CLI so `--require-complete-provider-inventory` permits cancellations only after every page completed and the response scope matches the requested symbol/date range; any partial, truncated, retry-exhausted, or malformed fetch blocks that symbol without cancellation.
- [ ] Run the manifest builder's `mutations` command to create `audit-final-mutations.json` containing only items satisfying all of:

  ```text
  eligible == true
  replacements is non-empty
  current source SHA-256 equals source_sha256
  resolved evidence root and audit SHA-256 match
  ```

  This avoids rewriting thousands of eligible no-op symbols through `repair-split-basis`.
- [ ] Run the builder's `close` command to join rebuild failures, final audit, action refresh, and provider-factor validation. Every symbol must be exactly one of `basis_replacement`, `ohlc_replacement`, `validated_provider_factor_revision`, `no_repair_required`, or `unresolved_blocker`.
- [ ] Route every blocker through a finite adjudication table: provider inventory correction, ticker/contract mapping, missing-boundary acquisition, second-source comparison, or evidence-backed manual/no-repair review. Each non-automated outcome requires reviewer, timestamp, source hashes, rationale, and evidence digest.
- [ ] Require `unresolved_blocker=0`. Never manufacture a basis/factor. If any adjudication lane remains open, preserve evidence and block publication.
- [ ] Before production reconciliation, run the same complete-inventory factor comparison across the full active dividend universe. Report every symbol whose historical Silver output would change; only explicitly validated factor revisions may enter the apply preset.

### Task R7: Freeze, back up, and apply approved repairs

**depends_on:** `[R6]`

**Interfaces:** Applies only reviewed action revisions and mutation-only Bronze replacements; produces rollback-complete backups and a zero-failure dry run.

- [ ] Implement `scripts/silver_writer_freeze.sh` with `capture`, `freeze`, `restore`, and `verify-restored` commands. State is atomic JSON containing UID, exact labels, plist paths, program paths, and prior loaded state. `restore` is idempotent and restores only previously loaded labels.
- [ ] Test the shell workflow against a temporary launchd domain or command mocks, including success, mid-freeze failure, signal handling, repeated restoration, and a previously-unloaded label. Install an `EXIT HUP INT TERM` trap before `freeze` and verify no writer/rebuild/repair process remains.
- [ ] Recompute every Bronze and corporate-action source hash. Any mismatch invalidates the affected evidence and returns execution to R6.
- [ ] Back up every equity and corporate-action Parquet that will change; verify source/backup SHA-256 equality.
- [ ] Present exact action revision counts, Bronze replacement counts, backup paths, hashes, commands, and rollback commands. Stop for fresh explicit destructive approval.
- [ ] After approval, apply targeted full reconciliation and then the mutation-only Bronze manifest:

  ```bash
  /Users/moremeds/market-warehouse/.venv/bin/python scripts/livewire_ingest.py corporate-actions \
    --preset "$RUN/dividend-residuals.json" \
    --full-reconcile --require-complete-provider-inventory --workers 4 \
    --cursor "$RUN/dividend-actions-apply-cursor.json"

  /Users/moremeds/market-warehouse/.venv/bin/python scripts/livewire_store.py repair-split-basis \
    --manifest "$RUN/audit-final-mutations.json" \
    --data-lake-root /Volumes/DATA_LAKE/livewire/data-lake \
    --approve
  ```
- [ ] Re-audit with current hashes and run `rebuild-silver --full --dry-run --failure-output ...`. Required: `failed=0`. Restore writers on any abort.

### Task R8: Publish and prove full-universe Silver

**depends_on:** `[R7]`

**Interfaces:** Produces one atomic complete Silver revision and hands its revision ID to the Apex adjusted-promotion plan.

- [ ] Run `rebuild-silver --full` from the clean merged runtime checkout. Require exit zero, zero failure report entries, and atomic `current.json` advancement.
- [ ] Implement and test `python -m livewire_scripts.verify_silver_revision`. It independently enumerates the current Bronze universe and verifies readable non-empty daily and factor Parquet, exact `SilverClient` schemas, stable symbol IDs, sorted unique dates, exhaustive non-overlapping factor intervals, one matching adjustment revision, and current-manifest SHA-256 matches. It writes atomic JSON and exits nonzero on any mismatch.
- [ ] Run the verifier. Use its discovered Bronze count rather than hard-coding 13,141; explain any drift and require Silver coverage to equal that count.
- [ ] Canary AAPL, NVDA, SPY, INTC, BCPC, `BCpC`, AAAP, all repaired OHLC symbols, all former currency/magnitude symbols, and the immutable Argon universe.
- [ ] Run a full-universe semantic before/after comparison for every symbol, not only canaries. Any changed symbol must map to an approved residual/action record; reject unexplained historical changes.
- [ ] Run a second full dry run. Required: `failed=0`, `rebuilt=0`, and `unchanged=<verifier Bronze count>`.
- [ ] Restore every originally loaded launchd job exactly once and verify its program path points at merged code.
- [ ] Record revision, hashes, coverage count, repair manifests, backups, canaries, and rollback commands. Hand the revision to (plan archived; see git history) Task 8; do not change Apex mode in this plan.

## Definition of Done

```text
rebuild-silver --full --dry-run: failed=0, rebuilt=0
Silver daily/factor coverage equals the current Bronze equity universe
no unresolved split, OHLC, currency, or dividend-magnitude blocker remains
all canonical mutations have verified backups and rollback metadata
Silver current manifest and artifact checksums agree
launchd writers are restored exactly once
Apex remains raw until the separate promotion gate
```
