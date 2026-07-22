# Silver Full Coverage and Apex Adjusted Promotion Implementation Plan

> **Execution:** Use `superpowers:executing-plans`. This plan has evidence and approval checkpoints; do not skip ahead when a checkpoint blocks.

**Goal:** Finish production Silver repair and coverage, then promote Apex globally to adjusted mode without weakening Bronze integrity, Silver atomicity, or rollback safety.

**Architecture:** Evidence-grade rebuild diagnostics have now proven that corporate actions on or before the first stored session are the dominant failure class. Land the diagnostic and narrowly proven adjustment boundary change together, rerun the complete production dry run, then classify every newly exposed residual. Use only existing supported mutation paths, publish Silver only after a zero-failure dry run, verify every artifact and revision checksum independently, restore writers, and then promote Apex with raw mode as the immediate serving rollback.

**Repositories:** Livewire owns Bronze, corporate actions, repair, and Silver publication. Apex owns adjusted serving and the global price-mode gate. Apex changes begin only after Livewire Silver is complete.

## Non-negotiable gates

- Bronze Parquet remains canonical; Silver is replayable and never becomes a second write authority.
- Work from clean, exact commits. Never modify the dirty local `main` checkout.
- Never push to `main`; never merge a PR without explicit approval for that PR.
- Never mutate corporate-action Parquet manually. Use `CorporateActionStore.reconcile(...)` with a fresh provider inventory.
- Never edit Bronze Parquet manually. Use the established repair or daily ingestion/catch-up path.
- A resolver artifact is usable only when `status == "resolved"` and its symbol, production `data_lake_root`, and Bronze `source_sha256` match.
- The destructive repair step requires fresh explicit approval immediately before execution.
- A Silver publish requires zero rebuild failures, not merely the partial-publish threshold.
- Never roll back Silver by restoring `current.json` alone. Apex raw mode is the first-line rollback.
- A release tag, registry publish, or PR merge requires explicit user approval.

## Dependency graph

```text
T1 -> T2 -> T3 -> T4 -> T5 -> T6 -> T7 -> T8 -> T9
```

## Recovered production evidence — 2026-07-16

The first full-universe dry run was interrupted when the host stopped after the
atomic failure manifest was published but before the final rebuild summary was
written. This is a valid classification checkpoint: the manifest is complete,
ordered from `AA` through `ZVOL`, and records production roots, source hashes,
date bounds, and active action identities. The zero-byte summary is not used as
completion evidence and will be regenerated after the semantic fix.

```text
diagnostic commit: a89d572be7c53f083e341834728602233c522153
run root: /Volumes/DATA_LAKE/livewire/data-lake/repairs/silver-full-coverage/20260716-032925
failure manifest: rebuild-failures.json (29.7 MB, schema v2)
classification: failure-classification.json
classification SHA-256: b03d9d64c17658f9cb551bbe8780dafac621c274caa5b3f37a77cd23b80e27e0
as-of date: 2026-07-15
total failures: 3,934
action on/before earliest stored session: 3,444
unknown split basis: 459
dividend currency mismatch: 20
invalid dividend magnitude: 11
unknown classification: 0
```

All 3,444 boundary-dividend failures have a matching active cash dividend with
`ex_date <= earliest_trade_date`; hypothesis mismatches are zero. The 490 other
failures are a lower bound on post-fix residuals, not a final count, because a
boundary failure can currently mask a later error for the same symbol.

The post-fix rerun completed at:

```text
run root: /Volumes/DATA_LAKE/livewire/data-lake/repairs/silver-full-coverage/20260716-044638
diagnostic commit: aae8d24e994e2c480b15857fcb421438e8010dbe
summary: failed=593, rebuilt=3350, unchanged=9198, revision=2
post-fix residuals: 518 unknown split basis, 61 dividend currency mismatch, 14 invalid dividend magnitude
boundary failures remaining: 0
manifest SHA-256: 5eefe72bccce5bdd5f17cc5f0152132304fd91ecbc203c7d2606651b909c1439
```

This is the new baseline for Task 6. Do not subtract counts from the pre-fix
manifest again; use this post-fix manifest as the sole residual inventory.

The fresh residual audit also completed in the same run root:

```text
audit symbols: 13,141
audit eligible: 12,616
audit ineligible: 525 (521 ambiguous split basis, 4 invalid OHLC)
audit SHA-256: 972e585f08695415727dea849ee3d1f2955660acb4279f1cfd19bc3de39be691
IB TCP probe: macmini:4001 open
IB API probe: timeout before historical requests
resolver status: blocked_before_historical_requests
```

The resolver status is recorded in `ib-resolver-status.json`; its evidence
directory is safe to resume because no historical request completed and no
Bronze file was changed.

## Task 1: Create the isolated branch and preserve this plan

**depends_on:** `[]`

**Files:** `CLAUDE.md`, `README.md`, `.codex/project-memory.md`, `tasks/lessons.md`, `docs/plans/silver-production-cutover-handover.md`, this plan, `tasks/todo.md`

- [x] Read the startup and handover files and record the exact `origin/main` commit.
- [ ] From `/Users/moremeds/projects/livewire`, run:

  ```bash
  git fetch --prune origin
  git status --short --branch
  git worktree add .worktrees/silver-full-coverage \
    -b fix/silver-full-coverage origin/main
  ```

- [ ] Copy the untracked plan into the worktree before leaving the dirty checkout:

  ```bash
  cp /Users/moremeds/projects/livewire/docs/superpowers/plans/2026-07-16-silver-full-coverage-apex-adjusted-promotion.md \
    /Users/moremeds/projects/livewire/.worktrees/silver-full-coverage/docs/superpowers/plans/
  cd /Users/moremeds/projects/livewire/.worktrees/silver-full-coverage
  git add docs/superpowers/plans/2026-07-16-silver-full-coverage-apex-adjusted-promotion.md
  git commit -m "docs(silver): add production promotion plan"
  ```

- [x] Reuse the clean `.worktrees/silver-rehearsal` branch based on `af8e3f3`; it already contains `3e436a0` and preserves distinct `BCPC`/`BCpC` paths.
- [x] Add the diagnostic dependency graph and execution state to `tasks/todo.md`.
- [ ] Copy this revised plan into the isolated worktree and commit it before opening the PR; the dirty local `main` copy remains untouched otherwise.

## Task 2: Add evidence-grade rebuild failure reporting

**depends_on:** `[T1]`

**Files:** `clients/bronze_client.py`, `livewire_scripts/rebuild_silver.py`, `tests/test_bronze_client.py`, `tests/test_rebuild_silver.py`

The diagnostic was implemented and production-tested before any adjustment semantic change. It may now share one PR with Task 5 because the captured evidence proves Task 5's premise.

- [x] Add and test a public read-only `BronzeClient.symbol_path(symbol) -> Path` delegating to the existing canonical path resolver. This avoids production diagnostics depending on a private method.
- [ ] Add a failing `--failure-output PATH` test whose fixture has a known source file and known active corporate action. Assert the complete payload, not values derived from the payload itself:

  ```json
  {
    "schema_version": 2,
    "generated_at": "<UTC RFC3339>",
    "data_lake_root": "<absolute root>",
    "silver_root": "<absolute silver root>",
    "as_of_date": "2026-01-04",
    "failures": [{
      "symbol": "BAD",
      "error_type": "ValueError",
      "error": "missing previous close for cash dividend on 2026-01-01",
      "bronze_path": "<absolute canonical path>",
      "source_sha256": "<known fixture digest>",
      "earliest_trade_date": "2026-01-01",
      "latest_trade_date": "2026-01-04",
      "active_actions": [{
        "action_id": "bad-dividend",
        "action_type": "cash_dividend",
        "ex_date": "2026-01-01",
        "status": "active"
      }]
    }]
  }
  ```

- [ ] Test exact error categorization, deterministic symbol/action ordering, UTC `generated_at`, empty-failure output, atomic replacement, and no Bronze/Silver mutation during `--dry-run`.
- [x] Implement the report using canonical Bronze path/date reads, `CorporateActionStore.latest_active`, SHA-256 streaming, and atomic temp-file + `os.replace` publication.
- [x] Run focused tests, `ruff`, full CI-equivalent coverage, and the RuntimeWarning suite: 1,667 passed, one skipped, 98.04% coverage.
- [x] Commit the diagnostic locally as `a89d572` on `fix/silver-case-preserving-paths`.
- [ ] Complete any missing empty-report/atomic-replacement assertions during Task 5, then open one PR containing the diagnostic, case-preserving fix, semantic boundary fix, tests, and revised plan. Stop for explicit merge approval.

## Task 3: Verify the diagnostic production checkout

**depends_on:** `[T2]`

**Runtime checkout:** `/Users/moremeds/projects/livewire/.worktrees/silver-full-coverage-runtime`

- [x] For the read-only evidence run, use the clean isolated checkout at exact commit `a89d572`; verify `git status --porcelain` is empty.
- [ ] After the combined PR is merged, create or reset the dedicated runtime checkout:

  ```bash
  cd /Users/moremeds/projects/livewire
  git fetch --prune origin
  git worktree add --detach .worktrees/silver-full-coverage-runtime origin/main
  cd .worktrees/silver-full-coverage-runtime
  git rev-parse HEAD
  test -z "$(git status --porcelain)"
  ```

- [x] Assert the evidence-run roots resolve exactly to `/Volumes/DATA_LAKE/livewire/data-lake` and `/Volumes/DATA_LAKE/livewire/data-lake/silver`.
- [ ] Repeat the assertions at the merged runtime commit before the post-fix rerun:

  ```bash
  export MDW_DATA_LAKE=/Volumes/DATA_LAKE/livewire/data-lake
  export MDW_SILVER_DIR=/Volumes/DATA_LAKE/livewire/data-lake/silver
  /Users/moremeds/market-warehouse/.venv/bin/python - <<'PY'
  from pathlib import Path
  from config import data_lake_dir
  import os
  assert data_lake_dir().resolve() == Path("/Volumes/DATA_LAKE/livewire/data-lake")
  assert Path(os.environ["MDW_SILVER_DIR"]).resolve() == Path("/Volumes/DATA_LAKE/livewire/data-lake/silver")
  PY
  ```

  If the actual config API differs, update this assertion to the imported production accessor and test the same two exact resolved paths. `rebuild-silver` has no `--data-lake-root`; environment and working directory are therefore part of the safety boundary.

## Task 4: Capture and classify the read-only production failure inventory

**depends_on:** `[T3]`

- [x] Create the unique run directory `20260716-032925`; never reuse it for a later run or resolver cursor.

  ```bash
  RUN_ID=$(date -u +%Y%m%d-%H%M%S)
  export CUTOVER="/Volumes/DATA_LAKE/livewire/data-lake/repairs/silver-full-coverage/$RUN_ID"
  test ! -e "$CUTOVER"
  mkdir -p "$CUTOVER"
  ```

- [ ] Record into `$CUTOVER/operator-state.txt`: run ID, UTC time, hostname, exact Livewire commit, clean status, resolved Bronze/Silver roots, Bronze symbol count, current Silver revision, free space, and SHA-256 of `current.json`. Do not record secrets.
- [x] From exact commit `a89d572`, run the read-only full dry run. The host stopped after atomic failure-manifest publication and before summary completion; preserve that interruption honestly rather than treating the zero-byte summary as successful completion.

  ```bash
  PY=/Users/moremeds/market-warehouse/.venv/bin/python
  "$PY" scripts/livewire_store.py rebuild-silver --full --dry-run \
    --failure-output "$CUTOVER/rebuild-failures.json" \
    >"$CUTOVER/rebuild-dry-run.json"
  ```

- [x] Produce `$CUTOVER/failure-classification.json` with every failure assigned to the exact observed classes in the recovered-evidence section.

  ```text
  action_on_or_before_earliest_trade_date
  resolved_split_or_invalid_ohlc
  incorrect_provider_action
  missing_boundary_bar
  unknown
  ```

- [x] Prove the semantic hypothesis mechanically: all 3,444 cases match an active cash dividend with `ex_date <= earliest_trade_date`; zero mismatches and zero unknown classifications.

## Task 5: Implement the proven out-of-range action semantics

**depends_on:** `[T4]`

Task 4 proved the hypothesis across 3,444 production failures, so this task is required.

**Files:** `clients/adjustment_engine.py`, `tests/test_adjustment_engine.py`

- [ ] Replace the existing `test_missing_previous_close_blocks_dividend`; do not add a contradictory second test. The replacement must prove dividends and splits on or before the first stored trade date are non-impacting, while an in-range dividend without a previous close still fails.
- [ ] Add boundary tests for action date before first bar, on first bar, after first bar, future of `as_of_date`, same-day split/dividend ordering, and empty bars.
- [ ] Implement the smallest boundary filter: only active actions satisfying `first_trade_date < ex_date <= as_of_date` participate. Explain in code comments that adjustment applies only to rows strictly before the ex-date.
- [ ] Run focused tests, full CI-equivalent coverage, RuntimeWarning checks, and `git diff --check`.
- [ ] Copy this revised plan into the branch, finish the diagnostic contract tests, and open one combined PR referencing the immutable production classification. Stop for explicit merge approval.
- [ ] After approved merge, advance the clean runtime checkout to the verified merge commit and repeat root/cleanliness assertions.
- [ ] Rerun the full dry run into a new unique run directory. Require a non-empty final summary plus a new evidence-grade failure manifest; never reuse the interrupted run directory or any evidence cursor.

## Task 6: Classify residuals and prepare only supported repairs

**depends_on:** `[T5]`

Execute `2026-07-16-silver-full-universe-residual-resolution.md` R1–R8 for the
complete 593-symbol residual program. Apex promotion remains blocked until that
plan proves a zero-failure, full-universe Silver revision.

The post-fix residual inventory is 593 symbols: 518 unknown split basis, 61
dividend currency mismatches, and 14 invalid dividend magnitudes. Boundary
failures are zero. Use only this manifest for all subsequent repair decisions.

- [ ] Capture the immutable Argon watchlist/subscription symbol universe into `$CUTOVER/argon-universe.json` before repair or canary work.
- [ ] Create a fresh split-basis audit. Confirm IB historical service, not merely the TCP port, and generate resolver evidence into this run's new `$CUTOVER/ib-evidence/`. Never reuse evidence from another audit/root/hash.
- [ ] Re-audit with that evidence and require all resolver-backed replacements to have `status == resolved`, matching root, symbol, and source hash.
- [ ] Assign each residual exactly one supported disposition:

  - `resolved_split_or_invalid_ohlc`: existing audit/resolver plus `repair-split-basis` eligible replacement.
  - `incorrect_provider_action`: fetch a fresh complete provider action inventory, review its provenance, and apply only through `CorporateActionStore.reconcile(symbol, provider_events, fetched_at, full_reconcile=...)`; add unit and integration tests before production use.
  - `missing_boundary_bar`: use the canonical daily ingestion/fixed-date catch-up path, validate the row, and preserve atomic Bronze publication.
  - `split_adjusted_basis_no_repair`: retain unchanged, with INTC as the required no-repair fixture when evidence supports it.

- [ ] If current code cannot perform a required disposition, stop. Amend this plan with exact TDD/code/rollback steps, land that scoped PR with explicit merge approval, deploy it to the clean runtime checkout, and rerun classification. Do not improvise a Parquet mutation or claim uninterrupted completion.
- [ ] Require zero `unknown`/unsupported/unexplained residuals before requesting destructive approval.

## Task 7: Freeze writers, repair, publish, verify, and always restore writers

**depends_on:** `[T6]`

- [ ] Record the initially loaded state and plist path for each label in `$CUTOVER/launchd-before.txt`:

  ```text
  com.livewire.daily-update
  com.livewire.intraday-catchup
  com.livewire.daily-update-watchdog
  ```

- [ ] Define and test an operator restoration procedure before unloading. It must `plutil -lint` and `launchctl bootstrap gui/$(id -u) <plist>` only for labels recorded as loaded, then verify each with `launchctl print`. This procedure runs on success and on every abort after freeze.
- [ ] Unload each previously loaded label without `|| true`; verify it is absent. Verify no Livewire writer/repair/rebuild process remains.
- [ ] Recompute source hashes after freeze. If any differ from classification evidence, discard stale evidence and return to T6.
- [ ] Back up every file that an approved repair can modify and verify source/backup SHA-256 equality.
- [ ] Present exact dispositions, mutation count, backup paths, checksums, commands, and rollback boundaries. Stop for fresh explicit destructive approval.
- [ ] Apply each approved supported repair. Re-audit and run a full Silver dry run with a new evidence-grade failure report. Require exactly zero failures.
- [ ] Publish with `rebuild-silver --full`; require exit zero, zero apply failures, and atomic advancement of `silver/revisions/current.json`.
- [ ] Run an independent coverage verifier and save `$CUTOVER/silver-coverage.json`. For every production Bronze equity symbol it must verify:

  ```text
  daily and factor files exist and are readable
  schemas equal SilverClient.daily_schema and SilverClient.factor_schema
  row counts are nonzero
  daily dates are unique and strictly sorted
  stable symbol identity equals stable_symbol_id(symbol)
  factor intervals are ordered, non-overlapping, and cover every daily date exactly once
  daily and factor adjustment_revision each contain one value and agree
  current manifest paths exist and SHA-256 digests match the current revision manifest
  corporate_actions_as_of agrees with the rebuild/action-store snapshot
  raw and adjusted semantic canaries behave as expected
  ```

- [ ] Canary AAPL, NVDA, SPY, INTC, BCPC, `BCpC`, AAAP, and the captured Argon universe. Rerun a full dry run and require `rebuilt=0`, zero failures, and `unchanged` equal to the production Bronze universe count.
- [ ] Immediately run the pre-tested launchd restoration procedure, before Apex promotion. Confirm every originally loaded job is loaded exactly once and paths point to the clean merged runtime checkout. Record `$CUTOVER/launchd-after.txt`.
- [ ] If any step after freeze aborts, restore launchd first, then report the failure. Never leave restoration deferred to a later task.

## Task 8: Promote Apex and observe production

**depends_on:** `[T7]`

**Apex files:** `/Users/moremeds/apex-deploy/compose.yml`; clean Apex release checkout; `/Users/moremeds/projects/apex/SILVER_AWARE_RELEASE_HANDOVER.md`

- [ ] Require Apex to report `observed_revision == last_fully_applied_revision == current Livewire revision`, zero consecutive failures, and no last error while still configured/effective raw.
- [ ] Run the standalone adjusted canary against the captured Argon universe plus AAPL, NVDA, SPY, INTC, BCPC, `BCpC`, and AAAP. Run only one full-lake canary at a time.
- [ ] Record Compose checksum, exact image ID/digest/revision label, health JSON, mount read-only state, raw response canaries, Xenon health, and Argon consumer state.
- [ ] Change only `APEX_LIVEWIRE_PRICE_MODE: raw` to `adjusted`; retain the Silver root and both read-only data-lake mounts. Validate with `docker-compose config -q`, then recreate only `api` with `docker-compose up -d --no-build api`.
- [ ] Require Docker healthy, Apex `status=ok`, Postgres connected, configured/effective modes adjusted, current revision fully applied, no revision/checksum/continuity errors, and successful bars/indicators/subscription tests.
- [ ] Verify Xenon again after the Apex recreate and verify active Argon subscriptions complete an adjusted-data refresh.
- [ ] Observe at promotion time, 15 minutes, and 60 minutes, including at least one complete active Argon subscription cycle. Do not declare global stability before all four observation gates pass.
- [ ] On any failure, return the gate to raw, validate Compose, recreate only Apex API, and prove raw health/canaries. Do not roll back Bronze or Silver to repair an Apex serving failure.

## Task 9: Make the release and operating state durable

**depends_on:** `[T8]`

- [ ] With explicit user approval, publish the verified Apex commit through its tag-driven workflow. Verify tests, immutable GHCR tag, `linux/arm64`, digest, and `org.opencontainers.image.revision`.
- [ ] Replace any local Apex image tag with the verified immutable registry tag and repeat Task 8 health, Xenon, Argon, and adjusted canaries.
- [ ] Re-enable Watchtower only after proving its configured channel cannot downgrade Apex below the Silver-aware release.
- [ ] Update the Livewire/Apex handovers, runbook, `tasks/todo.md`, and project memory only for facts proven stable. Record commit/revision IDs, universe and coverage counts, evidence paths, image digest, promotion time, observation results, launchd state, and raw rollback.
- [ ] Final definition of done: zero Silver failures; complete schema/identity/interval/revision/checksum coverage; current revision fully observed; Apex globally adjusted; Argon and Xenon healthy; launchd restored exactly once; immutable Silver-aware Apex release deployed; raw rollback still mechanically available.

## Stop and rollback boundaries

- Before mutation: read-only artifacts may remain for audit; production data is unchanged.
- After an approved Bronze repair: use only that repair mechanism's verified backup/rollback and re-audit before publishing.
- After Silver publication: switch Apex to raw if Silver is suspect. Correct and republish Silver from canonical Bronze. A data rollback is allowed only from a checksum-verified, matched whole Silver artifact tree plus its marker; restoring `current.json` alone is prohibited.
- After Apex promotion: raw mode plus API-only recreate is the immediate rollback.
- At every abort after writer freeze: restore the jobs recorded as originally loaded before doing any other recovery work.
