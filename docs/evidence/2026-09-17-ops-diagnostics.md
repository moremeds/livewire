# Evidence — 2026-09-17 ops diagnostics (O1: run/attempt facts + runner consistency)

Plan: `docs/plans/2026-09-17-ops-diagnostics.md` (commit f08d9d4, `fix/ops-diagnostics`).
Executor: lw-devin-ops (SWE-2 Max). Execution base: 68f2d614edd14f3cfd16ad55ea47bf05e2094cef.
Commands run from the worktree root (`.worktrees/ops-diagnostics`) with the prepared
`.venv` (uv unusable in this sandbox: `~/.cache/uv` denied). Lead-side verification:
`o1-lead-tests.json` (231 tests, 9.46s, `-W error::RuntimeWarning`).

## O1 scope implemented

- `livewire_scripts/job_runner_common.py` — new shared body `run_in_own_process_group`
  (Popen + `start_new_session` + `process_group_guard`, `check=` support); identity helpers
  `executing_code_sha` / `pin_executing_sha` / `deployment_sha` / `hash_files`;
  `emit_process_attempt` writing `executions` rows with `receipt_json.schema_version=1`,
  `kind="process_attempt"`, `raw_exit_code`, `effective_exit_code`, `completion_reason`,
  `deployment_sha`; bounded `args_json` (24 argv + `truncated_args`); telemetry failures
  logged, never fatal.
- `livewire_scripts/sync_runner.py` — `_run_in_own_process_group` aliases the shared body;
  `run_phase` captures the raw returncode before SUMMARY_JSON override and emits one
  process-attempt receipt per phase (`exit` | `timeout` | `summary_override`, with a small
  `summary` excerpt on override); `lane_results.exit_code` stays effective; `main()` pins
  identity once and records `release_sha`/`presets_sha`/`registry_sha` on the run row.
- `livewire_scripts/run_daily_update_job.py` — same alias for the lane runner;
  `run_daily_update_attempt` gained `scope`/`attempt` and emits one receipt per real
  subprocess attempt (synthetic no-budget timeouts emit none); all lanes route through
  `_run_scheduled_lane`/`run_with_retries`; `_run_main` pins identity and records the
  run's configured input hashes.
- `livewire_scripts/run_intraday_catchup_job.py` — outer dispatch RETAINS `subprocess.run`
  (lead correction: a detached outer group SIGKILLs the intermediate before its guard can
  reap the leaf); `run_intraday_catchup` keeps its injected `runner` seam; `main()` mints
  `LW_RUN_ID` when absent, pins identity, and emits a wrapper-level process-attempt receipt.

## Identity and input-hash semantics

- `release_sha` on run rows is the resolved physical identity: a `releases/<40-hex-sha>`
  directory name, else `git rev-parse HEAD` at the executing root. `LW_RELEASE_SHA` is a
  cross-check only — disagreement warns and the physical root wins; with no verifiable
  physical identity it is never accepted on format alone (unverifiable claims are cleared
  by `pin_executing_sha`, not recorded).
- `deployment_sha` inside the receipt is the mutable `warehouse/current` selection —
  recorded as metadata, never used to label executing code.
- Daily `presets_sha` is the legacy shipped-bundle fingerprint (`presets/*.json`) PLUS any
  explicit `--preset` file the operator selected. The bundle stays in the hash because
  auxiliary lanes keep their configured inputs regardless of the filter; the hash is a
  configured-input fingerprint, not a claim of bar coverage. Any unreadable input → `None`
  (UNKNOWN), never an implied hash.
- Sync `presets_sha` covers the configured equity presets + vol presets; `registry_sha`
  covers `registry/gaps.json` in both runners. Missing inputs → `None`.

## Commands and results

| Command | Exit | Result |
|---|---|---|
| `.venv/bin/python -m pytest tests/test_job_runner_common.py tests/test_sync_runner.py tests/test_run_daily_update_job.py tests/test_run_intraday_catchup_job.py -q -W error::RuntimeWarning` | 0 | 192 passed in ~8.8s |
| `.venv/bin/python -m pytest tests/test_ledger.py tests/test_notify.py -q` | 0 | 39 passed in 0.68s |
| `.venv/bin/python -m pytest tests/test_nightly_digest.py tests/test_coverage_report.py tests/test_rebuild_silver.py tests/test_sync_corporate_actions.py tests/test_livewire_entrypoints.py tests/test_ingest_preflight_scope.py tests/test_constants.py -q` | 1 | 295 passed; 1 known-unrelated failure (see below) |
| `.venv/bin/python -m ruff check` + `ruff format` on the 8 changed files | 0 | clean |
| `probe-nested-interrupt.py` (lead artifact, re-run post-fix) | 0 | both dispatch modes `descendant_lock_released: true`, `outer_exit: -15` — nested-SIGTERM regression resolved by retaining `subprocess.run` |

## Deviations / known limits

- `tests/test_nightly_digest.py::TestWaitForCoverageFact::test_digest_waits_for_todays_coverage_fact`
  fails in this environment: `date.today()` in local HKT (2026-09-17) vs `measured_at` in
  UTC (2026-09-16). Lead reproduced the failure and verified both the test and production
  module were unchanged from the execution base. The test now uses one fixed UTC clock
  for its date and emitted facts; production behavior is unchanged. The full digest
  suite passes without deselection as part of the 249-test lead check below.
- First-SIGINT behavior of the outer wrapper is unchanged (lead's probe showed it failing
  under both dispatch variants before O1); not claimed fixed.
- Mac mini validation not performed — lead-owned, pending O2–O4.

## O1 lead acceptance

Accepted worker commit `89e5bde6095c7a48d4180e31d98c9b1cde1b8ed3` after inspecting
the complete runtime/test diff and callers. A proposed detached outer process group
was rejected using a real child-lock SIGTERM reproduction; the existing outer
dispatch remains. An unverified environment-only SHA fallback was also removed.
Post-fix controlled checks passed, and the timeout regression requires evidence
that the descendant actually acquired its lock.

Lead verification: `uv run pytest tests/test_job_runner_common.py tests/test_sync_runner.py tests/test_run_daily_update_job.py tests/test_run_intraday_catchup_job.py tests/test_ledger.py tests/test_notify.py tests/test_nightly_digest.py -q --no-cov -W error::RuntimeWarning`
— **249 passed in 9.74s**, exit 0, with the UTC fixture correction. This is the
bounded O1 gate; full configured coverage/CI and Mini acceptance remain pending.

Worker reverse messaging is unavailable inside its sandbox. The lead reads the
terminal and committed evidence instead; no Herdr configuration access is needed.

## O2 — Silver/catalog publication receipts and failure evidence (worker)

Implemented in this worktree on top of `c6072d7`:

- `livewire_scripts/rebuild_silver.py`
  - `_emit_symbol_evidence`: batches existing `_failure`/`window_regressions` facts
    into `evidence` rows, `kind="silver_symbol_failure"`, `schema_version=1`.
    `stage` is `staging` or `withheld_window_regression`. Payload carries symbol,
    original error, input path+sha, input date bounds (context only — never exact
    failing sessions), reference-only `baseline_artifacts` (path+sha from the
    pre-attempt manifest index), resolved `data_lake_root`/`silver_root`, and
    `baseline_revision` so a fault root-matches even when the publication
    receipt is absent. Whole preparation+emit is inside one guard; failures go
    to stderr and never fail the run.
  - `_emit_silver_publication`: one `executions` row, `script="rebuild-silver"`,
    receipt `schema_version=1`, `kind="silver_publication"`, emitted outside the
    publisher transaction. `result` is `committed` (revision advanced via the
    actual `transaction.commit()` return), `noop` (validated attempt, manifest
    unchanged — names the preexisting generation_id), or `attempt_only`
    (exception may have landed either side of the pointer swap, or no committed
    revision exists at all — `attempt_reason="no_committed_revision"`,
    `published_revision=null`, no manifest ref; the manifest stays the arbiter,
    the receipt never claims what shipped or did not).
    `manifest_sha256` is hashed from the immutable `revisions/revision=N.json`
    after commit — `current` is never reread for the hash. Payload lists
    selected/staged/validated/failed/withheld/omitted symbol scopes and resolved
    roots; the manifest is referenced, never copied.
    `validated_symbols` = publishable staged symbols only — withheld/failed are
    never listed. `--allow-window-regression` regressions are excluded from the
    withheld scope and emit no failure evidence.
  - `attempt_started` precedes all work; `run_id` = `LW_RUN_ID` or a fresh
    `silver-*` id. `--dry-run` emits no progress beats, no evidence, no receipt
    (early return before the transaction); `emit_progress` is gated on dry-run.
- `clients/duckdb_catalog.py`
  - `_emit_catalog_publication`: one `executions` row, `script="duckdb-build"`,
    `schema_version=1`, `kind="catalog_publication"`, after the local atomic
    commit. `result` is `committed` or `copy_failed`; `local` carries path+sha256
    (hashed once under the publish lock); `lake` carries the target path,
    `expected_sha256` = the local hash (the shared target is never re-hashed —
    another writer could swap it first), `result` copied/failed, and the copy
    error. External-copy failure still raises and fails the lane after the
    receipt lands; receipt/telemetry failure prints a warning and cannot roll
    back committed bytes or skip the copy.
- `livewire_scripts/run_daily_update_job.py`: dry-run Silver summaries no longer
  land as `silver_failed`/`silver_window_regressions` measurements — the parsed
  preview is not a fact about the lake.

### Commands and results

| Command | Exit | Result |
|---|---|---|
| `.venv/bin/python -m pytest tests/test_rebuild_silver.py tests/test_silver_atomic_publication.py tests/test_duckdb_catalog.py tests/test_ledger.py tests/test_run_daily_update_job.py -q -W error::RuntimeWarning` | 0 | 244 passed in 5.96s |
| `.venv/bin/ruff check` + `ruff format` on the 3 runtime files + 4 test files | 0 | clean |

New coverage: committed/noop receipts incl. noop generation naming; missing-input
first run → `attempt_only`/`no_committed_revision` with no manifest ref; partial
staging-failure evidence with root+baseline context; withheld regression as
evidence (never validated); `--allow-window-regression` exclusion; dry-run
ledger silence (executions/evidence/measurements all empty); manifest-hash
failure mid-receipt still commits revision 1 (bytes intact, warning, no row);
concurrent-commit stale attempt → `attempt_only`/`RuntimeError` while the
manifest shows the concurrent revision; catalog committed and `copy_failed`
receipts (lane still raises); catalog receipt emit failure preserves local +
lake bytes; daily dry-run suppresses Silver measurements while a real run lands
them.

### O3 routing fields

- `executions`: `script="rebuild-silver"` → `receipt_json` `schema_version=1`,
  `kind="silver_publication"`, `result∈{committed,noop,attempt_only}`,
  `attempt_reason`, `baseline_revision`, `published_revision`,
  `manifest_ref` (`revisions/revision=N.json`), `manifest_sha256`,
  `generation_id`, `data_lake_root`, `silver_root`, scope lists
  (`selected_symbols`, `staged_symbols`, `validated_symbols`,
  `failed_symbols`, `withheld_symbols`, `omitted_symbols`), `deployment_sha`.
  `script="duckdb-build"` → `kind="catalog_publication"`,
  `result∈{committed,copy_failed}`, `local{path,sha256}`,
  `lake{path,expected_sha256,result,error}`, `coverage_rows`,
  `data_lake_root`, `deployment_sha`.
- `evidence`: `kind="silver_symbol_failure"`, `subject=<symbol>`,
  `payload_json` `schema_version=1`, `stage∈{staging,withheld_window_regression}`,
  `symbol`, `error*`/`previous_start`,`new_start`,`reason`, `input{path,sha256}`,
  `input_date_bounds{earliest,latest}` (context only), `baseline_artifacts[]`
  (ref-only), `data_lake_root`, `silver_root`, `baseline_revision`.
- Missing/unknown-version/malformed rows → UNKNOWN linkage; consumers filter
  script/kind/version before decoding. `attempt_only` never asserts what the
  manifest did — resolve against `revisions/current.json` directly.

### Deviations

- `attempt=1` on both receipts: neither publisher retries within a run; the
  row's `run_id`+`started` already distinguish invocations.
- Executions rows mint `silver-*`/`duckdb-build-*` run ids when `LW_RUN_ID` is
  unset (standalone CLI use); under the daily runner they attach to the parent
  run id.
- Broad CI / Mac mini validation remain pending O4/O5, unchanged.

### Lead O2 acceptance

Accepted `1612fd2cf422f59520bc517f0217a2c58f9e077f` after inspecting the runtime
diff and tests. Lead repeated the five-file suite with `uv run pytest ... -q
--no-cov -W error::RuntimeWarning`: **244 passed in 6.11s**, exit 0. A separate
owned-empty-lake probe first reproduced a false revision-0 no-op receipt; after
correction it preserves exit 1 with `attempt_only`, no publication reference and
no validated symbols. Telemetry preparation is guarded, and catalog receipts
identify the protected source hash instead of re-hashing the mutable lake target.

Simplification: the added concurrent-publication test duplicated the existing
fault setup. Lead moved its receipt assertions into that existing test and removed
the duplicate setup. Existing publication/data-policy behavior remains unchanged.

Evidence limits for O3: staging failures carry input path/hash/date bounds;
withheld regressions carry previous/new window starts, reason and baseline artifact
references, not an exact failing session or an input checksum. Exceptions before
the publication transaction may have only the outer process-attempt record (or no
record for a standalone invocation); absent linkage must remain UNKNOWN. These
receipts are not a transaction with the lake and do not prove consumer reads.

## O3 — status/path diagnostics and capacity planner

Files: `livewire_scripts/status.py`, `livewire_scripts/paths.py`,
`livewire_scripts/flatfile_planner.py` + matching tests. No notify/digest edits
— both render `collect()` output unchanged; `Section.notification_key` is the
existing shared contract.

### Capacity paths

- `paths.resolve_capacity_target(path, allow_missing)` resolves symlinks
  component-wise and creates nothing. `allow_missing=False` (status): a missing
  or dangling destination → `None`, never the ancestor's free space.
  `allow_missing=True` (planner): a missing leaf resolves to its nearest
  existing resolved ancestor on the intended filesystem; a dangling link in the
  chain is still `None` — falling past it would measure the internal disk.
- `flatfile_planner.capacity_path` targets the actual writer destination,
  `<warehouse>/data-lake/raw/massive` (both raw stores root there; they do not
  honor `MDW_DATA_LAKE`). `discover_plan` raises `RuntimeError` on an
  unreachable destination instead of measuring a substitute.
- Regressions: internal `raw/` + external `raw/massive` symlink measures the
  child target; an unrelated `MDW_DATA_LAKE` override does not redirect it;
  dangling link → planner failure; nothing is created.

### Disk section

- `_disk_targets` lists every configured destination resolved the way writers
  resolve it: lake, bronze, lake catalog, Silver (`MDW_SILVER_DIR`), logs,
  cursors, `LW_LEDGER_ROOT`, local catalog parent, temp dir, warehouse, and the
  raw writer's `raw/massive`. `collect` passes `warehouse_dir()` — not
  `log_dir.parent`, which follows `MDW_LOG_DIR` elsewhere.
- Resolution, `disk_usage` and `st_dev` lookup are inside the per-target
  `OSError` guard: one vanished volume is UNKNOWN for that label only. Dedup is
  on `st_dev` (physical filesystem), not the usage triple. Missing/dangling →
  `UNKNOWN` floor; thresholds unchanged.

### Silver symbol faults section

- Projects `evidence` rows (`kind='silver_symbol_failure'`) + `executions`
  `rebuild-silver` receipts for THESE resolved `data_lake_root`/`silver_root`
  only. Malformed/foreign/unknown-version rows are counted, never fatal, never
  erased (`undecodable`, `for other roots`).
- Issues group by symbol + stage + classification + known scope; staging scope
  is `UNKNOWN` (failing session was never recorded — input date bounds render
  as context, not identity). Withheld regressions keep
  `previous_start->new_start` + reason. Detail lines: bounded error/reason,
  full input path + hash, evidence ref, run id (context), baseline revision and
  artifact references (explicitly not proof the bytes exist), evidence-fact and
  receipt-failure counts, last observation.
- Every failed/withheld receipt contributes `receipt_failures += 1` and
  advances `last_seen` at its own `ended` (or `started`) — an inherited
  `LW_RUN_ID` never dedupes observations and receipt-only failures reopen a
  closed issue. Counts are honest: evidence facts and receipt-listed failures
  are separate; no distinct-attempt claim.
- Closure requires a later `committed`/`noop` receipt on the same roots naming
  the symbol in `validated_symbols` (never `failed`/`withheld`), with
  `started >= last_seen`, AND the symbol present in the receipt's VERIFIED
  immutable manifest: strict positive-int `published_revision`, rebuilt
  `revisions/revision=N.json` filename, contained resolved path, SHA-256,
  `schema_version=1`, matching `revision` and nonempty matching
  `generation_id`. `attempt_only` never closes. Manifest verification is
  cached per unique (result, revision, ref, sha, generation) within one
  projection call — a local dict, no persistent cache — so N noop receipts
  read the manifest once, and a contradictory revision/result cannot inherit a
  verified set.
- Legacy unscoped positives (`silver_failed>0`, `silver_window_regressions>0`)
  stay visible as UNKNOWN forever — no later run, full or targeted, can
  establish which symbols the counter meant. Verdict: open issues → WARN; any
  legacy/malformed/undecodable uncertainty or absence of a verified
  committed/noop receipt → UNKNOWN; else OK. `notification_key` carries issue
  identity + legacy name only — timestamps, run ids and malformed counts do
  not re-page an unchanged fault.

### Silver publication / catalog receipt lines

- Publication section resolves the pointer via `MDW_SILVER_DIR`-aware
  `_configured_silver_path`, reports the committed manifest as its own fact,
  the latest terminal lane row, the latest decoded receipt (completion-ordered
  `coalesce(ended, started)` — overlapping invocations sort by finish), and
  verified-receipt linkage separately. A newer same-root receipt than the lane
  drives the attempt verdict: `attempt_only` → UNKNOWN, failed/withheld scope
  → BAD. Undecodable receipt rows floor a healthy lane at UNKNOWN
  (`receipts-undecodable` key) without demoting BAD. Headlines say "terminal
  lane" — lane facts and receipt facts are never conflated.
- `_latest_catalog_receipt` attaches the newest `duckdb-build` receipt only
  when its resolved lake root and both destination paths match THIS
  deployment's configured paths; undecodable or foreign newest row → UNKNOWN
  linkage line, not a scan to an older success. A same-root `copy_failed`
  receipt grades the section BAD with a copy-retry fix; the receipt line
  reports local commit + copy outcome, never Apex consumption.

### Checks run

- `.venv/bin/python -m pytest tests/test_status.py tests/test_paths.py
  tests/test_flatfile_planner.py -q -W error::RuntimeWarning` →
  **168 passed**
- `ruff check` + `ruff format` on the six owned files → clean.

### Deviations

- `staging-tmp` uses `tempfile.gettempdir()` (honors `TMPDIR`); the local
  catalog target is the configured database's parent dir.
- `_observe_receipt_fault` merges a receipt-only failure into the latest
  same-symbol+stage issue (conservative coarseness — no execution-id
  framework); distinct classifications still key separately.
- No production/Mac mini validation claimed; O4/O5 remain lead-owned.
