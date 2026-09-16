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
  UTC (2026-09-16) — a date-boundary flake in a file O1 does not touch. Left as-is for
  lead-owned O4 full-suite verification; not silently deselected for acceptance.
- First-SIGINT behavior of the outer wrapper is unchanged (lead's probe showed it failing
  under both dispatch variants before O1); not claimed fixed.
- Mac mini validation not performed — lead-owned, pending O1 acceptance.
