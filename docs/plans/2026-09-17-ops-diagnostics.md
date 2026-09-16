# Existing-ledger diagnostics and runner consistency

User-approved scope: implement items 1 and 2 of the September 16 rewrite
proposal, have Cursor Grok review this plan, have Devin implement, and run the
candidate on the actual Mac mini. Simplicity, health and stability are acceptance
criteria. Storage layout changes are explicitly out of scope.

Baseline: execution branch `fix/ops-diagnostics`, base
`68f2d614edd14f3cfd16ad55ea47bf05e2094cef`. At the initial September 17 check the
Mini checkout/current were `a9965e016508c697ef62a25566ebf40dc0eb7150`; no matching
Livewire/backfill process was returned. Refresh before any remote run.

## Boundaries

- Keep six ledger schemas and existing `receipt_json`/`payload_json` extension
  points. No database, daemon, queue, plugin system, job DSL or new runtime module.
- Reuse `job_runner_common.py`, `ledger.py`, `paths.py`, existing publication and
  status functions. Add a small helper only for demonstrated callers; remove the
  duplicate body it replaces. Keep job-specific phase order, budgets, retries,
  fallback and failure dependencies explicit.
- No bar schema/layout, Silver manifest schema, Apex API, provider policy, trading
  calendar, repair algorithm, launchd schedule, data retention or dependency changes.
- Preserve unrelated checkout changes. Implementation stays in this worktree.
  Worker does not push, merge, deploy, contact providers, send mail or access the
  production lake. Lead owns delivery and authorized Mini validation.
- Existing data faults must remain visible; do not lower thresholds or make UNKNOWN
  green. A passing command is not proof that all lake data is healthy.

## Existing evidence and changes

`sync_runner.run_phase` replaces a nonzero process return code with zero after a
successful SUMMARY_JSON, then records the replacement. Daily and intraday have
different process wrappers and run-identity emitters. Intraday reads an often
unset LW_RELEASE_SHA and records null preset/registry hashes. Daily reads the
mutable current symlink rather than the executing release directory.

`rebuild_silver._failure` already captures symbol, input path/hash, date range,
error and action identities; the optional failure JSON is not linked to its run
in the shared ledger. `status._silver_publication_section` explicitly cannot link
an attempt to the current manifest. Its generic warning text does not use facts
already available. Watchdog/digest already call the shared collector.

`flatfile_planner.capacity_path` checks raw's parent filesystem instead of the
actual raw/massive subtree; status checks root/warehouse, missing child symlinks.
These are narrow diagnostic corrections, not storage migration.

## Tasks and dependencies

Graph: `O0 -> O1 -> O2 -> O3 -> O4 -> O5`.

### O0 — baseline and Grok plan review
depends_on: []

Lead records source/dirty state, reads relevant callers/tests and runtime baseline.
Grok reviews this exact plan against source, especially unnecessary complexity,
publication/ledger failure ordering, compatibility and meaningful Mini validation.
Resolve material findings before implementation. No code implementation until
this gate is accepted by the lead.

### O1 — accurate run/attempt facts; reuse execution mechanics
depends_on: [O0]

Devin owns the implementation in `job_runner_common.py`, `run_daily_update_job.py`,
`sync_runner.py`, `run_intraday_catchup_job.py`, existing ledger helpers and tests.

1. Capture executing code identity once from the physically resolved immutable
   release/repo root; validate any supplied identity, and leave UNKNOWN when it
   cannot be established. Do not label mutable current or origin/main as the
   process SHA. Populate existing daily/intraday run fields consistently, including
   actual selected preset/registry hashes where available. Preserve manual-entry
   context and inherited LW_RUN_ID. Record deployment selection separately only
   where needed; no new schema column.
2. Preserve the raw subprocess exit code in durable attempt evidence. Preserve the
   existing effective completion decision for callers while making its distinction
   explicit (raw process code, effective result, reason/summary). Reuse executions
   receipts; do not change historical lane_results interpretation silently.
   Specifically, keep `run_phase` return value and `lane_results.exit_code/outcome`
   as the existing effective result. Store `raw_exit_code`, `effective_exit_code`
   and `completion_reason` in `executions.receipt_json` with
   `schema_version=1`, `kind=process_attempt`; use the existing job/phase name as
   script identity. Do not change daily's policy to match sync's override policy.
3. Consolidate the duplicated subprocess/process-group body in the existing common
   module. Preserve timeout group cleanup, cancellation, unbuffered output, retries,
   IB 86 versus timeout 124, budget accounting and independent-lane continuation.
   Do not turn jobs into declarative tables or consolidate different policies just
   because their functions look similar.
4. Do not expand into a process-ownership subsystem. `_abandon_stale` behavior must
   remain accurately described as inferred closure; do not add PID-only liveness
   or present ABANDONED as proven death. If preserving it makes the new diagnostic
   contract impossible, report a narrow blocking finding before changing policy.

Acceptance: original code survives a SUMMARY_JSON override; 86/124 and unexpected
exceptions preserve caller behavior; both execution paths clean descendants; a
current symlink change cannot relabel an existing run; old ledger rows still read.

### O2 — publication and failure evidence in the existing ledger
depends_on: [O1]

Expected files: `rebuild_silver.py`, `duckdb_catalog.py`, `ledger.py` only if a
shared receipt helper is actually needed, plus their existing tests.

1. Batch existing structured Silver failure/window-regression facts into evidence
   rows, including symbol, known session/date scope, stage, original error, source
   checksum/evidence, and baseline committed artifact references when available.
   Reuse the existing `_failure` data; do not invent a second detector or store.
   Never infer an exact failing session from the whole input range; label unknowns.
2. Emit an execution receipt tied to run/attempt after the actual Silver commit
   (or no-op), containing before/after revision, generation/reference hash, result
   and failed/withheld scope. Failed/precommit attempts must not claim publication.
   Keep payload bounded: reference the manifest rather than copying every artifact.
   Dry-run must not write production ledger or publication receipts.
3. Record catalog publication destination/hash/results after each actual publication,
   distinguishing local success from external-copy failure. Do not claim a joint
   transaction or fail a valid artifact merely because telemetry failed.
4. Artifact publication and ledger emission are separate operations. If a receipt
   is missing, report UNKNOWN linkage; preserve the committed artifact and stderr.
   Do not recursively emit failures into a broken ledger or retry data publication
   solely to manufacture telemetry.

Pin receipt routing: executions `script='rebuild-silver'`, JSON
`schema_version=1, kind='silver_publication'`; catalog executions
`script='duckdb-build'`, JSON `schema_version=1, kind='catalog_publication'`.
Use evidence `kind='silver_symbol_failure'` with a versioned JSON envelope around
the existing failure-output v2 fields; distinguish staging failure and withheld
window regression explicitly. Consumers filter script/kind/version before decoding;
unknown versions, malformed rows and old absent receipts produce UNKNOWN linkage.

Symbol evidence can follow staging; a publication receipt is emitted outside the
publisher transaction only after commit or an explicitly completed no-op. Record
precommit/concurrent-baseline rejection as attempt-only evidence, never a successful
publication. Keep the existing silver_failed/window_regressions measurements for
legacy readers. Do not remove compatibility summary output in this task.

Receipts include the actual selected scope and explicitly validated successful
symbols, failed/withheld scope, and resolved lake/Silver roots. Listing selected
symbol names is acceptable; copying the full artifact manifest is not. Failure
matching uses root + symbol + stage/error classification, with known session scope
only; input date bounds are context, not invented failing sessions. Later receipts
may close only explicitly successful symbols they actually validated. An explicit
validated no-op may resolve its scoped prior failure; a bare zero exit/no-op flag
cannot. Historical failures without this evidence remain visible/UNKNOWN.

Acceptance: success/no-op/partial failure/precommit error/receipt-write failure
and stale concurrent Silver baseline are distinct. Existing immutable reader
contracts, monotonic revisions, carry-forward hashes and quarantine rules stay
unchanged. Existing optional failure-output CLI remains compatible.

### O3 — one diagnostic projection and correct physical disk targets
depends_on: [O2]

Expected files: existing `status.py`, `paths.py`, `flatfile_planner.py`, and relevant
tests; `notify.py`/`nightly_digest.py` only if their rendering contract requires it.

1. Enhance the existing collector/sections using the linked facts. A warning must
   state affected symbols/sessions (or bounded range/UNKNOWN), stage, evidence,
   available last-valid artifact, actual automatic attempts, next action and closure
   condition. Group recurring facts without losing old unresolved scope. Detailed
   output may show a bounded excerpt plus total count and exact ledger query.
2. Keep current publication, last attempt, and consumer observation separate.
   Receipt matching requires the relevant root and manifest identity, not revision
   alone. A no-op or process exit 0 cannot clear unresolved symbol failures.
   Targeted success cannot clear failures for untouched symbols. Retained metadata
   is not proof that old bytes exist or that Apex consumed them.
3. Status/watchdog/digest render the same section facts. Preserve notification
   dedup: attempt timestamps alone must not create new pages. Do not send mail in
   tests or Mini probes. No new network polling or bar scans inside status.
4. Fix disk resolution through actual dataset paths (raw/massive, Bronze, Silver,
   catalog and internal metadata/staging as applicable), deduplicated by physical
   filesystem. Resolve symlinks and missing-path ancestors; never create lake paths
   during a read-only status check. Reuse a helper in paths.py for planner/status.
   For status, a missing destination is UNKNOWN, not proof that its parent's free
   space belongs to the expected mounted lake. The planner may use a resolved
   existing ancestor for a genuinely new directory on the intended filesystem,
   but must not silently fall back through a dangling configured symlink onto the
   internal disk. Preserve first-run planning without claiming an absent mount is
   healthy. Catalog-copy failure still fails its lane as today; telemetry failure
   is a different failure and cannot invalidate already-published bytes.
5. Do not add caching/database/query-session machinery unless required to prevent
   a measured regression from these changes. No arbitrary recent-date truncation
   that hides old unresolved findings; no full bar-directory scan.

Acceptance: common render source agrees on identical as-of facts; legacy receipts
remain UNKNOWN rather than corrupt; malformed/missing receipt is safely visible;
two roots with same revision cannot cross-link; targeted recovery retains other
issues; internal parent/external child capacity uses the real destination.

### O4 — lead review, simplification and configured checks
depends_on: [O3]

Inspect the cumulative diff and all changed callers. Challenge each new helper:
if removing it still meets acceptance, remove it. No new service/dependency/runtime
module and no duplicate status store. Grok inspects the bounded final diff for
complexity/contract regressions; lead owns acceptance, not worker consensus.

Run the narrow regression suites first, then the configured CI command:
`uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning`, configured
Ruff checks, and `npm run test:alerts` when notification surfaces are touched.
Do not weaken coverage or substitute the wrapper-only coverage command.
Commit explicit task files; deliver through a branch PR, never push to main.

### O5 — actual Mac mini acceptance
depends_on: [O4]

Lead refreshes current SHA, active jobs, volume targets, production status,
latest Silver pointer and Apex health/read before testing. Never interrupt a
running job/backfill or restart IB Gateway.

Run the exact candidate code on the Mini, not merely local tests or the old
release. Use a uniquely owned isolated warehouse/lake/log/cursor/ledger root for
writes, populated with a bounded copy of real Bronze/action inputs and selected
retained artifacts. Pin source hashes before/after copying. Avoid provider calls
and email. Exercise a targeted real-input Silver publication/no-op and its receipt,
catalog destination evidence, status rendering and the shared execution path.
Use the existing tests on that host for controlled failure/cancellation cases;
label those cases fault injection, not observed market faults.

Pin every write surface explicitly: `MDW_WAREHOUSE_DIR`, `MDW_DATA_LAKE`,
`MDW_SILVER_DIR`, `LW_LEDGER_ROOT`, `MDW_LOG_DIR`, `MDW_CURSOR_DIR`,
`MDW_DUCKDB_PATH` and temporary scratch all point into the uniquely owned probe
tree. Verify resolved paths first; no symlink to production. Production status is
a separate emit-free process using the real roots, not these probe settings.

Separately run candidate read-only status against actual production facts, compare
verdicts/timing with baseline, and confirm live Apex remains healthy with a scoped
real read. Missing historical receipts are expected UNKNOWN, not fabricated
backfill. Capture commands, candidate SHA, roots, results and remaining faults.

Candidate-on-Mini proof, merged code, promoted release and a full scheduled cycle
are separate states. This task authorizes implementation and real Mini runs; it
does not silently authorize a main merge or production promotion. Prepare the
reviewable PR and real-run evidence first. If promotion is needed to complete the
user's intended production acceptance, present that exact release as the final
approval step under the repository's explicit merge/promote rule.

## Execution ownership

Lead: this plan, tasks/todo.md, review decisions, PR/delivery, Mini actions and
evidence capture. Grok: read-only plan/diff review, exact model pinned. Devin:
scoped worktree source/test/docs implementation, no further agents; report actual
loaded rules, cwd and model. No Git restore/reset/stash of unrelated work, no
history rewriting. Worker may commit cohesive tasks, without attribution trailers;
no push/PR/merge/deploy. Scope expansions return to lead before edits.

Milestones are complete only with evidence. Existing lake correctness problems,
historical receipts not present, cold whole-universe performance and hardware
failure recovery must not be reported as solved by this change.
