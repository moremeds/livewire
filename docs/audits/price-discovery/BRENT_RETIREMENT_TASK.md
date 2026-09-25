# Task 4 — retire BZ from active ingestion, onboard COIL configuration

User decision, 2026-09-23: "BZ 可以retire coil更好". Implement the bounded source/config change in this worktree; no deployment or lake mutation implied. BZ retirement is an explicit product decision, not a claim that IB globally delisted BZ.

Worker: gold-coverage-scout (SWE-2 Max), now implementation mode. Read AGENTS.md, CLAUDE.md, README.md, .codex/project-memory.md and relevant lessons before edits; record HEAD and complete dirty state. Reuse existing futures ingestion/retirement mechanisms. Add this task to tasks/todo.md with dependencies as required. No further delegation.

## Outcome

1. Remove BZ from maintained active presets; ensure ordinary nightly discovery of existing bronze directories cannot continue querying retired BZ. Trace every relevant scheduler/daily/historical caller first. Prefer the smallest existing filtering mechanism; no generic retirement framework for one root. Log/count excluded retired contracts using existing conventions.
2. Configure COIL -> IPE in the actual contract construction path, and replace active Brent selections with verified COIL delivery months from saved IB contract evidence. Do not guess months or confuse expiration date with delivery month. Use bounded read-only qualification if saved evidence is insufficient.
3. Preserve every BZ file and historical read path. Never rename BZ rows/files to COIL, rewrite identifiers, concatenate histories, delete/archive/move production files or imply the two are the same contract. COIL is a separate series requiring future controlled seeding.
4. Test that active daily work skips existing BZ directories, GC/CL/NG continue unchanged, and COIL is constructed with IPE and correct delivery month. Cover explicit BZ request behavior consistently and document it. Use real saved contract metadata if fixtures needed; no invented observed market values.
5. Update CHANGELOG, minimal user-facing configuration notes and a Task 4 report. State clearly that configuration alone does not seed COIL lake files; distinguish code-ready from deployed/ingesting. Include an exact reviewed future seed command only if CLI/source verifies it, but do not execute it.

Own relevant futures preset files, minimal existing ingestion-selection/contract-construction code, their existing tests, CHANGELOG and tasks/todo plus this task's evidence/report. Preserve all pre-existing price-discovery edits. Do not edit this lead-owned assignment. No new dependencies, schema/API changes, other commodity onboarding, general roll engine or unrelated refactors. Never edit private memory files; report needed durable-memory changes to lead instead.

Run narrow relevant tests first, then required Livewire CI-equivalent coverage check per AGENTS; record actual commands/results. No live provider calls from tests. Allowed external effects remain bounded read-only IB verification through the established client if necessary; no production writes, ingestion, backfill, orders, subscriptions, service/Gateway restart, .env or credential edits.

Rule 5: no staging, commit, push, PR, merge or deploy; return `commit none`.
Rule 6: stop after Task 4 and return `herd-report gold-coverage-scout task 4: commit none, evidence docs/audits/price-discovery/BRENT_RETIREMENT.md, deviations: <text|none>`. Save complete evidence and terminal summary; reverse IPC is unavailable. Lead owns integration/review/acceptance.
