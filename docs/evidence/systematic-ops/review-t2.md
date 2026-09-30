# T2 independent code review

Verdict: **ACCEPT for the bounded conservative-retention milestone. No blocking findings.** This is not automatic active-job coordination, maintenance authorization or production rollout acceptance.

Reviewer requested/configured: GPT-6 Astra, canonical Astra; observed backend identity independently unverified. Author configured: GPT-6 Sol high. Native independent review, not a tribunal. Worktree `.worktrees/fix-systematic-ops`, HEAD `34a0e9f` (T3 committed), deployed/source baseline `956674d`.

Reviewed actual changes in `livewire_scripts/release.py`, `livewire_scripts/housekeeping.py`, their two test files, README, `docs/runbook.md`, `.codex/project-memory.md`, and untracked `docs/evidence/systematic-ops/t2.md`. Lead-owned untracked plan files were not part of this code diff.

## Deletion-path and contract review

- Searched all Python runtime callers in clients/scripts/livewire_scripts. The only release-retention deletion callers are promotion, housekeeping's imported alias, and explicit release GC. Promotion now calls default-preview `prune`; housekeeping passes `dry_run=True` even under `--apply`; explicit GC passes destructive mode only for `--apply`. No other runtime prune caller bypasses the gate.
- `prune()` defaults to preview and rejects `dry_run=False` without `maintenance_window=True` before selecting/deleting candidates (`release.py:203-216`). CLI GC requires both `--apply` and `--maintenance-window` to reach deletion. Existing `--keep` and current-release exclusion remain intact; existing explicit retention selection and failed-delete reporting are tested in authorized temporary fixtures.
- Promotion still flips to the selected release, but retention no longer deletes older releases. The remaining `_discard(staging)` in the unchanged build path cleans a `.building` target, not an old served release; this patch does not claim to redesign concurrent release building. Housekeeping's other existing permitted cleanup behavior remains unchanged.
- The implementation intentionally avoids process-snapshot TOCTOU by performing no automatic release deletion at all. A test directory named old-active does not pretend to simulate OS liveness; it proves a deletion candidate survives housekeeping apply. No lease, process scanner or launcher protocol is introduced.
- The maintenance switch is plainly an **operator assertion**. Code, CLI help, runbook and evidence state that it neither proves quiescence nor prevents manual launches. Runbook explicitly requires pausing new launchers and waiting for old-release jobs and children to exit, and separately calls out first-rollout drainage of old-code housekeeping tails. Thus docs do not falsely claim that the patch protects an already-running legacy pruner.
- README correctly distinguishes release-promote/universe-refresh checkout launchers from the release-based jobs. Project memory records the changed durable policy. Capacity accumulation and the still-unknown historical TLS cause remain disclosed in worker evidence.

## Independent verification

Executed:

`rtk proxy .venv/bin/python -B -m pytest tests/test_release.py tests/test_housekeeping.py -q -p no:cacheprovider`

Result: **88 passed in 0.22s**, exit 0. `git diff --check` passed. The tests preserve destructive retention/current-protection assertions behind an explicit maintenance gate, add gate-refusal/default-preview assertions, and change promotion/housekeeping expectations to the newly approved no-deletion contract rather than deleting their checks. CLI tests exercise preview, refused apply and explicitly gated apply. Worker evidence separately reports red-before-green tests and Ruff checks; no independent full-suite or CI result is asserted here.

Reviewed SHA-256:

- release.py: `38a729fd9134444949ce63405d0e0f9a92e2786f3d8f8b05ddfe570540089aad`
- housekeeping.py: `723901759e9650c9bfac45bb779d13d6831d20a8c6048adabde6136b09f2567f`
- test_release.py: `9809790323c8aaa43079e9f495fe8b20dbebb1e4e55231effef4bca1684205c3`
- test_housekeeping.py: `7baa7457d99286838ea7a41c11ccd7e6308631663a2a6fce5e0f43593e370fa5`

## Remaining gates and limits

Full candidate tests/CI remain required. Deployment must update the actual promoter checkout and drain legacy code that could still prune; new files do not retrofit old running processes. Explicit GC is unsafe if an operator falsely asserts the maintenance window, so no production deletion is authorized by this review. Obtain the required chosen confirmation phrase and retain exclusive quiescence for the entire deletion window. Conservatively retaining releases may increase internal disk use; inventory capacity before later bounded writes and do not compensate with unreviewed cleanup. Historical CA-path causation remains UNKNOWN.

herd-report independent_review T2: commit none, evidence docs/evidence/systematic-ops/review-t2.md, deviations: accepted conservative no-automatic-deletion design in place of an active-job lease/coordination mechanism, as approved by lead. No code edits, nested agents, production queries or mutations.

