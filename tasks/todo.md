# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Land PR #3 by updating `feat/postgres-analytical-layer` onto current `main`, preserving the five-script operator surface, verifying, and merging through the pull request path.

## Success Criteria
- PR #3 merges cleanly into `main`.
- Postgres analytical rebuild and smoke commands remain available through `scripts/livewire_store.py`.
- `scripts/` still contains only the five operator-facing files.
- Verification evidence is collected after conflict resolution and after merge.
- Local `main` is aligned to the remote merge commit.

## Dependency Graph
- T1 -> T2 -> T3 -> T4 -> T5

## Tasks
- [x] T1 Inspect PR #3 merge state and identify conflicts
  depends_on: []
- [x] T2 Resolve conflicts against current `main`
  depends_on: [T1]
- [x] T3 Run verification on the resolved PR branch
  depends_on: [T2]
- [ ] T4 Push the updated PR branch and merge PR #3
  depends_on: [T3]
- [ ] T5 Align local `main` and record final evidence
  depends_on: [T4]
