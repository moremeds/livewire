# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Consolidate the operator-facing script surface to five files while preserving current behavior and tests.

## Success Criteria
- `scripts/` contains exactly five files:
  - `livewire_ingest.py`
  - `livewire_quality.py`
  - `livewire_ops.py`
  - `livewire_store.py`
  - `setup_market_warehouse.sh`
- Removed script implementations are available as importable modules outside `scripts/`.
- Existing functionality remains reachable through subcommands.
- Internal subprocess references use the new entrypoints.
- README, CLAUDE, project memory, launchd templates, and hook docs point to the new commands.
- Python and Node tests pass with coverage gate evidence.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4
- T4 -> T5
- T5 -> T6
- T6 -> T7

## Tasks
- [ ] T1 Baseline and red tests
  depends_on: []
- [ ] T2 Move implementation modules out of `scripts/`
  depends_on: [T1]
- [ ] T3 Add five operator entrypoints
  depends_on: [T2]
- [ ] T4 Update internal subprocess references
  depends_on: [T3]
- [ ] T5 Update tests and docs
  depends_on: [T4]
- [ ] T6 Remove redundant scripts and verify count
  depends_on: [T5]
- [ ] T7 Full verification and milestone commit
  depends_on: [T6]
