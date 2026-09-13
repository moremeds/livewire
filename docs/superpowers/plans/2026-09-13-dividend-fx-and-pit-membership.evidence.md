# Evidence — dividend-fx-pit

## Task 0

Worktree + plan/spec copy.

```
$ git fetch origin && git rev-parse origin/main
6a680c7e67fd977ee1517cfdcfb67ad018185e6b   # = merged PR #127 (notify rewrite)

$ git worktree add .worktrees/dividend-fx-pit -b dividend-fx-pit origin/main
HEAD is now at 6a680c7 notify rewrite: every email is a ledger row; ... (#127)

$ cp <main>/docs/superpowers/plans/2026-09-13-dividend-fx-and-pit-membership.md docs/superpowers/plans/
$ cp <main>/docs/superpowers/specs/2026-09-13-dividend-fx-and-pit-membership-design.md docs/superpowers/specs/
$ touch docs/superpowers/plans/2026-09-13-dividend-fx-and-pit-membership.evidence.md
$ cmp <worktree copy> <main copy>   # both files
IDENTICAL

$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2710 passed, 2 warnings in 94.59s — Total coverage: 95.04%
```

Deviations: none.
