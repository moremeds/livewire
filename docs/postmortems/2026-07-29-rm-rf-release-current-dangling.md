# Never rm -rf the release current points at

**Rule:** Never delete the release directory `current` points at; recover a dangling symlink with `release rollback` then `promote`.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

- ⚠️ **Never `rm -rf` the release `current` points at.** `promote` short-circuits
  on `current already at <sha> — nothing to promote`, checking the symlink and
  not the directory, so deleting the target leaves `current` **dangling** and
  `promote` refuses to rebuild it. Recover with `release rollback` (restores a
  real target), then `promote`. Jobs already running are unaffected —
  `os.getcwd()` is physical — but any new job would fail.

**Source:** CLAUDE.md section "Immutable release artifacts — production does not run from the checkout" (moved 2026-09-02)
