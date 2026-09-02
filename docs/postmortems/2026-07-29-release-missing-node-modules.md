# A release carries no node_modules, so every alert path was dead

**Rule:** Run `npm ci --omit=dev` between `build_venv` and `freeze` in `release promote`, and import-check the result.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

- **A release carries no `node_modules`.** `git archive` exports only tracked
  files and `node_modules/` is gitignored, so releases shipped without
  `nodemailer` and every alert path was dead. `release promote` now runs
  `npm ci --omit=dev` between `build_venv` and `freeze` (it must precede the
  `chmod -R a-w`) and import-checks the result.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
