# launchd pointed at a worktree with no .env killed ingestion and its own alert

**Rule:** Point every warehouse job at `<warehouse>/current` and keep credentials in `~/market-warehouse/.env`, never in a checkout or a release.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

- **A release carries no `.env`** (gitignored, so `git archive` omits it).
  Credentials must live in `~/market-warehouse/.env`, which
  `livewire_scripts/scheduled_env.py` already loads. `promote` warns when that
  file is absent — without it a scheduled job resolves every credential to
  nothing, the same failure the worktree note below describes.

- **The four warehouse job plists point at `<warehouse>/current`, never at a checkout.**
  They used to `cd` into the repo and run whatever was on disk at that moment —
  branch, uncommitted edits and all. Only `release-promote` still reads the
  repo, because building the artifact is its job. The older trap this replaced:
  pointing launchd at `.worktrees/<branch>/`, which has no `.env` (gitignored)
  and so resolved every credential to nothing, killing both ingestion and the
  failure alert that would have reported it. A release has no `.env` either —
  which is why credentials must live in `~/market-warehouse/.env`.
- **Alerts that fail to send are persisted** to `<log_dir>/alerts_undelivered/`
  and counted by the watchdog. A WARNING in the log the job just broke is not
  an alert.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
