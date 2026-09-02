# The daily-job deadline is a total, not per-lane

**Rule:** Keep MDW_DAILY_JOB_DEADLINE_SECONDS a single total wall-clock budget shared across all seven lanes, and kill an exhausted lane by process group.

**Incident / measurement:**

- `MDW_DAILY_JOB_DEADLINE_SECONDS` (default `14400`, 4h): **total** wall-clock
  budget for one `run-daily-job` run, shared across every lane. It is
  deliberately a total, not per-lane: `main()` runs seven lanes sequentially
  (corporate-actions, equity, futures, cmdty, CBOE, FX, Silver), so a per-lane
  budget of N hours would permit a 7N-hour job. Measured whole-job wall clock
  over 2026-07-01..28: healthy runs peak at **3.27h**, the watchdog checks at
  **+4.5h**, so the budget must sit in that narrow band. A lane that exhausts
  the budget is killed **by process group** (`subprocess.run`'s own timeout
  signals only the direct child, orphaning `--workers` pools that keep holding
  `fcntl.flock`), is never retried, and **pages**.

DuckDB analytical catalog environment variables:

**Source:** CLAUDE.md section "Massive S3 flat-file environment variables" (moved 2026-09-02)
