# Status grading: UNKNOWN is not OK, and every log-derived line must be escaped

**Rule:** Grade a missing input UNKNOWN on an ordered IntEnum, reuse resolve_exit_code for outcomes, grade the OLDEST DuckDB view and the change in Silver failed, and escape every log-derived line.

**Incident / measurement:**

- **`UNKNOWN` is not `OK`.** A missing input is a failure to measure, which is
  how coverage stayed dead for four weeks while the digest printed a green line
  every night. `Verdict` is an `IntEnum` ordered `OK < UNKNOWN < WARN < BAD`, so
  `max()` over a run can never report green on a gap — a plain `Enum` is
  unorderable and `max()` over it raises, which would have made the ordering
  documentation with no mechanism behind it. UNKNOWN ranking *below* WARN also
  cuts the other way: a missing Silver baseline makes the delta unmeasurable
  and must not downgrade a window regression that was measured.
- **A missing log is BAD; a log with no `SUMMARY_JSON` is UNKNOWN.** "The job
  never ran" and "the job ran and told us nothing" are different problems.
- **Outcomes reuse `daily_outcomes.resolve_exit_code`**, never a flat
  `errors > 0`. Grading every nonzero error the same would put
  `updated=0, errors=13311` at the severity of one flaky warrant — the exact
  disease this command exists to cure, reproduced inside the cure.
- **`launchctl` exit codes carry no timestamp**, so a nonzero exit is capped at
  WARN and annotated. Overstating a stale red is the fastest way to make the
  whole surface ignorable. A job that is *not loaded* is BAD — it cannot recover
  on its own, and the fix branches on whether the plist is even installed
  (the repo ships `.plist.example` templates that must be rendered first).
- **Both undelivered-alert queues are counted.** `MDW_UNDELIVERED_DIR` holds
  per-flag quality alerts (4,408 files on 2026-08-10); `<log_dir>/alerts_undelivered`
  holds the scheduled-job failure pages. The split is deliberate — see
  `run_daily_update_job.undelivered_dir` — so a section named "Undelivered
  alerts" that reads one of them is misnamed.
- **The DuckDB check grades the OLDEST view, not the newest.** `max(last_date)`
  would let one current view green the whole check while `bronze_equity_1d` sat
  frozen. Staleness is counted in **trading sessions**, not calendar days:
  Friday against Monday is one session but three days.

- **Silver is graded on the change in `failed`, never an absolute.** Nothing
  measured says whether `failed=233` is normal, so the baseline is the previous
  run. The baseline lookup **parses** the log date rather than string-comparing
  it: `daily_update_*.log` also matches `daily_update_watchdog_<date>.log`.
- **Every non-OK verdict carries a fix with no unsubstituted placeholder**, and
  the renderer uses `soft_wrap` — rich's default word-wrap inserts real newlines
  at the console width, so a long command pasted as two commands. A test asserts
  no fix string contains `<`.
- **Every log-derived line goes through `rich.markup.escape()`.** Measured: a
  line containing `[/]` raises `MarkupError` and kills the command; `[bold red]`
  is silently consumed as a style and the text vanishes.

**Source:** CLAUDE.md section "Status — one graded view" (moved 2026-09-02)
