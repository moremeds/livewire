# Any alert value beginning with -- was unsendable

**Rule:** Emit every alert argument in the single-token `--key=value` form.

**Incident / measurement:**

- ⚠️ **Any alert value beginning with `--` was unsendable.** `parseArgs` had no
  `--key=value` form and rejected a value starting with `--`. The error summary
  is log-derived text; on 2026-08-08 it began with `--- Runbook: ...` and the
  page was never sent — the watchdog caught it 5.5h later. All five Python call
  sites now emit the single-token form; the two-token form still parses.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
