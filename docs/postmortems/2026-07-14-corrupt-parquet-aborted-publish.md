# One truncated 1m.parquet aborted the whole-market publish every night

**Rule:** Quarantine a corrupt per-symbol parquet to `<lake>/quarantine/<stamp>/` and report the symbol, instead of aborting the publish.

**Incident / measurement:**

- **A corrupt per-symbol parquet is quarantined, not fatal.** One truncated
  `1m.parquet` aborted the entire whole-market publish every night from
  2026-07-14; the file is now moved to `<lake>/quarantine/<stamp>/` and the
  symbol reported for targeted backfill while the rest of the market publishes.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
