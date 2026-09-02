# Presets overlap, so the denominator deduplicates

**Rule:** Deduplicate ExpectedSeries across presets — sp500 n ndx100 is 87 symbols, and two repair instructions against one parquet path is a concurrent write.

**Incident / measurement:**

- ⚠️ **Presets overlap, so the denominator deduplicates.** `sp500 ∩ ndx100` is
  87 symbols. Emitting one `ExpectedSeries` per occurrence put every gap those
  87 had into the Tier A manifest **twice** — and once that manifest feeds
  `shepherd_repair`, two repair instructions against one parquet path is a
  concurrent write. `shepherd_repair.py:905` holds an `fcntl.flock`, but the
  fix is not to manufacture the situation the lock exists to survive.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)
