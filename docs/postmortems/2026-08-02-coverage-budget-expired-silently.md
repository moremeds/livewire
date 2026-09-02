# Coverage outgrew every guessed budget and stayed silently dead for four weeks

**Rule:** Give coverage its own untimed launchd job, count the warnings a spawned quality job emits, and quote whole-run numbers rather than per-file ones.

**Incident / measurement:**

- ⚠️ **Coverage's cost is per-file, and it outgrew its budget silently.**
  `compute_coverage` opens one parquet footer per symbol per timeframe. Measured
  2026-08-02: `1d` alone is 13,270 files at 11.8 ms each = **154s single-threaded**,
  and there are five timeframes. Against the 600s budget in
  `_spawn_post_success_quality` it timed out **every night from 2026-07-07** —
  coverage logs stop at 2026-06-17, so `weekly` (a pure parser over those logs)
  has produced nothing but 83-byte `No coverage logs found` stubs ever since.
  Nothing was wrong with the data; the detector was blind.
  `FOOTER_READ_WORKERS=16` takes 5.3x off it (154.0s → 29.2s; 32 threads only
  reaches 25.2s, so the curve is flat past 16), and the budget is now 1800s to
  absorb what threads cannot — a **cold glob measured at 281s for one
  timeframe**, against 0.6s warm for the next. Cold is the normal morning state,
  the same asymmetry the DuckDB catalog is built around.
- **A swallowed WARNING is how this hid for four weeks.** `_spawn_post_success_quality`
  must never flip a successful run to failure — that part is right — but nothing
  counted the warnings. `nightly_digest._quality_jobs_section` now reports them;
  keep it, it is the only thing standing between a dead detector and another
  month of silence.

- ⚠️ **Coverage has its own job because every budget guessed for it expired.**
  600s (from 2026-07-07), then 1800s (5 of 6 nights after PR #78). Both numbers
  came from warm-cache measurements; a cold full pass measured **2858s** on
  2026-08-09. The lake is on an external exFAT volume and the nightly 23.57 GB
  of intraday writes evict the cache, so **cold is the normal state** and thread
  count does not help. `com.livewire.coverage` runs at **11:00 UTC** with no
  timeout — chosen against the daily job's 4h *deadline* (06:00 + 4h = 10:00
  UTC), not its 3.27h healthy peak, because a slow-but-legal run still
  publishing would give a mixed-time snapshot. `livewire_quality.py` loads the
  scheduled env for `coverage` too; launchd starts it cold and without it the
  job resolves every credential to nothing.
- **The footer pass caches `(mtime, size) → latest` per file.** An unchanged
  pair cannot mean a later max date. Size is in the key because exFAT stores
  mtime at 2-second granularity and bronze publishes by `os.replace()`, so a
  republish can land inside the bucket. The post-recovery re-check is
  deliberately **uncached** for that same reason. The lookup is read-only and
  the caller rebuilds the cache single-threaded from returned tuples — 16
  threads writing a shared dict would rest correctness on GIL atomicity.
  ⚠️ **The cache does not touch the glob, and the glob is the cold cost.**
  Measured 2026-08-09 over 400 real equity `1d` files: a footer read is
  **7.13 ms/file**, a cache hit **0.01 ms/file** (~1100×, identical results).
  But `compute_coverage` still runs `sorted(bronze_root.glob(...))` per
  timeframe, and a cold glob measured **281s for one timeframe** — five of
  those is most of a cold run, and no cache entry avoids any of it.
  ⚠️ **Quote the whole-run number, not the per-file one.** Measured 2026-08-10
  back-to-back on the real lake (`--no-recover --force`, 71,763 cached
  entries): **1534.04s** with no cache file, **1398.77s** with every entry
  hitting — **−135s, 8.8%**. Both figures are honest; the per-file 1100× is
  the *footer read* alone, which 16 threads had already compressed to ~135s of
  a ~1500s run. So the **no-timeout job is the fix and the cache is only an
  optimisation** — now measured, not inferred. `user+sys` was **26.7s of
  1534s (1.7%)**: the process is 98% blocked on I/O, which is also why adding
  threads does not help. Note run 2 ran with the filesystem cache still warm
  from run 1 and *still* took 1399s; with the 2858s cold pass, one run ranges
  over **~1400–2860s** and crosses both retired budgets (600s, 1800s). No
  constant is safe here. `(mtime, size)` is also not content identity — a same-bucket
  republish compressing to an identical length serves a stale entry, which can
  only hold an *earlier* date, so the symbol reads as MISSING and triggers
  recovery. It over-reports gaps; it cannot hide one.
- **The digest reads the newest `coverage_*.log`, not an exact filename**, and
  warns when it is more than 3 days old. Decoupling the schedules removes the
  ordering bug but buys back a new silence: a coverage job that stopped firing
  would leave the newest log frozen and the digest printing a green line forever.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
