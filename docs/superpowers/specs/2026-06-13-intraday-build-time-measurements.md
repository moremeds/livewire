# Intraday Build-Time Measurements

Date: 2026-06-13
Companion to: `2026-06-13-intraday-multifile-migration-design.md`
Purpose: quantify where data-lake build time actually goes, to justify the
multi-file migration over cheaper alternatives (e.g. compression).

All figures gathered on the build host (Mac mini, lake on a 14 TB USB HDD).

## 1. Build time by phase (run logs, content timestamps)

Source: 49 run logs in `~/market-warehouse/logs/` that carry in-content
timestamps, spanning ~May 18 → Jun 13. **This is a floor** — see caveats.

| Phase | Wall-clock | Runs |
|---|---:|---:|
| Equity intraday build (Massive flatfile) | 103.2 h | 13 |
| Equity daily seed/backfill (IB) | 37.5 h | 12 |
| Intraday catch-up (scheduled) | 37.5 h | 7 |
| Daily updates (scheduled) | 20.4 h | 10 |
| Other (misc backfill) | 17.4 h | 5 |
| Volatility (CBOE/IB) | 4.9 h | 2 |
| **TOTAL logged** | **≈ 221 h (~9.2 days)** | 49 |

Largest single efforts:
- `daily_backfill_intraday_equity_flatfiles` — **98.4 h in one run** (full whole-market intraday build)
- `intraday_catchup` — 37.5 h / 7 runs (~5.4 h each)
- `backfill_full` — 26.7 h / 8 runs
- `daily_update` — 20.4 h / 10 runs
- `backfill_r2k` — 15.8 h / 1 run

Equity intraday is ~47% of measured build time (higher in reality — IB intraday
backfill is uncounted, see caveats).

## 2. Massive flatfile build — phase decomposition

Source: matched `*_started`/`*_completed` events in
`~/market-warehouse/cursors/massive_flatfile_manifest.jsonl` (all-time, summed
**active work time** = 59.2 h; the gap to the 103 h log figure is
inter-operation idle, setup, and Python overhead).

| Step | Hours | Share |
|---|---:|---:|
| 1. Download + stage gzip → buckets (1033 day-files) | 10.4 h | 18% |
| **2. Publish buckets → per-ticker bronze (1814 buckets)** | **48.8 h** | **82%** |
| &nbsp;&nbsp;2a. per-ticker merge + rewrite (90,795 ticker-publishes) | **35.1 h** | **59% of total** |
| &nbsp;&nbsp;2b. bucket scan + skip overhead | 13.6 h | 23% |
| **Total active** | **59.2 h** | |

Per-ticker merge timing: **avg 1.39 s, p50 0.89 s, p99 7.4 s, max 44.8 s**.

**The dominant step is 2a — the per-ticker merge+rewrite** (read the existing
5-yr file, concat the new day, sort, rewrite the whole file). It is 59% of all
active build time and 72% of publish. This is the immutable-single-file rewrite
the migration eliminates; the p99/max tail is the liquid names with large 1m
files, rewritten in full on every run.

Data volume: manifest holds 172,424 records / ~3.3 TB compressed processed.

## 3. zstd A/B (codec is not the lever)

Measured on 118 real equity tickers (1m), same data both codecs.

- **Storage:** zstd-3 is ~24–28% smaller (231 MB vs 306 MB). Real, kept.
- **Write:** time-neutral (compression CPU offsets fewer bytes; 7.9 s both in an earlier bench).
- **Read (scattered, in-place, F_NOCACHE):** ~27 ms/file regardless of codec —
  the per-ticker read is **seek-bound**, so fewer bytes barely change read time.
- **Placement dominates codec:** contiguous read 139 MB/s vs scattered 53 MB/s
  (2.6×) vs codec ~1.0×.

Conclusion: zstd is a storage win only; it does not speed up the catch-up. The
seek-bound scattered read + immutable rewrite (step 2a) is the real bottleneck,
addressed only by the multi-file/contiguous layout.

## 4. Methodology & caveats

- **Floor, not total.** IB intraday backfill logs (`backfill_intraday_*`,
  `intraday_*`) log per-ticker progress without timestamps, so their time is not
  counted. The r2k intraday log alone is 43 MB of activity.
- **File-time fallback is unusable.** An HFS+→exFAT disk copy (the source of the
  ~117K `._*` resource-fork files) reset birthtime/mtime, so `mtime − birthtime`
  spans the copy history, not run duration (it produced absurd 7707 h totals).
  Only in-content timestamps and manifest event timestamps are trusted here.
- **Log coverage starts ~May 18**, but git history runs Mar 6 → Jun 13 (221
  commits, ~99 days); earlier development/ingestion predates these logs.
- **Hardware context:** the lake's 14 TB USB enclosure (ASMT105x) dropped off
  the bus mid-run twice during this work, killing long jobs — an operational
  risk independent of software.

## 5. Takeaway

The per-ticker immutable rewrite (step 2a, ~35 h / 59% of active intraday build)
is the single largest cost and is what the multi-file migration removes. Codec
and download/transpose are secondary. This is the quantitative basis for
prioritising the migration in the companion design doc.
