# 2026-10-15 — an IB 1m history seed exited 0 with a 51-day hole

**Rule:** a `--since` seed of `ib-1m` is judged on what the lake holds in its window after
the merge. A hole longer than `HOLE` (6 days) fails the symbol (exit 1, `ib_1m_failed`). The
fetched bars are kept, because the merge is additive and a rerun of the window adds the rest.

**What happened.** The operator driver `/tmp/lw1_1m_backfill.sh` seeds IB 1m MIDPOINT history
one (symbol, year) segment per ledger run. Segment `XAGUSD:2020-12-31:2022-01-01` started at
14:24Z on the mini. During it, IB connectivity dropped (1100 → 1102), and four chunk requests
timed out (`reqHistoricalData: Timeout`, reqId 41–44). The run printed
`XAGUSD: hole 2021-01-29 21:59 -> 2021-03-21 22:00Z (51d)`, stored 306,855 bars, and exited 0.
The driver recorded the segment `ok`, so it would never have rerun it.

**Causes.**
1. The driver ran release `f7f0b08`, which is before PR #175. In that release a timed-out
   request still answered `[]`. HEAD raises `IBTimeoutError`.
2. Even at HEAD, `sync_ib` checked holes only when `since is None`. In seed mode a hole was
   printed and counted (`ib_1m_holes`), and the run still exited 0. `status` does not read
   `ib_1m_holes`, so the count was never graded. A chunk that IB answers `[]` quickly, not
   after the timeout, still produced this silent hole at HEAD.

**Fix.** In seed mode, `sync_ib` reads the window `[since, cutoff)` from the stored file
after the merge and computes holes from those bars. In `main`, a hole in seed mode adds a
failure, prints `FAILED … rerun it`, and emits `ib_1m_failed` with the symbol as scope.

**Repair.** Backup at `repairs/backups/2026-10-15-xagusd-1m-hole/1m.parquet`. The window was
refetched with `main(["--symbols", "XAGUSD", "--since", "2021-01-29"], now=2021-03-23)`.

**Not fixed (open):** `backfill_intraday.py` treats an empty chunk as `continue`. That is the
same class of silent hole for volatility/equity intraday, and interior holes there are not
emitted (`G2` is not emitted).

**Test:** `tests/test_ib_midpoint_1m.py::test_a_history_seed_that_leaves_a_hole_keeps_its_bars_and_fails_the_symbol`,
`::test_a_history_seed_is_judged_on_the_window_the_lake_holds_after_the_merge`.
