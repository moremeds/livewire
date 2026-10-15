# 2026-10-15 — a seed published the windows that answered as the whole file

**Rule:** a historical request that timed out is an error, never "no data". Seed windows
overlap enough that every date sits in two windows whose ends are far apart. Intraday
chunks step shorter than their request.

**What happened.** The seam-fix reseed (WINDOW_STEP 335 days) of 9 futures contracts lost
data instead of filling it. Measured against the backup: CL_202608 −670 dates, NG_202801
−235, CL_202609 −231, the rest ~−30 at new seams. All 9 were restored byte-identical from
`repairs/backups/2026-10-15-futures-reseed/` (the ledger still shows that run OK).

**Causes, measured on IB from the mini (read-only probes):**
1. `get_historical_data_async` passed no timeout, so ib_async's 60 s applied. Five "1 Y"
   windows queued behind six concurrent requests timed out; ib_async answers a timeout
   with `[]`, and `fetch_ticker` replaced each file with what the other windows returned.
2. For expired futures IB's daily answer leaves out ~4 sessions about 260 days before the
   window's end: CL_202610 ending 2019-06-14 omits 2018-09-27..10-02, ending 2018-12-31
   omits 2018-04-13..18; "2 Y" omits them too, "1 M" has them. One 30-day overlap per seam
   cannot cover a hole 260 days back. (The old 365-day seeds lost one such block per year:
   ≥354 sessions over 13 contracts.)
3. The window loop stopped once a window reached the head, so the oldest dates had one
   window (scratch reseed lost 2018-02-09..14).
4. Sibling: `compute_intraday_chunks` stepped exactly one request ("1 W" for 5m). The
   2026-05-20 VIX 5m backfill (run as `--asset-class equity`, so RTH-only) lost each chunk's
   end-day: every Wednesday 2025-05-21..2026-05-13.

**Fix.** `IBClient.get_historical_data(_async)` raises `IBTimeoutError` for an empty answer
that took the whole timeout; the seed passes `ib_request_timeout_s/1d` (300 s) and a raised
window fails the ticker, so nothing is published. `WINDOW_STEP` 182 days and the loop runs
until the cursor passes the head. Intraday steps: 1m 18 h, 5m 5 d, 30m/1h 25 d.

**Verified** on a scratch warehouse (`MDW_WAREHOUSE_DIR`) against the production backup:
CL_202610 +35/−0, CL_202608 +31/−0, NG_202801 +31/−0. 2023-02-06..09 is absent even from a
"1 M" request: an IB hole, not a seed loss.

**Not fixed:** BZ is a retired root (`RETIRED_FUTURES_ROOT`); its 4 contracts keep 131 lost
sessions.

**Test:** `tests/test_fetch_ib_historical.py::TestComputeDateWindows::test_every_date_sits_in_two_windows_whose_ends_are_far_apart`,
`::TestFetchTickerBars::test_a_timed_out_window_fails_the_ticker_so_nothing_is_published`,
`::TestComputeIntradayChunks::test_consecutive_chunks_overlap`,
`tests/test_ib_client.py::TestHistoricalDataTimeout`.
