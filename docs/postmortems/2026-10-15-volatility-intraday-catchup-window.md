# 2026-10-15 — the volatility intraday catch-up could not outlast a Gateway outage

**Rule:** a catch-up reaches back to the newest bar it holds, not a fixed number of days.
Every IB-fed series has a registry row, or its gaps are invisible.

**What happened.** On the mini, 13 of the 14 `volatility-intraday` 5m series (all but VIX,
which a full refill had just fixed) were missing 2026-08-21..26. The Gateway was down
2026-08-24..09-01: every intraday-catchup run in that span exited 86, which is
correctly DEGRADED. The catch-up passed `--days 7`, which is one "1 W" chunk. The first
run back (09-02 13:30Z) got 312 bars per series (4 sessions, back to 08-27). Nothing
fetched 08-21..26 afterwards. No registry row covered volatility intraday, so the hole was
found only seven weeks later, by hand.

A second hole has a different cause. 8 series miss up to 19 sessions in 2026-05-20..06-16,
because 5m was not in the catch-up until 2026-06-12 (#32). The first 7-day run reached back
only one week.

**Unexplained.** On 08-22 (Saturday) the 5m run inserted 0 bars for every series, and 08-21
is missing. `--from-latest` does not heal one interior day like this, and the gap scan
does not report it (G2 is off, pm:2026-09-01-g2-g13-not-emitted).

**Fix.** `intraday-backfill --from-latest` widens `--days` per ticker to its newest stored
bar. It is capped at `intraday_catchup_max_days` (120), so a retired index costs at most
24 5m requests a night. `sync_runner` passes the flag for 5m and 30m. The registry
gains `g1-g3-volatility-5m`/`-30m` over `volatility-intraday`. `actual_sessions` reads the
UTC date of `bar_timestamp` (these bars sit 13:00–20:00Z), and
`repair_source("volatility", <intraday>)` is `ib`, so a tail is Tier B.

**Verified** read-only on the mini: the two rows find one real tail, VXSLV 5m missing
2026-10-14. The data holes were refilled by hand (`repairs/backups/2026-10-15-vol-5m-refill/`).

**Test:** `tests/test_backfill_intraday.py::TestCatchupLookbackDays`,
`tests/test_sync_runner.py::test_the_volatility_intraday_catchup_reaches_back_to_the_newest_bar`,
`tests/test_gap_registry.py::test_volatility_intraday_rows_read_bar_timestamps_and_route_to_ib`.
