# A seed stored the session still trading, and the daily lane never refetched it

**Rule:** `fetch_ticker` stores no daily bar dated on or after the UTC date it runs. IB
answers a full-history request with the session still trading as a bar dated today, and
the daily lane fetches only after the latest stored date, so a partial bar that lands
is permanent. `historical` is one ledger run (`historical_unsettled_bars_dropped`), or,
under the daily lane's rolling seed, measurements on the parent run.

**Incident / measurement:** host **macmini**. On 2026-09-23 a manual commodity seed
(`docs/audits/price-discovery/COMMODITY_BACKFILL_MANIFEST_2026-09-23.json`, probe
04:05–04:31Z) ran while ICE Brent and ICE cotton were trading. The 09-24 daily run
reported `Up to date: 28` for target 09-23. All 14 `COIL` contracts and `CT_202612`
carry a 09-23 bar with 1–4% of trailing-median volume. COIL_202612 closes at 94.31 on
7,801 contracts; Massive BZ settled 98.12. The other ICE roots (KC, CC, SB, OJ) and
NYMEX were normal that day. signal-lab caught it on 2026-10-01. The seed left no ledger
row.

**Cost:** for eight days, every COIL curve on 09-23 was $2.5–3.8 low. signal-lab
filtered the bar in its own PR #100.

**Test:** `tests/test_fetch_ib_historical.py::test_a_bar_for_the_session_still_trading_is_never_stored`,
`::test_historical_is_one_ledger_run_and_a_crash_reads_failed`,
`::test_under_the_daily_lane_historical_never_writes_the_parents_run_row`.
