# A seed's yearly windows only touched, and most seams lost their session

**Rule:** `compute_date_windows` steps back `WINDOW_STEP` (335 days) per "1 Y" request,
so consecutive windows overlap by 30 days, and `fetch_ticker_bars` keeps the first copy of
a date in newest-first window order. IB's "1 Y" daily answer leaves out the session that
opened before the window start and the session still trading at the window end, so two
windows that only touch can both skip the session at their seam.

**Incident / measurement:** host **macmini**, 2026-10-14. The `fx_ib` and `cmdty` seeds
(`historical --years 0`) were checked against signal-lab's IB 1m archive. EURUSD, USDJPY and
XAUUSD each missed the same 9 sessions (2010-10-19, 2011-10-19, 2012-10-18, 2016-10-17,
2017-10-17, 2018-10-17, 2019-10-17, 2023-10-16, 2024-10-15): each one is the session after
a window end (365-day steps back from 2026-10-14 23:43). Probe: "1 Y" EURUSD MIDPOINT
ending `20241014-23:50:00` ends at 2024-10-14, and the window ending `20251014-23:50:00`
starts at 2024-10-16. The older XAUUSD file (seeded 2026-05-24) missed its own window ends
(05-21..05-23 of 2017/2018/2019/2023/2024). 9 of the 16 seams lost a bar; the 7 that ended on a Thursday, Friday or Saturday did not.
The overlap makes the result independent of IB's edge rule.

**Cost:** up to one daily bar per year of history in every IB `historical` seed with
sessions longer than RTH: futures, volatility, cmdty and fx_ib. The daily lane never
refetches a date older than the latest one it holds, so these holes are permanent until a
reseed. Only `fx_ib` and `cmdty` (XAUUSD, XAGUSD) were reseeded on 2026-10-14. The other
IB-seeded classes are not measured.

**Sibling (unverified):** `compute_intraday_chunks` steps exactly one request duration
(1h/30m: "1 M" vs 30 days). A 1m "14 D" request spans 14 sessions (~20 calendar days,
measured), so `ib-1m`'s 14-day step overlaps.

**Test:** `tests/test_fetch_ib_historical.py::TestComputeDateWindows::test_consecutive_windows_overlap_so_no_seam_session_is_lost`,
`::TestFetchTickerBars::test_a_seam_date_keeps_the_newer_windows_complete_bar`.
