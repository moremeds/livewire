# Futu OpenD history K-line verification (stock and option)

Read-only probe. Date: 2026-10-15 13:07 to 13:40 ET (2026-10-16 HKT). Host: MacBook, OpenD at 127.0.0.1:11111, futu-api 10.03.
Mini checks were read-only (parquet reads, one `psql` with `statement_timeout=60s`). Nothing was written to the lake, Postgres or the repo.
Scripts and raw output are in `/tmp/futu-probe2/` (not in the repo).
This report checks claims in `2026-10-16-futu-opend-market-data-probe.md`.

Quota used by this probe:
- Stock history-K: 4 new symbols (US.SPY, US.GOGO, US.META, US.ELV). `US.AAPL` and `HK.00700` were already counted. `get_history_kl_quota` shows used 6 / remain 994.
- Option history-K: 3 new chains (SPX-index 2026-09-15, SPX-index 2026-09-17, SPY 2026-09-18). The chains SPX 2026-12-18 and SPY 2026-11-20 were already in the list and were reused. The option list now has 8 entries.
- Failed lookups ("Unknown stock") added nothing to either list.

## A. Stock history K

### A1. Floors

| Item | Result | Numbers | Call |
|---|---|---|---|
| 1m floor moves with the clock | VERIFIED (rolling) | At 13:12:37 ET the first AAPL, SPY and GOGO 1m bar was `2018-10-09 13:13`. At 13:25:51 ET the first AAPL and SPY bar was `2018-10-09 13:26`. The offset to "now" stayed 2,928 days (8 years + 6 days, computed). The floor follows the minute of the day. | `request_history_kline(code, '2018-01-01', '2026-10-15', K_1M, AuType.NONE, extended_time=False)`, first page |
| 1m floor is month-aligned or fixed | REFUTED | A fixed or month-aligned floor would not move 13 minutes in 13 minutes. | same |
| 5m floor | VERIFIED | AAPL first 5m bar `2018-10-09 13:15` at 13:12 ET. | `K_5M`, start 2016-01-01 |
| Daily floor | VERIFIED value, rolling UNVERIFIED | AAPL and SPY: `2006-10-02`. GOGO: `2013-06-21` (its own first bar). Same on both measurements. Both runs were on the same ET date, so a day-level move is not tested. | `K_DAY`, start 2000-01-01 |
| Day offset stable across days | UNVERIFIED | Needs a re-run on another date. | |

### A2. Survivorship (delisted stocks)

| Item | Result | Numbers | Call |
|---|---|---|---|
| Mini delisted candidates | VERIFIED | `bronze-delisted` has 8,622 symbols. 3,214 have a last `trade_date` from 2019-01-01 to 2023-06-30. Every delisted file starts at `2021-06-11`. | pyarrow footer statistics of `1d.parquet`, column `trade_date` |
| Futu serves delisted stocks | REFUTED | 5 of 5 failed for daily and 1m: TWTR (last 2022-10-27), XLNX (2022-02-11), SIVB (2023-03-09), CERN (2022-06-07), FRC (2023-04-28). Error: `Unknown stock. <SYM>`. 10 requests, 0 quota used. | `request_history_kline('US.<SYM>', ...)`, `K_DAY` and `K_1M`. Only the plain `US.<SYM>` form was tried. |
| OHLC match on 3 dates | UNVERIFIED (no Futu data) | Not possible. | |

### A3. 1m label and accuracy (RTH, `AuType.NONE`, `extended_time=False`)

Sessions: 2022-03-15 (EDT), 2023-01-18 (EST), 2026-10-08 (EDT).

| Item | Result | Numbers | Call |
|---|---|---|---|
| Label convention | VERIFIED | Futu `time_key` is the bar end. A session has 390 bars labeled 09:31 to 16:00 ET. Subtract 1 minute, then convert ET to UTC, to match Massive `bar_timestamp` (bar start). Shift -1 wins on all 9 symbol-sessions. Shifts 0, +1, -2, +2: 0 exact matches for AAPL and SPY. For GOGO they give at most 19.5% (2026-10-08) of Futu's 390 bars. | `request_history_kline(code, d, d, K_1M, NONE)`; join on UTC minute |
| Exact OHLC match at shift -1 | VERIFIED | AAPL: 333/390 (85.4%), 341/390 (87.4%), 390/390 (100%). SPY: 331/390 (84.9%), 325/390 (83.3%), 388/390 (99.5%). GOGO (share of bars on both sides): 333/338, 222/222, 349/350. | |
| Size of the OHLC differences | VERIFIED | AAPL 2022-03-15: 57 bars differ. Max abs difference: open 0.025, high 0.01, low 0.005, close 0.07. | |
| Bar counts per session | VERIFIED | Futu: 390 every session, including no-trade minutes padded with volume 0 (GOGO: 9, 32, 4 zero-volume bars). Mini RTH minutes (09:30 to 15:59): AAPL 390, 390, 390. SPY 390, 390, 390. GOGO 338, 222, 350. Mini total with extended hours: AAPL 864, 760, 836. SPY 884, 797, 916. GOGO 341, 226, 369. | |
| Extra small bars in Futu for GOGO | VERIFIED | Futu bars absent from mini with volume > 0: 43, 136, 36. They hold 1.07%, 4.47%, 0.32% of the day volume. AAPL and SPY: none. | |
| Volume ratio (Futu / mini, day total, shift -1) | VERIFIED | AAPL 1.131, 1.115, 1.228. SPY 1.018, 1.047, 1.031. GOGO 1.060, 1.212, 1.359. The per-bar median ratio is 1.0000 (AAPL 2022-03-15 and 2026-10-08, GOGO 2023-01-18). | |
| Cause of the day-total gap | VERIFIED | The last bar (label 16:00) holds the closing auction in Futu. AAPL 2026-10-08: 6,380,238 vs 868,506. SPY 2,072,281 vs 889,501. GOGO 601,490 vs 80,893. Without the last bar the ratios are 0.9945 (AAPL 2022-03-15), 1.0001 (AAPL 2026-10-08) and 1.0000 (GOGO 2026-10-08). Computed from the printed sums. | |
| Mini 1d source field | REFUTED (for the task brief) | AAPL `1d.parquet`: 11,486 rows `source='legacy'`, 65 rows `source='massive'` (2026 only). | |

### A4. Ticker changes

| Item | Result | Numbers | Call |
|---|---|---|---|
| `US.META` daily before 2022-06-09 | VERIFIED | First bar `2012-05-18`, 3,623 bars. Mini `META` 1d (3,622 rows, from 2012-05-18) is Facebook history. On 5 dates, high, low and close match on 4 of 5. Only 2022-06-08 matches all four. Differences: open 79.3 vs 79.4 (2015-06-01), 343.015 vs 343.0, 201.165 vs 201.16, and close 38.2318 vs 38.18 (2012-05-18). | `request_history_kline('US.META', '2000-01-01', '2026-10-15', K_DAY, NONE)` |
| `US.META` 1m before 2022-06-09 vs mini `META` 1m | REFUTED as a like-for-like test | Mini `META` 1m before 2022-06-09 is a different security: monthly median close 13.8 to 16.2 from 2021-06 to 2022-01. It also has no bars from 2022-01-31 to 2022-06-08. | pyarrow read of mini `symbol=META/1m.parquet` |
| `US.META` 1m vs mini `FB` 1m | VERIFIED | Mini `FB` 1m holds the Facebook prices (about 333 on 2021-12-15). Exact OHLC at shift -1: 367/390 on 2021-12-15 and 362/390 on 2022-04-04. Close-only: 377/390 and 376/390. | `K_1M` for the single day |
| Second rename ANTM -> ELV | VERIFIED | `US.ELV` daily starts `2006-10-02`. Exact OHLC on 3 of 4 dates (2021-06-11, 2022-01-04, 2022-03-25). On 2022-03-28 only the low differs: 477.0201 vs 477.02. Mini `ELV` starts 2001-10-30 and mini delisted `ANTM` covers 2021-06-11 to 2022-06-27. | `K_DAY` |
| Daily volume vs mini 1d | VERIFIED difference, cause UNVERIFIED | Futu/mini ratio: ELV 2.58, 2.30, 2.36, 2.57. META 1.08 (2012), 1.12 (2015), 1.74, 1.31, 1.39 (2021-22). AAPL 2022-03-15: mini 1d 92.2M (legacy), Futu 1m sum 86.7M, Massive 1m sum 76.6M. Mini `legacy` 1d volume is not a reliable reference. | |
| Mini `FB` symbol | VERIFIED | `FB` 1d: 250 rows in 2021-22 (price 175 to 382). It also has 328 rows in 2025-26 at price 40 to 46, which is another security. Ticker reuse affects both `FB` and `META`. | |

### A5. Throughput

| Item | Result | Numbers | Call |
|---|---|---|---|
| Full 8-year 1m pull, one symbol | VERIFIED | AAPL, 2018-10-01 to 2026-10-15: 783 pages, 782,401 bars, 48.9 s, 0 errors. First bar `2018-10-09 13:13`. The last bar was the minute still open (`2026-10-15 13:13`). | `request_history_kline(..., max_count=1000, page_req_key=...)` in a loop |
| Time for 1,000 symbols (serial) | Computed | 1,000 x 48.9 s = 13.6 h. | |

## B. Option history K

### B6. Retention of expired contracts (probe date 2026-10-15)

| Item | Result | Numbers | Call |
|---|---|---|---|
| SPX monthly (AM) | VERIFIED | 2026-09-18 served (27 days after expiry). 2026-08-21 not served (`Unknown stock`, tried strikes 6000 and 6500). No SPX monthly exists between them. | `US.SPX260821C6000000`, daily K |
| SPXW | VERIFIED | Served: 2026-09-17 and 2026-09-15 (30 days after expiry). Not served: 2026-09-14, 09-11, 09-04. | `US.SPXW<yymmdd>C7500000` |
| SPY | VERIFIED | Served: 2026-09-18. Not served: 2026-09-11 and 09-04 (strikes 700, 650, 750). | `US.SPY<yymmdd>C700000` |
| Rule | Inference | Served if expiry >= one calendar month before the probe date (2026-09-15 on 2026-10-15). Three roots agree. A single probe date; re-test on another day. | |
| Last bar of an expired contract | VERIFIED | SPXW 2026-09-15 and 09-17: last bar one trading day after expiry (09-16 for the first). | |

### B7. Full-chain daily K, SPX expiry 2026-12-18

| Item | Result | Numbers | Call |
|---|---|---|---|
| Chain size | VERIFIED | 1,744 contracts: SPX 972 and SPXW 772. 872 calls and 872 puts. Strikes 200 to 13,400. | `get_option_chain('US..SPX', start='2026-12-18', end='2026-12-18')` |
| Cost | VERIFIED | 1,838 requests (1,744 first pages + 94 follow-up pages for contracts with more than 1,000 bars). 1,026 s (17.1 min) at 0.55 s pacing. 0 rate-limit errors, 0 other errors. 301,771 rows. | `request_history_kline(code, '2021-01-01', '2026-10-15', K_DAY, NONE)` per contract |
| Option usage in `get_history_kl_quota` | The existing audit claim "no API can read option usage" is REFUTED in part | `get_history_kl_quota(get_detail=True)` lists each option chain as an entry named like `S&P 500 Index [opt exp 2026-12-18]`. The list had 14 entries before and after the 1,744-contract pull (6 stock + 8 option). The `used` number (6) counts stock symbols only. No number for the 200-chain option limit is shown. A repeat request updates `request_time` and does not add an entry. SPX and SPXW share one entry (inferred: the SPXW contracts added no entry). | `c.get_history_kl_quota(get_detail=True)` |

### B8. Data quality

| Item | Result | Numbers | Call |
|---|---|---|---|
| Rows with volume = 0 | VERIFIED | 69.5% of 301,771 rows. SPX 69.8%, SPXW 67.5%. 256 of 1,744 contracts (14.7%) never traded. | pandas on the B7 pull |
| Close on zero-volume days | VERIFIED | 100% of zero-volume rows are flat (O=H=L=C). Close equals the previous close on 60.6%. On the other 39.4% (82,205 rows) the close changes. Example: `SPX261218C200000` on 2025-12-24 has close 6701.3963 (previous 6646.0). Origin of those values: UNVERIFIED. Filter `volume > 0` before using any close. | |
| Non-zero-volume rows with O=H=L=C | VERIFIED | 34.6%. | |
| UW cross-check, same date | REFUTED (0 of 10 match) | 5 SPY puts (expiry 2026-11-20, strikes 700, 730, 740, 750, 760) on 2026-10-09 and 2026-10-12. UW last snapshot of the day (16:55 ET) vs Futu close and volume of the same date: no match. | `request_history_kline('US.SPY261120P<k>000', K_DAY)`; psql on `uw_scan.option_contract_snapshots` with `inserted_at` bounds |
| UW cross-check, prior session | VERIFIED (10 of 10 exact) | UW value of day D equals the Futu bar of the previous trading day: close within 0.005 and volume exact. Example P750: UW 10-09 = 6.20 / 13,516 = Futu 10-08; UW 10-12 = 5.04 / 1,868 = Futu 10-09. | same |
| UW value static intraday | VERIFIED (one contract, SPY only) | `SPY261120P00750000` on 2026-10-15, snapshots 12:40 to 13:15 ET: last 6.81, bid 6.78, ask 6.84, volume 13,868 in every run. Futu 10-14 bar = 6.81 / 13,868. Futu live snapshot at 13:19 ET: last 6.25, volume 6,952, equal to the Futu 10-15 daily bar. | `get_market_snapshot` |
| Futu date labels are correct | VERIFIED | SPY fell 778.01 -> 769.60 on 2026-10-14 (mini bronze). Futu P750 rose 4.73 -> 6.81 on the 10-14 bar. | |
| Futu option 1m vs daily | VERIFIED | P750: 405 1m bars per day (09:31 to 16:15). Volume sum 1,868 (10-09) and 6,992 (10-12), equal to the daily bars. 228 and 203 bars have volume > 0. | `K_1M` |

## C. Snapshot claims

### C9. Quote lag, 0DTE SPXW (2026-10-15 expiry), during RTH

8 codes: strikes 7695, 7700, 7705, 7710, call and put. The strike is chosen from SPY x 10 = 7701.4. Lag = local ET after the call minus `update_time`. `update_time` has a 1-second resolution.

| Sample (ET) | Result | Min / median / max lag (s) |
|---|---|---|
| 13:12:03 | VERIFIED | 0.6 / 1.6 / 9.6 |
| 13:23:03 | VERIFIED | 1.8 / 3.8 / 56.8 |
| 13:34:03 | VERIFIED | 3.0 / 15.0 / 30.0 |

No 15-minute delay. The lag measures the age of the last update, so a quiet strike shows more. Across all 400 codes in the batch the median was about 6,060 to 6,470 s and the max about 595,000 s (strikes that did not trade for days). `update_time` is not a staleness test for far strikes. The clock source of the Mac was not checked.

### C10. Subscription quota

| Item | Result | Numbers | Call |
|---|---|---|---|
| `get_market_snapshot` on 400 option codes leaves `total_used` at 0 | VERIFIED | 3 of 3 samples, 400 rows each, `total_used` 0 before and 0 after. Also 0 after all history requests. | `query_subscription()['total_used']` around `get_market_snapshot` |

## Existing-audit claims refuted or settled

- REFUTED: "No API can read option quota usage." The per-chain list is readable. The remaining count of the 200-chain limit is not shown.
- REFUTED: delisted stocks may still be served. Futu returns `Unknown stock` for 5 of 5.
- SETTLED: the 1m floor is rolling, to the minute (2,928 days before now). It was "not determined".
- SETTLED: Futu 1m bars are labeled at the bar end (shift -1 gives the Massive start label).
- SETTLED: the expired option contract retention is about one calendar month.
- CONFIRMED: 0DTE quote lag of seconds, not 15 minutes. Snapshot does not use subscription quota.
- NEW, outside the Futu scope: mini `AAPL` 1d is `source='legacy'` for 11,486 of 11,551 rows. Mini `META` 1m before 2022-06-09 is another security with a 4-month hole in 2022.
- NEW: UW `option_contract_snapshots` (SPY) shows the previous session's last, bid, ask and volume all day. It is not a live quote source in that table.

## Implications for the proposal

1. 1m backfill for symbols alive today is feasible. Align with `utc = ET(time_key) - 1 minute`. Expect 83 to 100% exact OHLC on liquid names (differences up to 0.07 in price) and a few extra small bars on thin names.
2. The backfill is survivor-only. Futu cannot serve the 3,214 delisted symbols with a last date in 2019 to mid-2023. The lake has no delisted bars before 2021-06-11 either.
3. The window start rolls about one day per day (inferred from the minute tracking; day-level move not tested). The quota is 1,000 symbols per week. The equity universe is 13,311 symbols (CLAUDE.md), so a full pass takes 14 weeks or more (computed). Over 14 weeks the floor moves about 98 days (computed). The oldest bars drop during the run. Pull the oldest dates first.
4. A pull costs 49 s and 782k bars per symbol, about 13.6 h per 1,000 symbols when run serially. Quota, not speed, limits the work.
5. Keep or drop the last bar's volume on purpose. Futu puts the closing auction in the 16:00-label bar. Without it, day volume matches Massive within 0.7% in the sampled sessions.
6. Futu pads no-trade minutes with volume 0 flat bars (390 per session). Massive omits them. Decide the rule before merging the two sources.
7. Use a security_id mapping, not ticker equality. `US.META` and `US.ELV` are continuous across the rename. The mini keys Facebook 1m under `FB` before 2022-06-09 and puts another security under `META`.
8. Option history cannot backfill. Expired contracts disappear after about one month. Only forward capture works.
9. A full SPX/SPXW chain for one expiry costs one chain entry, 1,838 requests and 17 minutes at safe pacing (0 rate-limit errors at 0.55 s). The count of distinct expiries per week is the quota to watch. A repeat request does not add an entry.
10. Option daily K needs a `volume > 0` filter. 69.5% of rows have zero volume, and 39.4% of those have a moving close that is not a trade.
11. Futu option daily K matches UW exactly once UW is shifted by one session. UW's contract snapshot table is a prior-session end-of-day copy, so Futu is not a duplicate of an intraday UW quote there. Recheck this on non-SPY tickers before relying on it.
12. Snapshot lag is seconds and does not use subscription quota. The 15:45 ET capture rule is not affected by lag.
